// Package ipfilter implements DNS-rebind-safe blocking of private, loopback,
// link-local, and unique-local IP addresses for outbound HTTPS requests from the
// koios-pool-meta-fetcher daemon.
//
// Three policies are supported (per openspec/specs/pool-meta-fetcher-config/spec.md
// §fetch-private-ip-policy):
//
//   - block        (default): deny all private/loopback/link-local/ULA IPs;
//     per-pool exemptions via allow_list.
//   - allow_list:                like block, but pool IDs in allow_list are exempt.
//   - allow_all:                 permit everything (operator override; logged at startup).
//
// A pool ID can be attached to a request via context.WithValue(...,PoolIDKey, ...);
// the DialContext reads it so the per-pool allow_list can apply.
package ipfilter

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

// Policy is the address-resolution gate.
type Policy int

const (
	PolicyBlock     Policy = iota // Default. Private ranges rejected.
	PolicyAllowList               // Private ranges rejected except for pool IDs in allowList.
	PolicyAllowAll                // Permits all IPs. Operator override.
)

// PolicyFromString accepts the canonical YAML strings.
func PolicyFromString(s string) (Policy, error) {
	switch strings.ToLower(s) {
	case "", "block":
		return PolicyBlock, nil
	case "allow_list":
		return PolicyAllowList, nil
	case "allow_all":
		return PolicyAllowAll, nil
	default:
		return 0, fmt.Errorf("unknown private_ip_policy: %s", s)
	}
}

// PoolIDKey is the context key under which the caller may stash a pool bech32
// ID so the DialContext can apply per-pool allow_list exemptions.
type PoolIDKey struct{}

// Filter wraps an http.Transport.DialContext with deny-list enforcement.
type Filter struct {
	policy    Policy
	allowList map[string]struct{}
	dialer    *net.Dialer
	resolver  *net.Resolver
	logger    *slog.Logger

	mu         sync.RWMutex
	warnedOnce bool
}

// New constructs a Filter. allowList is the set of pool bech32 IDs exempt from
// the deny list when policy is PolicyAllowList.
func New(policy Policy, allowList []string, logger *slog.Logger) *Filter {
	set := make(map[string]struct{}, len(allowList))
	for _, p := range allowList {
		set[p] = struct{}{}
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Filter{
		policy:    policy,
		allowList: set,
		dialer:    &net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second},
		resolver:  net.DefaultResolver,
		logger:    logger.With(slog.String("subsystem", "ipfilter")),
	}
}

// Policy returns the active policy.
func (f *Filter) Policy() Policy { return f.policy }

// ErrIPBlocked is returned when the resolved IP is in the deny list (or no IPs are allowed).
type ErrIPBlocked struct {
	Host string
	IP   net.IP
}

func (e *ErrIPBlocked) Error() string {
	if e.IP != nil {
		return fmt.Sprintf("ip_blocked host=%s ip=%s", e.Host, e.IP)
	}
	return fmt.Sprintf("ip_blocked host=%s reason=no_allowed_ip", e.Host)
}

// IsIPBlocked returns true if err is an *ErrIPBlocked.
func IsIPBlocked(err error) bool {
	var e *ErrIPBlocked
	return errors.As(err, &e)
}

// DialContext satisfies http.Transport.DialContext. It:
//
//  1. Splits host:port,
//  2. Resolves host (via the configured Resolver),
//  3. Checks every resolved IP against the deny list (skipping when policy is
//     PolicyAllowAll or when the pool ID is in allowList),
//  4. Dials the first allowed IP and returns its net.Conn.
//
// The TLS ServerName remains the original hostname (the http.Transport sets it
// from the request URL), so SNI/Host header continue to work even though we
// dial the IP directly — eliminating the standard DNS-rebind window.
func (f *Filter) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	if f.policy == PolicyAllowAll {
		f.warnAllowAllOnce()
		return f.dialer.DialContext(ctx, network, addr)
	}

	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, fmt.Errorf("split host:port: %w", err)
	}

	ips, err := f.resolver.LookupIPAddr(ctx, host)
	if err != nil {
		return nil, fmt.Errorf("resolve %s: %w", host, err)
	}
	if len(ips) == 0 {
		return nil, &ErrIPBlocked{Host: host, IP: nil}
	}

	// Per-pool exemption: if the caller's context carries a PoolIDKey value
	// present in allowList, skip the deny check for this fetch.
	poolID, _ := ctx.Value(PoolIDKey{}).(string)
	_, exempt := f.allowList[poolID]

	var chosen net.IP
	for _, ipa := range ips {
		ip := ipa.IP
		if exempt || !isBlocked(ip) {
			chosen = ip
			break
		}
	}
	if chosen == nil {
		return nil, &ErrIPBlocked{Host: host, IP: ips[0].IP}
	}

	dialAddr := net.JoinHostPort(chosen.String(), port)
	return f.dialer.DialContext(ctx, network, dialAddr)
}

// WrapTransport returns a copy of t with DialContext set to f.DialContext. If t
// is nil, a fresh transport is returned.
func (f *Filter) WrapTransport(t *http.Transport) *http.Transport {
	if t == nil {
		t = &http.Transport{}
	}
	// Don't inherit DefaultTransport's proxy — operators configure proxy_url
	// explicitly via fetch.http.proxy_url.
	t.Proxy = nil
	t.DialContext = f.DialContext
	return t
}

// warnAllowAllOnce emits the warning at most once per daemon lifetime.
func (f *Filter) warnAllowAllOnce() {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.warnedOnce {
		return
	}
	f.warnedOnce = true
	f.logger.Warn("private_ip_policy_allow_all: explicitly allowing all addresses; production deployments must use 'block'")
}

// -----------------------------------------------------------------------------
// IP classification
// -----------------------------------------------------------------------------

// isBlocked returns true for any IP in:
//
//   - RFC1918               (10/8, 172.16/12, 192.168/16)
//   - IPv4 loopback          (127/8)
//   - IPv4 link-local        (169.254/16)
//   - IPv4 unspecified       (0.0.0.0)
//   - IPv4 multicast         (224/4) — also blocks 169.254.169.254 SSRF attempts
//   - IPv6 loopback          (::1)
//   - IPv6 unspecified       (::)
//   - IPv6 link-local        (fe80::/10)
//   - IPv6 unique-local      (fc00::/7)
//   - IPv6 multicast         (ff00::/8)
//   - IPv4-mapped IPv6       (::ffff:a.b.c.d that maps to any of the above)
//
// Broadcast (255.255.255.255) is treated as multicast, see above.
func isBlocked(ip net.IP) bool {
	if ip == nil {
		return true
	}
	if v4 := ip.To4(); v4 != nil {
		return isBlockedV4(v4)
	}
	return isBlockedV6(ip)
}

// isBlockedV4 matches inet_pton blocks for IPv4. Operates on the 4-byte form.
func isBlockedV4(ip net.IP) bool {
	if len(ip) != 4 {
		return true
	}
	a, b := ip[0], ip[1]
	switch {
	case a == 10:
		return true // 10/8
	case a == 127:
		return true // 127/8 loopback
	case a == 172 && b >= 16 && b <= 31:
		return true // 172.16/12
	case a == 192 && b == 168:
		return true // 192.168/16
	case a == 169 && b == 254:
		return true // 169.254/16 link-local
	case a == 0:
		return true // 0.0.0.0/8
	case a >= 224:
		return true // 224/4 multicast (also catches 255.255.255.255)
	default:
		return false
	}
}

// isBlockedV6 matches IPv6 ranges.
func isBlockedV6(ip net.IP) bool {
	if len(ip) != 16 {
		return true
	}
	if ip[0] == 0 && ip[1] == 0 && ip[2] == 0 && ip[3] == 0 &&
		ip[4] == 0 && ip[5] == 0 && ip[6] == 0 && ip[7] == 0 &&
		ip[8] == 0 && ip[9] == 0 && ip[10] == 0 && ip[11] == 0 &&
		ip[12] == 0 && ip[13] == 0 && ip[14] == 0 && ip[15] == 1 {
		return true // ::1 loopback
	}
	allZero := true
	for _, b := range ip {
		if b != 0 {
			allZero = false
			break
		}
	}
	if allZero {
		return true // :: unspecified
	}
	// fe80::/10
	if ip[0] == 0xfe && (ip[1]&0xc0) == 0x80 {
		return true
	}
	// fc00::/7
	if ip[0]&0xfe == 0xfc {
		return true
	}
	// ff00::/8 multicast
	if ip[0] == 0xff {
		return true
	}
	// IPv4-mapped (::ffff:a.b.c.d): unwrap and recheck.
	if len(ip) >= 12 && ip[10] == 0xff && ip[11] == 0xff {
		return isBlockedV4(net.IP(ip[12:16]))
	}
	return false
}

// IsBlockedIP is exported for tests and tooling.
func IsBlockedIP(ip net.IP) bool { return isBlocked(ip) }
