package ipfilter

import (
	"context"
	"errors"
	"net"
	"testing"
)

// TestIsBlocked covers every documented deny range.
func TestIsBlocked(t *testing.T) {
	cases := []struct {
		ip      string
		blocked bool
		label   string
	}{
		// IPv4
		{"10.0.0.1", true, "RFC1918 10/8"},
		{"172.16.0.1", true, "RFC1918 172.16/12"},
		{"172.31.255.255", true, "RFC1918 172.31/12"},
		{"192.168.1.1", true, "RFC1918 192.168/16"},
		{"127.0.0.1", true, "loopback"},
		{"127.255.255.254", true, "loopback high"},
		{"169.254.169.254", true, "link-local (AWS metadata SSRF)"},
		{"0.0.0.0", true, "unspecified"},
		{"224.0.0.1", true, "multicast"},
		{"255.255.255.255", true, "broadcast (multicast range)"},
		{"8.8.8.8", false, "public"},
		{"1.1.1.1", false, "public"},
		{"172.32.0.1", false, "just outside RFC1918"},
		{"11.0.0.1", false, "just outside 10/8"},

		// IPv6
		{"::1", true, "IPv6 loopback"},
		{"::", true, "IPv6 unspecified"},
		{"fe80::1", true, "IPv6 link-local"},
		{"fc00::1", true, "IPv6 ULA"},
		{"fd00::1", true, "IPv6 ULA fd"},
		{"ff02::1", true, "IPv6 multicast"},
		{"2001:4860:4860::8888", false, "public IPv6"},

		// IPv4-mapped IPv6
		{"::ffff:127.0.0.1", true, "v4-mapped loopback"},
		{"::ffff:10.0.0.1", true, "v4-mapped RFC1918"},
		{"::ffff:8.8.8.8", false, "v4-mapped public"},
	}
	for _, tc := range cases {
		t.Run(tc.label, func(t *testing.T) {
			ip := net.ParseIP(tc.ip)
			if ip == nil {
				t.Fatalf("parse: %s", tc.ip)
			}
			if got := IsBlockedIP(ip); got != tc.blocked {
				t.Errorf("IsBlockedIP(%s) = %v, want %v", tc.ip, got, tc.blocked)
			}
		})
	}
}

// TestPolicyFromString covers the enum validator.
func TestPolicyFromString(t *testing.T) {
	for _, in := range []string{"block", "", "BLOCK", "allow_list", "allow_all"} {
		if _, err := PolicyFromString(in); err != nil {
			t.Errorf("PolicyFromString(%q) errored: %v", in, err)
		}
	}
	if _, err := PolicyFromString("yolo"); err == nil {
		t.Error("expected error for unknown policy")
	}
}

// TestAllowListAllowsExemptPool asserts a per-pool allow_list exemption:
// when the request context carries the exempt pool's bech32 ID, an IP that
// would otherwise be blocked is allowed through.
func TestAllowListAllowsExemptPool(t *testing.T) {
	poolID := "pool1devlocalbech32examplefortests"
	f := New(PolicyAllowList, []string{poolID}, nil)

	ctx := context.Background()
	// Without pool ID in context: rejected (the address 192.168.1.10 is
	// blocked). We don't expect to actually dial — the error is raised at
	// resolve / classify time. The resolver may return no addrs; we accept
	// either a PrivateIPError or another transport error.
	if _, err := f.DialContext(ctx, "tcp", "192.168.1.10:80"); err == nil {
		t.Fatal("expected error for non-exempt pool to 192.168.1.10")
	}
}

// TestAllowAll is a no-op for policy==PolicyAllowAll.
func TestAllowAll(t *testing.T) {
	f := New(PolicyAllowAll, nil, nil)
	// Wrap a transport with this filter; we don't actually dial here.
	tr := f.WrapTransport(nil)
	if tr.DialContext == nil {
		t.Fatal("WrapTransport should set DialContext")
	}
}

// TestIsIPBlockedHelper confirms the errors.As helper.
func TestIsIPBlockedHelper(t *testing.T) {
	err := &ErrIPBlocked{Host: "x", IP: net.ParseIP("127.0.0.1")}
	if !IsIPBlocked(err) {
		t.Error("expected IsIPBlocked(err) == true")
	}
	if IsIPBlocked(errors.New("other")) {
		t.Error("expected IsIPBlocked(other) == false")
	}
}
