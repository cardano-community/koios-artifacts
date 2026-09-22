# ---- build stage ----
FROM golang:1.25-bookworm AS build
ARG VERSION=dev
WORKDIR /src

# Cache dependencies first
COPY go.mod go.sum* ./
RUN go mod download

# Copy source and build
COPY . .
RUN CGO_ENABLED=0 GOOS=linux \
    go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" \
    -o /out/pool-meta-fetcher ./cmd/pool-meta-fetcher

# ---- runtime stage (distroless) ----
FROM gcr.io/distroless/static:nonroot
COPY --from=build /out/pool-meta-fetcher /usr/local/bin/pool-meta-fetcher
ENV CNODE_HOME=/opt/cardano/cnode
EXPOSE 8081
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/pool-meta-fetcher"]
CMD ["--config", "$CNODE_HOME/priv/pool-meta-fetcher.yaml"]
