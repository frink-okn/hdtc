# syntax=docker/dockerfile:1

# The toolchain the image is built with; Cargo.toml's rust-version is only the
# minimum.
ARG RUST_VERSION=1.94.1
# The tool that records the binary's dependencies in the binary; see below.
ARG CARGO_AUDITABLE_VERSION=0.7.7

# The same Debian release as the runtime image, so the binary is linked against
# the glibc and gcc runtime it runs with. Slim is enough: liblzma, libbz2 and
# libzstd are compiled from the sources their -sys crates bundle (Cargo.toml), so
# no -dev package or pkg-config is needed.
FROM rust:${RUST_VERSION}-slim-trixie AS builder
ARG CARGO_AUDITABLE_VERSION

# `cargo auditable build` compiles exactly what `cargo build` does and also
# embeds the resolved dependency list in the binary. Without it the binary is
# opaque to an image scanner (Trivy, Grype, `cargo audit bin`), which then
# reports only the runtime image's OS packages and never a vulnerable crate.
RUN cargo install cargo-auditable --locked --version "${CARGO_AUDITABLE_VERSION}"

WORKDIR /build
COPY . .
# `--locked`, so the image is built from exactly the Cargo.lock that CI tests
# and audits.
RUN cargo auditable build --release --locked --bin hdtc

# The binary may load glibc and libgcc_s, which every Rust binary on this
# target needs, and no other shared library. Any other C library is linked
# statically, so the binary does not depend on what else the runtime image
# happens to carry. A binary that breaks this would still build, then fail to
# start in the runtime image, so the check is made here, where it names the
# library.
RUN set -eu; \
    for lib in $(readelf -d target/release/hdtc | sed -n 's/.*(NEEDED).*\[\(.*\)\]/\1/p'); do \
      case "$lib" in \
        libc.so.6|libm.so.6|libgcc_s.so.1) ;; \
        *) echo "hdtc links $lib dynamically; only glibc and libgcc_s may be" >&2; exit 1 ;; \
      esac; \
    done

# glibc, the gcc runtime libraries, CA certificates and time zone data: what a
# dynamically linked Rust binary needs, with no shell, package manager or
# coreutils. Every OS package in the image is one a scanner reports against,
# so the ones hdtc never runs are left out rather than patched. The tag follows
# Debian's security updates, so each build picks them up.
#
# This tag runs as root, as the image always has, so that output written into a
# directory mounted at /data needs no ownership arrangement. Pass
# `--user "$(id -u):$(id -g)"` to `docker run` to write files as yourself.
FROM gcr.io/distroless/cc-debian13

COPY --from=builder /build/target/release/hdtc /usr/local/bin/hdtc

# Default working directory for mounted data
WORKDIR /data

ENTRYPOINT ["hdtc"]
