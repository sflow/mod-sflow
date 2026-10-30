# ==========================================
# STAGE 1: Build the Apache module
# ==========================================
FROM debian:trixie-slim AS builder

# Install compilation tools and Debian 13 Apache development headers
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    apache2-dev \
    && rm -rf /var/lib/apt/lists/*

# Set up the working directory inside the container
WORKDIR /build

# Copy your module source code into the container
COPY mod_sflow.c sflow_api.c sflow.h sflow_api.h .

# Compile the module using apxs
# -c: Compile the source code into a dynamic shared object (.so)
RUN apxs -c mod_sflow.c sflow_api.c

# ==========================================
# STAGE 2: Export the binary
# ==========================================
FROM scratch AS exporter
COPY --from=builder /build/.libs/mod_sflow.so /
