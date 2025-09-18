# syntax=docker/dockerfile:1
# NOTE: run this in the NDN root directory

FROM ubuntu:22.04

# Install dependencies
RUN <<EOF
    set -eux
    apt-get update -y
    apt-get install --no-install-recommends -y \
        lsb-release sudo \
        zip unzip wget git ca-certificates \
        curl iproute2 iputils-ping net-tools \
        python3 python3-pip python-is-python3 \
        tcpdump vim x11-xserver-utils xterm
    rm -rf /var/lib/apt/lists/*
    update-ca-certificates
EOF

# Install golang
RUN <<EOF
    curl -LO https://go.dev/dl/go1.23.0.linux-amd64.tar.gz
    rm -rf /usr/local/go && tar -C /usr/local -xzf go1.23.0.linux-amd64.tar.gz
    export PATH="/usr/local/go/bin:$PATH"
EOF

# Clone ndnd from Haotian's fork
RUN git clone https://github.com/AnoldGH/ndnd.git /ndnd

COPY . /repo

COPY ../../mini-ndn /mini-ndn
WORKDIR /mini-ndn

RUN <<EOF
    set -eux
    pip3 install -r requirements.txt
    ./install.sh -y --source
    make -C dl/mininet install
    make -C dl/mininet-wifi install
    rm -rf dl /var/lib/apt/lists/*
EOF

# Expose ports for openvswitch-switch
EXPOSE 6633 6653 6640

ENTRYPOINT ["docker/entrypoint.sh"]
