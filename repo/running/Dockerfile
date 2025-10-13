# syntax=docker/dockerfile:1
# NOTE: run this in the NDN root directory

FROM ghcr.io/named-data/mini-ndn:latest

# Clone ndnd from Haotian's fork
RUN git clone https://github.com/a-thieme/ndnd.git /ndnd

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
