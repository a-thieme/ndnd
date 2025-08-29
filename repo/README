# Distributed Repo (DRepo)

Distributed Repo (DRepo) is a distributed, sharded storage system built on top of Named Data Networking (NDN). It enables multiple Repo nodes to form a cohesive group, providing data replication, sharding, and failure recovery across potentially a large cluster of nodes. DRepo is designed for high availability, fault tolerance, and scalability in NDN-based environments, with eventual consistency model in most operations.

## Features
- **Sharded Storage:** Data is partitioned and distributed across multiple nodes.
- **Replication:** Each data partition is replicated to a configurable number of nodes for reliability.
- **Group Mode:** Multiple Repo nodes can join a group, automatically handling membership and data consistency.
- **NDN Native:** Built natively for Named Data Networking, leveraging NDN's data-centric security (wip) and routing.

## Usage
To start a DRepo node in group mode:

```sh
go run cmd/main.go <group_configuration_file> <node_configuration_file>
```

- `<group_configuration_file>`: YAML file describing the Repo group, partitions, and replication settings, etc.
- `<node_configuration_file>`: YAML file with the local node's configuration (name, trust anchors, storage settings, etc).

### Example

```sh
go run cmd/main.go repo_group_1.yml repo_test_1.yml
```

## Configuration
Example configuration files can be found in this directory:
- `repo_group_1.yml` — Example group configuration
- `repo_test_1.yml`, `repo_test_2.yml`, ... — Example node configurations

## Documentation
- Still WIP. For high level documentations, please refer to [DRepo Design](https://docs.google.com/presentation/d/1pjTrxT7Wynu8PIHxLnWZd_WhBoFOgwR42hFqjYKADxc/edit?usp=sharing)
- For more information on NDN, visit [https://named-data.net/](https://named-data.net/)

## Names & Prefixes
- To retrieve data (including sync group data), simply send an interest for that, which will be handled by NDN routing.
- DRepo commands have the prefix: `<repo_group_name>/notify/<digest>`
- DRepo status requests have the prefix: `<repo_group_name>/status/<digest>`
- DRepo uses multicast for group state awareness. Please set the strategy for following prefixes to multicast:
    - `<repo_group_name>/heartbeat`
    - `<repo_group_name>/awareness`

You can set the multicast strategy for a prefix with the following command:

    ndnd fw strategy-set prefix=<target_prefix> strategy=/localhost/nfd/strategy/multicast