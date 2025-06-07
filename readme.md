# S4

## Why

S3 is awesome, but can be expensive, slow, and doesn't expose data local compute or efficient shuffle.

## What

An S3 CLI compatible storage cluster that is cheap and fast, with data local compute and efficient shuffle.

Data local compute maps arbitrary commands over immutable keys in 1:1, n:1 and 1:n operations.

Data shuffle is implicit in 1:n mappings.

Server placement is based on the hash of basename or a numeric prefix.

| Key | Method | Placement |
| -- | -- | -- |
| s4://bucket/dir/name.txt | int(hash("name.txt")) | ? |
| s4://bucket/dir/000_bucket0.txt | int("000") | 0 |
| s4://bucket/dir/000 | int("000") | 0 |

Keys are strongly consistent and cannot be updated unless first deleted.

## When

Use this for efficiently processing ephemeral data.

Keep durable inputs, outputs, and checkpoints in S3.

## How

A ring of servers store files on disk.

A metadata controller on each server orchestrates out of process operations for data transfer and local compute.

A CLI client coordinates cluster activity.

## Non Goals

High availability. Every key lives on one and only one server.

High durability. Data lives on a single disk, and is as durable as that disk.

Security. Data transfers are checked for integrity, but not encrypted. Service access is unauthenticated. Secure the network with [WireGuard](https://www.wireguard.com/) if needed.

Fine granularity. Data should be medium to coarse granularity.

Safety for all inputs. Service access should be considered to be at the level of root SSH. Any user input should be escaped for shell.

Cluster resizing. Clusters should be short lived and data ephemeral. Instead of resizing create a new cluster.

Pagination of list results. Data layout and partitioning must be considered.

## Install

Go install:

```bash
go install github.com/nathants/s4/cmd/s4@latest
go install github.com/nathants/s4/cmd/s4_server@latest
sudo mv -f $(go env GOPATH)/bin/s4 /usr/local/bin/s4
sudo mv -f $(go env GOPATH)/bin/s4_server /usr/local/bin/s4-server
```

Git clone:

```bash
git clone https://github.com/nathants/s4
cd s4
git clone go
make -j
sudo mv -fv bin/s4 bin/s4-server /usr/local/bin/
```

## Test

```bash
>> tox
```

## Automatic Deployment

```bash
cd s4
name=s4-cluster
bash scripts/new_cluster.sh $name
```

## Manual Deployment

Deploy
```bash
ssh $server1 "curl -s https://raw.githubusercontent.com/nathants/s4/go/scripts/install_archlinux.sh | bash"
ssh $server2 "curl -s https://raw.githubusercontent.com/nathants/s4/go/scripts/install_archlinux.sh | bash"
```

Configure
```bash
echo $server1:8080 >  ~/.s4.conf
echo $server2:8080 >> ~/.s4.conf
scp ~/.s4.conf $server1:
scp ~/.s4.conf $server2:
```

Start
```bash
ssh $server1 s4-server
ssh $server2 s4-server
```

## Usage

```bash
echo hello world | s4 cp - s4://bucket/data.txt
s4 cp s4://bucket/data.txt -
s4 ls s4://bucket --recursive
s4 --help
```

## Examples

[Structured analysis of NYC taxi data with BSV and Hive](https://github.com/nathants/s4/blob/go/examples/nyc_taxi_bsv)

[Adhoc exploration of NYC taxi data with Python](https://github.com/nathants/s4/blob/go/examples/nyc_taxi_python)

## Related Projects

[BSV](https://github.com/nathants/bsv) - A simple and efficient data format for easily manipulating chunks of rows of columns while minimizing allocations and copies.

## Related Posts

[Optimizing a BSV data processing pipeline](https://nathants.com/posts/optimizing-a-bsv-data-processing-pipeline)

[Performant batch processing with BSV, S4, and Presto](https://nathants.com/posts/performant-batch-processing-with-bsv-s4-and-presto)

[Discovering a baseline for data processing performance](https://nathants.com/posts/discovering-a-baseline-for-data-processing-performance)

[Refactoring common distributed data patterns into S4](https://nathants.com/posts/refactoring-common-distributed-data-patterns-into-s4)

[Scaling Python data processing horizontally](https://nathants.com/posts/scaling-python-data-processing-horizontally)

[Scaling Python data processing vertically](https://nathants.com/posts/scaling-python-data-processing-vertically)

## API

| Name | Description |
| -- | -- |
| [S4 rm](#s4-rm) | Delete data from S4 |
| [S4 eval](#s4-eval) | Eval a Bash cmd with key data as stdin |
| [S4 ls](#s4-ls) | List keys |
| [S4 cp](#s4-cp) | Copy data to or from S4 |
| [S4 map](#s4-map) | Process data |
| [S4 map-to-n](#s4-map-to-n) | Shuffle data |
| [S4 map-from-n](#s4-map-from-n) | Merge shuffled data |
| [S4 config](#s4-config) | List the server addresses |
| [S4 health](#s4-health) | Health check every server |

## Usage

### S4 rm
```
usage: s4 rm [-h] [-r] prefix

    delete data from s4.

    - recursive to delete directories.


positional arguments:
  prefix           -

optional arguments:
  -h       show this help message and exit
  -r       False
```

### S4 eval
```
usage: s4 eval [-h] key cmd

    eval a bash cmd with key data as stdin


positional arguments:
  key         -
  cmd         -

optional arguments:
  -h  show this help message and exit
```

### S4 ls
```
usage: s4 ls [-h] [-r] [prefix]

    list keys


positional arguments:
  prefix           -

optional arguments:
  -h, --help       show this help message and exit
  -r, --recursive  False
```

### S4 cp
```
usage: s4 cp [-h] [-r] src dst

    copy data to or from s4.

    - paths can be:
      - remote:       "s4://bucket/key.txt"
      - local:        "./dir/key.txt"
      - stdin/stdout: "-"
    - use recursive to copy directories.
    - keys cannot be updated, but can be deleted and recreated.
    - note: to copy from s4, the local machine must be reachable by the cluster, otherwise use `s4 eval`.


positional arguments:
  src              -
  dst              -

optional arguments:
  -h       show this help message and exit
  -r       False
```

### S4 map
```
usage: s4 map [-h] indir outdir cmd

    process data.

    - map a bash cmd 1:1 over every key in indir putting result in outdir.
    - cmd receives data via stdin and returns data via stdout.
    - every key in indir will create a key with the same name in outdir.
    - indir will be listed recursively to find keys to map.


positional arguments:
  indir       -
  outdir      -
  cmd         -

optional arguments:
  -h  show this help message and exit
```

### S4 map-to-n
```
usage: s4 map-to-n [-h] indir outdir cmd

    shuffle data.

    - map a bash cmd 1:n over every key in indir putting results in outdir.
    - cmd receives data via stdin, writes files to disk, and returns file paths via stdout.
    - every key in indir will create a directory with the same name in outdir.
    - outdir directories contain zero or more files output by cmd.
    - cmd runs in a tempdir which is deleted on completion.


positional arguments:
  indir       -
  outdir      -
  cmd         -

optional arguments:
  -h  show this help message and exit
```

### S4 map-from-n
```
usage: s4 map-from-n [-h] indir outdir cmd

    merge shuffled data.

    - map a bash cmd n:1 over every key in indir putting result in outdir.
    - indir will be listed recursively to find keys to map.
    - cmd receives file paths via stdin and returns data via stdout.
    - each cmd receives all keys with the same name or numeric prefix
    - output name is that name


positional arguments:
  indir       -
  outdir      -
  cmd         -

optional arguments:
  -h  show this help message and exit
```

### S4 config
```
usage: s4 config [-h]

    list the server addresses


optional arguments:
  -h  show this help message and exit
```

### S4 health
```
usage: s4 health [-h]

    health check every server


optional arguments:
  -h  show this help message and exit
```
