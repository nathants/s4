## why

s3 is awesome, but can be expensive, slow, and doesn't expose data local compute or efficient shuffle.

## what

an s3 cli compatible storage cluster that is cheap and fast, with data local compute and efficient shuffle.

data local compute maps arbitrary commands over immutable keys in 1:1, n:1 and 1:n operations.

data shuffle is implicit in 1:n mappings.

server placement is based on the hash of basename or a numeric prefix.

| key | method | placement |
| -- | -- | -- |
| s4://bucket/dir/name.txt | int(hash("name.txt")) | ? |
| s4://bucket/dir/000_bucket0.txt | int("000") | 0 |
| s4://bucket/dir/000 | int("000") | 0 |

keys are strongly consistent and cannot be updated unless first deleted.

## when

use this for efficiently processing ephemeral data.

keep durable inputs, outputs, and checkpoints in s3.

## how

a ring of servers store files on disk.

a metadata controller on each server orchestrates out of process operations for data transfer and local compute.

a cli client coordinates cluster activity.

## non goals

high availability. every key lives on one and only one server.

high durability. data lives on a single disk, and is as durable as that disk.

security. data transfers are checked for integrity, but not encrypted. service access is unauthenticated. secure the network with [wireguard](https://www.wireguard.com/) if needed.

fine granularity. data should be medium to coarse granularity.

safety for all inputs. service access should be considered to be at the level of root ssh. any user input should be escaped for shell.

cluster resizing. clusters should be short lived and data ephemeral. instead of resizing create a new cluster.

pagination of list results. data layout and partitioning must be considered.

## implementations

there are two implementations in [python](https://github.com/nathants/s4/tree/python) and [go](https://github.com/nathants/s4/tree/go). they share a test suite and both are production ready.

if you need fine granularity performance, prefer the [go](https://github.com/nathants/s4/tree/go) implementation as it has less overhead per put/get operation.

operations and interface are identical.

updates and bugfixes are applied to both.

## install

go get:

```bash
go get github.com/nathants/s4/cmd/s4
go get github.com/nathants/s4/cmd/s4_server
```

git clone:

```bash
git clone https://github.com/nathants/s4
cd s4
git clone go
make -j
sudo mv -fv bin/s4 bin/s4-server /usr/local/bin/
```

## automatic deployment

```bash
cd s4
name=s4-cluster
bash scripts/new_cluster.sh $name
```

## manual deployment

deploy
```bash
ssh $server1 "curl -s https://raw.githubusercontent.com/nathants/s4/master/scripts/install_archlinux.sh | bash"
ssh $server2 "curl -s https://raw.githubusercontent.com/nathants/s4/master/scripts/install_archlinux.sh | bash"
```

configure
```bash
echo $server1:8080 >  ~/.s4.conf
echo $server2:8080 >> ~/.s4.conf
scp ~/.s4.conf $server1:
scp ~/.s4.conf $server2:
```

start
```bash
ssh $server1 s4-server
ssh $server2 s4-server
```

## usage

```bash
echo hello world | s4 cp - s4://bucket/data.txt
s4 cp s4://bucket/data.txt -
s4 ls s4://bucket --recursive
s4 --help
```

## examples

[structured analysis of nyc taxi data with bsv and hive](https://github.com/nathants/s4/blob/master/examples/nyc_taxi_bsv)

[adhoc exploration of nyc taxi data with python](https://github.com/nathants/s4/blob/master/examples/nyc_taxi_python)

## related projects

[bsv](https://github.com/nathants/bsv) - a simple and efficient data format for easily manipulating chunks of rows of columns while minimizing allocations and copies.

## related posts

[optimizing a bsv data processing pipeline](https://nathants.com/posts/optimizing-a-bsv-data-processing-pipeline)

[performant batch processing with bsv, s4, and presto](https://nathants.com/posts/performant-batch-processing-with-bsv-s4-and-presto)

[discovering a baseline for data processing performance](https://nathants.com/posts/discovering-a-baseline-for-data-processing-performance)

[refactoring common distributed data patterns into s4](https://nathants.com/posts/refactoring-common-distributed-data-patterns-into-s4)

[scaling python data processing horizontally](https://nathants.com/posts/scaling-python-data-processing-horizontally)

[scaling python data processing vertically](https://nathants.com/posts/scaling-python-data-processing-vertically)

## api

| name | description |
| -- | -- |
| [s4 rm](#s4-rm) | delete data from s4 |
| [s4 eval](#s4-eval) | eval a bash cmd with key data as stdin |
| [s4 ls](#s4-ls) | list keys |
| [s4 cp](#s4-cp) | copy data to or from s4 |
| [s4 map](#s4-map) | process data |
| [s4 map-to-n](#s4-map-to-n) | shuffle data |
| [s4 map-from-n](#s4-map-from-n) | merge shuffled data |
| [s4 config](#s4-config) | list the server addresses |
| [s4 health](#s4-health) | health check every server |

## usage

### s4 rm
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

### s4 eval
```
usage: s4 eval [-h] key cmd

    eval a bash cmd with key data as stdin


positional arguments:
  key         -
  cmd         -

optional arguments:
  -h  show this help message and exit
```

### s4 ls
```
usage: s4 ls [-h] [-r] [prefix]

    list keys


positional arguments:
  prefix           -

optional arguments:
  -h, --help       show this help message and exit
  -r, --recursive  False
```

### s4 cp
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

### s4 map
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

### s4 map-to-n
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

### s4 map-from-n
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

### s4 config
```
usage: s4 config [-h]

    list the server addresses


optional arguments:
  -h  show this help message and exit
```

### s4 health
```
usage: s4 health [-h]

    health check every server


optional arguments:
  -h  show this help message and exit
```
