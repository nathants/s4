## why

s3 is awesome, but can be expensive, slow, and doesn't expose data local compute or efficient shuffle.

## what

an s3 cli [compatible](https://github.com/nathants/s4/blob/master/tests/test_server.py) storage cluster that is cheap and fast, with data local compute and efficient shuffle.

use this for maximum performance processing of ephemeral data, with durable inputs, outputs, and checkpoints going to s3.

## how

a ring of servers store files on disk placed via consistent hashing.

a single metadata controller per server orchestrates out of process operations for data transfer, query, and compute.

a thicker client allows the metadata controller to be thinner.

## non goals

high availability. every key lives on one and only one server.

high durability. data lives on a single disk, and is as durable as that disk.

security. data transfers are checked for integrity, but not encrypted. service access is unauthenticated. secure the network with [wireguard](https://www.wireguard.com/) if needed.

fine granularity performance. data should be medium to coarse granularity.

safety for all inputs. service access should be considered to be at the level of root ssh. any user input should be escaped for shell.

## install

install
```
curl -s https://raw.githubusercontent.com/nathants/s4/master/scripts/install_archlinux.sh | bash
ssh $server1 "curl -s https://raw.githubusercontent.com/nathants/s4/master/scripts/install_archlinux.sh | bash"
ssh $server2 "curl -s https://raw.githubusercontent.com/nathants/s4/master/scripts/install_archlinux.sh | bash"
```

configure
```
echo $server1:8080 >  ~/.s4.conf
echo $server2:8080 >> ~/.s4.conf
scp ~/.s4.conf $server1:
scp ~/.s4.conf $server2:
```

start
```
ssh $server1 s4-server
ssh $server2 s4-server
```

use
```
echo hello world | s4 cp - s4://bucket/data.txt
s4 cp s4://bucket/data.txt -
s4 ls s4://bucket --recursive
s4 --help
```

## examples

[structured analysis of nyc taxi data with bsv and hive](./examples/nyc_taxi_bsv)

[adhoc exploration of nyc taxi data with python](./examples/nyc_taxi_python)

## api

- [s4 cp](#s4-cp) - cp data to or from s4
- [s4 eval](#s4-eval) - eval a bash cmd with key data as stdin
- [s4 health](#s4-health) - health check every server in the cluster
- [s4 ls](#s4-ls) - list keys
- [s4 map](#s4-map) - process data
- [s4 map-from-n](#s4-map-from-n) - merge shuffled data
- [s4 map-to-n](#s4-map-to-n) - shuffle data
- [s4 rm](#s4-rm) - delete a key from s4
- [s4 servers](#s4-servers) - list the server addresses in the cluster

## usage


### [s4 cp](https://github.com/nathants/s4/search?q="def+cp")

cp data to or from s4
```
usage: s4 cp [-h] [-r] src dst

    cp data to or from s4.

    - paths can be:
      - remote:       "s4://bucket/key.txt"
      - local:        "./dir/key.txt"
      - stdin/stdout: "-"
    - use recursive to copy directories.
    - keys cannot be updated, but can be deleted then recreated.
    - note: to copy from s4, the local machine must be reachable by the s4-server, otherwise use `s4 eval`.
    

positional arguments:
  src              -
  dst              -

optional arguments:
  -h, --help       show this help message and exit
  -r, --recursive  False
```

### [s4 eval](https://github.com/nathants/s4/search?q="def+cp")

eval a bash cmd with key data as stdin
```
usage: s4 eval [-h] key cmd

    eval a bash cmd with key data as stdin
    

positional arguments:
  key         -
  cmd         -

optional arguments:
  -h, --help  show this help message and exit
```

### [s4 health](https://github.com/nathants/s4/search?q="def+cp")

health check every server in the cluster
```
usage: s4 health [-h]

    health check every server in the cluster
    

optional arguments:
  -h, --help  show this help message and exit
```

### [s4 ls](https://github.com/nathants/s4/search?q="def+cp")

list keys
```
usage: s4 ls [-h] [-r] [prefix]

    list keys
    

positional arguments:
  prefix           -

optional arguments:
  -h, --help       show this help message and exit
  -r, --recursive  False
```

### [s4 map](https://github.com/nathants/s4/search?q="def+cp")

process data
```
usage: s4 map [-h] indir outdir cmd

    process data.

    - map a bash cmd 1:1 over every key in indir putting result in outdir.
    - no network usage.
    - cmd receives data via stdin and returns data via stdout.
    - every key in indir will create a key with the same name in outdir.
    - indir will be listed recursively to find keys to map.
    - only keys on exact servers can be mapped since mapped inputs and outputs need to be on the same server.
    - you want your key names to be monotonic integers, which round robins their server placement.
    - server placement is based on either the full key path or a numeric key name prefix:
      - hash full key path:     s4://bucket/dir/name.txt
      - use numeric key prefix: s4://bucket/dir/000_name.txt
      - use numeric key prefix: s4://bucket/dir/000
    

positional arguments:
  indir       -
  outdir      -
  cmd         -

optional arguments:
  -h, --help  show this help message and exit
```

### [s4 map-from-n](https://github.com/nathants/s4/search?q="def+cp")

merge shuffled data
```
usage: s4 map-from-n [-h] indir outdir cmd

    merge shuffled data.

    - map a bash cmd n:1 over every dir in indir putting result in outdir.
    - no network usage.
    - cmd receives file paths via stdin and returns data via stdout.
    - each cmd receives all keys with a given numeric prefix and the output key uses that numeric prefix as its name.
    

positional arguments:
  indir       -
  outdir      -
  cmd         -

optional arguments:
  -h, --help  show this help message and exit
```

### [s4 map-to-n](https://github.com/nathants/s4/search?q="def+cp")

shuffle data
```
usage: s4 map-to-n [-h] indir outdir cmd

    shuffle data.

    - map a bash cmd 1:n over every key in indir putting results in outdir.
    - network usage.
    - cmd receives data via stdin, writes files to disk, and returns file paths via stdout.
    - every key in indir will create a directory with the same name in outdir.
    - outdir directories contain zero or more files output by cmd.
    - cmd runs in a tempdir which is deleted on completion.
    - you want your outdir file paths to be monotonic integers, which round robins their server placement.
    - server placement is based on either the full key path or a numeric key name prefix:
      - hash full key path:     s4://bucket/dir/name.txt
      - use numeric key prefix: s4://bucket/dir/000_name.txt
      - use numeric key prefix: s4://bucket/dir/000
    

positional arguments:
  indir       -
  outdir      -
  cmd         -

optional arguments:
  -h, --help  show this help message and exit
```

### [s4 rm](https://github.com/nathants/s4/search?q="def+cp")

delete a key from s4
```
usage: s4 rm [-h] [-r] prefix

    delete a key from s4.

    - recursive to delete directories.
    

positional arguments:
  prefix           -

optional arguments:
  -h, --help       show this help message and exit
  -r, --recursive  False
```

### [s4 servers](https://github.com/nathants/s4/search?q="def+cp")

list the server addresses in the cluster
```
usage: s4 servers [-h]

    list the server addresses in the cluster
    

optional arguments:
  -h, --help  show this help message and exit
```
