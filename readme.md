## why

s3 is awesome, but can be expensive, slow, and doesn't expose data local compute or efficient shuffle.

## what

an s3 cli compatible storage cluster that is cheap and fast, with data local compute and efficient shuffle.

data local compute maps arbitrary commands over immutable keys in 1:1, n:1 and 1:n operations.

keys are strongly consistent and cannot be updated unless first deleted.

use this for processing ephemeral data, with durable inputs, outputs, and checkpoints in s3.

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

## install

install
```bash
curl -s https://raw.githubusercontent.com/nathants/s4/master/scripts/install_archlinux.sh | bash
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

use
```bash
echo hello world | s4 cp - s4://bucket/data.txt
s4 cp s4://bucket/data.txt -
s4 ls s4://bucket --recursive
s4 --help
```

## examples

[structured analysis of nyc taxi data with bsv and hive](https://github.com/nathants/s4/blob/master/examples/nyc_taxi_bsv)

[adhoc exploration of nyc taxi data with python](https://github.com/nathants/s4/blob/master/examples/nyc_taxi_python)

## api

| name | description |
| -- | -- |
| [s4 cp](#s4-cp) | copy data to or from s4 |
| [s4 eval](#s4-eval) | eval a bash cmd with key data as stdin |
| [s4 health](#s4-health) | health check every server |
| [s4 ls](#s4-ls) | list keys |
| [s4 map](#s4-map) | process data |
| [s4 map-from-n](#s4-map-from-n) | merge shuffled data |
| [s4 map-to-n](#s4-map-to-n) | shuffle data |
| [s4 rm](#s4-rm) | delete data from s4 |
| [s4 servers](#s4-servers) | list the server addresses |

## usage

### [s4 cp](https://github.com/nathants/s4/search?l=Python&q="def+cp")
```
usage: s4 cp [-h] [-r] src dst

    copy data to or from s4.

    - paths can be:
      - remote:       "s4://bucket/key.txt"
      - local:        "./dir/key.txt"
      - stdin/stdout: "-"
    - use recursive to copy directories.
    - keys cannot be updated, but can be deleted then recreated.
    - note: to copy from s4, the local machine must be reachable by the servers, otherwise use `s4 eval`.


positional arguments:
  src              -
  dst              -

optional arguments:
  -h, --help       show this help message and exit
  -r, --recursive  False
```

### [s4 eval](https://github.com/nathants/s4/search?l=Python&q="def+eval")
```
usage: s4 eval [-h] key cmd

    eval a bash cmd with key data as stdin


positional arguments:
  key         -
  cmd         -

optional arguments:
  -h, --help  show this help message and exit
```

### [s4 health](https://github.com/nathants/s4/search?l=Python&q="def+health")
```
usage: s4 health [-h]

    health check every server


optional arguments:
  -h, --help  show this help message and exit
```

### [s4 ls](https://github.com/nathants/s4/search?l=Python&q="def+ls")
```
usage: s4 ls [-h] [-r] [prefix]

    list keys


positional arguments:
  prefix           -

optional arguments:
  -h, --help       show this help message and exit
  -r, --recursive  False
```

### [s4 map](https://github.com/nathants/s4/search?l=Python&q="def+map")
```
usage: s4 map [-h] indir outdir cmd

    process data.

    - map a bash cmd 1:1 over every key in indir putting result in outdir.
    - cmd receives data via stdin and returns data via stdout.
    - every key in indir will create a key with the same name in outdir.
    - indir will be listed recursively to find keys to map.
    - only keys with a numeric prefix can be mapped since outputs need to be on the same server.
    - key names should be monotonic integers, which distributes their server placement.
    - server placement is based on either path hash or a numeric prefix:
      - hash full key path: s4://bucket/dir/name.txt
      - use numeric prefix: s4://bucket/dir/000_name.txt
      - use numeric prefix: s4://bucket/dir/000


positional arguments:
  indir       -
  outdir      -
  cmd         -

optional arguments:
  -h, --help  show this help message and exit
```

### [s4 map-from-n](https://github.com/nathants/s4/search?l=Python&q="def+map-from-n")
```
usage: s4 map-from-n [-h] indir outdir cmd

    merge shuffled data.

    - map a bash cmd n:1 over every dir in indir putting result in outdir.
    - cmd receives file paths via stdin and returns data via stdout.
    - each cmd receives all keys for a numeric prefix.
    - output name is the numeric prefix.


positional arguments:
  indir       -
  outdir      -
  cmd         -

optional arguments:
  -h, --help  show this help message and exit
```

### [s4 map-to-n](https://github.com/nathants/s4/search?l=Python&q="def+map-to-n")
```
usage: s4 map-to-n [-h] indir outdir cmd

    shuffle data.

    - map a bash cmd 1:n over every key in indir putting results in outdir.
    - cmd receives data via stdin, writes files to disk, and returns file paths via stdout.
    - every key in indir will create a directory with the same name in outdir.
    - outdir directories contain zero or more files output by cmd.
    - cmd runs in a tempdir which is deleted on completion.
    - outdir file paths should be monotonic integers, which distributes their server placement.
    - server placement is based on either the path hash or a numeric prefix:
      - hash full key path: s4://bucket/dir/name.txt
      - use numeric prefix: s4://bucket/dir/000_name.txt
      - use numeric prefix: s4://bucket/dir/000


positional arguments:
  indir       -
  outdir      -
  cmd         -

optional arguments:
  -h, --help  show this help message and exit
```

### [s4 rm](https://github.com/nathants/s4/search?l=Python&q="def+rm")
```
usage: s4 rm [-h] [-r] prefix

    delete data from s4.

    - recursive to delete directories.


positional arguments:
  prefix           -

optional arguments:
  -h, --help       show this help message and exit
  -r, --recursive  False
```

### [s4 servers](https://github.com/nathants/s4/search?l=Python&q="def+servers")
```
usage: s4 servers [-h]

    list the server addresses


optional arguments:
  -h, --help  show this help message and exit
```
