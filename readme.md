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
