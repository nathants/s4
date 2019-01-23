## why

s3 is awesome, but can be expensive, slow, and has no actual data locality.

## what

an s3 cli replacement that's cheaper, faster, and exposes data locality. use this for ephemeral data. it's strongly consistent, but not highly durable or available. this project was born out of the need to have more efficient intermediate storage when doing [mapreduce the hard way](https://github.com/nathants/py-aws#more-what-aka-mapreduce-the-hard-way).

## non goals

- high availability. by default every key lives on one and only one server in the hash ring.

- high durability. data lives on disk, so it depends on the disk. nvme instance store on ec2 is recommended.

- network security. data is checked for integrity, but not encrypted, as it moves around the network. ssh/scp is an option if needed.

- [partial compatability](https://github.com/nathants/s4/blob/master/tests/test_server.py) with the s3 cli.

## install

note: tested only on linux

on every server:

- install s4
```
git clone https://github.com/nathants/s4
cd s4
pip install -r requirements.txt .
```

- configure s4 with the ipv4:port of every server. make sure to use the ipv4 local to the machine, as the conf file defines the hash ring, and each server recognizes itself in the conf by comparing its ipv4 as reported by ifconfig.
   ```
   touch ~/.s4.conf
   echo $server1:$port1 >> ~/.s4.conf
   echo $server2:$port2 >> ~/.s4.conf
   echo ... >> ~/.s4.conf
   ```

- start the server, possibly with sudo if binding on port 80.

`s4-server`

python entrypoints are quite slow, so create a bash function like:

```
s4() {
    python3 -c 'import s4.cli, sys; sys.argv[0] = "s4"; s4.cli.main()' "$@";
}
```

cli startup times:

```
>> time s4-cli -h
usage: s4-cli [-h] [-p PORT] [-d]

real    0m0.428s
user    0m0.388s
sys     0m0.032s

>> time s4 -h
usage: -c [-h] {cp,ls,rm} ...

real    0m0.179s
user    0m0.140s
sys     0m0.028s
```

## related projects

the [s3 stub](http://github.com/nathants/py-aws#testing-with-s3) in py-aws shares a test suite with s4.
