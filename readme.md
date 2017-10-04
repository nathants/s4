## why

s3 is awesome, but can be expensive, slow, and has no actual data locality. so lets be cheaper, faster, and able to get at the data from metal. use this only for temporary, ephemeral data. it's strongly consistent, but not (by default) highly durable or available. this project was born out of the need to have more efficient intermediate storage when doing [mapreduce the hard way](https://github.com/nathants/py-aws#more-why-aka-map-reduce-the-hard-way).

## non goals

- high availability. by default every key lives on one and only one server in the hash ring.

- high durability. data lives on disk, so it depends on the disk. for best perfomance embrace ephemerality and use instance local storage on ec2 i3.

- network security. data is checked for integrity, but not encrypted, as it moves around the network. use on trusted networks only. ssh/scp is an option, but at significant performance penalty, even with hardware acceleartion.

- full compatability with s3. this is a drop in replacement for [some](https://github.com/nathants/s4/blob/master/tests/test_server.py) of the s3 cli functionality.

## install

note: tested only on ubuntu

on every server:

- install s4
   ```
   pip3 install git+https://github.com/nathants/s4
   ```

- configure s4 with the ipv4:port of every server. make sure to use the ipv4 local to the machine, as the conf file defines the hash ring, and each server recognizes itself in the conf by comparing its ipv4 as reported by ifconfig.
   ```
   touch ~/.s4.conf
   echo server.1.ipv4:port >> ~/.s4.conf
   echo server.2:ipv4:port >> ~/.s4.conf
   echo ... >> ~/.s4.conf
   ```

- start the server, possibly with sudo if binding on port 80.

`s4-server`

python entrypoints are quite slow, so it's recommended to create a bash alias like:

`s4() { python3 -c 'import s4.cli; s4.cli.main()'; }`

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
