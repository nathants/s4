## why

s3 is awesome, but can be expensive, slow, and doesn't expose data locality.

## what

an s3 cli [compatible](https://github.com/nathants/s4/blob/master/tests/test_server.py) storage cluster that's cheap, fast, and exposes data local compute. use this for ephemeral data.

## non goals

- high availability. every key lives on one and only one server.

- high durability. data lives on a single disk, and is as durable as that disk.

- security. data transfers are checked for integrity, but not encrypted. service access is unauthenticated. secure the network with wireguard if needed.

- fine granularity performance. data should be medium to coarse granularity.

## install

on every server:

- install s4
```
git clone https://github.com/nathants/s4
cd s4
pip install -r requirements.txt .
```

- configure s4 conf with the ipv4:port of every server. make sure to use the ipv4 local to the machine, as the conf file defines the hash ring, and each server recognizes itself in the conf by comparing its ipv4 as reported by ifconfig.
   ```
   rm ~/.s4.conf
   echo $server1:$port1 >> ~/.s4.conf
   echo $server2:$port2 >> ~/.s4.conf
   ...
   ```

- start the server.

`s4-server`
