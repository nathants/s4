## why

s3 is awesome, but can be expensive, slow, and doesn't expose data locality.

## what

an s3 cli [compatible](https://github.com/nathants/s4/blob/master/tests/test_server.py) storage cluster that's cheap, fast, and exposes data local compute. use this for ephemeral data.

## non goals

- high availability. every key lives on one and only one server.

- high durability. data lives on a single disk, and is as durable as that disk.

- security. data transfers are checked for integrity, but not encrypted. service access is unauthenticated. secure the network with wireguard if needed.

- fine granularity performance. data should be medium to coarse granularity.

- safety for all inputs. service access should be considered to be at the level of root ssh. any user input should be escaped for shell.

## install

on every server:

- install
  ```
  curl -s https://raw.githubusercontent.com/nathants/s4/master/scripts/install_arch.sh | bash
  ```

- configure
  ```
  echo $server1:$port1 >  ~/.s4.conf
  echo $server2:$port2 >> ~/.s4.conf
  ...
  ```

- start
  ```
  PYPY_GC_MAX=1GB s4-server
  ```
