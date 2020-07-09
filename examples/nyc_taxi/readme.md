### tldr;

on a 10 node i3en.xlarge cluster, after importing the dataset to local disk:

- [hive](./count_rides_by_passengers.hql): 35 seconds

- [s4](./count_rides_by_passengers.sh): 5 seconds

- result:
  ```
  1 1123201451
  2 237297233
  5 102473332
  3 69809608
  6 38247631
  4 33778065
  0 6797121
  7 1974
  8 1567
  ```
