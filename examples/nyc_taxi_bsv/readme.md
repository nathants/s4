### summary

on a 10 node i3en.xlarge cluster, after importing the dataset to local disk:

| s4 query | seconds |
| --- | --- |
| [count rides by passengers](./count_rides_by_passengers.sh) | 4.152 |
| [count rides by date](./count_rides_by_date.sh) | 5.958 |
| [sum distance by date](./sum_distance_by_date.sh) | 11.391 |
| [top n by distance](./top_n_by_distance.sh) | 4.159 |
| [sort by distance](./sort_by_distance.sh) | 162.052 |

| presto query | seconds |
| --- | --- |
| [count rides by passengers](./count_rides_by_passengers.pql) | 7.834 |
| [count rides by date](./count_rides_by_date.pql) | 14.138 |
| [sum distance by date](./sum_distance_by_date.pql) | 15.497 |
| [top n by distance](./top_n_by_distance.pql) | 8.965 |
| [sort by distance](./sort_by_distance.pql) | 700.628 |

### setup

install [cli-aws](https://github.com/nathants/cli-aws#installation)

make sure region is us-east-1 since that is where the taxi data is
```bash
>> aws-zones | grep us-east-1
```

### launching a presto cluster

launch an emr cluster with 10 nodes spot, this costs about $1.50/hour
```bash
>> cluster_id=$(aws-emr-new --count 10 test-cluster)
```

wait for the cluster to become ready
```bash
>> time aws-emr-wait-for-state $cluster_id
396.704 seconds
```

pull the csv data from s3 to hdfs
```bash
>> time aws-emr-ssh $cluster_id --cmd 's3-dist-cp --src="s3://nyc-tlc/trip data/" --srcPattern=".*yellow.*" --dest=/taxi_csv/'
292.210 seconds
```

create the tables
```bash
>> time aws-emr-hive -i $cluster_id schema.hql
```


convert csv to orc
```bash
>> time aws-emr-presto -i $cluster_id csv_to_orc.pql
309.197 seconds
```

run queries
```bash
>> aws-emr-presto -i $cluster_id count_rides_by_passengers.pql
7.834 seconds

>> aws-emr-presto -i $cluster_id count_rides_by_date.pql
14.138 seconds

>> aws-emr-presto -i $cluster_id sum_distance_by_date.pql
15.497 seconds

>> aws-emr-presto -i $cluster_id top_n_by_distance.pql
8.965 seconds

>> aws-emr-hive   -i $cluster_id sort_by_distance.hql
>> aws-emr-presto -i $cluster_id sort_by_distance.pql
700.628 seconds
```

delete the cluster
```bash
>> aws-emr-rm $cluster_id
```

### launching an s4 cluster

launch an s4 cluster with 10 nodes spot, this costs about $1.50/hour
```bash
>> time num=10 bash scripts/new_cluster.sh s4-cluster
223.798 seconds
```

tunnel cluster internal traffic through a cluster node via ssh
```bash
>> bash scripts/connect_to_cluster.sh s4-cluster
```

pull csv data from s3 and convert to bzv
```bash
>> time bash schema.sh
134.444 seconds
```

run queries
```bash
>> bash count_rides_by_passengers.sh
3.136 seconds

>> bash count_rides_by_date.sh
4.743 seconds

>> bash sum_distance_by_date.sh
10.419 seconds

>> bash top_n_by_distance.sh
1.804 seconds

>> bash sort_by_distance.sh
149.200 seconds
```
