### summary

on a 10 node i3en.xlarge cluster, after importing the dataset to local disk:

| s4 query | seconds |
| --- | --- |
| [count rides by passengers](./count_rides_by_passengers.sh) | 4.152 |
| [count rides by date](./count_rides_by_date.sh) | 5.958 |
| [sum distance by date](./sum_distance_by_date.sh) | 11.391 |
| [sort by distance](./sort_by_distance.sh) | 34.562 |

| hive query | seconds |
| --- | --- |
| [count rides by passengers](./count_rides_by_passengers.hql) | 39.808 |
| [count rides by date](./count_rides_by_date.hql) | 53.473 |
| [sum distance by date](./sum_distance_by_date.hql) | 54.825 |
| [sort by distance](./sort_by_distance.hql) | 56.859 |

### setup

install [cli-aws](https://github.com/nathants/cli-aws#installation)

make sure region is us-east-1 since that is where the taxi data is
```
>> aws-zones | grep us-east-1
```

### launching a hive cluster

launch an emr cluster with 10 nodes spot, this costs about $1.50/hour
```
>> cluster_id=$(aws-emr-new --count 10 hive)
```

wait for the cluster to become ready
```
>> time aws-emr-wait-for-state $cluster_id
396.704 seconds
```

pull the csv data from s3 to hdfs
```
>> time aws-emr-ssh $cluster_id --cmd 's3-dist-cp --src="s3://nyc-tlc/trip data/" --srcPattern=".*yellow.*" --dest=/taxi_csv/'
292.210 seconds
```

convert csv to orc
```
>> time aws-emr-script $cluster_id schema.hql csv_to_orc.hql --interactive
492.626 seconds
```

run queries
```
>> aws-emr-script --interactive $cluster_id schema.hql count_rides_by_passengers.hql
39.808 seconds

>> aws-emr-script --interactive $cluster_id schema.hql count_rides_by_date.hql
53.473 seconds

>> aws-emr-script --interactive $cluster_id schema.hql sum_distance_by_date.hql
54.825 seconds

>> aws-emr-script --interactive $cluster_id schema.hql sort_by_distance.hql
56.859 seconds
```

delete the cluster
```
>> aws-emr-rm $cluster_id
```

### launching an s4 cluster

launch an s4 cluster with 10 nodes spot, this costs about $1.50/hour
```
>> time num=10 bash scripts/new_cluster.sh s4-cluster
223.798 seconds
```

tunnel cluster internal traffic through a cluster node via ssh
```
>> bash scripts/connect_to_cluster.sh s4-cluster
```

pull csv data from s3 and convert to bzv
```
>> time bash schema.sh
139.286 seconds
```

run queries
```
>> bash count_rides_by_passengers.sh
4.152 seconds

>> bash count_rides_by_date.sh
5.958 seconds

>> bash sum_distance_by_date.sh
11.391 seconds

>> bash sort_by_distance.sh
34.562 seconds
```
