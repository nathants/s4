### tldr;

on a 10 node i3en.xlarge cluster, after importing the dataset to local disk, for query count-rides-by-passengers:

- [hive](./count_rides_by_passengers.hql): 42 seconds

- [s4](./count_rides_by_passengers.sh): 5 seconds

### setup

- install [cli-aws](https://github.com/nathants/cli-aws#installation)

- make sure region is us-east-1 since that is where the taxi data is
  ```
  >> aws-zones | grep us-east-1
  ```

### launching a hive cluster

- launch an emr cluster with 10 nodes spot, this costs about $1.50/hour
  ```
  >> cluster_id=$(aws-emr-new --count 10 hive)
  ```

- wait for the cluster to become read
  ```
  >> aws-emr-wait-for-state $cluster_id
  ```

- pull the csv data from s3 to hdfs
  ```
  >> aws-emr-ssh $cluster_id --cmd 's3-dist-cp --src="s3://nyc-tlc/trip data/" --srcPattern=".*yellow.*" --dest=/taxi_csv/'
  ```

- convert csv to orc
  ```
  >> aws-emr-script $cluster_id schema.hql csv_to_orc.hql --interactive
  ```

- run queries
  ```
  >> aws-emr-script --interactive $cluster_id schema.hql count_rides_by_passengers.hql
  >> aws-emr-script --interactive $cluster_id schema.hql count_rides_by_date.hql
  >> aws-emr-script --interactive $cluster_id schema.hql sum_distance_by_date.hql
  >> aws-emr-script --interactive $cluster_id schema.hql sort_by_distance.hql
  ```

- delete the cluster
  ```
  >> aws-emr-rm $cluster_id
  ```

### launching a s4 cluster
