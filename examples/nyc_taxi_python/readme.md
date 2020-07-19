### summary

on a 10 node i3en.xlarge cluster, after importing the dataset to local disk:

| python query | seconds |
| --- | --- |
| [count columns](./count_columns.sh) | 79.358 |
| [count payment type by year](./count_payment_type_by_year.sh) | 91.307 |
| [sum total amount by date](./sum_total_amount_by_date.sh) | 86.736 |
| [histogram distance](./histogram_distance.sh) | 93.039 |

### setup

install [cli-aws](https://github.com/nathants/cli-aws#installation)

make sure region is us-east-1 since that is where the taxi data is
```bash
>> aws-zones | grep us-east-1
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
79.358 seconds
```

run queries
```bash
>> bash count_columns.sh
62.795 seconds seconds

>> bash count_payment_type_by_year.sh
91.307 seconds

>> bash sum_total_amount_by_date.sh
86.736 seconds

>> bash histogram_distance.sh
93.039 seconds
```
