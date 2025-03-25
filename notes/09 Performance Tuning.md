# Performance Tuning

- [x] API Selection
- [x] Avoid InferSchema
- [x] Efficient Data Distribution
- [x] Shuffle Memory Partitioning
- [x] Partitioning
- [x] Bucketing
- [x] FileFormat

## API Selection

```
 val records = List(
(1,"Germany"),
(2,"Mexico"),
(3,"Mexico"),
(4,"UK"),
(5,"Sweden"),
(6,"Germany"),
(7,"France"),
(8,"UK"),
(9,"France"),
(10,"Argentina"))

val rdd = sc.parallelize(records)
val filteredRDD = rdd.filter(record=>record._2=="Mexico").filter(record=>record._1==2)

val df = rdd.toDF("id","country")
val filteredDF = df.filter(col("country")==="Mexico").filter(col("id")===2)
filteredDF.explain(true)
```

## Avoid using inferSchema

```
spark.read.csv(path="", header=True,inferSchema=True)
```

## Efficient Data Distribution

EDD plays a acrucial role in optimizing spark performance.

```
rdd = sc.paralleliz(list(range(1,101)))

```

1. repartition
2. coalescing

### Repartition

* Used for increasing or decreasing the number of partitions
* performs a full shuffle of the data across the partitions
* Ideal for achieving better parallelism  

```
partition 00 : 1,2,3
partition 01 : 4,5,6,7,8,9
partition 02 : 11,12,14,15,13,12,14,15,16,17,

partition 00 : 1,11,5,6,13
partition 01 :
partition 02 :
partition 04 :
```

## Partitioning

> Partitioning is the process of dividing a large dataset into smaller, more manageable subsets or partitions based on specfic columns
>
> Partitioning is a way to split data into separate folder based on one or more columns

```
id,year
1,2010
2,2010
3,2011
4,2012
5,2013
6,2011

```

year=2010

1

2

year=2011

3

6

1. **Data Segregation** : Each partition is stored in  separate folders
2. **Reduce Data Scanning**
3. Improved Query Performance
4. Efficient Resource Utilization



```
customer_id
1
2
3
4
5
6
7
8
9
10

hashcode(customer_id)%no_of_buckets
hash(1)=21%2=1
hash(2)=18%2=0

Bucket 00
2


Bucket 0
1
```

 
