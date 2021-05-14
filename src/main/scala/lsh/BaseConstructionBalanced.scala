package lsh

import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext


class BalancedPartitioner(partitions: Int, bounds: Array[Int]) extends Partitioner with Serializable {
  override def numPartitions: Int = partitions
  override def getPartition(key: Any): Int = {
    val id = key.asInstanceOf[Int]
    if (bounds.isEmpty){
      return 0
    }
    var assigned = 0
    for (b <- bounds) {
      if (id > b) {
        assigned = assigned + 1
      }
      else {
        if (assigned > partitions - 1){
          return partitions - 1
        }
        else {
          return assigned
        }
      }
    }
    //values exceeding last bound are assigned to last partitionh
    if (assigned > partitions - 1){
      return partitions - 1
    }
    else {
      return assigned
    }
  }
}
class BaseConstructionBalanced(sqlContext: SQLContext, data: RDD[(String, List[String])], seed : Int, partitions : Int) extends Construction {
  //build buckets here
  val minHash = new MinHash(seed)
  val buckets: RDD[(Int, Set[String])] = minHash.execute(data)
    .groupBy{case (name, id) => id}
    .mapValues(f => f.map(_._1).toSet)
    .cache()

  def computeMinHashHistogram(queries: RDD[(String, Int)]): Array[(Int, Int)] = {
    // compute histogram for target buckets of queries
    // return histogram sorted by hashmin key ASC as <id, number>
    queries.groupBy(f => f._2).mapValues(f => f.toArray.length)
      .sortByKey(ascending = true).collect()
  }

  def computeDepth(histogram: Array[(Int, Int)]): Int = {
    var d = 0
    histogram.foreach(f => d = d + f._2)
    (d / partitions).ceil.toInt
  }

  def computePartitions(histogram: Array[(Int, Int)]): Array[Int] = {
    //compute the boundaries of bucket partitions
    //partitions are computed returning the last element of the partition
    // i.e: histogram = [(id1,1),(id2,3),(id3,1)]
    // partitions = 2
    // depth = ceil(5/2)=3
    // bounds = [id2, id3] -> meaning [id1;id2], (id2;id3] (but actually (id2; +inf) )
    var bounds = Array[Int]()
    var buffer = 0
    val depth = computeDepth(histogram)

    for (h <- histogram) {
      buffer += h._2
      if (buffer > depth) {
        bounds :+= h._1
        buffer = 0
      }
    }
    //see if one needs to add the last id of the histogram
    if (histogram.nonEmpty && bounds.nonEmpty) {
      if (histogram.last._1 != bounds.last) {
        bounds :+= histogram.last._1
      }
      return bounds
    }
    bounds
  }

  override def eval(queries: RDD[(String, List[String])]): RDD[(String, Set[String])] = {
    //compute near neighbors with load balancing here
    val hashQueries = minHash.execute(queries)
    val histogram = computeMinHashHistogram(hashQueries)
    val bounds = computePartitions(histogram)

    val partitioner = new BalancedPartitioner(partitions, bounds)

    // each entry of this RDD is a List of queries as <film, minhash> belonging to the same partition
    val partitionedQueries: RDD[(Int, String)] = hashQueries
      .map(f => f.swap)
      .partitionBy(partitioner)

    // each entry of this RDD is a List of <bucketId, List of films in bucket> belonging to same partition
    val partitionedBuckets: RDD[(Int, Set[String])] = buckets
      .partitionBy(partitioner)

    // each entry is a List of queries<Film, hash> followed by a bucket <hash, List[films] >
    val joinedRdd = partitionedQueries
      .join(partitionedBuckets)
      .map{ case (id, (qname, neighs)) => (qname, neighs)}

    //see partitionJoin
    joinedRdd
  }
}
