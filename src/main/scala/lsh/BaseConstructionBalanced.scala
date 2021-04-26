package lsh

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.broadcast.Broadcast


object MyFunctions {
  def assignPartition(bounds: Array[Int], id: Int): Int = {
    //ex: bounds = [2, 4, 5] meaning partitions are [0;2], (2;4], (4;5]
    // id = 3
    if (bounds.isEmpty){
      return 0
    }
    var assigned = 0
    for (b <- bounds) {
      if (id > b) {
        assigned = assigned + 1
      }
      else {
        return assigned
      }
    }
    assigned
  }

  def partitionJoin(queries: List[(String, Int)], buckets: List[(Int, List[String])]): List[(String, Set[String])] = {
    //for each query in partition, extract the bucket from the partition and return the films in there
    var joinRes = List[(String, Set[String])]()
    for (query <- queries) {
      val neighs = buckets.filter(p => p._1 == query._2).flatMap(f => f._2).toSet
      joinRes = joinRes :+ (query._1, neighs)
    }
    joinRes
  }
}

class BaseConstructionBalanced(sqlContext: SQLContext, data: RDD[(String, List[String])], seed : Int, partitions : Int) extends Construction {
  //build buckets here
  val minHash = new MinHash(seed)
  val buckets: RDD[(Int, Set[String])] = minHash.execute(data).groupBy(f => f._2)
    .map(f => (f._1, f._2.map(t => t._1).toSet)).sortBy(f => f._1, ascending = true)

  def computeMinHashHistogram(queries: RDD[(String, Int)]): Array[(Int, Int)] = {
    // compute histogram for target buckets of queries
    // return histogram sorted by hashmin key ASC as <id, number>
    queries.groupBy(f => f._2).map(f => (f._1, f._2.toArray.length))
      .sortBy(f => f._1, ascending = true).collect()
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
    // bounds = [id2, id3] -> meaning [id1;id2], (id2;id3] = (id3)
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
    //note that both buckets and queries are sorted by minHash ASC
    val hashQueries = minHash.execute(queries)
    val histogram = computeMinHashHistogram(hashQueries)
    val bounds = computePartitions(histogram)

    // each entry of this RDD is a List of queries belonging to the same partition
    val partitionedQueries: RDD[(Int, List[(String, Int)])] = hashQueries.sortBy(f => f._2)
      .groupBy(f => MyFunctions.assignPartition(bounds, f._2))
      .map(f => (f._1, f._2.toList))
    // each entry of this RDD is a List of <bucketId, List of films in bucket> belonging to same partition
    val partitionedBuckets: RDD[(Int, List[(Int, List[String])])] = buckets.sortBy(f => f._1)
      .groupBy(f => MyFunctions.assignPartition(bounds, f._1))
      .map(f => (f._1, f._2.map(t => (t._1,t._2.toList)).toList))

    // each entry is a List of queries<Film, hash> followed by a bucket <hash, List[films] >
    val joinedRdd = partitionedQueries.join(partitionedBuckets).map(f => (f._2._1, f._2._2))

    //see partionJoin
    val result = joinedRdd.flatMap(f => MyFunctions.partitionJoin(f._1, f._2))

    result
  }
}

//TO DO: fix a non serializable bug here