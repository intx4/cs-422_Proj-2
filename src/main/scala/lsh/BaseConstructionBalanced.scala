package lsh

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

class BaseConstructionBalanced(sqlContext: SQLContext, data: RDD[(String, List[String])], seed : Int, partitions : Int) extends Construction {
  //build buckets here
  val minHash = new MinHash(seed)
  val buckets: RDD[(Int, Set[String])] = minHash.execute(data).groupBy(f => f._2)
    .map(f => (f._1, f._2.map(t => t._1).toSet))

  def computeMinHashHistogram(queries : RDD[(String, Int)]) : Array[(Int, Int)] = {
    // compute histogram for target buckets of queries
    // return histogram sorted by hashmin key ASC
    queries.groupBy(f => f._2).map(f => (f._1, f._2.toArray.length))
      .sortBy(f => f._1, ascending = true).collect()
  }
  def computeDepth(histogram: Array[(Int,Int)]) : Int = {
    var d = 0
    histogram.foreach(f => d = d + f._2)
    (d/partitions).ceil.toInt
  }

  def computePartitions(histogram : Array[(Int, Int)]) : Array[Int] = {
    //compute the boundaries of bucket partitions
    //partitions are computed returning the last element of the bucket
    // i.e: histogram = [(id1,1),(id2,3),(id3,1)]
    // partitions = 2
    // depth = ceil(5/2)=3
    // bounds = [id2] -> meaning (id1,id2), (id3, +inf) = (id3)
    var bounds = Array[Int]()
    var buffer = 0
    val depth = computeDepth(histogram)
    val iter = histogram.iterator
    while (iter.hasNext){
      val h = iter.next()
      buffer += h._2
      if (buffer > depth) {
        bounds :+= h._1
        buffer = 0
      }
    }
    bounds
  }

  override def eval(queries: RDD[(String, List[String])]): RDD[(String, Set[String])] = {
    //compute near neighbors with load balancing here
    val hashQueries = minHash.execute(queries)
    val histogram = computeMinHashHistogram(hashQueries)
    val bounds = computePartitions(histogram)


    null
  }
}