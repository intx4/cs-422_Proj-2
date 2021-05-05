package lsh

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.broadcast.Broadcast

class BaseConstructionBroadcast(sqlContext: SQLContext, data: RDD[(String, List[String])], seed : Int) extends Construction with Serializable {
  //build buckets here
  val minHash = new MinHash(seed)

  //collect buckets in driver node
  //previously array, Map much better: you do always one accesss
  val buckets: scala.collection.Map[Int, Set[String]] = minHash.execute(data).groupBy{case (name, id) => id}
    .mapValues(f => f.map(_._1).toSet)
    .collectAsMap()

  // at this point, buckets should be effectively distributed in all workers
  val broadcastBuckets: Broadcast[scala.collection.Map[Int, Set[String]]] =
    sqlContext.sparkSession.sparkContext.broadcast(buckets)

  override def eval(queries: RDD[(String, List[String])]): RDD[(String, Set[String])] = {

    val hashQueries: RDD[(Int, String)] = minHash.execute(queries).map(f => f.swap)

    hashQueries.map{case (id, name) => (name, broadcastBuckets.value.get(id).get)}
  }
}
