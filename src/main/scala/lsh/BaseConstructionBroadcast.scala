package lsh

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.broadcast.Broadcast


//define a wrapper object to make func serializable by spark
object MyFunctionsBC {
  def getNeighs(id: Int, buckets: Array[(Int, Set[String])]): Option[Set[String]] = {
    for (b <- buckets) {
      if (b._1 == id) {
        return Option(b._2)
      }
    }
    None
  }
}

class BaseConstructionBroadcast(sqlContext: SQLContext, data: RDD[(String, List[String])], seed : Int) extends Construction with Serializable {
  //build buckets here
  val minHash = new MinHash(seed)

  //collect buckets in driver node
  val buckets: Array[(Int, Set[String])] = minHash.execute(data).groupBy(f => f._2)
    .map(f => (f._1, f._2.map(t => t._1).toSet)).sortBy(f => f._1, ascending = true)
    .collect()

  // at this point, buckets should be effectively distributed in all workers
  val broadcastBuckets: Broadcast[Array[(Int, Set[String])]] =
    sqlContext.sparkSession.sparkContext.broadcast(buckets)

  override def eval(queries: RDD[(String, List[String])]): RDD[(String, Set[String])] = {

    val hashQueries: RDD[(Int, String)] = minHash.execute(queries).map(f => (f._2, f._1))

    //Option wrapper->extract set
    hashQueries.map(f => (f._2, MyFunctionsBC.getNeighs(f._1, broadcastBuckets.value)))
      .filter(f => f._2.nonEmpty).map(f => (f._1, f._2.get))
  }
}
