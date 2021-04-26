package lsh

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import lsh.MinHash
class BaseConstruction(sqlContext: SQLContext, data: RDD[(String, List[String])], seed : Int) extends Construction {
  //build buckets here
  val minHash = new MinHash(seed)
  val buckets: RDD[(Int, Set[String])] = minHash.execute(data).groupBy(f => f._2)
    .map(f => (f._1, f._2.map(t => t._1).toSet))

  override def eval(queries: RDD[(String, List[String])]): RDD[(String, Set[String])] = {
    //compute near neighbors here
    // First compute MinHash on the queries and map it to (Int, String)
    // Then compute the join with the buckets and map it to a form String, Option[Set[String]]
    // Filter out those Options which are empty and map it to String, Set[String]

    val hashQueries: RDD[(Int, String)] = minHash.execute(queries).map(f => (f._2, f._1))

    // result is ordered to enforce consistency in complex constructions
    hashQueries.leftOuterJoin(buckets).map(f => (f._2._1, f._2._2))
      .filter(f => f._2.nonEmpty).map(f => (f._1, f._2.get)).sortBy(f => f._1)
  }
}
