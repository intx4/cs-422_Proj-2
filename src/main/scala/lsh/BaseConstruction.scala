package lsh

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import lsh.MinHash
class BaseConstruction(sqlContext: SQLContext, data: RDD[(String, List[String])], seed : Int) extends Construction {
  //build buckets here
  val minHash = new MinHash(seed)
  val buckets: RDD[(Int, Set[String])] = minHash.execute(data)
    .groupBy{case (name, id) => id}
    .mapValues(f => f.map(_._1).toSet)
    .cache()

  override def eval(queries: RDD[(String, List[String])]): RDD[(String, Set[String])] = {
    //compute near neighbors here
    // First compute MinHash on the queries and map it to (Int, String)
    // Then compute the join with the buckets and map it to a form String, Option[Set[String]]
    // Filter out those Options which are empty and map it to String, Set[String]

    val hashQueries: RDD[(Int, String)] = minHash.execute(queries).map(f => f.swap)

    hashQueries.join(buckets).map{ case (id,(qname, neighs)) => (qname, neighs)}
  }
}
