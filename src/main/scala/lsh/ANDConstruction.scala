package lsh
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.util.Random

object QueriOrdering extends Ordering[(String, Set[String])] {
  def compare(a:(String, Set[String]), b:(String, Set[String])): Int = a._1.compareTo(b._1)
}

class ANDConstruction(children: List[Construction]) extends Construction {

  override def eval(rdd: RDD[(String, List[String])]): RDD[(String, Set[String])] = {
    //compute AND construction results here
    // embed a unique key for handling duplicates in the queries: query = Film+key, keywords
    val rand = new Random(42)
    val queries = rdd.map(f => (f._1+"__"+rand.nextString(8), f._2))

    //Steps:
      // 1 - collect all the results from children
      // 2 - form a single RDD with union
      // 3 - reduce by key (film to query + unique id) performing set intersection
      // 4 - remove key
    val intermediateResults = children.map(f => f.eval(queries))
      .reduce(_.union(_)).reduceByKey(_.intersect(_))
      .map(f => (f._1.split("__").head, f._2))
    intermediateResults
  }
}