package lsh

import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.util.Random

class ORConstruction(children: List[Construction]) extends Construction {
  override def eval(rdd: RDD[(String, List[String])]): RDD[(String, Set[String])] = {
    // embed a unique key for handling duplicates in the queries
    val rand = new Random(42)
    val queries = rdd.map(f => (f._1+"__"+rand.nextString(8), f._2))


    //Steps:
    // 1 - collect all the results from children
    // 2 - form a single RDD with union
    // 3 - reduce by key (film to query + unique id) performing set union
    // 4 - remove key
    val intermediateResults = children.map(f => f.eval(queries))
      .reduce(_.union(_)).reduceByKey(_.union(_))
      .map(f => (f._1.split("__").head, f._2))
    intermediateResults
  }
}
