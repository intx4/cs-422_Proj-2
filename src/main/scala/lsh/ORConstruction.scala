package lsh

import org.apache.spark.rdd.RDD
import scala.collection.mutable

class ORConstruction(children: List[Construction]) extends Construction {
  override def eval(rdd: RDD[(String, List[String])]): RDD[(String, Set[String])] = {
    val queries = rdd
    //this queue should contain the results for each construction
    var intermediateResults = mutable.Queue[RDD[(String, Set[String])]]()

    for (child <- children){
      intermediateResults += child.eval(queries)
    }

    while (intermediateResults.length >= 2){
      val res1 = intermediateResults.dequeue()
      val res2 = intermediateResults.dequeue()
      intermediateResults += res1.join(res2).map(f =>(f._1, f._2._1.union(f._2._2)))
    }
    intermediateResults.dequeue()
  }
}
