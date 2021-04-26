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
      // create an incrementing id and join on that, to force a 1:1 join between rows
      val res1 = intermediateResults.dequeue().coalesce(1, shuffle = true)(QueriOrdering)
        .zipWithIndex().map(f => (f._2, f._1))
      val res2 = intermediateResults.dequeue().coalesce(1, shuffle = true)(QueriOrdering)
        .zipWithIndex().map(f => (f._2, f._1))
      // restore the ordering for consistency
      intermediateResults += res1.join(res2).
        map(f => (f._2._1._1, f._2._1._2.union(f._2._2._2))).
        sortBy(f => f._1)
    }
    intermediateResults.dequeue()
  }
}
