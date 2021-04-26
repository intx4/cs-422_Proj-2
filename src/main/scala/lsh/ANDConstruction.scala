package lsh
import org.apache.spark.rdd.RDD

import scala.collection.mutable

object QueriOrdering extends Ordering[(String, Set[String])] {
  def compare(a:(String, Set[String]), b:(String, Set[String])): Int = a._1.compareTo(b._1)
}

class ANDConstruction(children: List[Construction]) extends Construction {
  override def eval(rdd: RDD[(String, List[String])]): RDD[(String, Set[String])] = {
    //compute AND construction results here
    // rdd is queries
    val queries = rdd
    //this queue should contain the results for each construction
    var intermediateResults = mutable.Queue[RDD[(String, Set[String])]]()

    for (child <- children){
      intermediateResults += child.eval(queries)
    }

    while (intermediateResults.length >= 2){
      val res1 = intermediateResults.dequeue().coalesce(1, shuffle = true)(QueriOrdering)
        .zipWithIndex().map(f => (f._2, f._1))
      val res2 = intermediateResults.dequeue().coalesce(1, shuffle = true)(QueriOrdering)
        .zipWithIndex().map(f => (f._2, f._1))
      intermediateResults += res1.join(res2).map(f => (f._2._1._1, f._2._1._2.intersect(f._2._2._2)))
    }
    intermediateResults.dequeue()
  }
}