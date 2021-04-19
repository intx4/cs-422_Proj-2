package lsh

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

class ExactNN(sqlContext: SQLContext, data: RDD[(String, List[String])], threshold : Double) extends Construction with Serializable {
  def jaccard(f1: List[String], f2: List[String]): Double = {
    val x = f1.toSet
    val y = f2.toSet

    if (x.isEmpty || y.isEmpty){
       0.0
    }
    else {
      x.intersect(y).size.toDouble / x.union(y).size.toDouble
    }
  }
  def makeSet(f: (String, String, Double)): (String, Set[String]) = {
    var s = Set[String]()
    s += f._2
    (f._1, s)
  }
  override def eval(rdd: RDD[(String, List[String])]): RDD[(String, Set[String])] = {
    //compute exact near neighbors here
    val prod = data.cartesian(rdd)
    // First maps the RDD [Film1, Keyw1, Film2, Keyw2] => [Film1, Film2, sim]
    // Then filter those with sim >= thresh
    // After that if makes an RDD of form [Film1, Set(Film2)]
    // Finally it does a reduceByKey Film1 computing the union between the sets

    /* Try this in case
    prod.map(f => (f._1._1, f._2._1, jaccard(f._1._2, f._2._2)))
    .filter(f => f._3 >= threshold).map(f => makeSet(f))
      .reduceByKey(_.union(_))
    */
    prod.map(f => (f._1._1, f._2._1, jaccard(f._1._2, f._2._2)))
      .filter(f => f._3 >= threshold).groupBy(f => f._1).map(f => (f._1,f._2.map(t => t._2).toSet))
  }
}