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
  override def eval(rdd: RDD[(String, List[String])]): RDD[(String, Set[String])] = {
    //compute exact near neighbors here
    val prod = rdd.cartesian(data)
    // First maps the RDD [Film1, Keyw1, Film2, Keyw2] => [Film1, Film2, sim]
    // Then filter those with sim >= thresh
    // After that it makes an RDD of form [Film1, Set(Film2)]
    prod.map{case ((qname,qkeyw), (nname, nkeyw)) => (qname, (nname, jaccard(qkeyw, nkeyw)))}
      .filter{case (qname, (nname, jac)) => jac > threshold}.groupByKey().mapValues(f => f.map(_._1).toSet)
  }
}