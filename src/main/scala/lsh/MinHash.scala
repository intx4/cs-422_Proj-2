package lsh

import org.apache.spark.rdd.RDD

class MinHash(seed : Int) extends Serializable {
  def hashSeed(key : String, seed : Int) : Int = {
    var k = (key + seed.toString).hashCode
    k = k * 0xcc9e2d51
    k = k >> 15
    k = k * 0x1b873593
    k = k >> 13
    k.abs
  }
  def minhash (tags: List[String]): Int = {
    val hashtags = tags.map(t => hashSeed(t, seed))
    hashtags.min(Ordering[Int])
  }
  def execute(data: RDD[(String, List[String])]) : RDD[(String, Int)] = {
    // compute minhash signature for each data/query point
    data.map(f => (f._1, minhash(f._2)))
  }
}
