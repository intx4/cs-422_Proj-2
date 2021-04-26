import java.io.File

import lsh._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite



class MainTest extends FunSuite {
  val master = "local[*]"
  val spark = SparkSession.builder.appName("Project2").master(master).getOrCreate

  //@Test
  test("MinHash") {
    val input = spark.sparkContext
      .parallelize(List(
        "Star Wars|space|force|jedi|empire|lightsaber",
        "The Lord of the Rings|fantasy|hobbit|orcs|swords",
        "Ghost in the Shell|cyberpunk|anime|hacker"
      ))

    val rdd = input
      .map(x => x.split('|'))
      .map(x => (x(0), x.slice(1, x.size).toList))

    val minHash21 = new MinHash(21)
    val minHash22 = new MinHash(22)
    val minHash23 = new MinHash(23)

    assert(minHash21.execute(rdd).map(x => x._2).collect().toList.equals(List(99766, 4722, 53951)))
    assert(minHash22.execute(rdd).map(x => x._2).collect().toList.equals(List(67943, 31621, 27051)))
    assert(minHash23.execute(rdd).map(x => x._2).collect().toList.equals(List(10410, 14613, 28224)))
  }

  //@Test
  test("BaseConstructionEmpty") {
    val input = spark.sparkContext
      .parallelize(List[(String, List[String])]())

    val rdd : RDD[(String, List[String])] = input

    val bc = new BaseConstruction(spark.sqlContext, rdd, 42)
    val res = bc.eval(rdd)

    assert(res.count() == 0)
  }

  //@Test
  test("BaseConstructionBroadcastEmpty") {
    val input = spark.sparkContext
      .parallelize(List[(String, List[String])]())

    val rdd : RDD[(String, List[String])] = input

    val bc = new BaseConstructionBroadcast(spark.sqlContext, rdd, 42)
    val res = bc.eval(rdd)

    assert(res.count() == 0)
  }

  //@Test
  test("BaseConstructionBalancedEmpty") {
    val input = spark.sparkContext
      .parallelize(List[(String, List[String])]())

    val rdd : RDD[(String, List[String])] = input

    val bc = new BaseConstructionBalanced(spark.sqlContext, rdd, 42, 8)
    val res = bc.eval(rdd)

    assert(res.count() == 0)
  }

  //@Test
  test("BaseConstructionReflective") {
    val input = spark.sparkContext
      .parallelize(List(
        "Star Wars|space|force|jedi|empire|lightsaber",
        "The Lord of the Rings|fantasy|hobbit|orcs|swords",
        "Ghost in the Shell|cyberpunk|anime|hacker"
      ))

    val rdd = input
      .map(x => x.split('|'))
      .map(x => (x(0), x.slice(1, x.size).toList))

    val bc = new BaseConstruction(spark.sqlContext, rdd, 42)
    val res = bc.eval(rdd)

    res.collect().foreach(x => {assert(x._2.size == 1 && x._2.contains(x._1))})
  }

  //@Test
  test("BaseConstructionBroadcastReflective") {
    val input = spark.sparkContext
      .parallelize(List(
        "Star Wars|space|force|jedi|empire|lightsaber",
        "The Lord of the Rings|fantasy|hobbit|orcs|swords",
        "Ghost in the Shell|cyberpunk|anime|hacker"
      ))

    val rdd = input
      .map(x => x.split('|'))
      .map(x => (x(0), x.slice(1, x.size).toList))

    val bc = new BaseConstructionBroadcast(spark.sqlContext, rdd, 42)
    val res = bc.eval(rdd)

    res.collect().foreach(x => {assert(x._2.size == 1 && x._2.contains(x._1))})
  }

  //@Test
  test("BaseConstructionBalancedReflective") {
    val input = spark.sparkContext
      .parallelize(List(
        "Star Wars|space|force|jedi|empire|lightsaber",
        "The Lord of the Rings|fantasy|hobbit|orcs|swords",
        "Ghost in the Shell|cyberpunk|anime|hacker"
      ))

    val rdd = input
      .map(x => x.split('|'))
      .map(x => (x(0), x.slice(1, x.size).toList))

    val bc = new BaseConstructionBalanced(spark.sqlContext, rdd, 42, 8)
    val res = bc.eval(rdd)

    res.collect().foreach(x => {assert(x._2.size == 1 && x._2.contains(x._1))})
  }

  //@Test
  test("ExactNNReflective") {
    val input = spark.sparkContext
      .parallelize(List(
        "Star Wars|space|force|jedi|empire|lightsaber",
        "The Lord of the Rings|fantasy|hobbit|orcs|swords",
        "Ghost in the Shell|cyberpunk|anime|hacker"
      ))

    val rdd = input
      .map(x => x.split('|'))
      .map(x => (x(0), x.slice(1, x.size).toList))

    val bc = new ExactNN(spark.sqlContext, rdd, 0.7)
    val res = bc.eval(rdd)

    res.collect().foreach(x => {assert(x._2.size == 1 && x._2.contains(x._1))})
  }

  //@Test
  test("BaseConstructionSmall") {
    val corpus_file = new File(getClass.getResource("/corpus-1.csv/part-00000").getFile).getPath

    val rdd_corpus = spark.sparkContext
      .textFile(corpus_file)
      .map(x => x.toString.split('|'))
      .map(x => (x(0), x.slice(1, x.size).toList))

    val query_file = new File(getClass.getResource("/queries-1-2.csv/part-00000").getFile).getPath

    val rdd_query = spark.sparkContext
      .textFile(query_file)
      .map(x => x.toString.split('|'))
      .map(x => (x(0), x.slice(1, x.size).toList))
      .sample(false, 0.05)

    val exact = new ExactNN(spark.sqlContext, rdd_corpus, 0.3)

    val lsh =  new BaseConstruction(spark.sqlContext, rdd_corpus, 42)

    val ground = exact.eval(rdd_query)
    val res = lsh.eval(rdd_query)

    assert(Main.recall(ground, res) >= 0.8)
    assert(Main.precision(ground, res) >= 0.9)

    assert(res.count() == rdd_query.count())
  }

  //@Test
  test("BaseConstructionBroadcastSmall") {
    val corpus_file = new File(getClass.getResource("/corpus-1.csv/part-00000").getFile).getPath

    val rdd_corpus = spark.sparkContext
      .textFile(corpus_file)
      .map(x => x.toString.split('|'))
      .map(x => (x(0), x.slice(1, x.size).toList))

    val query_file = new File(getClass.getResource("/queries-1-2.csv/part-00000").getFile).getPath

    val rdd_query = spark.sparkContext
      .textFile(query_file)
      .map(x => x.toString.split('|'))
      .map(x => (x(0), x.slice(1, x.size).toList))
      .sample(false, 0.05)

    val exact = new ExactNN(spark.sqlContext, rdd_corpus, 0.3)

    val lsh =  new BaseConstructionBroadcast(spark.sqlContext, rdd_corpus, 42)

    val ground = exact.eval(rdd_query)
    val res = lsh.eval(rdd_query)

    assert(Main.recall(ground, res) >= 0.8)
    assert(Main.precision(ground, res) >= 0.9)

    assert(res.count() == rdd_query.count())
  }

  //@Test
  test("BaseConstructionBalancedSmall") {
    val corpus_file = new File(getClass.getResource("/corpus-1.csv/part-00000").getFile).getPath

    val rdd_corpus = spark.sparkContext
      .textFile(corpus_file)
      .map(x => x.toString.split('|'))
      .map(x => (x(0), x.slice(1, x.size).toList))

    val query_file = new File(getClass.getResource("/queries-1-2.csv/part-00000").getFile).getPath

    val rdd_query = spark.sparkContext
      .textFile(query_file)
      .map(x => x.toString.split('|'))
      .map(x => (x(0), x.slice(1, x.size).toList))
      .sample(false, 0.05)

    val exact = new ExactNN(spark.sqlContext, rdd_corpus, 0.3)

    val lsh =  new BaseConstructionBalanced(spark.sqlContext, rdd_corpus, 42, 8)

    val ground = exact.eval(rdd_query)
    val res = lsh.eval(rdd_query)

    assert(Main.recall(ground, res) >= 0.8)
    assert(Main.precision(ground, res) >= 0.9)

    assert(res.count() == rdd_query.count())
  }

  //@Test
  test("ANDConstruction") {
    val corpus_file = new File(getClass.getResource("/corpus-10.csv/part-00000").getFile).getPath

    val rdd_corpus = spark.sparkContext
      .textFile(corpus_file)
      .map(x => x.toString.split('|'))
      .map(x => (x(0), x.slice(1, x.size).toList))

    val query_file = new File(getClass.getResource("/queries-10-2.csv/part-00000").getFile).getPath

    val rdd_query_collect = spark.sparkContext
      .textFile(query_file)
      .map(x => x.toString.split('|'))
      .map(x => (x(0), x.slice(1, x.size).toList))
      .collect()

    val rdd_query = spark.sparkContext.parallelize(rdd_query_collect.slice(0, rdd_query_collect.size/1000))

    val exact = new ExactNN(spark.sqlContext, rdd_corpus, 0.3)

    val lsh1 =  new BaseConstruction(spark.sqlContext, rdd_corpus, 42)
    val lsh2 =  new BaseConstruction(spark.sqlContext, rdd_corpus, 43)
    val lsh = new ANDConstruction(List(lsh1, lsh2))

    val ground = exact.eval(rdd_query)
    val res1 = lsh1.eval(rdd_query)
    val res2 = lsh2.eval(rdd_query)
    val res = lsh.eval(rdd_query)

    assert(Main.precision(ground, res) > Main.precision(ground, res1))

    assert(res.count() == rdd_query.count())
    assert(res.flatMap(x => x._2).count() < res1.flatMap(x => x._2).count())
  }

  //@Test
  test("ORConstruction") {
    val corpus_file = new File(getClass.getResource("/corpus-10.csv/part-00000").getFile).getPath

    val rdd_corpus = spark.sparkContext
      .textFile(corpus_file)
      .map(x => x.toString.split('|'))
      .map(x => (x(0), x.slice(1, x.size).toList))

    val query_file = new File(getClass.getResource("/queries-10-2.csv/part-00000").getFile).getPath

    val rdd_query_collect = spark.sparkContext
      .textFile(query_file)
      .map(x => x.toString.split('|'))
      .map(x => (x(0), x.slice(1, x.size).toList))
      .collect()

    val rdd_query = spark.sparkContext.parallelize(rdd_query_collect.slice(0, rdd_query_collect.size/1000))

    val exact = new ExactNN(spark.sqlContext, rdd_corpus, 0.3)

    val lsh1 =  new BaseConstruction(spark.sqlContext, rdd_corpus, 42)
    val lsh2 =  new BaseConstruction(spark.sqlContext, rdd_corpus, 43)
    val lsh = new ORConstruction(List(lsh1, lsh2))

    val ground = exact.eval(rdd_query)
    val res1 = lsh1.eval(rdd_query)
    val res2 = lsh2.eval(rdd_query)
    val res = lsh.eval(rdd_query)

    assert(Main.recall(ground, res) > Main.recall(ground, res1))

    assert(res.count() == rdd_query.count())
    assert(res.flatMap(x => x._2).count() > res1.flatMap(x => x._2).count())
  }

  test("Simple vs BCast") {
    val corpus_file = new File(getClass.getResource("/corpus-1.csv/part-00000").getFile).getPath

    val rdd_corpus = spark.sparkContext
      .textFile(corpus_file)
      .map(x => x.toString.split('|'))
      .map(x => (x(0), x.slice(1, x.size).toList))

    val query_file = new File(getClass.getResource("/queries-1-10.csv/part-00000").getFile).getPath

    val rdd_query = spark.sparkContext
      .textFile(query_file)
      .map(x => x.toString.split('|'))
      .map(x => (x(0), x.slice(1, x.size).toList))

    val lsh1 =  new BaseConstruction(spark.sqlContext, rdd_corpus, 42)
    val lsh2 =  new BaseConstructionBroadcast(spark.sqlContext, rdd_corpus, 43)

    val t1 = System.nanoTime

    val res1 = lsh1.eval(rdd_query).count()

    val duration1 = (System.nanoTime - t1) / 1e9d

    val t2 = System.nanoTime

    val res2 = lsh2.eval(rdd_query).count()

    val duration2 = (System.nanoTime - t2) / 1e9d

    println(duration1)
    println(duration2)

    assert(res1 == res2)
    assert(duration1 > 1.5*duration2)
  }

  //@Test
  test("ConstructionImplementation1") {
    val corpus_file = new File(getClass.getResource("/corpus-10.csv/part-00000").getFile).getPath

    val rdd_corpus = spark.sparkContext
      .textFile(corpus_file)
      .map(x => x.toString.split('|'))
      .map(x => (x(0), x.slice(1, x.size).toList))

    val query_file = new File(getClass.getResource("/queries-10-2.csv/part-00000").getFile).getPath

    val rdd_query_collect = spark.sparkContext
      .textFile(query_file)
      .map(x => x.toString.split('|'))
      .map(x => (x(0), x.slice(1, x.size).toList))
      .collect()

    val rdd_query = spark.sparkContext.parallelize(rdd_query_collect.slice(0, rdd_query_collect.size/1000))

    val exact = new ExactNN(spark.sqlContext, rdd_corpus, 0.3)

    val lsh = Main.construction1(spark.sqlContext, rdd_corpus)

    val ground = exact.eval(rdd_query)
    val res = lsh.eval(rdd_query)

    assert(Main.precision(ground, res) > 0.94)
  }

  //@Test
  test("ConstructionImplementation2") {
    val corpus_file = new File(getClass.getResource("/corpus-10.csv/part-00000").getFile).getPath

    val rdd_corpus = spark.sparkContext
      .textFile(corpus_file)
      .map(x => x.toString.split('|'))
      .map(x => (x(0), x.slice(1, x.size).toList))

    val query_file = new File(getClass.getResource("/queries-10-2.csv/part-00000").getFile).getPath

    val rdd_query_collect = spark.sparkContext
      .textFile(query_file)
      .map(x => x.toString.split('|'))
      .map(x => (x(0), x.slice(1, x.size).toList))
      .collect()

    val rdd_query = spark.sparkContext.parallelize(rdd_query_collect.slice(0, rdd_query_collect.size/1000))

    val exact = new ExactNN(spark.sqlContext, rdd_corpus, 0.3)

    val lsh = Main.construction2(spark.sqlContext, rdd_corpus)

    val ground = exact.eval(rdd_query)
    val res = lsh.eval(rdd_query)

    assert(Main.recall(ground, res) > 0.95)
  }
}
