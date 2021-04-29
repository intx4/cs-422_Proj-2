import java.io.File
import lsh._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.scalatest.{FunSuite, color}

import java.io.{BufferedWriter, FileWriter}
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.util.Random
import au.com.bytecode.opencsv.CSVWriter
import lsh.Main.{average, jaccard, sc, sqlContext}

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

    val rdd: RDD[(String, List[String])] = input

    val bc = new BaseConstruction(spark.sqlContext, rdd, 42)
    val res = bc.eval(rdd)

    assert(res.count() == 0)
  }

  //@Test
  test("BaseConstructionBroadcastEmpty") {
    val input = spark.sparkContext
      .parallelize(List[(String, List[String])]())

    val rdd: RDD[(String, List[String])] = input

    val bc = new BaseConstructionBroadcast(spark.sqlContext, rdd, 42)
    val res = bc.eval(rdd)

    assert(res.count() == 0)
  }

  //@Test
  test("BaseConstructionBalancedEmpty") {
    val input = spark.sparkContext
      .parallelize(List[(String, List[String])]())

    val rdd: RDD[(String, List[String])] = input

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

    res.collect().foreach(x => {
      assert(x._2.size == 1 && x._2.contains(x._1))
    })
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

    res.collect().foreach(x => {
      assert(x._2.size == 1 && x._2.contains(x._1))
    })
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

    res.collect().foreach(x => {
      assert(x._2.size == 1 && x._2.contains(x._1))
    })
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

    res.collect().foreach(x => {
      assert(x._2.size == 1 && x._2.contains(x._1))
    })
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

    val lsh = new BaseConstruction(spark.sqlContext, rdd_corpus, 42)

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

    val lsh = new BaseConstructionBroadcast(spark.sqlContext, rdd_corpus, 42)

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

    val lsh = new BaseConstructionBalanced(spark.sqlContext, rdd_corpus, 42, 8)

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

    val rdd_query = spark.sparkContext.parallelize(rdd_query_collect.slice(0, rdd_query_collect.size / 1000))

    val exact = new ExactNN(spark.sqlContext, rdd_corpus, 0.3)

    val lsh1 = new BaseConstruction(spark.sqlContext, rdd_corpus, 42)
    val lsh2 = new BaseConstruction(spark.sqlContext, rdd_corpus, 43)
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

    val rdd_query = spark.sparkContext.parallelize(rdd_query_collect.slice(0, rdd_query_collect.size / 1000))

    val exact = new ExactNN(spark.sqlContext, rdd_corpus, 0.3)

    val lsh1 = new BaseConstruction(spark.sqlContext, rdd_corpus, 42)
    val lsh2 = new BaseConstruction(spark.sqlContext, rdd_corpus, 43)
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

    val lsh1 = new BaseConstruction(spark.sqlContext, rdd_corpus, 42)
    val lsh2 = new BaseConstructionBroadcast(spark.sqlContext, rdd_corpus, 43)

    val t1 = System.nanoTime

    val res1 = lsh1.eval(rdd_query).count()

    val duration1 = (System.nanoTime - t1) / 1e9d

    val t2 = System.nanoTime

    val res2 = lsh2.eval(rdd_query).count()

    val duration2 = (System.nanoTime - t2) / 1e9d

    println(duration1)
    println(duration2)

    assert(res1 == res2)
    assert(duration1 > 1.5 * duration2)
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

    val rdd_query = spark.sparkContext.parallelize(rdd_query_collect.slice(0, rdd_query_collect.size / 1000))

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

    val rdd_query = spark.sparkContext.parallelize(rdd_query_collect.slice(0, rdd_query_collect.size / 1000))

    val exact = new ExactNN(spark.sqlContext, rdd_corpus, 0.3)

    val lsh = Main.construction2(spark.sqlContext, rdd_corpus)

    val ground = exact.eval(rdd_query)
    val res = lsh.eval(rdd_query)

    assert(Main.recall(ground, res) > 0.95)
  }
  //---------------------------------------------------TASK 8 Tests------------------------------------------------------

  //@Test
  test("ExactNN Runtime") {
    var corpus_files = List[String]()
    corpus_files :+= new File(getClass.getResource("/corpus-1.csv/part-00000").getFile).getPath
    corpus_files :+= new File(getClass.getResource("/corpus-10.csv/part-00000").getFile).getPath
    corpus_files :+= new File(getClass.getResource("/corpus-20.csv/part-00000").getFile).getPath

    var query_files = List[List[String]]()

    var query_file_1 = List[String]()
    query_file_1 :+= new File(getClass.getResource("/queries-1-2.csv/part-00000").getFile).getPath
    query_file_1 :+= new File(getClass.getResource("/queries-1-2-skew.csv/part-00000").getFile).getPath
    query_file_1 :+= new File(getClass.getResource("/queries-1-10.csv/part-00000").getFile).getPath
    query_file_1 :+= new File(getClass.getResource("/queries-1-10-skew.csv/part-00000").getFile).getPath
    query_files :+= query_file_1
    var query_file_10 = List[String]()
    query_file_10 :+= new File(getClass.getResource("/queries-10-2.csv/part-00000").getFile).getPath
    query_file_10 :+= new File(getClass.getResource("/queries-10-2-skew.csv/part-00000").getFile).getPath
    query_file_10 :+= new File(getClass.getResource("/queries-10-10.csv/part-00000").getFile).getPath
    query_file_10 :+= new File(getClass.getResource("/queries-10-10-skew.csv/part-00000").getFile).getPath
    query_files :+= query_file_10
    var query_file_20 = List[String]()
    query_file_20 :+= new File(getClass.getResource("/queries-20-2.csv/part-00000").getFile).getPath
    query_file_20 :+= new File(getClass.getResource("/queries-20-2-skew.csv/part-00000").getFile).getPath
    query_file_20 :+= new File(getClass.getResource("/queries-20-10.csv/part-00000").getFile).getPath
    query_file_20 :+= new File(getClass.getResource("/queries-20-10-skew.csv/part-00000").getFile).getPath
    query_files :+= query_file_20

    val outputFile = new BufferedWriter(new FileWriter("./task8_tests/exactnn_time.csv"))
    val csvWriter = new CSVWriter(outputFile)
    val csvSchema = Array("corpus", "query", "duration")
    var listOfRecords = new ListBuffer[Array[String]]()
    listOfRecords += csvSchema

    for (i <- corpus_files.indices) {
      for (query_file <- query_files(i)) {

        val corpus_file = corpus_files(i)
        val rdd_corpus = spark.sparkContext
          .textFile(corpus_file)
          .map(x => x.toString.split('|'))
          .map(x => (x(0), x.slice(1, x.size).toList))


        val rdd_query = spark.sparkContext
          .textFile(query_file)
          .map(x => x.toString.split('|'))
          .map(x => (x(0), x.slice(1, x.size).toList))

        val exact = new ExactNN(spark.sqlContext, rdd_corpus, 0.3)
        val tic = System.nanoTime()
        val ground = exact.eval(rdd_query)
        val toc = System.nanoTime()
        val duration = (toc - tic) / 1e9d

        listOfRecords += Array(corpus_file, query_file, duration.toString)
      }
    }
    csvWriter.writeAll(listOfRecords.toList)
    outputFile.close()
  }

  //@Test
  test("BaseConstruction Runtime") {
    var corpus_files = List[String]()
    corpus_files :+= new File(getClass.getResource("/corpus-1.csv/part-00000").getFile).getPath
    corpus_files :+= new File(getClass.getResource("/corpus-10.csv/part-00000").getFile).getPath
    corpus_files :+= new File(getClass.getResource("/corpus-20.csv/part-00000").getFile).getPath

    var query_files = List[List[String]]()

    var query_file_1 = List[String]()
    query_file_1 :+= new File(getClass.getResource("/queries-1-2.csv/part-00000").getFile).getPath
    query_file_1 :+= new File(getClass.getResource("/queries-1-2-skew.csv/part-00000").getFile).getPath
    query_file_1 :+= new File(getClass.getResource("/queries-1-10.csv/part-00000").getFile).getPath
    query_file_1 :+= new File(getClass.getResource("/queries-1-10-skew.csv/part-00000").getFile).getPath
    query_files :+= query_file_1
    var query_file_10 = List[String]()
    query_file_10 :+= new File(getClass.getResource("/queries-10-2.csv/part-00000").getFile).getPath
    query_file_10 :+= new File(getClass.getResource("/queries-10-2-skew.csv/part-00000").getFile).getPath
    query_file_10 :+= new File(getClass.getResource("/queries-10-10.csv/part-00000").getFile).getPath
    query_file_10 :+= new File(getClass.getResource("/queries-10-10-skew.csv/part-00000").getFile).getPath
    query_files :+= query_file_10
    var query_file_20 = List[String]()
    query_file_20 :+= new File(getClass.getResource("/queries-20-2.csv/part-00000").getFile).getPath
    query_file_20 :+= new File(getClass.getResource("/queries-20-2-skew.csv/part-00000").getFile).getPath
    query_file_20 :+= new File(getClass.getResource("/queries-20-10.csv/part-00000").getFile).getPath
    query_file_20 :+= new File(getClass.getResource("/queries-20-10-skew.csv/part-00000").getFile).getPath
    query_files :+= query_file_20

    val outputFile = new BufferedWriter(new FileWriter("./task8_tests/basecon_time.csv"))
    val csvWriter = new CSVWriter(outputFile)
    val csvSchema = Array("corpus", "query", "duration")
    var listOfRecords = new ListBuffer[Array[String]]()
    listOfRecords += csvSchema

    for (i <- corpus_files.indices) {
      for (query_file <- query_files(i)) {

        val corpus_file = corpus_files(i)
        val rdd_corpus = spark.sparkContext
          .textFile(corpus_file)
          .map(x => x.toString.split('|'))
          .map(x => (x(0), x.slice(1, x.size).toList))


        val rdd_query = spark.sparkContext
          .textFile(query_file)
          .map(x => x.toString.split('|'))
          .map(x => (x(0), x.slice(1, x.size).toList))

        val exact = new BaseConstruction(spark.sqlContext, rdd_corpus, 42)
        val tic = System.nanoTime()
        val ground = exact.eval(rdd_query)
        val toc = System.nanoTime()
        val duration = (toc - tic) / 1e9d

        listOfRecords += Array(corpus_file, query_file, duration.toString)
      }
    }
    csvWriter.writeAll(listOfRecords.toList)
    outputFile.close()
  }

  test("BalancedConstr Runtime") {
    var corpus_files = List[String]()
    corpus_files :+= new File(getClass.getResource("/corpus-1.csv/part-00000").getFile).getPath
    corpus_files :+= new File(getClass.getResource("/corpus-10.csv/part-00000").getFile).getPath
    corpus_files :+= new File(getClass.getResource("/corpus-20.csv/part-00000").getFile).getPath

    var query_files = List[List[String]]()

    var query_file_1 = List[String]()
    query_file_1 :+= new File(getClass.getResource("/queries-1-2.csv/part-00000").getFile).getPath
    query_file_1 :+= new File(getClass.getResource("/queries-1-2-skew.csv/part-00000").getFile).getPath
    query_file_1 :+= new File(getClass.getResource("/queries-1-10.csv/part-00000").getFile).getPath
    query_file_1 :+= new File(getClass.getResource("/queries-1-10-skew.csv/part-00000").getFile).getPath
    query_files :+= query_file_1
    var query_file_10 = List[String]()
    query_file_10 :+= new File(getClass.getResource("/queries-10-2.csv/part-00000").getFile).getPath
    query_file_10 :+= new File(getClass.getResource("/queries-10-2-skew.csv/part-00000").getFile).getPath
    query_file_10 :+= new File(getClass.getResource("/queries-10-10.csv/part-00000").getFile).getPath
    query_file_10 :+= new File(getClass.getResource("/queries-10-10-skew.csv/part-00000").getFile).getPath
    query_files :+= query_file_10
    var query_file_20 = List[String]()
    query_file_20 :+= new File(getClass.getResource("/queries-20-2.csv/part-00000").getFile).getPath
    query_file_20 :+= new File(getClass.getResource("/queries-20-2-skew.csv/part-00000").getFile).getPath
    query_file_20 :+= new File(getClass.getResource("/queries-20-10.csv/part-00000").getFile).getPath
    query_file_20 :+= new File(getClass.getResource("/queries-20-10-skew.csv/part-00000").getFile).getPath
    query_files :+= query_file_20

    val outputFile = new BufferedWriter(new FileWriter("./task8_tests/balancedconstr_time.csv"))
    val csvWriter = new CSVWriter(outputFile)
    val csvSchema = Array("corpus", "query", "duration")
    var listOfRecords = new ListBuffer[Array[String]]()
    listOfRecords += csvSchema

    for (i <- corpus_files.indices) {
      for (query_file <- query_files(i)) {

        val corpus_file = corpus_files(i)
        val rdd_corpus = spark.sparkContext
          .textFile(corpus_file)
          .map(x => x.toString.split('|'))
          .map(x => (x(0), x.slice(1, x.size).toList))


        val rdd_query = spark.sparkContext
          .textFile(query_file)
          .map(x => x.toString.split('|'))
          .map(x => (x(0), x.slice(1, x.size).toList))

        val exact = new BaseConstructionBalanced(spark.sqlContext, rdd_corpus, 42, partitions = 8)
        val tic = System.nanoTime()
        val ground = exact.eval(rdd_query)
        val toc = System.nanoTime()
        val duration = (toc - tic) / 1e9d

        listOfRecords += Array(corpus_file, query_file, duration.toString)
      }
    }
    csvWriter.writeAll(listOfRecords.toList)
    outputFile.close()
  }

  test("BroadcastConstr Runtime") {
    var corpus_files = List[String]()
    corpus_files :+= new File(getClass.getResource("/corpus-1.csv/part-00000").getFile).getPath
    corpus_files :+= new File(getClass.getResource("/corpus-10.csv/part-00000").getFile).getPath
    corpus_files :+= new File(getClass.getResource("/corpus-20.csv/part-00000").getFile).getPath

    var query_files = List[List[String]]()

    var query_file_1 = List[String]()
    query_file_1 :+= new File(getClass.getResource("/queries-1-2.csv/part-00000").getFile).getPath
    query_file_1 :+= new File(getClass.getResource("/queries-1-2-skew.csv/part-00000").getFile).getPath
    query_file_1 :+= new File(getClass.getResource("/queries-1-10.csv/part-00000").getFile).getPath
    query_file_1 :+= new File(getClass.getResource("/queries-1-10-skew.csv/part-00000").getFile).getPath
    query_files :+= query_file_1
    var query_file_10 = List[String]()
    query_file_10 :+= new File(getClass.getResource("/queries-10-2.csv/part-00000").getFile).getPath
    query_file_10 :+= new File(getClass.getResource("/queries-10-2-skew.csv/part-00000").getFile).getPath
    query_file_10 :+= new File(getClass.getResource("/queries-10-10.csv/part-00000").getFile).getPath
    query_file_10 :+= new File(getClass.getResource("/queries-10-10-skew.csv/part-00000").getFile).getPath
    query_files :+= query_file_10
    var query_file_20 = List[String]()
    query_file_20 :+= new File(getClass.getResource("/queries-20-2.csv/part-00000").getFile).getPath
    query_file_20 :+= new File(getClass.getResource("/queries-20-2-skew.csv/part-00000").getFile).getPath
    query_file_20 :+= new File(getClass.getResource("/queries-20-10.csv/part-00000").getFile).getPath
    query_file_20 :+= new File(getClass.getResource("/queries-20-10-skew.csv/part-00000").getFile).getPath
    query_files :+= query_file_20

    val outputFile = new BufferedWriter(new FileWriter("./task8_tests/broadcastconstr_time.csv"))
    val csvWriter = new CSVWriter(outputFile)
    val csvSchema = Array("corpus", "query", "duration")
    var listOfRecords = new ListBuffer[Array[String]]()
    listOfRecords += csvSchema

    for (i <- corpus_files.indices) {
      for (query_file <- query_files(i)) {

        val corpus_file = corpus_files(i)
        val rdd_corpus = spark.sparkContext
          .textFile(corpus_file)
          .map(x => x.toString.split('|'))
          .map(x => (x(0), x.slice(1, x.size).toList))


        val rdd_query = spark.sparkContext
          .textFile(query_file)
          .map(x => x.toString.split('|'))
          .map(x => (x(0), x.slice(1, x.size).toList))


        val exact = new BaseConstructionBroadcast(spark.sqlContext, rdd_corpus, 42)
        val tic = System.nanoTime()
        val ground = exact.eval(rdd_query)
        val toc = System.nanoTime()
        val duration = (toc - tic) / 1e9d

        listOfRecords += Array(corpus_file, query_file, duration.toString)
      }
    }
    csvWriter.writeAll(listOfRecords.toList)
    outputFile.close()
  }


  //@Test
  test("Average Distance") {

    var corpus_files = List[String]()
    corpus_files :+= "/home/intx/cs-422/bis/src/main/resources/corpus-1.csv/part-00000"
    corpus_files :+= "/home/intx/cs-422/bis/src/main/resources/corpus-10.csv/part-00000"
    corpus_files :+= "/home/intx/cs-422/bis/src/main/resources/corpus-20.csv/part-00000"

    var query_files = List[List[String]]()

    var query_file_1 = List[String]()
    query_file_1 :+= "/home/intx/cs-422/bis/src/main/resources/queries-1-2.csv/part-00000"
    query_file_1 :+= "/home/intx/cs-422/bis/src/main/resources/queries-1-2-skew.csv/part-00000"
    query_file_1 :+= "/home/intx/cs-422/bis/src/main/resources/queries-1-10.csv/part-00000"
    query_file_1 :+= "/home/intx/cs-422/bis/src/main/resources/queries-1-10-skew.csv/part-00000"
    query_files :+= query_file_1
    var query_file_10 = List[String]()
    query_file_10 :+= "/home/intx/cs-422/bis/src/main/resources/queries-10-2.csv/part-00000"
    query_file_10 :+= "/home/intx/cs-422/bis/src/main/resources/queries-10-2-skew.csv/part-00000"
    query_file_10 :+= "/home/intx/cs-422/bis/src/main/resources/queries-10-10.csv/part-00000"
    query_file_10 :+= "/home/intx/cs-422/bis/src/main/resources/queries-10-10-skew.csv/part-00000"
    query_files :+= query_file_10
    var query_file_20 = List[String]()
    query_file_20 :+= "/home/intx/cs-422/bis/src/main/resources/queries-20-2.csv/part-00000"
    query_file_20 :+= "/home/intx/cs-422/bis/src/main/resources/queries-20-2-skew.csv/part-00000"
    query_file_20 :+= "/home/intx/cs-422/bis/src/main/resources/queries-20-10.csv/part-00000"
    query_file_20 :+= "/home/intx/cs-422/bis/src/main/resources/queries-20-10-skew.csv/part-00000"
    query_files :+= query_file_20

    val outputFile = new BufferedWriter(new FileWriter("./task8_tests/average_distance.csv"))
    val csvWriter = new CSVWriter(outputFile)
    val csvSchema = Array("corpus", "query", "duration")
    var listOfRecords = new ListBuffer[Array[String]]()
    listOfRecords += csvSchema

    for (i <- corpus_files.indices) {
      for (query_file <- query_files(i)) {

        val corpus_file = corpus_files(i)
        val rdd_corpus = sc
          .textFile(corpus_file)
          .map(x => x.toString.split('|'))
          .map(x => (x(0), x.slice(1, x.size).toList))


        val rdd_query = sc
          .textFile(query_file)
          .map(x => x.toString.split('|'))
          .map(x => (x(0), x.slice(1, x.size).toList))


        val exact = new ExactNN(sqlContext, rdd_corpus, 0.3)
        val exact_res = exact.eval(rdd_query)

        val base = new BaseConstruction(sqlContext, rdd_corpus, 42)
        val base_res = base.eval(rdd_query)

        val balanced = new BaseConstructionBalanced(sqlContext, rdd_corpus, 42, partitions = 8)
        val balanced_res = balanced.eval(rdd_query)

        val broadcast = new BaseConstructionBroadcast(sqlContext, rdd_corpus, 42)
        val broadcast_res = broadcast.eval(rdd_query)

        var exact_d = average(exact_res.flatMapValues(f => f.toList)
          .sample(withReplacement = false, fraction = 0.25)
          .join(rdd_query)
          .map(f => (f._2._1, (f._1, f._2._2)))
          .join(rdd_corpus)
          .map(f => (f._2._1._1, f._1, f._2._1._2, f._2._2))
          .map(f => (f._1, f._2, jaccard(f._3, f._4)))
          .groupBy(f => f._1)
          .map(f => average(f._2.map(t => t._3).toList))
          .collect().toList)

        var base_d = average(base_res.flatMapValues(f => f.toList)
          .sample(withReplacement = false, fraction = 0.25)
          .join(rdd_query)
          .map(f => (f._2._1, (f._1, f._2._2)))
          .join(rdd_corpus)
          .map(f => (f._2._1._1, f._1, f._2._1._2, f._2._2))
          .map(f => (f._1, f._2, jaccard(f._3, f._4)))
          .groupBy(f => f._1)
          .map(f => average(f._2.map(t => t._3).toList))
          .collect().toList)

        var balanced_d = average(balanced_res.flatMapValues(f => f.toList)
          .sample(withReplacement = false, fraction = 0.25)
          .join(rdd_query)
          .map(f => (f._2._1, (f._1, f._2._2)))
          .join(rdd_corpus)
          .map(f => (f._2._1._1, f._1, f._2._1._2, f._2._2))
          .map(f => (f._1, f._2, jaccard(f._3, f._4)))
          .groupBy(f => f._1)
          .map(f => average(f._2.map(t => t._3).toList))
          .collect().toList)

        var broadcast_d = average(broadcast_res.flatMapValues(f => f.toList)
          .sample(withReplacement = false, fraction = 0.25)
          .join(rdd_query)
          .map(f => (f._2._1, (f._1, f._2._2)))
          .join(rdd_corpus)
          .map(f => (f._2._1._1, f._1, f._2._1._2, f._2._2))
          .map(f => (f._1, f._2, jaccard(f._3, f._4)))
          .groupBy(f => f._1)
          .map(f => average(f._2.map(t => t._3).toList))
          .collect().toList)

        listOfRecords += Array("ExactNN", corpus_file, query_file, exact_d.toString)
        listOfRecords += Array("Base", corpus_file, query_file, base_d.toString)
        listOfRecords += Array("Balanced", corpus_file, query_file, balanced_d.toString)
        listOfRecords += Array("Broadcast", corpus_file, query_file, broadcast_d.toString)
      }
    }
    csvWriter.writeAll(listOfRecords.toList)
    outputFile.close()
  }
}