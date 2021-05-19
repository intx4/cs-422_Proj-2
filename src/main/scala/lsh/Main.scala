package lsh

import au.com.bytecode.opencsv.CSVWriter
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import java.io.{BufferedWriter, File, FileWriter}
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import au.com.bytecode.opencsv.CSVWriter
import scala.collection.JavaConverters._

object Main {
  /*
  val conf = new SparkConf().setAppName("app").setMaster("local[*]")
  //conf.set("spark.executor.memory", "2g")
  //conf.set("spark.driver.memory", "2g")
  val sc = SparkContext.getOrCreate(conf)
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  sc.setLogLevel("WARN")
  */
  val conf = new SparkConf()
    .setAppName("app")
    .set("spark.executor.memory", "16G")
    .set("spark.shuffle.file.buffer", "16M")
  val sc = SparkContext.getOrCreate(conf)
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)

  sc.setLogLevel("OFF")

  def generate(sc : SparkContext, input_file : String, output_file : String, fraction : Double) : Unit = {
    val rdd_corpus = sc
      .textFile(input_file)
      .sample(false, fraction)

    rdd_corpus.coalesce(1).saveAsTextFile(output_file)
  }

  def recall(ground_truth : RDD[(String, Set[String])], lsh_truth : RDD[(String, Set[String])]) : Double = {
    val recall_vec = ground_truth
      .join(lsh_truth)
      .map(x => (x._1, x._2._1.intersect(x._2._2).size, x._2._1.size))
      .map(x => (x._2.toDouble/x._3.toDouble, 1))
      .reduce((x,y) => (x._1+y._1, x._2+y._2))

    val avg_recall = recall_vec._1/recall_vec._2

    avg_recall
  }

  def precision(ground_truth : RDD[(String, Set[String])], lsh_truth : RDD[(String, Set[String])]) : Double = {
    val precision_vec = ground_truth
      .join(lsh_truth)
      .map(x => (x._1, x._2._1.intersect(x._2._2).size, x._2._2.size))
      .map(x => (x._2.toDouble/x._3.toDouble, 1))
      .reduce((x,y) => (x._1+y._1, x._2+y._2))

    val avg_precision = precision_vec._1/precision_vec._2

    avg_precision
  }

  def construction1(sqlContext: SQLContext, rdd_corpus : RDD[(String, List[String])]) : Construction = {
    //implement construction1 composition here
    var baseConstrs = List[BaseConstruction]()
    var seed = 1234
    for (i <- 0 to 6) {
      baseConstrs :+= new BaseConstruction(sqlContext, rdd_corpus, seed)
      seed += 1
    }
    val lsh = new ANDConstruction(baseConstrs)
    lsh
  }

  def construction2(sqlContext: SQLContext, rdd_corpus : RDD[(String, List[String])]) : Construction = {
    //implement construction2 composition here
    var baseConstrs = List[BaseConstruction]()
    var seed = 1234
    for (i <- 0 to 6) {
      baseConstrs :+= new BaseConstruction(sqlContext, rdd_corpus, seed)
      seed += 1
    }
    val lsh = new ORConstruction(baseConstrs)
    lsh
  }

  //----------------------------------------------------TASK 8 TESTS-------------------------------------------------
  // Feel free to have a look. Some tests were changed to repair specific failures
  // in run due to Out of memory errors, so they might not reflect the original tests
  // from wich measurements were taken for the report. Still, I wanted to keep them,
  // especially the average distance ones, since I had to optimize them quite a bit to solve
  // the aformentioned problems

  var corpus_files = List[String]()
  corpus_files :+= "/home/intx/cs-422/bis/src/main/resources/corpus-1.csv"
  corpus_files :+= "/home/intx/cs-422/bis/src/main/resources/corpus-10.csv"
  corpus_files :+= "/home/intx/cs-422/bis/src/main/resources/corpus-20.csv"

  var query_files = List[List[String]]()

  var query_file_1 = List[String]()
  query_file_1 :+= "/home/intx/cs-422/bis/src/main/resources/queries-1-2.csv"
  query_file_1 :+= "/home/intx/cs-422/bis/src/main/resources/queries-1-2-skew.csv"
  query_file_1 :+= "/home/intx/cs-422/bis/src/main/resources/queries-1-10.csv"
  query_file_1 :+= "/home/intx/cs-422/bis/src/main/resources/queries-1-10-skew.csv"
  query_files :+= query_file_1
  var query_file_10 = List[String]()
  query_file_10 :+= "/home/intx/cs-422/bis/src/main/resources/queries-10-2.csv"
  query_file_10 :+= "/home/intx/cs-422/bis/src/main/resources/queries-10-2-skew.csv"
  query_file_10 :+= "/home/intx/cs-422/bis/src/main/resources/queries-10-10.csv"
  query_file_10 :+= "/home/intx/cs-422/bis/src/main/resources/queries-10-10-skew.csv"
  query_files :+= query_file_10
  var query_file_20 = List[String]()
  query_file_20 :+= "/home/intx/cs-422/bis/src/main/resources/queries-20-2.csv"
  query_file_20 :+= "/home/intx/cs-422/bis/src/main/resources/queries-20-2-skew.csv"
  query_file_20 :+= "/home/intx/cs-422/bis/src/main/resources/queries-20-10.csv"
  query_file_20 :+= "/home/intx/cs-422/bis/src/main/resources/queries-20-10-skew.csv"
  query_files :+= query_file_20

  var corpus_files_r = List[String]()
  corpus_files_r :+= "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/corpus-1.csv"
  corpus_files_r :+= "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/corpus-10.csv"
  corpus_files_r :+= "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/corpus-20.csv"

  var query_files_r = List[List[String]]()

  var query_file_1_r = List[String]()
  query_file_1_r :+= "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/queries-1-2.csv"
  query_file_1_r :+= "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/queries-1-2-skew.csv"
  query_file_1_r :+= "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/queries-1-10.csv"
  query_file_1_r :+= "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/queries-1-10-skew.csv"
  query_files_r :+= query_file_1_r
  var query_file_10_r = List[String]()
  query_file_10_r :+= "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/queries-10-2.csv"
  query_file_10_r :+= "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/queries-10-2-skew.csv"
  query_file_10_r :+= "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/queries-10-10.csv"
  query_file_10_r :+= "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/queries-10-10-skew.csv"
  query_files_r :+= query_file_10_r
  var query_file_20_r = List[String]()
  query_file_20_r :+= "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/queries-20-2.csv"
  query_file_20_r :+= "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/queries-20-2-skew.csv"
  query_file_20_r :+= "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/queries-20-10.csv"
  query_file_20_r :+= "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/queries-20-10-skew.csv"
  query_files_r :+= query_file_20_r

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

  def compute_average(rdd: RDD[(String,Set[String])], rdd_query: RDD[(String, List[String])], rdd_corpus: RDD[(String, List[String])]) : Double = {
      rdd.flatMapValues(f => f.toList)
      .join(rdd_query)
      .map { case (qname, (neigh, keyw)) => (neigh, keyw) }
      .join(rdd_corpus)
      .map{ case (neigh, (keywq, keywd)) => (keywq, keywd) }
      .map(f =>1.0 - jaccard(f._1, f._2)).mean()
  }
  //@Tes
  def test_accuracy(): Unit = {
    val outputFile = new BufferedWriter(new FileWriter("./task8_tests/accuracy.csv"))
    val csvWriter = new CSVWriter(outputFile)
    val csvSchema = Array("corpus", "query","construction", "precision", "recall")
    var listOfRecords = new ListBuffer[Array[String]]()
    listOfRecords += csvSchema

    for (i <- corpus_files.indices) {
      val corpus_file = corpus_files(i)

      val rdd_corpus = sc
        .textFile(corpus_file)
        .map(x => x.toString.split('|'))
        .map(x => (x(0), x.slice(1, x.size).toList)).cache()

      val exact = new ExactNN(sqlContext, rdd_corpus, 0.3)
      val base = new BaseConstruction(sqlContext, rdd_corpus, seed = 24)
      val and = construction1(sqlContext, rdd_corpus)
      val or = construction2(sqlContext, rdd_corpus)

      for (j <- 0 until(2)) {

        val query_file = query_files(i)(j)

        val rdd_query = sc
          .textFile(query_file)
          .map(x => x.toString.split('|'))
          .map(x => (x(0), x.slice(1, x.size).toList)).cache()


        val ground = exact.eval(rdd_query)


        val base_res = base.eval(rdd_query)
        val base_acc = precision(ground, base_res).toString
        val base_rec = recall(ground, base_res).toString
        base_res.unpersist()
        val and_res = and.eval(rdd_query)
        val and_acc = precision(ground, and_res).toString
        val and_rec = recall(ground, and_res).toString
        and_res.unpersist()
        val or_res = or.eval(rdd_query)
        val or_acc = precision(ground, or_res).toString
        val or_rec = recall(ground, or_res).toString
        or_res.unpersist()

        listOfRecords += Array(corpus_file, query_file, "Base", base_acc, base_rec)
        listOfRecords += Array(corpus_file, query_file, "AND", and_acc, and_rec)
        listOfRecords += Array(corpus_file, query_file, "OR", or_acc, or_rec)
        println(query_file)
        println("Base", base_acc, base_rec)
        println("and", and_acc, and_rec)
        println("or", or_acc, or_rec)
        rdd_query.unpersist()
      }
      rdd_corpus.unpersist()
    }
    csvWriter.writeAll(listOfRecords.toList.asJava)
    outputFile.close()
  }
  def test_constructions_sim(): Unit = {
    val outputFile = new BufferedWriter(new FileWriter("./task8_tests/constr_simil.csv"))
    val csvWriter = new CSVWriter(outputFile)
    val csvSchema = Array("corpus", "query","construction", "precision", "recall")
    var listOfRecords = new ListBuffer[Array[String]]()
    listOfRecords += csvSchema

    val corpus_file = corpus_files.head

    val rdd_corpus = sc
      .textFile(corpus_file)
      .map(x => x.toString.split('|'))
      .map(x => (x(0), x.slice(1, x.size).toList)).cache()
    val exact = new ExactNN(sqlContext, rdd_corpus, 0.3)
    val base = new BaseConstruction(sqlContext, rdd_corpus, seed = 24)
    val balanced = new BaseConstructionBalanced(sqlContext, rdd_corpus, seed = 24, partitions = 8)
    val broadcast = new BaseConstructionBroadcast(sqlContext, rdd_corpus, seed = 24)
    for (query_file <- query_file_1){

      val rdd_query = sc
        .textFile(query_file)
        .map(x => x.toString.split('|'))
        .map(x => (x(0), x.slice(1, x.size).toList))


      val ground = exact.eval(rdd_query)


      val base_res = base.eval(rdd_query)


      val bal_res = balanced.eval(rdd_query)


      val br_res = broadcast.eval(rdd_query)

      val base_acc = precision(ground, base_res).toString
      val base_rec = recall(ground, base_res).toString

      val bal_acc = precision(ground, bal_res).toString
      val bal_rec = recall(ground, bal_res).toString

      val br_acc = precision(ground,  br_res).toString
      val br_rec = recall(ground, br_res).toString

      listOfRecords += Array(corpus_file, query_file, "Base", base_acc, base_rec)
      listOfRecords += Array(corpus_file, query_file, "Balanced", bal_acc, bal_rec)
      listOfRecords += Array(corpus_file, query_file, "Broadcast", br_acc, br_rec)
    }
    csvWriter.writeAll(listOfRecords.toList.asJava)
    outputFile.close()
  }

  def test_accuracy_remote(): Unit = {
      val corpus_file = corpus_files_r(2)

      val rdd_corpus = sc
        .textFile(corpus_file)
        .map(x => x.toString.split('|'))
        .map(x => (x(0), x.slice(1, x.size).toList))

      val exact = new ExactNN(sqlContext, rdd_corpus, 0.3)
      val base = new BaseConstruction(sqlContext, rdd_corpus, seed = 24)
      val and = construction1(sqlContext, rdd_corpus)
      val or = construction2(sqlContext, rdd_corpus)

      val query_file = query_files_r(2)(1)

        val rdd_query = sc
          .textFile(query_file)
          .map(x => x.toString.split('|'))
          .map(x => (x(0), x.slice(1, x.size).toList))


        val ground = exact.eval(rdd_query)


        val base_res = base.eval(rdd_query)
        val base_acc = precision(ground, base_res).toString
        val base_rec = recall(ground, base_res).toString
        base_res.unpersist()

        val and_res = and.eval(rdd_query)
        val and_acc = precision(ground, and_res).toString
        val and_rec = recall(ground, and_res).toString
        and_res.unpersist()

        val or_res = or.eval(rdd_query)
        val or_acc = precision(ground, or_res).toString
        val or_rec = recall(ground, or_res).toString
        or_res.unpersist()
        rdd_query.unpersist()
        println(corpus_file, query_file)
        println("Base: ", base_acc, base_rec)
        println("AND: ", and_acc, and_rec)
        println("Or: ", or_acc, or_rec)
      rdd_corpus.unpersist()
  }

  def test_base_average_distance(): Unit = {

    val outputFile = new BufferedWriter(new FileWriter("./task8_tests/avg_distance.csv"))
		val csvWriter = new CSVWriter(outputFile)
		val csvSchema = Array("corpus","query","construction","distance")
		var listOfRecords = new ListBuffer[Array[String]]()
		listOfRecords += csvSchema

    val corpus_file = corpus_files(0)
    val query_file = query_files(0)(2)
    val rdd_corpus = sc
				.textFile(corpus_file)
				.map(x => x.toString.split('|'))
				.map(x => (x(0), x.slice(1, x.size).toList)).cache()

    val rdd_query = sc
				.textFile(query_file)
				.map(x => x.toString.split('|'))
				.map(x => (x(0), x.slice(1, x.size).toList))
				.distinct()
				.cache()

    val exact = new ExactNN(sqlContext, rdd_corpus, 0.9)
    val exact_res = exact.eval(rdd_query)
    val exact_d = compute_average(exact_res, rdd_query, rdd_corpus)
    exact_res.unpersist()

    val base = new BaseConstruction(sqlContext, rdd_corpus, 24)
    val base_res = base.eval(rdd_query)
    val base_d = compute_average(base_res, rdd_query, rdd_corpus)
    base_res.unpersist()

    val andc = construction1(sqlContext, rdd_corpus)
    val and_res = andc.eval(rdd_query)
    val and_d = compute_average(and_res, rdd_query, rdd_corpus)
    and_res.unpersist()

		val orc = construction2(sqlContext, rdd_corpus)
		val or_res = orc.eval(rdd_query)
		val or_d = compute_average(or_res, rdd_query, rdd_corpus)

    or_res.unpersist()

    listOfRecords += Array(corpus_file, query_file, "Exact", exact_d.toString)
    listOfRecords += Array(corpus_file, query_file, "Base", base_d.toString)
    listOfRecords += Array(corpus_file, query_file, "AND", and_d.toString)
    listOfRecords += Array(corpus_file, query_file, "OR", or_d.toString)

    csvWriter.writeAll(listOfRecords.toList.asJava)
		outputFile.close()
	}

  def test_base_average_distance_remote(): Unit = {

    for (i <- corpus_files_r.indices) {
      val corpus_file = corpus_files_r(i)
      val rdd_corpus = sc
        .textFile(corpus_file)
        .map(x => x.toString.split('|'))
        .map(x => (x(0), x.slice(1, x.size).toList))
      val exact = new ExactNN(sqlContext, rdd_corpus, 0.9)
      val base = new BaseConstruction(sqlContext, rdd_corpus, 24)
      val andc = construction1(sqlContext, rdd_corpus)
      val orc = construction2(sqlContext, rdd_corpus)
      for (query_file <- query_files_r(i)) {
        val rdd_query = sc
          .textFile(query_file)
          .map(x => x.toString.split('|'))
          .map(x => (x(0), x.slice(1, x.size).toList))
          .distinct()
          .cache()


        val exact_res = exact.eval(rdd_query)
        val exact_d = compute_average(exact_res, rdd_query, rdd_corpus)
        exact_res.unpersist()


        val base_res = base.eval(rdd_query)
        val base_d = compute_average(base_res, rdd_query, rdd_corpus)
        base_res.unpersist()


        val and_res = andc.eval(rdd_query)
        val and_d = compute_average(and_res, rdd_query, rdd_corpus)
        and_res.unpersist()


        val or_res = orc.eval(rdd_query)
        val or_d = compute_average(or_res, rdd_query, rdd_corpus)

        or_res.unpersist()
        println(corpus_file, query_file)
        println( "Exact", exact_d.toString)
        println("Base", base_d.toString)
        println( "AND", and_d.toString)
        println( "OR", or_d.toString)
      }
    }
  }

  def test_extreme_skew(): Unit ={
    val corpus_file = "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/corpus-skewplusplus.csv/part-00000"

    val rdd_corpus_fragment = sc
      .textFile(corpus_file)
      .map(x => x.toString.split('|'))
      .map(x => (x(0), x.slice(1, x.size).toList))
      .repartition(2)
      .cache()

    val rdd_corpus = ((1 to 10).map(x => rdd_corpus_fragment).reduce(_ ++ _)).repartition(2)

    val query_file = "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/queries-skewplusplus.csv/part-00000"
    val rdd_query = sc
      .textFile(query_file)
      .map(x => x.toString.split('|'))
      .map(x => (x(0), x.slice(1, x.size).toList))
      .repartition(8)
      .cache()

    println(rdd_corpus.count() + rdd_query.count())

    val lsh1 = new BaseConstruction(sqlContext, rdd_corpus, 42)
    val lsh2 = new BaseConstructionBalanced(sqlContext, rdd_corpus, 42, 8)

    for (i <- 1 to 10) {
      val t1 = System.nanoTime

      val res2 = lsh1.eval(rdd_query).flatMap(x => x._2).count()

      val duration1 = (System.nanoTime - t1) / 1e9d

      val t2 = System.nanoTime

      val res1 = lsh2.eval(rdd_query).flatMap(x => x._2).count()

      val duration2 = (System.nanoTime - t2) / 1e9d

      println(duration1, "Base")
      println(duration2, "Balanced")
    }
  }
  def main(args: Array[String]) {
      test_extreme_skew()
    }
}