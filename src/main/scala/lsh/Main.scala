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
  val conf = new SparkConf().setAppName("app").setMaster("local[*]").set("spark.executor.instances", "4")
  val sc = SparkContext.getOrCreate(conf)
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)

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

  def average(d : List[Double]): Double = d.sum / d.size

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
    var seed = 42
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
    var seed = 42
    for (i <- 0 to 6) {
      baseConstrs :+= new BaseConstruction(sqlContext, rdd_corpus, seed)
      seed += 1
    }
    val lsh = new ORConstruction(baseConstrs)
    lsh
  }

  //----------------------------------------------------TASK 8 TESTS-------------------------------------------------
  def test_exactnn_runtime(): Unit = {
    var corpus_files = List[String]()
    corpus_files :+= "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/corpus-1.csv/part-00000"
    corpus_files :+= "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/corpus-10.csv/part-00000"
    corpus_files :+= "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/corpus-20.csv/part-00000"

    var query_files = List[List[String]]()

    var query_file_1 = List[String]()
    query_file_1 :+= "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/queries-1-2.csv/part-00000"
    query_file_1 :+= "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/queries-1-2-skew.csv/part-00000"
    query_file_1 :+= "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/queries-1-10.csv/part-00000"
    query_file_1 :+= "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/queries-1-10-skew.csv/part-00000"
    query_files :+= query_file_1
    var query_file_10 = List[String]()
    query_file_10 :+= "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/queries-10-2.csv/part-00000"
    query_file_10 :+= "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/queries-10-2-skew.csv/part-00000"
    query_file_10 :+= "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/queries-10-10.csv/part-00000"
    query_file_10 :+= "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/queries-10-10-skew.csv/part-00000"
    query_files :+= query_file_10
    var query_file_20 = List[String]()
    query_file_20 :+= "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/queries-20-2.csv/part-00000"
    query_file_20 :+= "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/queries-20-2-skew.csv/part-00000"
    query_file_20 :+= "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/queries-20-10.csv/part-00000"
    query_file_20 :+= "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/queries-20-10-skew.csv/part-00000"
    query_files :+= query_file_20

    val outputFile = new BufferedWriter(new FileWriter("./task8_tests/exactnn_time.csv"))
    val csvWriter = new CSVWriter(outputFile)
    val csvSchema = Array("corpus","query","duration")
    var listOfRecords = new ListBuffer[Array[String]]()
    listOfRecords += csvSchema

    for (i<-corpus_files.indices) {
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
        print(corpus_file, query_file)
        val exact = new ExactNN(sqlContext, rdd_corpus, 0.3)
        val tic = System.nanoTime()
        val ground = exact.eval(rdd_query)
        val toc = System.nanoTime()
        val duration = (toc - tic) / 1e9d

        listOfRecords += Array(corpus_file, query_file, duration.toString)
      }
    }
    csvWriter.writeAll(listOfRecords.toList.asJava)
    outputFile.close()
  }

  //@Test
  def test_base_constr_time(): Unit = {
    var corpus_files = List[String]()
    corpus_files :+= "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/corpus-1.csv/part-00000"
    corpus_files :+= "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/corpus-10.csv/part-00000"
    corpus_files :+= "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/corpus-20.csv/part-00000"

    var query_files = List[List[String]]()

    var query_file_1 = List[String]()
    query_file_1 :+= "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/queries-1-2.csv/part-00000"
    query_file_1 :+= "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/queries-1-2-skew.csv/part-00000"
    query_file_1 :+= "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/queries-1-10.csv/part-00000"
    query_file_1 :+= "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/queries-1-10-skew.csv/part-00000"
    query_files :+= query_file_1
    var query_file_10 = List[String]()
    query_file_10 :+= "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/queries-10-2.csv/part-00000"
    query_file_10 :+= "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/queries-10-2-skew.csv/part-00000"
    query_file_10 :+= "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/queries-10-10.csv/part-00000"
    query_file_10 :+= "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/queries-10-10-skew.csv/part-00000"
    query_files :+= query_file_10
    var query_file_20 = List[String]()
    query_file_20 :+= "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/queries-20-2.csv/part-00000"
    query_file_20 :+= "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/queries-20-2-skew.csv/part-00000"
    query_file_20 :+= "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/queries-20-10.csv/part-00000"
    query_file_20 :+= "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/queries-20-10-skew.csv/part-00000"
    query_files :+= query_file_20

    val outputFile = new BufferedWriter(new FileWriter("./task8_tests/basecon_time.csv"))
    val csvWriter = new CSVWriter(outputFile)
    val csvSchema = Array("corpus","query","duration")
    var listOfRecords = new ListBuffer[Array[String]]()
    listOfRecords += csvSchema

    for (i<-corpus_files.indices) {
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


        val exact = new BaseConstruction(sqlContext, rdd_corpus, 42)
        val tic = System.nanoTime()
        val ground = exact.eval(rdd_query)
        val toc = System.nanoTime()
        val duration = (toc - tic) / 1e9d

        listOfRecords += Array(corpus_file, query_file, duration.toString)
      }
    }
    csvWriter.writeAll(listOfRecords.toList.asJava)
    outputFile.close()
  }

  def test_balanced_constr_time() : Unit = {
    var corpus_files = List[String]()
    corpus_files :+= "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/corpus-1.csv/part-00000"
    corpus_files :+= "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/corpus-10.csv/part-00000"
    corpus_files :+= "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/corpus-20.csv/part-00000"

    var query_files = List[List[String]]()

    var query_file_1 = List[String]()
    query_file_1 :+= "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/queries-1-2.csv/part-00000"
    query_file_1 :+= "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/queries-1-2-skew.csv/part-00000"
    query_file_1 :+= "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/queries-1-10.csv/part-00000"
    query_file_1 :+= "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/queries-1-10-skew.csv/part-00000"
    query_files :+= query_file_1
    var query_file_10 = List[String]()
    query_file_10 :+= "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/queries-10-2.csv/part-00000"
    query_file_10 :+= "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/queries-10-2-skew.csv/part-00000"
    query_file_10 :+= "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/queries-10-10.csv/part-00000"
    query_file_10 :+= "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/queries-10-10-skew.csv/part-00000"
    query_files :+= query_file_10
    var query_file_20 = List[String]()
    query_file_20 :+= "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/queries-20-2.csv/part-00000"
    query_file_20 :+= "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/queries-20-2-skew.csv/part-00000"
    query_file_20 :+= "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/queries-20-10.csv/part-00000"
    query_file_20 :+= "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/queries-20-10-skew.csv/part-00000"
    query_files :+= query_file_20

    val outputFile = new BufferedWriter(new FileWriter("./task8_tests/balancedconstr_time.csv"))
    val csvWriter = new CSVWriter(outputFile)
    val csvSchema = Array("corpus","query","duration")
    var listOfRecords = new ListBuffer[Array[String]]()
    listOfRecords += csvSchema

    for (i<-corpus_files.indices) {
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

        val exact = new BaseConstructionBalanced(sqlContext, rdd_corpus, 42, partitions = 8)
        val tic = System.nanoTime()
        val ground = exact.eval(rdd_query)
        val toc = System.nanoTime()
        val duration = (toc - tic) / 1e9d

        listOfRecords += Array(corpus_file, query_file, duration.toString)
      }
    }
    csvWriter.writeAll(listOfRecords.toList.asJava)
    outputFile.close()
  }

  def test_broadcast_constr_time() : Unit = {
    var corpus_files = List[String]()
    corpus_files :+= "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/corpus-1.csv/part-00000"
    corpus_files :+= "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/corpus-10.csv/part-00000"
    corpus_files :+= "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/corpus-20.csv/part-00000"

    var query_files = List[List[String]]()

    var query_file_1 = List[String]()
    query_file_1 :+= "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/queries-1-2.csv/part-00000"
    query_file_1 :+= "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/queries-1-2-skew.csv/part-00000"
    query_file_1 :+= "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/queries-1-10.csv/part-00000"
    query_file_1 :+= "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/queries-1-10-skew.csv/part-00000"
    query_files :+= query_file_1
    var query_file_10 = List[String]()
    query_file_10 :+= "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/queries-10-2.csv/part-00000"
    query_file_10 :+= "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/queries-10-2-skew.csv/part-00000"
    query_file_10 :+= "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/queries-10-10.csv/part-00000"
    query_file_10 :+= "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/queries-10-10-skew.csv/part-00000"
    query_files :+= query_file_10
    var query_file_20 = List[String]()
    query_file_20 :+= "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/queries-20-2.csv/part-00000"
    query_file_20 :+= "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/queries-20-2-skew.csv/part-00000"
    query_file_20 :+= "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/queries-20-10.csv/part-00000"
    query_file_20 :+= "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/queries-20-10-skew.csv/part-00000"
    query_files :+= query_file_20

    val outputFile = new BufferedWriter(new FileWriter("./task8_tests/broadcastconstr_time.csv"))
    val csvWriter = new CSVWriter(outputFile)
    val csvSchema = Array("corpus","query","duration")
    var listOfRecords = new ListBuffer[Array[String]]()
    listOfRecords += csvSchema

    for (i<-corpus_files.indices) {
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


        val exact = new BaseConstructionBroadcast(sqlContext, rdd_corpus, 42)
        val tic = System.nanoTime()
        val ground = exact.eval(rdd_query)
        val toc = System.nanoTime()
        val duration = (toc - tic) / 1e9d

        listOfRecords += Array(corpus_file, query_file, duration.toString)
      }
    }
    csvWriter.writeAll(listOfRecords.toList.asJava)
    outputFile.close()
  }

  def test_average_dinstance() : Unit = {
    var corpus_files = List[String]()
    corpus_files :+= "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/corpus-1.csv/part-00000"
    corpus_files :+= "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/corpus-10.csv/part-00000"
    corpus_files :+= "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/corpus-20.csv/part-00000"

    var query_files = List[List[String]]()

    var query_file_1 = List[String]()
    query_file_1 :+= "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/queries-1-2.csv/part-00000"
    query_file_1 :+= "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/queries-1-2-skew.csv/part-00000"
    query_file_1 :+= "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/queries-1-10.csv/part-00000"
    query_file_1 :+= "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/queries-1-10-skew.csv/part-00000"
    query_files :+= query_file_1
    var query_file_10 = List[String]()
    query_file_10 :+= "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/queries-10-2.csv/part-00000"
    query_file_10 :+= "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/queries-10-2-skew.csv/part-00000"
    query_file_10 :+= "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/queries-10-10.csv/part-00000"
    query_file_10 :+= "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/queries-10-10-skew.csv/part-00000"
    query_files :+= query_file_10
    var query_file_20 = List[String]()
    query_file_20 :+= "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/queries-20-2.csv/part-00000"
    query_file_20 :+= "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/queries-20-2-skew.csv/part-00000"
    query_file_20 :+= "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/queries-20-10.csv/part-00000"
    query_file_20 :+= "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/queries-20-10-skew.csv/part-00000"
    query_files :+= query_file_20

    val outputFile = new BufferedWriter(new FileWriter("./task8_tests/average_distance.csv"))
    val csvWriter = new CSVWriter(outputFile)
    val csvSchema = Array("corpus","query","duration")
    var listOfRecords = new ListBuffer[Array[String]]()
    listOfRecords += csvSchema

    for (i<-corpus_files.indices) {
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
          .sample(withReplacement=false, fraction = 0.5)
          .join(rdd_query)
          .map(f => (f._2._1,(f._1, f._2._2)))
          .join(rdd_corpus)
          .map(f => (f._2._1._1, f._1, f._2._1._2, f._2._2))
          .map(f => (f._1, f._2, jaccard(f._3, f._4)))
          .groupBy(f => f._1)
          .map(f => average(f._2.map(t => t._3).toList))
          .collect().toList)

        var base_d = average(base_res.flatMapValues(f => f.toList)
          .sample(withReplacement=false, fraction = 0.5)
          .join(rdd_query)
          .map(f => (f._2._1,(f._1, f._2._2)))
          .join(rdd_corpus)
          .map(f => (f._2._1._1, f._1, f._2._1._2, f._2._2))
          .map(f => (f._1, f._2, jaccard(f._3, f._4)))
          .groupBy(f => f._1)
          .map(f => average(f._2.map(t => t._3).toList))
          .collect().toList)

        var balanced_d = average(balanced_res.flatMapValues(f => f.toList)
          .sample(withReplacement=false, fraction = 0.5)
          .join(rdd_query)
          .map(f => (f._2._1,(f._1, f._2._2)))
          .join(rdd_corpus)
          .map(f => (f._2._1._1, f._1, f._2._1._2, f._2._2))
          .map(f => (f._1, f._2, jaccard(f._3, f._4)))
          .groupBy(f => f._1)
          .map(f => average(f._2.map(t => t._3).toList))
          .collect().toList)

        var broadcast_d = average(broadcast_res.flatMapValues(f => f.toList)
          .sample(withReplacement=false, fraction = 0.5)
          .join(rdd_query)
          .map(f => (f._2._1,(f._1, f._2._2)))
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
    csvWriter.writeAll(listOfRecords.toList.asJava)
    outputFile.close()
  }

  def main(args: Array[String]) {
    test_exactnn_runtime()
    test_base_constr_time()
    test_balanced_constr_time()
    test_broadcast_constr_time()
    test_average_dinstance()
  }     
}
