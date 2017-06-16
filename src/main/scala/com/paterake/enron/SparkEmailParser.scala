package com.paterake.enron

import com.databricks.spark.xml.XmlReader
import com.paterake.enron.util.ZipFileInputFormat
import org.apache.hadoop.io.{BytesWritable, Text}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types._

class SparkEmailParser(sc: SparkContext) extends java.io.Serializable {

  def getTxtFileRdd(folder: String): RDD[(String, String)] = {
    val zipFileRDD = sc.newAPIHadoopFile(folder, classOf[ZipFileInputFormat],
      classOf[Text],
      classOf[BytesWritable],
      sc.hadoopConfiguration)
        .filter(x => x._1.toString.toLowerCase.endsWith(".txt"))
        .map { case(a,b) => (a.toString, new String( b.getBytes(), "UTF-8" )) }
    zipFileRDD
  }

  def getXmlFileRdd(folder: String): RDD[(String, String)] = {
    val zipFileRDD = sc.newAPIHadoopFile(folder, classOf[ZipFileInputFormat],
      classOf[Text],
      classOf[BytesWritable],
      sc.hadoopConfiguration)
      .filter(x => x._1.toString.toLowerCase.endsWith(".xml"))
      .map { case(a,b) => (a.toString, new String( b.getBytes(), "UTF-8" )) }
    zipFileRDD
  }

  def fileWordStats(fileRdd : RDD[(String, String)]): Map[String, AnyVal] = {
    var cntFile: Int = 0;
    fileRdd.collect().foreach(x => {
      cntFile += 1
      //println(x._1)
    })
    val cntWord = fileRdd.flatMap(x => x._2.split(" ")).count()
    val mapOutput = Map(("fileCount", cntFile), ("wordCount", cntWord), ("avgWordPerFile", cntWord/cntFile))
    mapOutput
  }

  def fileRecipientStats(fileRdd : RDD[(String, String)]): Map[String, AnyVal] = {
    var cntFile: Int = 0;
    fileRdd.collect().foreach(x => {
      cntFile += 1
      //println(x._1)
    })
    val mapOutput = Map(("fileCount", cntFile))
    mapOutput
  }

  def processWordStats(folder: String): Map[String, AnyVal] = {
    val rddFile = getTxtFileRdd(folder)
    val mapStats = fileWordStats(rddFile)
    println("files = " + mapStats.get("fileCount").get)
    println("words = " + mapStats.get("wordCount").get)
    println("avg Words Per file = " + mapStats.get("avgWordPerFile").get)
    mapStats
  }

  def processRecipientStats(folder: String): Map[String, AnyVal] = {
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val rddFile = getXmlFileRdd(folder)
    val mapStats = fileRecipientStats(rddFile)
    println("files = " + mapStats.get("fileCount").get)
    val rddXml = rddFile.map(x => x._2)
    val df = new XmlReader().withRowTag("Root").xmlRdd(sqlContext, rddXml)
    df.printSchema()
    val df2 = df
      .select(functions.explode(df.col("Batch.Documents.Document.Tags")).as("t1"))
      .select(functions.col("t1.*"))
      .select(functions.explode(functions.col("tag")).as("t2"))
      .select("t2._TagName", "t2._TagValue")
      .filter("t2._TagName in ('#To', '#CC')")


    df2.printSchema()
    df2.show(10)
    mapStats
  }

  def process(folder: String): Unit = {
    val mapWordStats = processWordStats(folder)
    val mapRecipientStats = processRecipientStats(folder)
  }

}

object SparkEmailParser {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkEmailParser").setMaster("local")
    val ctxt = new SparkContext(conf)
    val emailParser = new SparkEmailParser(ctxt)
    emailParser.process("file:///home/rakesh/Documents/__code/__git/enronEmail/src/test/resources/data/edrm-enron-v2_quenet-j_xml*")
  }
}