package com.paterake.enron

import com.databricks.spark.xml.XmlReader
import com.paterake.enron.util.ZipFileInputFormat
import org.apache.hadoop.io.{BytesWritable, Text}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SparkSession, functions}

import scala.util.matching.Regex

class SparkEmailParser(spark: SparkSession) extends java.io.Serializable {

  def getZipFileRdd(folder: String, fileSuffix: String):RDD[(String, String)] = {
    val zipFileRDD = spark.sparkContext.newAPIHadoopFile(folder, classOf[ZipFileInputFormat],
      classOf[Text],
      classOf[BytesWritable],
      spark.sparkContext.hadoopConfiguration)
      .filter(x => x._1.toString.toLowerCase.endsWith(fileSuffix))
      .map { case(a,b) => (a.toString, new String( b.getBytes(), "UTF-8" )) }
    zipFileRDD
  }

  def rddCount(rdd : RDD[(String, String)]): Int = {
    var cntRdd: Int = 0
    rdd.collect().foreach(x => {
      cntRdd += 1
      //println(x._1)
    })
    cntRdd
  }

  def fileWordStats(fileRdd: RDD[(String, String)]): Map[String, AnyVal] = {
    val cntFile: Int = rddCount(fileRdd)
    val cntWord = fileRdd.flatMap(x => x._2.split(" ")).count()
    val mapOutput = Map(("fileCount", cntFile), ("wordCount", cntWord), ("avgWordPerFile", cntWord/cntFile))
    mapOutput
  }

  def getRecipientSet(fileRdd : RDD[(String, String)]): Dataset[Row] = {
    val rddXml = fileRdd.map(x => x._2)
    val sqlContext = spark.sqlContext
    val df = new XmlReader().withRowTag("Root").xmlRdd(sqlContext, rddXml)
    val df2 = df
      .select(functions.explode(df.col("Batch.Documents.Document.Tags")).as("t1"))
      .select(functions.col("t1.*"))
      .select(functions.explode(functions.col("tag")).as("t2"))
      .select("t2._TagName", "t2._TagValue")
      .filter("t2._TagName in ('#To', '#CC')")
      .withColumn("weight", functions.when(functions.col("_TagName") === functions.lit("#To"), 2).otherwise(1))
    df2.printSchema()
    df2.show(10)
    df2
  }

  def getRecipient(df: Dataset[Row], recipientType: String, regex: Regex): Array[String] = {
    val clcnRecipient = df.select("_TagValue", "weight").filter("_TagName = '" + recipientType + "'")
      .rdd.map(r => r(0)).collect().flatMap(x => regex.findAllIn(x.toString.toLowerCase))
    clcnRecipient
  }

  def getRecipient(df: Dataset[Row], recipientType: String): Array[String] = {
    val regexEmail : Regex = """(?i)\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,4}\b""".r
    val regexLdap : Regex = "CN=\\w+>".r
    val clcnEmail = getRecipient(df, recipientType, regexEmail)
    val clcnLdap = getRecipient(df, recipientType, regexLdap)
    val clcnRecipient = clcnEmail ++ clcnLdap
    //println(clcnRecipient.deep.mkString("\n"))
    clcnRecipient
  }

  def fileRecipientStats(fileRdd : RDD[(String, String)]): Map[String, Any] = {
    val cntFile: Int = rddCount(fileRdd)
    val df = getRecipientSet(fileRdd)
    val clcnTo = getRecipient(df, "#To")
    val clcnCc = getRecipient(df, "#CC")

    val clcnTop = (clcnTo.map((_, 2)) ++ clcnCc.map((_, 1))).groupBy(_._1).toList.map(x => (x._1, x._2.map(_._2).sum))
      .sortBy(-_._2)
      .take(100)

    val mapOutput = Map(("fileCount", cntFile), ("top100", clcnTop))
    mapOutput
  }

  def processWordStats(folder: String): Map[String, AnyVal] = {
    val rddFile = getZipFileRdd(folder, ".txt")
    val mapStats = fileWordStats(rddFile)
    mapStats
  }

  def processRecipientStats(folder: String): Map[String, Any] = {
    val rddFile = getZipFileRdd(folder, ".xml")
    val mapStats = fileRecipientStats(rddFile)
    mapStats
  }

  def process(folder: String): Unit = {
    val mapWordStats = processWordStats(folder)
    val mapRecipientStats = processRecipientStats(folder)
    println("files = " + mapWordStats.get("fileCount").get)
    println("words = " + mapWordStats.get("wordCount").get)
    println("avg Words Per file = " + mapWordStats.get("avgWordPerFile").get)
    println("files = " + mapRecipientStats.get("fileCount").get)
    println("Top100 = " + mapRecipientStats.get("top100").get)
  }

}

object SparkEmailParser {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("SparkEmailParser")
      .master("local")
      .getOrCreate()
      .set("yarn.nodemanager.vmem-check-enabled","false")
    val emailParser = new SparkEmailParser(spark)
    emailParser.process(args(0))
  }
}