package com.paterake.enron

import com.databricks.spark.xml.XmlReader
import com.paterake.enron.util.ZipFileInputFormat
import org.apache.hadoop.io.{BytesWritable, Text}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SparkSession, functions}

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

  def getTxtFileRdd(folder: String): RDD[(String, String)] = {
    val fileRdd = getZipFileRdd(folder, ".txt")
    fileRdd
  }

  def getXmlFileRdd(folder: String): RDD[(String, String)] = {
    val fileRdd = getZipFileRdd(folder, ".xml")
    fileRdd
  }

  def rddCount(rdd : RDD[(String, String)]): Int = {
    var cntRdd: Int = 0;
    rdd.collect().foreach(x => {
      cntRdd += 1
      //println(x._1)
    })
    cntRdd
  }

  def fileWordStats(fileRdd: RDD[(String, String)]): Map[String, AnyVal] = {
    val cntFile: Int = rddCount(fileRdd);
    val cntWord = fileRdd.flatMap(x => x._2.split(" ")).count()
    val mapOutput = Map(("fileCount", cntFile), ("wordCount", cntWord), ("avgWordPerFile", cntWord/cntFile))
    mapOutput
  }

  def fileRecipientStats(fileRdd : RDD[(String, String)]): Map[String, AnyVal] = {
    val cntFile: Int = rddCount(fileRdd);
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
    val sqlContext = spark.sqlContext
    val rddFile = getXmlFileRdd(folder)
    val mapStats = fileRecipientStats(rddFile)
    println("files = " + mapStats.get("fileCount").get)
    val rddXml = rddFile.map(x => x._2)
    val df = new XmlReader().withRowTag("Root").xmlRdd(sqlContext, rddXml)
    val df2 = df
      .select(functions.explode(df.col("Batch.Documents.Document.Tags")).as("t1"))
      .select(functions.col("t1.*"))
      .select(functions.explode(functions.col("tag")).as("t2"))
      .select("t2._TagName", "t2._TagValue")
      .filter("t2._TagName in ('#To', '#CC')")

    df2.foreach( x => {
      if (x.get(0) == "#CC") {
        println(x.get(1))
      }
    })

    mapStats
  }

  def process(folder: String): Unit = {
    val mapWordStats = processWordStats(folder)
    val mapRecipientStats = processRecipientStats(folder)
  }

}

object SparkEmailParser {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("SparkEmailParser")
      .master("local")
      .getOrCreate()
    val emailParser = new SparkEmailParser(spark)
    emailParser.process("file:///home/rakesh/Documents/__code/__git/enronEmail/src/test/resources/data/edrm-enron-v2_quenet-j_xml*")
  }
}