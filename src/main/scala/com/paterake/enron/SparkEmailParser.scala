package com.paterake.enron

import com.databricks.spark.xml.XmlReader
import com.paterake.enron.util.ZipFileInputFormat
import org.apache.hadoop.io.{BytesWritable, Text}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SparkSession, functions}

import scala.util.matching.Regex

/**
  * Created by paterake on 16/06/2017.
  *
  * @param spark SparkSession instance
  */
class SparkEmailParser(spark: SparkSession) extends java.io.Serializable {

  /**
    * Produce an RDD of the contents of the ZIP file
    *
    * @param folder File Pattern for the input zip files.
    * @param fileSuffix file type for contents of zip file to produce RDDs against
    * @return an RDD representing FileName and File Content
    */
  def getZipFileRdd(folder: String, fileSuffix: String):RDD[(String, String)] = {
    val zipFileRDD = spark.sparkContext.newAPIHadoopFile(folder, classOf[ZipFileInputFormat],
      classOf[Text],
      classOf[BytesWritable],
      spark.sparkContext.hadoopConfiguration)
      .filter(x => x._1.toString.toLowerCase.endsWith(fileSuffix))
      .map { case(a,b) => (a.toString, new String( b.getBytes(), "UTF-8" )) }
    zipFileRDD
  }

  /**
    * A Count of the files represented by the RDDs
    *
    * @param rdd representing FileName and File Content
    * @return Count of RDD rows.
    */
  def rddCount(rdd : RDD[(String, String)]): Int = {
    var cntRdd: Int = 0
    rdd.collect().foreach(x => {
      cntRdd += 1
      //println(x._1)
    })
    cntRdd
  }

  /**
    * Obtain average word-count for files
    *
    * @param fileRdd representing FileName and File Content
    * @return Map of results, keys:
    *         fileCount      : Total number of files
    *         wordCount      : Total number of words across all files
    *         avgWordPerFile : Average number of words per file
    */
  def fileWordStats(fileRdd: RDD[(String, String)]): Map[String, AnyVal] = {
    val cntFile: Int = rddCount(fileRdd)
    val cntWord = fileRdd.flatMap(x => x._2.split(" ")).count()
    val mapOutput = Map(("fileCount", cntFile), ("wordCount", cntWord), ("avgWordPerFile", cntWord/cntFile))
    mapOutput
  }

  /**
    * Take input XML file, and extract the Tags for #To and #CC
    *
    * @param fileRdd representing FileName and File Content
    * @return DataSet defined with columns
    *         _TagName  : recipient type : #To or #CC
    *         _TagValue : Email recipient
    *         weight    : 2 (for #To) 1 (for #CC)
    */
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

  /**
    * Extract each Email Address and unpivot into individual rows
    *
    * @param df : Input DataSet of email addresses.
    * @param recipientType : Filter DataSet on #To or #CC
    * @param regex : Pattern to match on the email address from the text.
    * @return Array List of individual email addresses
    */
  def getRecipient(df: Dataset[Row], recipientType: String, regex: Regex): Array[String] = {
    val clcnRecipient = df.select("_TagValue", "weight").filter("_TagName = '" + recipientType + "'")
      .rdd.map(r => r(0)).collect().flatMap(x => regex.findAllIn(x.toString.toLowerCase))
    clcnRecipient
  }

  /**
    * Extract each Email Address and unpivot into individual rows
    * Handling both normal email addresses and LDAP email addresses.
    *
    * @param df : Input DataSet of email addresses.
    * @param recipientType : Filter DataSet on #To or #CC
    * @return Array List of individual email addresses
    */
  def getRecipient(df: Dataset[Row], recipientType: String): Array[String] = {
    val regexEmail : Regex = """(?i)\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,4}\b""".r
    val regexLdap : Regex = "CN=\\w+>".r
    val clcnEmail = getRecipient(df, recipientType, regexEmail)
    val clcnLdap = getRecipient(df, recipientType, regexLdap)
    val clcnRecipient = clcnEmail ++ clcnLdap
    //println(clcnRecipient.deep.mkString("\n"))
    clcnRecipient
  }

  /**
    * Take XML File and parse out the recipients and establish the Top 100.
    *
    * @param fileRdd representing FileName and File Content
    * @return Map of results, keys:
    *         fileCount      : Total number of files
    *         top100         : Array List of the Top 100 recipients
    */
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

  /**
    * Obtain average word-count for files
    *
    * @param folder : Location of the input data files.
    * @return Map of results, keys:
    *         fileCount      : Total number of files
    *         wordCount      : Total number of words across all files
    *         avgWordPerFile : Average number of words per file
    */
  def processWordStats(folder: String): Map[String, AnyVal] = {
    val rddFile = getZipFileRdd(folder, ".txt")
    val mapStats = fileWordStats(rddFile)
    mapStats
  }

  /**
    * Take XML File and parse out the recipients and establish the Top 100.
    *
    * @param folder : Location of the input data files.
    * @return Map of results, keys:
    *         fileCount      : Total number of files
    *         top100         : Array List of the Top 100 recipients
    */
  def processRecipientStats(folder: String): Map[String, Any] = {
    val rddFile = getZipFileRdd(folder, ".xml")
    val mapStats = fileRecipientStats(rddFile)
    mapStats
  }

  /**
    * Take location of input data and process to get word counts and Top 100 recipients
    *
    * @param folder : Location of the input data files.
    * @return Map of results, keys:
    *         fileCount      : Total number of files
    *         wordCount      : Total number of words across all files
    *         avgWordPerFile : Average number of words per file
    *         top100         : Array List of the Top 100 recipients
    */
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

/**
  * Take location of input data and process to get word counts and Top 100 recipients
  *
  */
object SparkEmailParser {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("SparkEmailParser")
      .master("local")
      .getOrCreate()
    val emailParser = new SparkEmailParser(spark)
    emailParser.process(args(0))
  }
}