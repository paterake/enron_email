package com.paterake.enron

import com.paterake.enron.util.ZipFileInputFormat
import org.apache.hadoop.io.{BytesWritable, Text}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

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

  def wordCount(content: String): Long = {
    0
  }


  def fileWordStats(fileRdd : RDD[(String, String)]): Map[String, AnyVal] = {
    var cntFile: Int = 0;
    var cntWord: Long = 0;
    fileRdd.collect().foreach(x => {
      cntFile += 1
      cntWord += wordCount(x._2)
      println(x._1)
    })
    val mapOutput = Map(("fileCount", cntFile), ("wordCount", cntWord))
    mapOutput
  }

  def fileRecipientStats(fileRdd : RDD[(String, String)]): Map[String, AnyVal] = {
    var cntFile: Int = 0;
    fileRdd.collect().foreach(x => {
      cntFile += 1
      println(x._1)
    })
    val mapOutput = Map(("fileCount", cntFile))
    mapOutput
  }

  def processWordStats(folder: String): Map[String, AnyVal] = {
    val rddFile = getTxtFileRdd(folder)
    val mapStats = fileWordStats(rddFile)
    println("files = " + mapStats.get("fileCount").get)
    mapStats
  }

  def processRecipientStats(folder: String): Map[String, AnyVal] = {
    val rddFile = getXmlFileRdd(folder)
    val mapStats = fileRecipientStats(rddFile)
    println("files = " + mapStats.get("fileCount").get)
    mapStats
  }

  def process(folder: String): Unit = {
    processWordStats(folder)
    processRecipientStats(folder)
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