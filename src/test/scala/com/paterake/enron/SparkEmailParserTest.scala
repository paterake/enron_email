package com.paterake.enron

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, FunSuite}

class SparkEmailParserTest extends FunSuite with BeforeAndAfter{
  var emailParser: SparkEmailParser = _
  val folder: String = "file:///home/rakesh/Documents/__code/__git/enronEmail/src/test/resources/data/edrm-enron-v2_quenet-j_xml*"

  before {
    val conf = new SparkConf().setAppName("SparkEmailParserTest").setMaster("local")
    val ctxt = new SparkContext(conf)
    emailParser = new SparkEmailParser(ctxt)
  }

  test("Text File Count") {
    val rddTxtFile = emailParser.getTxtFileRdd(folder)
    val mapStats = emailParser.fileWordStats(rddTxtFile)
    assert(mapStats.get("fileCount").get==665)
  }

  test("XML File Count") {
//    val rddXmlFile = emailParser.getXmlFileRdd(folder)
//    val mapStats = emailParser.fileRecipientStats(rddXmlFile)
//    assert(mapStats.get("fileCount").get==1)
  }



}
