package com.paterake.enron

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSuite}

class SparkEmailParserTest extends FunSuite with BeforeAndAfterAll{

  @transient
  private var _ctxt: SparkContext = _

  def ctxt: SparkContext = _ctxt

  var emailParser: SparkEmailParser = _
  val folder: String = "file:///home/rakesh/Documents/__code/__git/enronEmail/src/test/resources/data/edrm-enron-v2_quenet-j_xml*"

  override def beforeAll {
    val conf = new SparkConf().setAppName("SparkEmailParserTest").setMaster("local")
    _ctxt = new SparkContext(conf)
    emailParser = new SparkEmailParser(ctxt)
    super.beforeAll()
  }

  override def afterAll() {
    if (ctxt != null) {
      _ctxt.stop()
    }
    System.clearProperty("spark.driver.port")
    _ctxt = null
    super.afterAll()
  }

  test("Text File Count") {
    val mapStats = emailParser.processWordStats(folder)
    assert(mapStats.get("fileCount").get==665)
  }

  test("XML File Count") {
//    val rddXmlFile = emailParser.getXmlFileRdd(folder)
//    val mapStats = emailParser.fileRecipientStats(rddXmlFile)
//    assert(mapStats.get("fileCount").get==1)
  }



}
