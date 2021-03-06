package com.paterake.enron

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

/**
  * Testing, using sample files:
  * 1. Test word counts, file counts and average words per file.
  * 2. Test Top 100 recipient processing by ensuring the correct number of XML files are processed
  * By performing these tests, this covers the root of the two processing paths
  *
  */
class SparkEmailParserTest extends FunSuite with BeforeAndAfterAll{

  @transient
  private var _spark: SparkSession = _

  def spark: SparkSession = _spark

  var emailParser: SparkEmailParser = _
  val folder: String = "file:///" + getClass.getResource("/data").getPath.toString + "/edrm-enron-v2_quenet-j_xml*"

  override def beforeAll {
    _spark = SparkSession
       .builder()
       .appName("SparkEmailParserTest")
       .master("local")
       .getOrCreate()

    emailParser = new SparkEmailParser(spark)
    super.beforeAll()
  }

  override def afterAll() {
    if (spark != null) {
      _spark.stop()
    }
    System.clearProperty("spark.driver.port")
    _spark = null
    super.afterAll()
  }

  test("Text File Count") {
    val mapStats = emailParser.processWordStats(folder)
    assert(mapStats.get("fileCount").get==665)
    assert(mapStats.get("avgWordPerFile").get==244)
  }

  test("XML File Count") {
    val mapStats = emailParser.processRecipientStats(folder)
    mapStats.get("top100").toList.length
    assert(mapStats.get("fileCount").get==1)
  }



}
