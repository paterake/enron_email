package com.paterake.enron

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, FunSuite}

class SparkEmailParserTest extends FunSuite with BeforeAndAfter{
  var emailParser: SparkEmailParser = _

  before  {
    val conf = new SparkConf().setAppName("SparkEmailParserTest").setMaster("local")
    val ctxt = new SparkContext(conf)
    emailParser = new SparkEmailParser(ctxt)
  }



}
