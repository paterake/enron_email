package com.paterake.enron

import org.apache.spark.{SparkConf, SparkContext}

class SparkEmailParser(sc: SparkContext) {
  def process(folder: String): Unit = {
    

  }

}

object SparkEmailParser {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkEmailParser").setMaster("local")
    val ctxt = new SparkContext(conf)
    val emailParser = new SparkEmailParser(ctxt)
    emailParser.process(args(0))
  }
}