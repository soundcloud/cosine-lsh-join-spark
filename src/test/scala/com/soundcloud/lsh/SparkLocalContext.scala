package com.soundcloud.lsh

import java.util.Properties

import org.scalatest.{BeforeAndAfterAll, Suite}
import org.apache.log4j.PropertyConfigurator
import org.apache.spark.{SparkConf, SparkContext}

trait SparkLocalContext extends BeforeAndAfterAll {
  self: Suite =>
  var sc: SparkContext = _

  override def beforeAll() {
    loadTestLog4jConfig()

    val conf = new SparkConf().
      setAppName("test").
      setMaster("local")
    sc = new SparkContext(conf)

    super.beforeAll()
  }

  override def afterAll() {
    if (sc != null) sc.stop()
    super.afterAll()
  }

  private def loadTestLog4jConfig(): Unit = {
    val props = new Properties
    props.load(getClass.getResourceAsStream("/log4j.properties"))
    PropertyConfigurator.configure(props)
  }
}
