package com.tomekl007.chapter_1

import com.tomekl007.UserTransaction
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

class TriggerComputationsReusingRDD extends FunSuite {
  val spark: SparkContext = SparkSession.builder().master("local[2]").getOrCreate().sparkContext


  test("should trigger computations using actions without reuse") {
    //given
    val input = spark.makeRDD(
      List(
        UserTransaction(userId = "A", amount = 1001),
        UserTransaction(userId = "A", amount = 100),
        UserTransaction(userId = "A", amount = 102),
        UserTransaction(userId = "A", amount = 1),
        UserTransaction(userId = "B", amount = 13)))

    //when apply transformation
    val rdd = input
      .filter(_.userId.contains("A"))
      .keyBy(_.userId)
      .map(_._2.amount)


    //then every call to action means that we are going up to the RDD chain
    //if we are loading data from external file-system (I.E.: HDFS), every action means
    //that we need to load it from FS.
    val start = System.currentTimeMillis()
    println(rdd.collect().toList)
    println(rdd.count())
    println(rdd.first())
    rdd.foreach(println(_))
    rdd.foreachPartition(t => t.foreach(println(_)))
    println(rdd.max())
    println(rdd.min())
    println(rdd.takeOrdered(1).toList)
    println(rdd.takeSample(false, 2).toList)
    val result = System.currentTimeMillis() - start

    println(s"time taken (no-cache): $result")


  }


  test("should trigger computations using actions with reuse") {
    //given
    val input = spark.makeRDD(
      List(
        UserTransaction(userId = "A", amount = 1001),
        UserTransaction(userId = "A", amount = 100),
        UserTransaction(userId = "A", amount = 102),
        UserTransaction(userId = "A", amount = 1),
        UserTransaction(userId = "B", amount = 13)))

    //when apply transformation
    val rdd = input
      .filter(_.userId.contains("A"))
      .keyBy(_.userId)
      .map(_._2.amount)
      .cache()


    //then every call to action means that we are going up to the RDD chain
    //if we are loading data from external file-system (I.E.: HDFS), every action means
    //that we need to load it from FS.
    val start = System.currentTimeMillis()
    println(rdd.collect().toList)
    println(rdd.count())
    println(rdd.first())
    rdd.foreach(println(_))
    rdd.foreachPartition(t => t.foreach(println(_)))
    println(rdd.max())
    println(rdd.min())
    println(rdd.takeOrdered(1).toList)
    println(rdd.takeSample(false, 2).toList)
    val result = System.currentTimeMillis() - start

    println(s"time taken(cache): $result")


  }
}

