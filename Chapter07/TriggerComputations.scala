package com.tomekl007.chapter_1

import com.tomekl007.UserTransaction
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

class TriggerComputations extends FunSuite {
  val spark: SparkContext = SparkSession.builder().master("local[2]").getOrCreate().sparkContext


  test("should trigger computations using actions") {
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


    //then
    println(rdd.collect().toList)
    println(rdd.count()) //and all count*
    println(rdd.first())
    rdd.foreach(println(_))
    rdd.foreachPartition(t => t.foreach(println(_)))
    println(rdd.max())
    println(rdd.min())
    println(rdd.takeOrdered(1).toList)
    println(rdd.takeSample(false, 2).toList)

    //all reduce will be covered in separate video


  }

}

