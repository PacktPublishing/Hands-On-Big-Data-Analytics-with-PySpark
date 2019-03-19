package com.tomekl007.chapter_1

import com.tomekl007.UserTransaction
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite
import org.scalatest.Matchers._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class GroupByKey extends FunSuite {
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
      .groupBy(_.userId)
      .map(x => (x._1,x._2.toList))
      .collect()
      .toList

    //then
    rdd should contain theSameElementsAs List(
      ("B", List(UserTransaction("B", 13))),
      ("A", List(
        UserTransaction("A", 1001),
        UserTransaction("A", 100),
        UserTransaction("A", 102),
        UserTransaction("A", 1))
      )
    )
  }

}

