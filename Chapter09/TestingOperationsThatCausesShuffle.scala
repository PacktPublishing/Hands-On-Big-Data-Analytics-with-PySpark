package com.tomekl007.chapter_3

import com.tomekl007.{UserData, UserTransaction}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.FunSuite

class TestingOperationsThatCausesShuffle extends FunSuite {
  val spark = SparkSession.builder().master("local[2]").getOrCreate()

  test("example of operation that is causing shuffle") {
    import spark.sqlContext.implicits._
    val userData =
      spark.sparkContext.makeRDD(List(
        UserData("user_1", "1"),
        UserData("user_2", "2"),
        UserData("user_4", "200")
      )).toDS()

    val transactionData =
      spark.sparkContext.makeRDD(List(
        UserTransaction("user_1", 100),
        UserTransaction("user_2", 300),
        UserTransaction("user_3", 1300)
      )).toDS()


    //shuffle: userData can stay on the current executors, but data from
    //transactionData needs to be send to those executors according to joinColumn
    //causing shuffle
    //when
    val res: Dataset[(UserData, UserTransaction)]
    = userData.joinWith(transactionData, userData("userId") === transactionData("userId"), "inner")


    //then
    res.show()
    assert(res.count() == 2)
  }
}
