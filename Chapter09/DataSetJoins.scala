package com.tomekl007.chapter_3

import com.tomekl007.{UserData, UserTransaction}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.FunSuite

class DataSetJoins extends FunSuite {
  val spark = SparkSession.builder().master("local[2]").getOrCreate()

  test("Should inner join two DS") {
    import spark.sqlContext.implicits._
    val userData =
      spark.sparkContext.makeRDD(List(
        UserData("a", "1"),
        UserData("b", "2"),
        UserData("d", "200")
      )).toDS()

    val transactionData =
      spark.sparkContext.makeRDD(List(
        UserTransaction("a", 100),
        UserTransaction("b", 300),
        UserTransaction("c", 1300)
      )).toDS()

    //when
    val res: Dataset[(UserData, UserTransaction)]
    = userData.joinWith(transactionData, userData("userId") === transactionData("userId"), "inner")


    //then
    res.show()
    assert(res.count() == 2)
  }

  test("Should left join two DS") {
    import spark.sqlContext.implicits._
    val userData =
      spark.sparkContext.makeRDD(List(
        UserData("a", "1"),
        UserData("b", "2"),
        UserData("d", "200")
      )).toDS()

    val transactionData =
      spark.sparkContext.makeRDD(List(
        UserTransaction("a", 100),
        UserTransaction("b", 300),
        UserTransaction("c", 1300)
      )).toDS()

    //when
    val res = userData
      .joinWith(transactionData, userData("userId") === transactionData("userId"), "left_outer")


    //then
    res.show()
    assert(res.count() == 3)
  }


  test("Should right join two DS") {
    import spark.sqlContext.implicits._
    val userData =
      spark.sparkContext.makeRDD(List(
        UserData("a", "1"),
        UserData("b", "2"),
        UserData("d", "200")
      )).toDS()

    val transactionData =
      spark.sparkContext.makeRDD(List(
        UserTransaction("a", 100),
        UserTransaction("b", 300),
        UserTransaction("c", 1300)
      )).toDS()

    //when
    val res = userData
      .joinWith(transactionData, userData("userId") === transactionData("userId"), "right_outer")


    //then
    res.show()
    assert(res.count() == 3)
  }

  test("Should full outer join two DS") {
    import spark.sqlContext.implicits._
    val userData =
      spark.sparkContext.makeRDD(List(
        UserData("a", "1"),
        UserData("b", "2"),
        UserData("d", "200")
      )).toDS()

    val transactionData =
      spark.sparkContext.makeRDD(List(
        UserTransaction("a", 100),
        UserTransaction("b", 300),
        UserTransaction("c", 1300)
      )).toDS()

    //when
    val res = userData
      .joinWith(transactionData, userData("userId") === transactionData("userId"), "full_outer")


    //then
    res.show()
    assert(res.count() == 4)
  }


}
