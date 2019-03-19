package com.tomekl007.chapter_3


import com.tomekl007.{UserData, UserTransaction}
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

class DataFrameJoins extends FunSuite {
  val spark = SparkSession.builder().master("local[2]").getOrCreate()

  test("Should inner join two DF") {
    import spark.sqlContext.implicits._
    val userData =
      spark.sparkContext.makeRDD(List(
        UserData("a", "1"),
        UserData("b", "2"),
        UserData("d", "200")
      )).toDF()

    val transactionData =
      spark.sparkContext.makeRDD(List(
        UserTransaction("a", 100),
        UserTransaction("b", 300),
        UserTransaction("c", 1300)
      )).toDF()

    //when
    val res = userData.join(transactionData, userData("userId") === transactionData("userId"), "inner")


    //then
    res.show()
    assert(res.count() == 2)
  }

  test("Should left join two DF") {
    import spark.sqlContext.implicits._
    val userData =
      spark.sparkContext.makeRDD(List(
        UserData("a", "1"),
        UserData("b", "2"),
        UserData("d", "200")
      )).toDF()

    val transactionData =
      spark.sparkContext.makeRDD(List(
        UserTransaction("a", 100),
        UserTransaction("b", 300),
        UserTransaction("c", 1300)
      )).toDF()

    //when
    val res = userData.join(transactionData, userData("userId") === transactionData("userId"), "left_outer")


    //then
    res.show()
    assert(res.count() == 3)
  }


  test("Should right join two DF") {
    import spark.sqlContext.implicits._
    val userData =
      spark.sparkContext.makeRDD(List(
        UserData("a", "1"),
        UserData("b", "2"),
        UserData("d", "200")
      )).toDF()

    val transactionData =
      spark.sparkContext.makeRDD(List(
        UserTransaction("a", 100),
        UserTransaction("b", 300),
        UserTransaction("c", 1300)
      )).toDF()

    //when
    val res = userData.join(transactionData, userData("userId") === transactionData("userId"), "right_outer")


    //then
    res.show()
    assert(res.count() == 3)
  }

  test("Should full outer join two DF") {
    import spark.sqlContext.implicits._
    val userData =
      spark.sparkContext.makeRDD(List(
        UserData("a", "1"),
        UserData("b", "2"),
        UserData("d", "200")
      )).toDF()

    val transactionData =
      spark.sparkContext.makeRDD(List(
        UserTransaction("a", 100),
        UserTransaction("b", 300),
        UserTransaction("c", 1300)
      )).toDF()

    //when
    val res = userData.join(transactionData, userData("userId") === transactionData("userId"), "full_outer")


    //then
    res.show()
    assert(res.count() == 4)
  }


}
