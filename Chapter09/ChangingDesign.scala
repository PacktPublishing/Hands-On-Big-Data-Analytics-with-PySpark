package com.tomekl007.chapter_3

import com.tomekl007.{UserData, UserTransaction}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.FunSuite

class ChangingDesign extends FunSuite {
  val spark = SparkSession.builder().master("local[2]").getOrCreate()

  test("example of operation that is causing shuffle") {
    import spark.sqlContext.implicits._
    val userData =
      spark.sparkContext.makeRDD(List(
        UserData("user_1", "1"),
        UserData("user_2", "2"),
        UserData("user_4", "200")
      )).toDS()
    val repartitionedUserData = userData.repartition(userData("userId"))

    val transactionData =
      spark.sparkContext.makeRDD(List(
        UserTransaction("user_1", 100),
        UserTransaction("user_2", 300),
        UserTransaction("user_3", 1300)
      )).toDS()

    val repartitionedTransactionData = transactionData.repartition(transactionData("userId"))


    //when
    //data is already partitioned using join-column. Don't need to shuffle
    val res: Dataset[(UserData, UserTransaction)]
    = repartitionedUserData.joinWith(repartitionedTransactionData, userData("userId") === transactionData("userId"), "inner")


		//then
		res.show()
		assert(res.count() == 2)
	  }
	}
