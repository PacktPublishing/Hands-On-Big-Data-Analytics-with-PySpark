package com.tomekl007.chapter_1

import com.tomekl007.UserTransaction
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite
import org.scalatest.Matchers._

class ReduceAPI extends FunSuite {
  val spark: SparkContext = SparkSession.builder().master("local[2]").getOrCreate().sparkContext


  test("should use reduce API") {
    //given
    val input = spark.makeRDD(List(
      UserTransaction("A", 10),
      UserTransaction("B", 1),
      UserTransaction("A", 101)
    ))

    //when
    val result = input
      .map(_.amount)
      .reduce((a, b) => if (a > b) a else b)

    //then
    assert(result == 101)
  }

  test("should use reduceByKey API") {
    //given
    val input = spark.makeRDD(
      List(
        UserTransaction("A", 10),
        UserTransaction("B", 1),
        UserTransaction("A", 101)
      )
    )

    //when
    val result = input
      .keyBy(_.userId)
      .reduceByKey((firstTransaction, secondTransaction) =>
        TransactionChecker.higherTransactionAmount(firstTransaction, secondTransaction))
      .collect()
      .toList

    //then
    result should contain theSameElementsAs
      List(("B", UserTransaction("B", 1)), ("A", UserTransaction("A", 101)))
  }

}

object TransactionChecker {
  def higherTransactionAmount(firstTransaction: UserTransaction, secondTransaction: UserTransaction): UserTransaction = {
    if (firstTransaction.amount > secondTransaction.amount) firstTransaction else secondTransaction
  }
}

