package com.tomekl007.chapter_5

import com.tomekl007.UserTransaction
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite
import org.scalatest.Matchers._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class AggregateByKey extends FunSuite {
  val spark: SparkContext = SparkSession.builder().master("local[2]").getOrCreate().sparkContext


  test("should use aggregateByKey instead of groupBy to reduce shuffle") {
    //given
    val keysWithValuesList =
      Array(
        UserTransaction("A", 100),
        UserTransaction("B", 4),
        UserTransaction("A", 100001),
        UserTransaction("B", 10),
        UserTransaction("C", 10)
      )
    val data = spark.parallelize(keysWithValuesList)
    val keyed = data.keyBy(_.userId)

    val amountForUser = mutable.ArrayBuffer.empty[Long]
    val addAmount = (responseTimes: mutable.ArrayBuffer[Long], transaction: UserTransaction) => responseTimes += transaction.amount
    val mergeAmounts = (p1: mutable.ArrayBuffer[Long], p2: mutable.ArrayBuffer[Long]) => p1 ++= p2

    //when
    val aggregatedTransactionsForUserId = keyed
      .aggregateByKey(amountForUser)(addAmount, mergeAmounts)

    //then
    aggregatedTransactionsForUserId.collect().toList should contain theSameElementsAs List(
      ("A", ArrayBuffer(100, 100001)),
      ("B", ArrayBuffer(4,10)),
      ("C", ArrayBuffer(10)))


  }
}
