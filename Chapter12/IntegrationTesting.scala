package com.tomekl007.chapter_6

import com.tomekl007.UserTransaction
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite
import org.scalatest.Matchers._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class IntegrationTesting extends FunSuite {
  val spark: SparkContext = SparkSession.builder().master("local[2]").getOrCreate().sparkContext


  test("Integration testing of already unit-tested logic") {
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


    //when
    val aggregatedTransactionsForUserId = data.filter(BonusVerifier.qualifyForBonus)

    //then
    aggregatedTransactionsForUserId.collect().toList should contain theSameElementsAs List(
      UserTransaction("A", 100001)
    )
  }
}

