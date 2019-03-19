package com.tomekl007.chapter_5

import com.tomekl007.UserTransaction
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite
import org.scalatest.Matchers._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class TransformationsOnPairs extends FunSuite {
  val spark: SparkContext = SparkSession.builder().master("local[2]").getOrCreate().sparkContext

  test("should use transformation on k/v pair") {
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

    //when
    val counted  = keyed.countByKey()
//    keyed.combineByKey()
//    keyed.aggregateByKey()
//    keyed.foldByKey()
//    keyed.groupByKey()

    //then
    counted should contain theSameElementsAs Map("B" -> 2, "A" -> 2, "C" -> 1)

  }
}
