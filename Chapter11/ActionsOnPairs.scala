package com.tomekl007.chapter_5

import com.tomekl007.UserTransaction
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite
import org.scalatest.Matchers._

class ActionsOnPairs extends FunSuite {
  val spark: SparkContext = SparkSession.builder().master("local[2]").getOrCreate().sparkContext

  test("should use action to see k/v data format after collect") {
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
    val res = keyed.collect().toList

    //then
    res should contain theSameElementsAs List(
      ("A",UserTransaction("A",100)),
      ("B",UserTransaction("B",4)),
      ("A",UserTransaction("A",100001)),
      ("B",UserTransaction("B",10)),
      ("C",UserTransaction("C",10))
    )//note duplicated key

  }
}
