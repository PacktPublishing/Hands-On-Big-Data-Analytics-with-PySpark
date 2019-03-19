package com.tomekl007.chapter_2

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite
import org.scalatest.Matchers._

class ImmutableRDD extends FunSuite {
  val spark: SparkContext = SparkSession
    .builder().master("local[2]").getOrCreate().sparkContext

  test("RDD should be immutable") {
    //given
    val data = spark.makeRDD(0 to 5)

    //when
    val res = data.map(_ * 2)

    //then
    res.collect().toList should contain theSameElementsAs List(
      0, 2, 4, 6, 8, 10
    )

    data.collect().toList should contain theSameElementsAs List(
      0, 1, 2, 3, 4, 5
    )

  }

}
