package com.tomekl007.chapter_2

import com.example.{MultipliedRDD, Record}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite
import org.scalatest.Matchers._

class InheritanceRdd extends FunSuite {
  val spark: SparkContext = SparkSession
    .builder().master("local[2]").getOrCreate().sparkContext

  test("use extended RDD") {
    //given
    val rdd = spark.makeRDD(List(Record(1, "d1")))
    val extendedRdd = new MultipliedRDD(rdd, 10)

    extendedRdd.collect().toList should contain theSameElementsAs List(
      Record(10, "d1")
    )
  }

}
