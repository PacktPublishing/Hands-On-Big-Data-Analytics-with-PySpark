package com.tomekl007.chapter_5

import com.tomekl007.UserTransaction
import org.apache.spark.{HashPartitioner, RangePartitioner, SparkContext}
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite
import org.scalatest.Matchers._

class UsePartitioner extends FunSuite {
  val spark: SparkContext = SparkSession.builder().master("local[2]").getOrCreate().sparkContext

  test("should use different partitioners") {
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

    //when, then
    val partitioner = keyed.partitioner
    assert(partitioner.isEmpty)

    val hashPartitioner = keyed.partitionBy(new HashPartitioner(100))
    println(hashPartitioner)
    assert(hashPartitioner.partitioner.isDefined)

    val rangePartitioner = keyed.partitionBy(new RangePartitioner(100, keyed))
    println(rangePartitioner)
    assert(rangePartitioner.partitioner.isDefined)

  }
}
