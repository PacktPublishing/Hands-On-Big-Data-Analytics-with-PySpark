package com.tomekl007.chapter_3


import com.tomekl007.InputRecord
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

class KeyByTesting extends FunSuite {
  val spark: SparkSession = SparkSession.builder().master("local[2]").getOrCreate()


  test("Should use keyBy to distribute traffic properly"){
    //given
    val rdd = spark.sparkContext.makeRDD(List(
      InputRecord("1234-3456-1235-1234", "user_1"),
      InputRecord("1123-3456-1235-1234", "user_1"),
      InputRecord("1123-3456-1235-9999", "user_2")
    ))

    //at this point data is spread randomly, records for the same user_id can be on different executors
    //when
    rdd.keyBy(_.userId)//shuffle -> all records for the same user_id go to the same executor

    //other operations that are key-wise doesn't require shuffle
    rdd.collect()
  }

}