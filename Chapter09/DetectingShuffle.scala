package com.tomekl007.chapter_3

import com.tomekl007.InputRecord
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

class DetectingShuffle extends FunSuite {
  val spark: SparkSession = SparkSession.builder().master("local[2]").getOrCreate()

  test("should explain plan showing logical and physical with UDF and DF") {
    //given
    import spark.sqlContext.implicits._
    val df = spark.sparkContext.makeRDD(List(
      InputRecord("1234-3456-1235-1234", "user_1"),
      InputRecord("1123-3456-1235-1234", "user_1"),
      InputRecord("1123-3456-1235-9999", "user_2")
    )).toDF()

    //when
    val q = df.repartition(df("userId"))

    q.explain(true)
  }


}
