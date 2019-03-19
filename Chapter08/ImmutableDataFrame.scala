package com.tomekl007.chapter_2


import com.tomekl007.UserData
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

class ImmutableDataFrame extends FunSuite {
  val spark: SparkSession = SparkSession
    .builder().master("local[2]").getOrCreate()

  test("Should use immutable DF API") {
    import spark.sqlContext.implicits._
    //given
    val userData =
      spark.sparkContext.makeRDD(List(
        UserData("a", "1"),
        UserData("b", "2"),
        UserData("d", "200")
      )).toDS()

    //when
    val res = userData.filter(userData("userId").isin("a"))


    assert(res.count() == 1)
    assert(userData.count() == 3)


  }
}

