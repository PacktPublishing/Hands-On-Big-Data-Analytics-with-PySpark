package com.tomekl007.chapter_1

import com.tomekl007.InputRecord
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

class DeferComputations extends FunSuite {
  val spark: SparkContext = SparkSession.builder().master("local[2]").getOrCreate().sparkContext


  test("should defer computations") {
    //given
    val input = spark.makeRDD(
      List(InputRecord(userId = "A"),
        InputRecord(userId = "B")))

    //when apply transformation
    val rdd = input
      .filter(_.userId.contains("A"))
      .keyBy(_.userId)
      .map(_._2.userId.toLowerCase)
    //.... built processing graph lazy

    if (shouldExecutePartOfCode()) {
      //rdd.saveAsTextFile("") ||
      rdd.collect().toList
    } else {
      //condition changed - don't need to evaluate DAG
    }

  }


  private def shouldExecutePartOfCode(): Boolean = {
    //domain logic that decide if we still need to calculate
    true
  }
}

