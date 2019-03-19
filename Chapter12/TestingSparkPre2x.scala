package com.tomekl007.chapter_6

import com.tomekl007.UserTransaction
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.FunSuite

class TestingSparkPre2x extends FunSuite {
  val spark = SparkSession.builder().master("local[2]").getOrCreate()


  ignore("loading data on prod from hive") {
    UserDataLogicPre2.loadAndGetAmount(spark, HiveDataLoaderPre2.loadUserTransactions)
  }

  test("mock loading data from hive"){
    //given
    import spark.sqlContext.implicits._
    val df = spark.sparkContext
      .makeRDD(List(UserTransaction("a", 100), UserTransaction("b", 200)))
      .toDF()
      .rdd

    //when
    val res = UserDataLogicPre2.loadAndGetAmount(spark, _ => df)

    //then
    println(res.collect().toList)
  }

}

object UserDataLogicPre2 {
  def loadAndGetAmount(sparkSession: SparkSession, provider: SparkSession => RDD[Row]): RDD[Int] = {
    provider(sparkSession).map(_.getAs[Int]("amount"))
  }
}

object HiveDataLoaderPre2 {
  def loadUserTransactions(sparkSession: SparkSession): RDD[Row] = {
    sparkSession.sql("select * from transactions").rdd
  }
}

