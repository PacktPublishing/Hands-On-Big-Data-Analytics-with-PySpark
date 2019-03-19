package com.tomekl007.chapter_6

import com.tomekl007.UserTransaction
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{FunSuite, Ignore}

class MockingDataSources extends FunSuite {
  val spark = SparkSession.builder().master("local[2]").getOrCreate()


  ignore("loading data on prod from hive") {
    UserDataLogic.loadAndGetAmount(spark, HiveDataLoader.loadUserTransactions)
  }

  test("mock loading data from hive"){
    //given
    import spark.sqlContext.implicits._
    val df = spark.sparkContext
      .makeRDD(List(UserTransaction("a", 100), UserTransaction("b", 200)))
      .toDF()

    //when
    val res = UserDataLogic.loadAndGetAmount(spark, _ => df)

    //then
    res.show()
  }

}

object UserDataLogic {
  def loadAndGetAmount(sparkSession: SparkSession, provider: SparkSession => DataFrame): DataFrame = {
    val df = provider(sparkSession)
    df.select(df("amount"))
  }
}

object HiveDataLoader {
  def loadUserTransactions(sparkSession: SparkSession): DataFrame = {
    sparkSession.sql("select * from transactions")
  }
}

