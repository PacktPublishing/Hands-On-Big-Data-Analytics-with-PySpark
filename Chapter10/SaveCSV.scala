package com.tomekl007.chapter_4

import com.tomekl007.UserTransaction
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterEach, FunSuite}

import scala.reflect.io.Path

class SaveCSV extends FunSuite with BeforeAndAfterEach {
  val spark = SparkSession.builder().master("local[2]").getOrCreate()

  private val FileName = "transactions.csv"

  override def afterEach() {
    val path = Path(FileName)
    path.deleteRecursively()
  }

  test("should save and load CSV with header") {
    //given
    import spark.sqlContext.implicits._
    val rdd = spark.sparkContext
      .makeRDD(List(UserTransaction("a", 100), UserTransaction("b", 200)))
      .toDF()

    //when
    rdd.coalesce(1)
      .write
      .format("csv")
      .option("header", "true")
      .save(FileName)

    val fromFile = spark.read.option("header", "true").csv(FileName)

    fromFile.show()
    assert(fromFile.count() == 2)
  }

  test("should save and load CSV without header") {
    //given
    import spark.sqlContext.implicits._
    val rdd = spark.sparkContext
      .makeRDD(List(UserTransaction("a", 100), UserTransaction("b", 200)))
      .toDF()

    //when
    rdd.coalesce(1)
      .write
      .format("csv")
      .option("header", "false")
      .save(FileName)

    val fromFile = spark.read.option("header", "false").csv(FileName)

    fromFile.show()
    assert(fromFile.count() == 2)
  }
}
