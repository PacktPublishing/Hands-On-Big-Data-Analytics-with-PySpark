package com.tomekl007.chapter_4

import com.tomekl007.UserTransaction
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.scalatest.Matchers._
import org.scalatest.{BeforeAndAfterEach, FunSuite}

import scala.reflect.io.Path

class SaveJSON extends FunSuite with BeforeAndAfterEach {
  val spark = SparkSession.builder().master("local[2]").getOrCreate()

  private val FileName = "transactions.json"

  override def afterEach() {
    val path = Path(FileName)
    path.deleteRecursively()
  }

  test("should save and load in JSON") {
    //given
    import spark.sqlContext.implicits._
    val rdd = spark.sparkContext
      .makeRDD(List(UserTransaction("a", 100), UserTransaction("b", 200)))
      .toDF()

    //when
    rdd.coalesce(1).write.format("json").save(FileName)

    val fromFile = spark.read.json(FileName)

    fromFile.show()
    assert(fromFile.count() == 2)
  }
}
