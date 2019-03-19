package com.tomekl007.chapter_4

import com.tomekl007.UserTransaction
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterEach, FunSuite}
import com.databricks.spark.avro._

import scala.reflect.io.Path

class SaveAvro extends FunSuite with BeforeAndAfterEach {
  val spark = SparkSession.builder().master("local[2]").getOrCreate()

  private val FileName = "transactions.avro"

  override def afterEach() {
    val path = Path(FileName)
    path.deleteRecursively()
  }

  test("should save and load avro") {
    //given
    import spark.sqlContext.implicits._
    val rdd = spark.sparkContext
      .makeRDD(List(UserTransaction("a", 100), UserTransaction("b", 200)))
      .toDF()

    //when
    rdd.coalesce(2)
      .write
      .avro(FileName)

    val fromFile = spark.read.avro(FileName)

    fromFile.show()
    assert(fromFile.count() == 2)
  }

}
