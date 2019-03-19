package com.tomekl007.chapter_4

import java.io.File

import com.tomekl007.UserTransaction
import org.apache.spark.sql.SparkSession
import org.apache.spark.{Partitioner, SparkContext}
import org.scalatest.{BeforeAndAfterEach, FunSuite}
import org.scalatest.Matchers._

import scala.reflect.io.Path

class SavePlainText extends FunSuite with BeforeAndAfterEach{
  val spark: SparkContext = SparkSession.builder().master("local[2]").getOrCreate().sparkContext

  private val FileName = "transactions.txt"

  override def afterEach() {
    val path = Path (FileName)
    path.deleteRecursively()
  }

  test("should save and load in plain text") {
    //given
    val rdd = spark.makeRDD(List(UserTransaction("a", 100), UserTransaction("b", 200)))

    //when
    rdd.coalesce(1).saveAsTextFile(FileName)

    val fromFile = spark.textFile(FileName)

    fromFile.collect().toList should contain theSameElementsAs List(
      "UserTransaction(a,100)", "UserTransaction(b,200)"
      //note - this is string!
    )
  }
}
