package com.tomekl007.chapter_3

import com.tomekl007.UserTransaction
import org.apache.spark.sql.SparkSession
import org.apache.spark.{Partitioner, SparkContext}
import org.scalatest.FunSuite
import org.scalatest.Matchers._

class CustomPartitioner extends FunSuite {
  val spark: SparkContext = SparkSession.builder().master("local[2]").getOrCreate().sparkContext

  test("should use custom partitioner") {
    //given
    val numberOfExecutors = 2
    val data = spark
      .parallelize(List(
        UserTransaction("a", 100),
        UserTransaction("b", 101),
        UserTransaction("a", 202),
        UserTransaction("b", 1),
        UserTransaction("c", 55)
      )
      ).keyBy(_.userId)
      .partitionBy(new Partitioner {
        override def numPartitions: Int = numberOfExecutors

        override def getPartition(key: Any): Int = {
          key.hashCode % numberOfExecutors
        }
      })

    println(data.partitions.length)

    //when
    val res = data.mapPartitions[Long](iter =>
      iter.map(_._2).map(_.amount)
    ).collect().toList

    //then
    res should contain theSameElementsAs List(55, 100, 202, 101, 1)
  }
}
