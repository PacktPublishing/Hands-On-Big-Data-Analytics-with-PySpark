package com.tomekl007.chapter_5

import com.tomekl007.UserTransaction
import org.apache.spark.sql.SparkSession
import org.apache.spark.{HashPartitioner, Partitioner, RangePartitioner, SparkContext}
import org.scalatest.FunSuite

class CustomRangePartitionerTest extends FunSuite {
  val spark: SparkContext = SparkSession.builder().master("local[2]").getOrCreate().sparkContext

  test("should use custom range partitioner") {
    //given
    val keysWithValuesList =
      Array(
        UserTransaction("A", 100),
        UserTransaction("B", 4),
        UserTransaction("A", 100001),
        UserTransaction("B", 10),
        UserTransaction("C", 10)
      )
    val data = spark.parallelize(keysWithValuesList)
    val keyed = data.keyBy(_.amount)

    //when, then
    val partitioned = keyed.partitionBy(new CustomRangePartitioner(List((0,100), (100, 10000), (10000, 1000000))))

    //then
    partitioned.collect().toList
  }
}

class CustomRangePartitioner(ranges: List[(Int,Int)]) extends Partitioner{
  override def numPartitions: Int = ranges.size

  override def getPartition(key: Any): Int = {
    if(!key.isInstanceOf[Int]){
      throw new IllegalArgumentException("partitioner works only for Int type")
    }
    val keyInt = key.asInstanceOf[Int]
    val index = ranges.lastIndexWhere(v => keyInt >= v._1 && keyInt <= v._2)
    println(s"for key: $key return $index")
    index
  }
}