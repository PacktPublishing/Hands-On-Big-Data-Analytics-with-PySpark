package com.tomekl007.chapter_4

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.scalatest.FunSuite

class ReUseWithCheckpoint extends FunSuite {
  private val spark: SparkContext = SparkSession.builder().master("local[2]").getOrCreate().sparkContext
  private val checkpointEnabled = true
  private val storageLevel = StorageLevel.MEMORY_AND_DISK

  test("should use checkpoint for re-usability of RDD") {
    //given
    val sortedRDD = spark.makeRDD(List(1, 2, 5, 77, 888))

    if (storageLevel != StorageLevel.NONE) {
      sortedRDD.persist(storageLevel)
    }
    if (checkpointEnabled) {
      sortedRDD.sparkContext.setCheckpointDir("hdfs://tmp/checkpoint")
      sortedRDD.checkpoint()
    }

    //when
    performALotOfExpensiveComputations(sortedRDD)

    //then
    sortedRDD.collect().toList
  }

  def performALotOfExpensiveComputations(sortedRDD: RDD[Int]): Unit = {
    //....
    sortedRDD.count()
    //failure
    sortedRDD.collect()
  }
}


