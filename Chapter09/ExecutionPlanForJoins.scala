package com.tomekl007.chapter_3

import org.apache.spark.sql.SparkSession
import org.apache.spark.{HashPartitioner, SparkContext}
import org.scalatest.FunSuite
import org.scalatest.Matchers._

class ExecutionPlanForJoins extends FunSuite {
  val spark: SparkContext = SparkSession.builder().master("local[2]").getOrCreate().sparkContext
  test("should use custom partitioner while join") {
    //given
    val transactions = spark.makeRDD(List((1, "bag"), (2, "dog"), (4, "car")))
    val persons = spark.makeRDD(List((1, "Tom"), (2, "Michael"), (3, "Johnny")))

    //when
    val personsDataPartitioner = transactions.partitioner match {
      case Some(p) => p
      case None => new HashPartitioner(persons.partitions.length)
    }


    val res = persons.join(transactions, personsDataPartitioner).collect().toList

    res should contain theSameElementsAs
      List((2, ("Michael", "dog")), (1, ("Tom", "bag")))
  }

  test("can broadcast small data set to every executor and join in-memory") {
    //given
    val smallDataSet = spark.makeRDD(List((1, "bag"), (2, "dog"), (4, "car")))
    val hugeDataSet = spark.makeRDD(List((1, "Tom"), (2, "Michael"), (3, "Johnny")))

    //when broadcast small rdd to all executors
    val smallInMemoryDataSet = spark.broadcast(smallDataSet.collectAsMap())

    //then join will not need to do shuffle
    val res = hugeDataSet.mapPartitions(iter => {
      iter.flatMap {
        case (k, v1) => smallInMemoryDataSet.value.get(k) match {
          case None => Seq.empty
          case Some(v2) => Seq((k, (v1, v2)))
        }
      }
    })

    res.collect().toList should contain theSameElementsAs
      List((2, ("Michael", "dog")), (1, ("Tom", "bag")))
  }

}


