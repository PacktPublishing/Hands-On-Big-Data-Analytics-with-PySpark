package com.tomekl007.chapter_7

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

class CreatingGraph extends FunSuite {
  val spark: SparkContext = SparkSession.builder().master("local[2]").getOrCreate().sparkContext

  test("should load graph from a file") {
    //given
    val path = getClass.getResource("/graph.g").getPath

    //when
    val graph = GraphBuilder.loadFromFile(spark, path)

    //then
    graph.triplets.foreach(println(_))
    assert(graph.triplets.count() == 4)
  }

}
