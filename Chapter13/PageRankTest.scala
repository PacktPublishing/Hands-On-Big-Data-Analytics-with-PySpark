package com.tomekl007.chapter_7

import org.apache.spark.graphx.GraphLoader
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite
import org.scalatest.Matchers._

class PageRankTest extends FunSuite {
  private val sc = SparkSession.builder().master("local[2]").getOrCreate().sparkContext

  test("should calculate page rank using GraphX API") {
    //given
    val graph = GraphLoader.edgeListFile(sc, getClass.getResource("/pagerank/followers.txt").getPath)
    val ranks = graph.pageRank(0.0001).vertices

    val users = sc.textFile(getClass.getResource("/pagerank/users.txt").getPath).map { line =>
      val fields = line.split(",")
      (fields(0).toLong, fields(1))
    }

    //when
    val rankByUsername = users.join(ranks).map {
      case (_, (username, rank)) => (username, rank)
    }.sortBy((t) => t._2, ascending = false)
      .collect()
      .toList

    println(rankByUsername)
    //then
    rankByUsername.map(_._1) should contain theSameElementsInOrderAs List(
      "BarackObama",
      "ladygaga",
      "odersky",
      "jeresig",
      "matei_zaharia",
      "justinbieber"
    )
  }


}
