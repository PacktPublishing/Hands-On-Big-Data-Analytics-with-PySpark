package com.tomekl007.chapter_7

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Graph, GraphLoader}

object GraphBuilder {

  def loadFromFile(sc: SparkContext, path: String): Graph[Int, Int] = {
    GraphLoader.edgeListFile(sc, path)
  }
}
