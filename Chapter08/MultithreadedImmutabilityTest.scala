package com.tomekl007.chapter_2

import java.util.concurrent.{CountDownLatch, Executors}

import org.scalatest.FunSuite

import scala.collection.mutable.ListBuffer

class MultithreadedImmutabilityTest extends FunSuite {

  test("warning: race condition with mutability") {
    //given
    var listMutable = new ListBuffer[String]()
    val executors = Executors.newFixedThreadPool(2)
    val latch = new CountDownLatch(2)

    //when
    executors.submit(new Runnable {
      override def run(): Unit = {
        latch.countDown()
        listMutable += "A"
      }
    })

    executors.submit(new Runnable {
      override def run(): Unit = {
        latch.countDown()
        if(!listMutable.contains("A")) {
          listMutable += "A"
        }
      }
    })

    latch.await()

    //then
    //listMutable can have ("A") or ("A","A")

  }

}
