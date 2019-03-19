package com.tomekl007.chapter_6

import org.scalacheck.Properties
import org.scalacheck.Prop.forAll

object PropertyBasedTesting extends Properties("StringType") {

  property("length of strings") = forAll { (a: String, b: String) =>
    a.length + b.length >= a.length
  }

  property("creating list of strings") = forAll { (a: String, b: String, c: String) =>
    List(a,b,c).map(_.length).sum == a.length + b.length + c.length
  }

}