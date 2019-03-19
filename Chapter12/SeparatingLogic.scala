package com.tomekl007.chapter_6

import com.tomekl007.UserTransaction
import org.scalatest.FunSuite

class SeparatingLogic extends FunSuite {

  test("test complex logic separately from spark engine") {
    //given
    val userTransaction = UserTransaction("X", 101)

    //when
    val res = BonusVerifier.qualifyForBonus(userTransaction)

    //then
    assert(res)
  }
}

