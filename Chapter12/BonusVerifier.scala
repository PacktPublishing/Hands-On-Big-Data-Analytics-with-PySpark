package com.tomekl007.chapter_6

import com.tomekl007.UserTransaction

object BonusVerifier {
  private val superUsers = List("A", "X", "100-million")

  def qualifyForBonus(userTransaction: UserTransaction): Boolean = {
    superUsers.contains(userTransaction.userId) && userTransaction.amount > 100
  }
}
