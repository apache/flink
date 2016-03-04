package org.apache.flink.api.table.codegen.calls

import org.apache.flink.api.table.codegen.calls.ScalarOperators.generateNot
import org.apache.flink.api.table.codegen.{GeneratedExpression, CodeGenerator}

/**
  * Inverts the boolean value of a CallGenerator result.
  */
class NotCallGenerator(callGenerator: CallGenerator) extends CallGenerator {

  override def generate(
      codeGenerator: CodeGenerator,
      operands: Seq[GeneratedExpression])
    : GeneratedExpression = {
    val expr = callGenerator.generate(codeGenerator, operands)
    generateNot(codeGenerator.nullCheck, expr)
  }

}
