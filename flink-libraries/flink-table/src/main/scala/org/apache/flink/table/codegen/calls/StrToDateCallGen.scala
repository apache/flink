package org.apache.flink.table.codegen.calls

import org.apache.flink.table.codegen.{CodeGenerator, GeneratedExpression}
import org.apache.flink.api.common.typeinfo.BasicTypeInfo._
import org.apache.flink.table.codegen.calls.CallGenerator.generateCallIfArgsNotNull

class StrToDateCallGen extends CallGenerator {
  override def generate(codeGenerator: CodeGenerator, operands: Seq[GeneratedExpression]) = {
    generateCallIfArgsNotNull(codeGenerator.nullCheck, STRING_TYPE_INFO, operands) {
      terms => s"""
                  |org.apache.flink.table.runtime.functions.
                  |DateTimeFunctions$$.MODULE$$.strToDate(${terms.head}, ${terms.last});
          """.stripMargin
    }
  }
}
