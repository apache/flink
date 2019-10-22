/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.codegen

import org.apache.flink.table.dataformat._
import org.apache.flink.table.planner.codegen.CodeGenUtils._
import org.apache.flink.table.planner.codegen.GenerateUtils.generateRecordStatement
import org.apache.flink.table.planner.codegen.GeneratedExpression.{NEVER_NULL, NO_CODE}
import org.apache.flink.table.runtime.generated.{GeneratedProjection, Projection}
import org.apache.flink.table.types.logical.{LogicalType, RowType}

import scala.collection.mutable

/**
  * CodeGenerator for projection, Take out some fields of [[BaseRow]] to generate
  * a new [[BaseRow]].
  */
object ProjectionCodeGenerator {

  // for loop optimization will only be enabled
  // if the number of fields to be project exceeds the limit
  //
  // this value is tuned by hand according to projecting
  // some string type fields with randomized order
  val FOR_LOOP_FIELD_LIMIT: Int = 25

  def generateProjectionExpression(
      ctx: CodeGeneratorContext,
      inType: RowType,
      outType: RowType,
      inputMapping: Array[Int],
      outClass: Class[_ <: BaseRow] = classOf[BinaryRow],
      inputTerm: String = DEFAULT_INPUT1_TERM,
      outRecordTerm: String = DEFAULT_OUT_RECORD_TERM,
      outRecordWriterTerm: String = DEFAULT_OUT_RECORD_WRITER_TERM,
      reusedOutRecord: Boolean = true,
      nullCheck: Boolean = true): GeneratedExpression = {

    // we use a for loop to do all the projections for the same field type
    // instead of generating separated code for each field.
    // when the number of fields of the same type is large, this can improve performance.
    def generateLoop(
        fieldType: LogicalType,
        inIdxs: mutable.ArrayBuffer[Int],
        outIdxs: mutable.ArrayBuffer[Int]): String = {
      // this array contains the indices of the fields
      // whose type equals to `fieldType` in the input row
      val inIdxArr = newName("inIdx")
      ctx.addReusableMember(s"int[] $inIdxArr = null;")
      ctx.addReusableInitStatement(s"$inIdxArr = new int[] {${inIdxs.mkString(", ")}};")

      // this array contains the indices of the fields
      // whose type equals to `fieldType` in the output row
      val outIdxArr = newName("outIdx")
      ctx.addReusableMember(s"int[] $outIdxArr = null;")
      ctx.addReusableInitStatement(s"$outIdxArr = new int[] {${outIdxs.mkString(", ")}};")

      val loopIdx = newName("i")

      val fieldVal = CodeGenUtils.baseRowFieldReadAccess(
        ctx, s"$inIdxArr[$loopIdx]", inputTerm, fieldType)

      val inIdx = s"$inIdxArr[$loopIdx]"
      val outIdx = s"$outIdxArr[$loopIdx]"
      val nullTerm = s"$inputTerm.isNullAt($inIdx)"
      s"""
         |for (int $loopIdx = 0; $loopIdx < $inIdxArr.length; $loopIdx++) {
         |  ${CodeGenUtils.baseRowSetField(ctx, outClass, outRecordTerm, outIdx,
                GeneratedExpression(fieldVal, nullTerm, "", fieldType),
                Some(outRecordWriterTerm))}
         |}
       """.stripMargin
    }

    val outFieldTypes = outType.getChildren
    val typeIdxs = new mutable.HashMap[
      LogicalType,
      (mutable.ArrayBuffer[Int], mutable.ArrayBuffer[Int])]()

    for (i <- 0 until outFieldTypes.size()) {
      val (inIdxs, outIdxs) = typeIdxs.getOrElseUpdate(
        outFieldTypes.get(i), (mutable.ArrayBuffer.empty[Int], mutable.ArrayBuffer.empty[Int]))
      inIdxs.append(inputMapping(i))
      outIdxs.append(i)
    }

    val codeBuffer = mutable.ArrayBuffer.empty[String]
    for ((fieldType, (inIdxs, outIdxs)) <- typeIdxs) {
      if (inIdxs.length >= FOR_LOOP_FIELD_LIMIT) {
        // for loop optimization will only be enabled
        // if the number of fields to be project exceeds the limit
        codeBuffer.append(generateLoop(fieldType, inIdxs, outIdxs))
      } else {
        // otherwise we do not use for loop
        for (i <- inIdxs.indices) {
          val nullTerm = s"$inputTerm.isNullAt(${inIdxs(i)})"
          codeBuffer.append(
            CodeGenUtils.baseRowSetField(ctx, outClass, outRecordTerm, outIdxs(i).toString,
              GeneratedExpression(baseRowFieldReadAccess(
                ctx, inIdxs(i), inputTerm, fieldType), nullTerm, "", fieldType),
              Some(outRecordWriterTerm)))
        }
      }
    }

    val setFieldsCode = codeBuffer.mkString("\n")

    val outRowInitCode = {
      val initCode = generateRecordStatement(
        outType, outClass, outRecordTerm, Some(outRecordWriterTerm))
      if (reusedOutRecord) {
        ctx.addReusableMember(initCode)
        NO_CODE
      } else {
        initCode
      }
    }

    val code = if (outClass == classOf[BinaryRow]) {
      val writer = outRecordWriterTerm
      val resetWriter = if (ctx.nullCheck) s"$writer.reset();" else s"$writer.resetCursor();"
      val completeWriter: String = s"$writer.complete();"
      s"""
         |$outRowInitCode
         |$resetWriter
         |$setFieldsCode
         |$completeWriter
        """.stripMargin
    } else {
      s"""
         |$outRowInitCode
         |$setFieldsCode
        """.stripMargin
    }
    GeneratedExpression(outRecordTerm, NEVER_NULL, code, outType)
  }

  /**
    * CodeGenerator for projection.
    * @param reusedOutRecord If objects or variables can be reused, they will be added a reusable
    * output record to the member area of the generated class. If not they will be as temp
    * variables.
    * @return
    */
  def generateProjection(
      ctx: CodeGeneratorContext,
      name: String,
      inType: RowType,
      outType: RowType,
      inputMapping: Array[Int],
      outClass: Class[_ <: BaseRow] = classOf[BinaryRow],
      inputTerm: String = DEFAULT_INPUT1_TERM,
      outRecordTerm: String = DEFAULT_OUT_RECORD_TERM,
      outRecordWriterTerm: String = DEFAULT_OUT_RECORD_WRITER_TERM,
      reusedOutRecord: Boolean = true,
      nullCheck: Boolean = true): GeneratedProjection = {
    val className = newName(name)
    val baseClass = classOf[Projection[_, _]]

    val expression = generateProjectionExpression(
      ctx, inType, outType, inputMapping, outClass,
      inputTerm, outRecordTerm, outRecordWriterTerm, reusedOutRecord, nullCheck)

    val code =
      s"""
         |public class $className implements ${
            baseClass.getCanonicalName}<$BASE_ROW, ${outClass.getCanonicalName}> {
         |
         |  ${ctx.reuseMemberCode()}
         |
         |  public $className(Object[] references) throws Exception {
         |    ${ctx.reuseInitCode()}
         |  }
         |
         |  @Override
         |  public ${outClass.getCanonicalName} apply($BASE_ROW $inputTerm) {
         |    ${ctx.reuseLocalVariableCode()}
         |    ${expression.code}
         |    return ${expression.resultTerm};
         |  }
         |}
        """.stripMargin

    new GeneratedProjection(className, code, ctx.references.toArray)
  }

  /**
    * For java invoke.
    */
  def generateProjection(
      ctx: CodeGeneratorContext,
      name: String,
      inputType: RowType,
      outputType: RowType,
      inputMapping: Array[Int]): GeneratedProjection =
    generateProjection(
      ctx, name, inputType, outputType, inputMapping, inputTerm = DEFAULT_INPUT1_TERM)
}
