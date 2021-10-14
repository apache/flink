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

import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.planner.codegen.CodeGenUtils._
import org.apache.flink.table.planner.codegen.Indenter.toISC
import org.apache.flink.table.planner.codegen.calls.ScalarOperatorGens.generateEquals
import org.apache.flink.table.runtime.generated.{GeneratedRecordEqualiser, RecordEqualiser}
import org.apache.flink.table.runtime.types.PlannerTypeUtils
import org.apache.flink.table.types.logical.LogicalTypeRoot._
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks.{getFieldTypes, isCompositeType}
import org.apache.flink.table.types.logical.{DistinctType, LogicalType, RowType}

import scala.annotation.tailrec
import scala.collection.JavaConverters._

class EqualiserCodeGenerator(fieldTypes: Array[LogicalType]) {

  private val RECORD_EQUALISER = className[RecordEqualiser]
  private val LEFT_INPUT = "left"
  private val RIGHT_INPUT = "right"

  def this(rowType: RowType) = {
    this(rowType.getChildren.asScala.toArray)
  }

  def generateRecordEqualiser(name: String): GeneratedRecordEqualiser = {
    // ignore time zone
    val ctx = CodeGeneratorContext(new TableConfig)
    val className = newName(name)

    val equalsMethodCodes = for (idx <- fieldTypes.indices) yield generateEqualsMethod(ctx, idx)
    val equalsMethodCalls = for (idx <- fieldTypes.indices) yield {
      val methodName = getEqualsMethodName(idx)
      s"""result = result && $methodName($LEFT_INPUT, $RIGHT_INPUT);"""
    }

    val classCode =
      j"""
        public final class $className implements $RECORD_EQUALISER {
          ${ctx.reuseMemberCode()}

          public $className(Object[] references) throws Exception {
            ${ctx.reuseInitCode()}
          }

          @Override
          public boolean equals($ROW_DATA $LEFT_INPUT, $ROW_DATA $RIGHT_INPUT) {
            if ($LEFT_INPUT instanceof $BINARY_ROW && $RIGHT_INPUT instanceof $BINARY_ROW) {
              return $LEFT_INPUT.equals($RIGHT_INPUT);
            }

            if ($LEFT_INPUT.getRowKind() != $RIGHT_INPUT.getRowKind()) {
              return false;
            }

            boolean result = true;
            ${equalsMethodCalls.mkString("\n")}
            return result;
          }

          ${equalsMethodCodes.mkString("\n")}
        }
      """.stripMargin

    new GeneratedRecordEqualiser(
      className, classCode, ctx.references.toArray, ctx.tableConfig.getConfiguration)
  }

  private def getEqualsMethodName(idx: Int) = s"""equalsAtIndex$idx"""

  private def generateEqualsMethod(ctx: CodeGeneratorContext, idx: Int): String = {
    val methodName = getEqualsMethodName(idx)
    ctx.startNewLocalVariableStatement(methodName)

    val Seq(leftNullTerm, rightNullTerm) = ctx.addReusableLocalVariables(
      ("boolean", "isNullLeft"),
      ("boolean", "isNullRight")
    )

    val fieldType = fieldTypes(idx)
    val fieldTypeTerm = primitiveTypeTermForType(fieldType)
    val Seq(leftFieldTerm, rightFieldTerm) = ctx.addReusableLocalVariables(
      (fieldTypeTerm, "leftField"),
      (fieldTypeTerm, "rightField")
    )

    val leftReadCode = rowFieldReadAccess(ctx, idx, LEFT_INPUT, fieldType)
    val rightReadCode = rowFieldReadAccess(ctx, idx, RIGHT_INPUT, fieldType)

    val (equalsCode, equalsResult) = generateEqualsCode(ctx, fieldType,
      leftFieldTerm, rightFieldTerm, leftNullTerm, rightNullTerm)

    s"""
       |private boolean $methodName($ROW_DATA $LEFT_INPUT, $ROW_DATA $RIGHT_INPUT) {
       |  ${ctx.reuseLocalVariableCode(methodName)}
       |
       |  $leftNullTerm = $LEFT_INPUT.isNullAt($idx);
       |  $rightNullTerm = $RIGHT_INPUT.isNullAt($idx);
       |  if ($leftNullTerm && $rightNullTerm) {
       |    return true;
       |  }
       |
       |  if ($leftNullTerm || $rightNullTerm) {
       |    return false;
       |  }
       |
       |  $leftFieldTerm = $leftReadCode;
       |  $rightFieldTerm = $rightReadCode;
       |  $equalsCode
       |
       |  return $equalsResult;
       |}
      """.stripMargin
  }

  private def generateEqualsCode(ctx: CodeGeneratorContext, fieldType: LogicalType,
                  leftFieldTerm: String, rightFieldTerm: String,
                  leftNullTerm: String, rightNullTerm: String) = {
    // TODO merge ScalarOperatorGens.generateEquals.
    if (isInternalPrimitive(fieldType)) {
      ("", s"$leftFieldTerm == $rightFieldTerm")
    } else if (isCompositeType(fieldType)) {
      val equaliserGenerator = new EqualiserCodeGenerator(
        getFieldTypes(fieldType).asScala.toArray)
      val generatedEqualiser = equaliserGenerator.generateRecordEqualiser("fieldGeneratedEqualiser")
      val generatedEqualiserTerm = ctx.addReusableObject(
        generatedEqualiser, "fieldGeneratedEqualiser")
      val equaliserTypeTerm = classOf[RecordEqualiser].getCanonicalName
      val equaliserTerm = newName("equaliser")
      ctx.addReusableMember(s"private $equaliserTypeTerm $equaliserTerm = null;")
      ctx.addReusableInitStatement(
        s"""
           |$equaliserTerm = ($equaliserTypeTerm)
           |  $generatedEqualiserTerm.newInstance(Thread.currentThread().getContextClassLoader());
           |""".stripMargin)
      ("", s"$equaliserTerm.equals($leftFieldTerm, $rightFieldTerm)")
    } else {
      val left = GeneratedExpression(leftFieldTerm, leftNullTerm, "", fieldType)
      val right = GeneratedExpression(rightFieldTerm, rightNullTerm, "", fieldType)
      val gen = generateEquals(ctx, left, right)
      (gen.code, gen.resultTerm)
    }
  }

  @tailrec
  private def isInternalPrimitive(t: LogicalType): Boolean = t.getTypeRoot match {
    case _ if PlannerTypeUtils.isPrimitive(t) => true

    case DATE | TIME_WITHOUT_TIME_ZONE | INTERVAL_YEAR_MONTH | INTERVAL_DAY_TIME => true

    case DISTINCT_TYPE => isInternalPrimitive(t.asInstanceOf[DistinctType].getSourceType)

    case _ => false
  }
}
