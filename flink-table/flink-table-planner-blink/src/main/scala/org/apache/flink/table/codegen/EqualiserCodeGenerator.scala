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
package org.apache.flink.table.codegen

import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.codegen.CodeGenUtils._
import org.apache.flink.table.codegen.Indenter.toISC
import org.apache.flink.table.generated.{GeneratedRecordEqualiser, RecordEqualiser}
import org.apache.flink.table.types.PlannerTypeUtils
import org.apache.flink.table.types.logical.LogicalTypeRoot._
import org.apache.flink.table.types.logical.{LogicalType, RowType}

import scala.collection.JavaConversions._

class EqualiserCodeGenerator(fieldTypes: Seq[LogicalType]) {

  private val RECORD_EQUALISER = className[RecordEqualiser]
  private val LEFT_INPUT = "left"
  private val RIGHT_INPUT = "right"

  def generateRecordEqualiser(name: String): GeneratedRecordEqualiser = {
    // ignore time zone
    val ctx = CodeGeneratorContext(new TableConfig)
    val className = newName(name)
    val header =
      s"""
         |if ($LEFT_INPUT.getHeader() != $RIGHT_INPUT.getHeader()) {
         |  return false;
         |}
       """.stripMargin

    val codes = for (i <- fieldTypes.indices) yield {
      val fieldType = fieldTypes(i)
      val fieldTypeTerm = primitiveTypeTermForType(fieldType)
      val result = s"cmp$i"
      val leftNullTerm = "leftIsNull$" + i
      val rightNullTerm = "rightIsNull$" + i
      val leftFieldTerm = "leftField$" + i
      val rightFieldTerm = "rightField$" + i
      val equalsCode = if (isInternalPrimitive(fieldType)) {
        s"$leftFieldTerm == $rightFieldTerm"
      } else if (isBaseRow(fieldType)) {
        val equaliserGenerator =
          new EqualiserCodeGenerator(fieldType.asInstanceOf[RowType].getChildren)
        val generatedEqualiser = equaliserGenerator
          .generateRecordEqualiser("field$" + i + "GeneratedEqualiser")
        val generatedEqualiserTerm = ctx.addReusableObject(
          generatedEqualiser, "field$" + i + "GeneratedEqualiser")
        val equaliserTypeTerm = classOf[RecordEqualiser].getCanonicalName
        val equaliserTerm = newName("equaliser")
        ctx.addReusableMember(s"private $equaliserTypeTerm $equaliserTerm = null;")
        ctx.addReusableInitStatement(
          s"""
             |$equaliserTerm = ($equaliserTypeTerm)
             |  $generatedEqualiserTerm.newInstance(Thread.currentThread().getContextClassLoader());
             |""".stripMargin)
        s"$equaliserTerm.equalsWithoutHeader($leftFieldTerm, $rightFieldTerm)"
      } else {
        s"$leftFieldTerm.equals($rightFieldTerm)"
      }
      val leftReadCode = baseRowFieldReadAccess(ctx, i, LEFT_INPUT, fieldType)
      val rightReadCode = baseRowFieldReadAccess(ctx, i, RIGHT_INPUT, fieldType)
      s"""
         |boolean $leftNullTerm = $LEFT_INPUT.isNullAt($i);
         |boolean $rightNullTerm = $RIGHT_INPUT.isNullAt($i);
         |boolean $result;
         |if ($leftNullTerm && $rightNullTerm) {
         |  $result = true;
         |} else if ($leftNullTerm|| $rightNullTerm) {
         |  $result = false;
         |} else {
         |  $fieldTypeTerm $leftFieldTerm = $leftReadCode;
         |  $fieldTypeTerm $rightFieldTerm = $rightReadCode;
         |  $result = $equalsCode;
         |}
         |if (!$result) {
         |  return false;
         |}
      """.stripMargin
    }

    val functionCode =
      j"""
        public final class $className implements $RECORD_EQUALISER {

          ${ctx.reuseMemberCode()}

          public $className(Object[] references) throws Exception {
            ${ctx.reuseInitCode()}
          }

          @Override
          public boolean equals($BASE_ROW $LEFT_INPUT, $BASE_ROW $RIGHT_INPUT) {
            if ($LEFT_INPUT instanceof $BINARY_ROW && $RIGHT_INPUT instanceof $BINARY_ROW) {
              return $LEFT_INPUT.equals($RIGHT_INPUT);
            } else {
              $header
              ${ctx.reuseLocalVariableCode()}
              ${codes.mkString("\n")}
              return true;
            }
          }

          @Override
          public boolean equalsWithoutHeader($BASE_ROW $LEFT_INPUT, $BASE_ROW $RIGHT_INPUT) {
            if ($LEFT_INPUT instanceof $BINARY_ROW && $RIGHT_INPUT instanceof $BINARY_ROW) {
              return (($BINARY_ROW)$LEFT_INPUT).equalsWithoutHeader((($BINARY_ROW)$RIGHT_INPUT));
            } else {
              ${ctx.reuseLocalVariableCode()}
              ${codes.mkString("\n")}
              return true;
            }
          }
        }
      """.stripMargin

    new GeneratedRecordEqualiser(className, functionCode, ctx.references.toArray)
  }

  private def isInternalPrimitive(t: LogicalType): Boolean = t.getTypeRoot match {
    case _ if PlannerTypeUtils.isPrimitive(t) => true

    case DATE => true
    case TIME_WITHOUT_TIME_ZONE => true
    case TIMESTAMP_WITHOUT_TIME_ZONE => true
    case INTERVAL_YEAR_MONTH => true
    case INTERVAL_DAY_TIME => true

    case _ => false
  }

  private def isBaseRow(t: LogicalType): Boolean = t match {
    case _: RowType => true
    case _ => false
  }
}
