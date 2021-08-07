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

package org.apache.flink.table.planner.codegen.over

import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.codegen.CodeGenUtils.{ROW_DATA, newName}
import org.apache.flink.table.planner.codegen.Indenter.toISC
import org.apache.flink.table.planner.codegen.{CodeGenUtils, CodeGeneratorContext, ExprCodeGenerator, GenerateUtils}
import org.apache.flink.table.runtime.generated.{GeneratedRecordComparator, RecordComparator}
import org.apache.flink.table.types.logical.{BigIntType, IntType, LogicalType, LogicalTypeRoot, RowType}

import org.apache.calcite.avatica.util.DateTimeUtils
import org.apache.calcite.rex.{RexInputRef, RexWindowBound}
import org.apache.calcite.sql.fun.SqlStdOperatorTable.{GREATER_THAN, GREATER_THAN_OR_EQUAL, MINUS}
import org.apache.calcite.tools.RelBuilder

import java.math.BigDecimal

/**
  * A code generator for generating [[RecordComparator]] on the [[RexWindowBound]] based range
  * over window.
  *
  * @param inputType       type of the input
  * @param bound        the bound value for the window, its type may be Long or BigDecimal.
  * @param key          key position describe which fields are keys in what order
  * @param keyType      type for the key field.
  * @param keyOrder     sort order for the key field.
  * @param isLowerBound the RexWindowBound is lower or not.
  */
class RangeBoundComparatorCodeGenerator(
    relBuilder: RelBuilder,
    config: TableConfig,
    inputType: RowType,
    bound: Any,
    key: Int = -1,
    keyType: LogicalType = null,
    keyOrder: Boolean = true,
    isLowerBound: Boolean = true) {

  def generateBoundComparator(name: String): GeneratedRecordComparator = {
    val className = newName(name)
    val input = CodeGenUtils.DEFAULT_INPUT1_TERM
    val current = CodeGenUtils.DEFAULT_INPUT2_TERM

    val ctx = CodeGeneratorContext(config)

    val inputExpr = GenerateUtils.generateFieldAccess(ctx, inputType, inputTerm = input, key)
    val currentExpr = GenerateUtils.generateFieldAccess(ctx, inputType, inputTerm = current, key)

    // See RangeSlidingOverFrame:
    //  return -1 with lower bound will be eliminate
    //  return 1 with higher bound will be eliminate

    // Except the null value from the window frame unless the last value is not null.
    val oneIsNull = if (isLowerBound) "return -1;" else "return 1;"

    def boundCompareZero: Int = {
      bound match {
        case bg: BigDecimal => bg.compareTo(BigDecimal.ZERO)
        case _ => bound.asInstanceOf[Long].compareTo(0)
      }
    }

    val allIsNull = if (isLowerBound) {
      //put the null value into the window frame if the last value is null and the lower bound
      // not more than 0.
      if (boundCompareZero <= 0) "return 1;" else "return -1;"
    } else {
      //put the null value into the window frame if the last value is null and the upper bound
      //not less than 0.
      if (boundCompareZero >= 0) "return -1;" else "return 1;"
    }

    val comparatorCode =
      j"""
        ${ctx.reuseLocalVariableCode()}
        ${inputExpr.code}
        ${currentExpr.code}
        if (${inputExpr.nullTerm} && ${currentExpr.nullTerm}) {
           $allIsNull
        } else if (${inputExpr.nullTerm} || ${currentExpr.nullTerm}) {
           $oneIsNull
        } else {
           ${getComparatorCode(inputExpr.resultTerm, {currentExpr.resultTerm})}
        }
     """.stripMargin

    val code =
      j"""
      public class $className implements ${classOf[RecordComparator].getCanonicalName} {

        private final Object[] references;
        ${ctx.reuseMemberCode()}

        public $className(Object[] references) {
          this.references = references;
          ${ctx.reuseInitCode()}
          ${ctx.reuseOpenCode()}
        }

        @Override
        public int compare($ROW_DATA $input, $ROW_DATA $current) {
          ${comparatorCode.mkString}
        }
      }
      """.stripMargin

    new GeneratedRecordComparator(
      className, code, ctx.references.toArray, ctx.tableConfig.getConfiguration)
  }

  private def getComparatorCode(inputValue: String, currentValue: String): String = {
    val (realBoundValue, realKeyType) = keyType.getTypeRoot match {
      case LogicalTypeRoot.DATE =>
        //The constant about time is expressed based millisecond unit in calcite, but
        //the field about date is expressed based day unit. So here should keep the same unit for
        // comparator.
        (bound.asInstanceOf[Long] / DateTimeUtils.MILLIS_PER_DAY, new IntType())
      case LogicalTypeRoot.TIME_WITHOUT_TIME_ZONE => (bound, new IntType())
      case LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE => (bound, new BigIntType())
      case _ => (bound, keyType)
    }

    val typeFactory = relBuilder.getTypeFactory.asInstanceOf[FlinkTypeFactory]
    val relKeyType = typeFactory.createFieldTypeFromLogicalType(realKeyType)

    //minus between inputValue and currentValue
    val ctx = CodeGeneratorContext(config)
    val exprCodeGenerator = new ExprCodeGenerator(ctx, false)
    val minusCall = if (keyOrder) {
      relBuilder.call(
        MINUS, new RexInputRef(0, relKeyType), new RexInputRef(1, relKeyType))
    } else {
      relBuilder.call(
        MINUS, new RexInputRef(1, relKeyType), new RexInputRef(0, relKeyType))
    }
    exprCodeGenerator.bindInput(realKeyType, inputValue).bindSecondInput(realKeyType, currentValue)
    val literal = relBuilder.literal(realBoundValue)

    // In order to avoid the loss of precision in long cast to int.
    val comCall = if (isLowerBound) {
      relBuilder.call(GREATER_THAN_OR_EQUAL, minusCall, literal)
    } else {
      relBuilder.call(GREATER_THAN, minusCall, literal)
    }

    val comExpr = exprCodeGenerator.generateExpression(comCall)

    j"""
       ${ctx.reuseMemberCode()}
       ${ctx.reuseLocalVariableCode()}
       ${ctx.reuseInputUnboxingCode()}
       ${comExpr.code}
       if (${comExpr.resultTerm}) {
         return 1;
       } else {
         return -1;
       }
     """.stripMargin
  }
}
