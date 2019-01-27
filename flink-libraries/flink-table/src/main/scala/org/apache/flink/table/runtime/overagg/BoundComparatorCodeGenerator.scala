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

package org.apache.flink.table.runtime.overagg

import java.math.BigDecimal

import org.apache.calcite.rex.RexInputRef
import org.apache.calcite.sql.fun.SqlStdOperatorTable.{GREATER_THAN, GREATER_THAN_OR_EQUAL, MINUS}
import org.apache.calcite.tools.RelBuilder
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.api.types._
import org.apache.flink.table.calcite.{FlinkTypeFactory, FlinkTypeSystem}
import org.apache.flink.table.codegen.CodeGenUtils.{boxedTypeTermForType, newName}
import org.apache.flink.table.codegen.Indenter.toISC
import org.apache.flink.table.codegen.CodeGeneratorContext.BASE_ROW
import org.apache.flink.table.codegen._
import org.apache.flink.table.runtime.functions.DateTimeFunctions
import org.apache.flink.table.typeutils.TypeUtils

import scala.collection.mutable

/**
 * A code generator for generating [[BoundComparator]] on the
 * [[org.apache.calcite.rex.RexWindowBound]] based row over window.
 * @param bound the bound value for the window, its type may be Long.
 */

class RowBoundComparatorCodeGenerator(config: TableConfig, bound: Long) {

  def generateBoundComparator(name: String): GeneratedBoundComparator = {

    val className = newName(name)

    val input = CodeGeneratorContext.DEFAULT_INPUT1_TERM

    val inputIndex = s"${input}Index"

    val current = CodeGeneratorContext.DEFAULT_INPUT2_TERM

    val currentIndex = s"${current}Index"

    val indexClass = classOf[Int].getCanonicalName

    val ctx = CodeGeneratorContext(config)

    val comparatorCode =
      j"""
          return $inputIndex - $currentIndex - ${bound}L;
      """.stripMargin

    val baseClass = classOf[BoundComparator]

    val code =
      j"""
      public class $className extends ${baseClass.getCanonicalName} {

         ${ctx.reuseMemberCode()}

         public $className() throws Exception {
          ${ctx.reuseInitCode()}
          }

         @Override
         public void reset() {
           ${ctx.reuseInitCode()}
         }

        @Override
        public long compare($BASE_ROW $input, $indexClass $inputIndex,
           $BASE_ROW $current, $indexClass $currentIndex) {
          ${comparatorCode.mkString}
        }

      }
      """.stripMargin

    GeneratedBoundComparator(className, code)
  }
}


/**
 * A code generator for generating [[BoundComparator]] on the
 * [[org.apache.calcite.rex.RexWindowBound]] based range over window.
 * @param inType type of the input
 * @param bound the bound value for the window, its type may be Long or BigDecimal.
 * @param key key position describe which fields are keys in what order
 * @param keyType type for the key field.
 * @param keyOrder sort order for the key field.
 * @param isLowerBound the RexWindowBound is lower or not.
 */
class RangeBoundComparatorCodeGenerator(
    relBuilder: RelBuilder,
    config: TableConfig,
    inType: RowType,
    bound: Any,
    key: Int = -1,
    keyType: InternalType = null,
    keyOrder: Boolean = true,
    isLowerBound: Boolean = true) {

  def generateBoundComparator(name: String): GeneratedBoundComparator = {

    val className = newName(name)

    val input = CodeGeneratorContext.DEFAULT_INPUT1_TERM

    val inputIndex = s"${input}Index"

    val current = CodeGeneratorContext.DEFAULT_INPUT2_TERM

    val currentIndex = s"${current}Index"

    val indexClass = classOf[Int].getCanonicalName

    val ctx = CodeGeneratorContext(config)

    val inputExpr = CodeGenUtils.generateFieldAccess(
      ctx, inType, inputTerm = input, key, nullCheck = true)
    val currentExpr = CodeGenUtils.generateFieldAccess(
      ctx, inType, inputTerm = current, key, nullCheck = true)
    val lastCurrentIndex = newName("lastCurrentIndex")
    ctx.addReusableMember(
      s"private transient $indexClass $lastCurrentIndex;", s"$lastCurrentIndex = -1;")
    val currentValue = newName("currentValue")
    val keyTypeTerm = boxedTypeTermForType(keyType)
    ctx.addReusableMember(s"private $keyTypeTerm $currentValue;", s"$currentValue = null;")
    val currentNullTerm = newName("currentNullTerm")
    ctx.addReusableMember(s"private boolean $currentNullTerm;", s"$currentNullTerm = true;")

    //Except the null value from the window frame unless the last value is not null.
    val oneIsNull = if (isLowerBound)  "return -1;" else "return 1;"

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
        ${ctx.reuseFieldCode()}
        ${inputExpr.code}
        if($lastCurrentIndex != $currentIndex) {
          ${currentExpr.code}
          $currentNullTerm = ${currentExpr.nullTerm};
          $currentValue =  ${currentExpr.resultTerm};
        }
        $lastCurrentIndex = $currentIndex;
        if(${inputExpr.nullTerm} && $currentNullTerm) {
           $allIsNull
        } else if(${inputExpr.nullTerm} || $currentNullTerm) {
           $oneIsNull
        } else {
           ${getComparatorCode(inputExpr.resultTerm, currentValue)}
        }
     """.stripMargin

    val baseClass = classOf[BoundComparator]

    val code =
      j"""
      public class $className extends ${baseClass.getCanonicalName} {

         ${ctx.reuseMemberCode()}

         public $className() throws Exception {
          ${ctx.reuseInitCode()}
          }

         @Override
         public void reset() {
           ${ctx.reuseInitCode()}
         }

        @Override
        public long compare($BASE_ROW $input, $indexClass $inputIndex,
           $BASE_ROW $current, $indexClass $currentIndex) {
          ${comparatorCode.mkString}
        }

      }
      """.stripMargin

    GeneratedBoundComparator(className, code)
  }

  private def boundCompareZero: Int = {
    bound match {
      case bg: BigDecimal => bg.compareTo(BigDecimal.ZERO)
      case _ => bound.asInstanceOf[Long].compareTo(0)
    }
  }

  private def getComparatorCode(inputValue: String, currentValue: String): String = {
    val (realBoundValue, realKeyType) = keyType match {
      case _: DateType =>
        //The constant about time is expressed based millisecond unit in calcite, but
        //the field about date is expressed based day unit. So here should keep the same unit for
        // comparator.
        (bound.asInstanceOf[Long] / DateTimeFunctions.MILLIS_PER_DAY, IntType.INSTANCE)
      case _: TimeType =>  (bound, IntType.INSTANCE)
      case _: TimestampType => (bound, LongType.INSTANCE)
      case _ => (bound, keyType)
    }

    val typeFactory = new FlinkTypeFactory(new FlinkTypeSystem())
    val relKeyType = typeFactory.createTypeFromInternalType(realKeyType, isNullable = false)

    //minus between inputValue and currentValue
    val ctx = CodeGeneratorContext(config)
    val exprCodeGenerator = new ExprCodeGenerator(ctx, false, false)
    val minusCall = if (keyOrder) {
      relBuilder.call(
        MINUS, new RexInputRef(0, relKeyType), new RexInputRef(1, relKeyType))
    } else {
      relBuilder.call(
        MINUS, new RexInputRef(1, relKeyType), new RexInputRef(0, relKeyType))
    }
    exprCodeGenerator.bindInput(realKeyType, inputValue).bindSecondInput(realKeyType, currentValue)
    val literal = relBuilder.literal(realBoundValue)
    val comCall = if (isLowerBound) {
      relBuilder.call(GREATER_THAN_OR_EQUAL, minusCall, literal)
    } else {
      relBuilder.call(GREATER_THAN, minusCall, literal)
    }

    val comExpr = exprCodeGenerator.generateExpression(comCall)

    j"""
       ${ctx.reuseMemberCode()}
       ${ctx.reuseFieldCode()}
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

/**
 * RANGE allow the compound ORDER BY and the random type when the bound is current row.
 */
class MultiFieldRangeBoundComparatorCodeGenerator(
    inType: RowType,
    keys: Array[Int],
    keyTypes: Array[InternalType],
    keyOrders: Array[Boolean],
    nullsIsLasts: Array[Boolean],
    isLowerBound: Boolean = true) {

  def generateBoundComparator(name: String): GeneratedBoundComparator = {

    val (comparators, _) = TypeUtils.flattenComparatorAndSerializer(
      inType.getArity, keys, keyOrders, inType.getFieldTypes.map(_.toInternalType))
    val sortCodeGen = new SortCodeGenerator(keys, keyTypes, comparators, keyOrders, nullsIsLasts)

    val className = newName(name)

    val indexClass = classOf[Int].getCanonicalName

    val baseClass = classOf[BoundComparator]

    val compares = new mutable.ArrayBuffer[String]

    def generateReturnCode(comp: String): String = {
      if (isLowerBound) {
        j"""
         if ($comp >= 0) {
            return 1;
         } else {
            return -1;
         }
       """.stripMargin
      } else {
        j"""
         if ($comp > 0) {
            return 1;
         } else {
            return -1;
         }
       """.stripMargin
      }
    }

    for (i <- keys.indices) {
      val index = keys(i)

      val symbol = if (sortCodeGen.orders(i)) "" else "-"

      val nullIsLast = if (sortCodeGen.nullsIsLast(i)) 1 else -1

      val t = keyTypes(i)

      val prefix = sortCodeGen.prefixGetFromBinaryRow(t)

      val compCompare = s"comparators[$i].compare"

      val compareCode = if (prefix != null) {
        t match {
          case bt: RowType =>
            val arity = bt.getArity
            s"$compCompare(o1.get$prefix($index, $arity), o2.get$prefix($index, $arity))"
          case _ =>
            val get = sortCodeGen.getter(t, index)
            s"$symbol${sortCodeGen.binaryRowUtil}.compare$prefix(o1.$get, o2.$get)"
        }
      } else {
        // Only builtIn keys need care about invertNormalizedKey(order).
        // Because comparators will handle order.
        s"""
           |$compCompare(
           |  o1.getGeneric($index, serializers[$i]),
           |  o2.getGeneric($index, serializers[$i]))
           |""".stripMargin
      }

      val code =
        s"""
           |boolean null${index}At1 = o1.isNullAt($index);
           |boolean null${index}At2 = o2.isNullAt($index);
           |int cmp$index = null${index}At1 && null${index}At2 ? 0 :
           |  (null${index}At1 ? $nullIsLast :
           |    (null${index}At2 ? ${-nullIsLast} : $compareCode));
           |if (cmp$index != 0) {
           |  ${generateReturnCode(s"cmp$index")}
           |}
           |""".stripMargin
      compares += code
    }
    compares += {
      if (isLowerBound) "return 1;" else "return -1;"
    }

    val code =
      j"""
      public class $className extends ${baseClass.getCanonicalName} {


         public $className() throws Exception {
          }

         @Override
         public void reset() {
         }

        @Override
        public long compare($BASE_ROW o1, $indexClass index1,
           $BASE_ROW o2, $indexClass index2) {
          ${compares.mkString}
        }

      }
      """.stripMargin
    GeneratedBoundComparator(className, code)
  }
}
