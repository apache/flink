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

import org.apache.flink.api.common.functions
import org.apache.flink.api.common.functions.Function
import org.apache.flink.api.common.io.InputFormat
import org.apache.flink.api.common.typeutils.{TypeComparator, TypeSerializer}
import org.apache.flink.cep.pattern.conditions.RichIterativeCondition
import org.apache.flink.cep._
import org.apache.flink.table.api.types.InternalType
import org.apache.flink.table.codegen.CodeGenUtils.{boxedTypeTermForType, newName}
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.runtime.functions.{AggsHandleFunction, SubKeyedAggsHandleFunction}
import org.apache.flink.table.runtime.sort.RecordEqualiser

/**
  * Describes a generated expression.
  *
  * @param resultTerm term to access the result of the expression
  * @param nullTerm boolean term that indicates if expression is null
  * @param code code necessary to produce resultTerm and nullTerm
  * @param resultType type of the resultTerm
  * @param literal flag to indicate a constant expression do not reference input and can thus
  *                 be used in the member area (e.g. as constructor parameter of a reusable
  *                 instance)
  * @param literalValue if this expression is literal, literalValue is non-null.
  */
case class GeneratedExpression(
  resultTerm: String,
  nullTerm: String,
  var code: String,
  resultType: InternalType,
  literal: Boolean = false,
  literalValue: Any = null,
  codeBuffer: Seq[String] = Seq(),
  preceding: String = "",
  flowing: String = "") {

  /**
    * Copy result term to target term if the reference is changed.
    * Note: We must ensure that the target can only be copied out, so that its object is definitely
    * a brand new reference, not the object being re-used.
    * @param target the target term that cannot be assigned a reusable reference.
    * @return code.
    */
  def copyResultTermToTargetIfChanged(ctx: CodeGeneratorContext, target: String): String = {
    if (CodeGenUtils.needCopyForType(resultType)) {
      val typeTerm = boxedTypeTermForType(resultType)
      val serTerm = ctx.addReusableTypeSerializer(resultType)
      s"""
         |if ($target != $resultTerm) {
         |  $target = (($typeTerm) $serTerm.copy($resultTerm));
         |}
       """.stripMargin
    } else {
      s"$target = $resultTerm;"
    }
  }

  /**
    * Copy result if `copyResult` parameter is enabled and the result type is a mutable type.
    * NOTE: Please use this method when the result will be buffered.
    * This method makes sure a new object/data is created when the type is mutable.
    *
    * @param copyResult copy result if true
    */
  def copyResultIfNeeded(ctx: CodeGeneratorContext, copyResult: Boolean): GeneratedExpression = {
    if (copyResult && CodeGenUtils.needCopyForType(resultType)) {
      val newResult = newName("field")
      // if the type need copy, it must be a boxed type
      val typeTerm = boxedTypeTermForType(resultType)
      val serTerm = ctx.addReusableTypeSerializer(resultType)
      val newCode =
        s"""
          |$code
          |$typeTerm $newResult = $resultTerm;
          |if (!$nullTerm) {
          |  $newResult = ($typeTerm) ($serTerm.copy($newResult));
          |}
        """.stripMargin
      this.copy(resultTerm = newResult, code = newCode)
    } else {
      this
    }
  }
}

object GeneratedExpression {
  val ALWAYS_NULL = "true"
  val NEVER_NULL = "false"
  val NO_CODE = ""
}

/**
  * Describes a generated [[functions.Function]]
  *
  * @param name class name of the generated Function.
  * @param code code of the generated Function.
  * @tparam F type of function
  * @tparam T type of function
  */
case class GeneratedFunction[F <: Function, T <: Any](
  name: String,
  var code: String)

/**
  * Describes a generated [[InputFormat]].
  *
  * @param name class name of the generated input function.
  * @param code code of the generated Function.
  * @tparam F type of function
  * @tparam T type of function
  */
case class GeneratedInput[F <: InputFormat[_, _], T <: Any](
    name: String,
    var code: String)

/**
  * Describes a generated [[org.apache.flink.util.Collector]].
  *
  * @param name class name of the generated Collector.
  * @param code code of the generated Collector.
  */
case class GeneratedCollector(name: String, var code: String)

/**
  * Describes a generated [[org.apache.flink.cep.pattern.conditions.RichIterativeCondition]].
  *
  * @param name class name of the generated IterativeCondition.
  * @param code code of the generated IterativeCondition.
  */
case class GeneratedIterativeCondition(
    name: String,
    var code: String,
    references: Array[AnyRef])
  extends GeneratedClass[RichIterativeCondition[BaseRow]]

/**
  * Describes a generated [[org.apache.flink.cep.RichPatternSelectFunction]].
  *
  * @param name class name of the generated PatternSelectFunction.
  * @param code code of the generated PatternSelectFunction.
  */
case class GeneratedPatternSelectFunction(
    name: String,
    var code: String,
    references: Array[AnyRef])
  extends GeneratedClass[RichPatternSelectFunction[BaseRow, BaseRow]]

/**
  * Describes a generated [[org.apache.flink.cep.RichPatternTimeoutFunction]].
  *
  * @param name class name of the generated PatternTimeoutFunction.
  * @param code code of the generated PatternTimeoutFunction.
  */
case class GeneratedPatternTimeoutFunction(
    name: String,
    var code: String,
    references: Array[AnyRef])
  extends GeneratedClass[RichPatternTimeoutFunction[BaseRow, BaseRow]]

/**
  * Describes a generated [[org.apache.flink.cep.RichPatternFlatSelectFunction]].
  *
  * @param name class name of the generated PatternFlatSelectFunction.
  * @param code code of the generated PatternFlatSelectFunction.
  */
case class GeneratedPatternFlatSelectFunction(
    name: String,
    var code: String,
    references: Array[AnyRef])
  extends GeneratedClass[RichPatternFlatSelectFunction[BaseRow, BaseRow]]

/**
  * Describes a generated [[org.apache.flink.cep.RichPatternFlatTimeoutFunction]].
  *
  * @param name class name of the generated PatternFlatTimeoutFunction.
  * @param code code of the generated PatternFlatTimeoutFunction.
  */
case class GeneratedPatternFlatTimeoutFunction(
    name: String,
    var code: String,
    references: Array[AnyRef])
  extends GeneratedClass[RichPatternFlatTimeoutFunction[BaseRow, BaseRow]]

/**
  * Describes a generated [[org.apache.flink.streaming.api.operators.StreamOperator]].
  *
  * @param name class name of the generated StreamOperator.
  * @param code code of the generated StreamOperator.
  */
case class GeneratedOperator(name: String, var code: String)

/**
  * Describes a generated [[org.apache.flink.table.runtime.sort.NormalizedKeyComputer]].
  *
  * @param name class name of the generated NormalizedKeyComputer.
  * @param code code of the generated NormalizedKeyComputer.
  */
case class GeneratedNormalizedKeyComputer(name: String, var code: String)

/**
  * Describes a generated [[org.apache.flink.table.runtime.sort.RecordComparator]].
  *
  * @param name class name of the generated RecordComparator.
  * @param code code of the generated RecordComparator.
  */
case class GeneratedRecordComparator(name: String, var code: String)

/**
  * Describes a generated [[org.apache.flink.table.codegen.Projection]].
  *
  * @param name class name of the generated Projection.
  * @param code code of the generated Projection.
  * @param expr projection code and result term.
  * @param inputMapping mapping to projection.
  */
case class GeneratedProjection(
    name: String, var code: String, expr: GeneratedExpression, inputMapping: Array[Int])

/**
  * Describes the members of generated sort.
  *
  * @param computer NormalizedKeyComputer.
  * @param comparator RecordComparator.
  * @param serializers serializers to init computer and comparator.
  * @param comparators comparators to init computer and comparator.
  */
case class GeneratedSorter(
    computer: GeneratedNormalizedKeyComputer, comparator: GeneratedRecordComparator,
    serializers: Array[TypeSerializer[_]], comparators: Array[TypeComparator[_]])

/**
  * Describes a generated [[org.apache.flink.table.codegen.JoinConditionFunction]].
  *
  * @param name class name of the generated JoinConditionFunction.
  * @param code code of the generated JoinConditionFunction.
  */
case class GeneratedJoinConditionFunction(name: String, var code: String)

/**
  * Describes a generated [[org.apache.flink.table.codegen.HashFunc]].
  *
  * @param name class name of the generated HashFunc.
  * @param code code of the generated HashFunc.
  */
case class GeneratedHashFunc(name: String, var code: String)

/**
  * Describes a generated [[org.apache.flink.table.runtime.functions.ProcessFunction]]
  * or [[org.apache.flink.table.runtime.functions.ProcessFunction]].
  *
  * @param name class name of the generated ProcessFunction.
  * @param code code of the generated ProcessFunction.
  */
case class GeneratedProcessFunction(name: String, var code: String)

/**
  * default implementation of GeneratedSplittable
  * @param definitions split function definitions like 'void check(Object in)'
  * @param bodies the split function body
  * @param callings the split function call
  */
case class GeneratedSplittableExpression(
  definitions: Seq[String],
  bodies: Seq[String],
  callings: Seq[String],
  isSplit: Boolean)

/**
 * Describes a generated [[org.apache.flink.table.runtime.overagg.BoundComparator]].
 *
 * @param name class name of the generated BoundComparator.
 * @param code code of the generated BoundComparator.
 */
case class GeneratedBoundComparator(name: String, var code: String)

/**
  * Describes a generated aggregate helper function
  *
  * @param name class name of the generated Function.
  * @param code code of the generated Function.
  */
case class GeneratedAggsHandleFunction(name: String, var code: String, references: Array[AnyRef])
  extends GeneratedClass[AggsHandleFunction]

/**
  * Describes a generated subkeyed aggregate helper function
  *
  * @param name class name of the generated Function.
  * @param code code of the generated Function.
  */
case class GeneratedSubKeyedAggsHandleFunction[N](
    name: String,
    var code: String,
    references: Array[AnyRef])
  extends GeneratedClass[SubKeyedAggsHandleFunction[N]]

/**
  * Describes a generated [[RecordEqualiser]]
  *
  * @param name class name of the generated Function.
  * @param code code of the generated Function.
  */
case class GeneratedRecordEqualiser(
    name: String,
    var code: String,
    references: Array[AnyRef])
  extends GeneratedClass[RecordEqualiser]

/**
  * Describes a generated [[FieldAccess]]
  *
  * @param name class name of the generated Function.
  * @param code code of the generated Function.
  */
case class GeneratedFieldExtractor(
    name: String,
    var code: String,
    references: Array[AnyRef])
  extends GeneratedClass[FieldAccess[_,_]]

object GeneratedSplittableExpression {
  /**
    * default implementation of GeneratedSplittable, which can't be split
    */
  val UNSPLIT_EXPRESSION = GeneratedSplittableExpression(Seq(), Seq(), Seq(), isSplit = false)
}
