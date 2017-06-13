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

import java.lang.reflect.ParameterizedType
import java.lang.{Iterable => JIterable}
import java.math.{BigDecimal => JBigDecimal}

import org.apache.calcite.avatica.util.DateTimeUtils
import org.apache.calcite.rex._
import org.apache.calcite.sql.SqlOperator
import org.apache.calcite.sql.`type`.SqlTypeName._
import org.apache.calcite.sql.fun.SqlStdOperatorTable._
import org.apache.flink.api.common.functions._
import org.apache.flink.api.common.io.GenericInputFormat
import org.apache.flink.api.common.typeinfo._
import org.apache.flink.api.common.typeutils.CompositeType
import org.apache.flink.api.java.typeutils._
import org.apache.flink.api.scala.typeutils.CaseClassTypeInfo
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.codegen.CodeGenUtils._
import org.apache.flink.table.codegen.GeneratedExpression.{NEVER_NULL, NO_CODE}
import org.apache.flink.table.codegen.Indenter.toISC
import org.apache.flink.table.codegen.calls.FunctionGenerator
import org.apache.flink.table.codegen.calls.ScalarOperators._
import org.apache.flink.table.functions.utils.UserDefinedFunctionUtils
import org.apache.flink.table.functions.{AggregateFunction, FunctionContext, TimeMaterializationSqlFunction, UserDefinedFunction}
import org.apache.flink.table.runtime.TableFunctionCollector
import org.apache.flink.table.typeutils.TypeCheckUtils._
import org.apache.flink.types.Row
import org.apache.flink.table.functions.utils.UserDefinedFunctionUtils.{getUserDefinedMethod, signatureToString}

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * A code generator for generating Flink [[org.apache.flink.api.common.functions.Function]]s.
  *
  * @param config configuration that determines runtime behavior
  * @param nullableInput input(s) can be null.
  * @param input1 type information about the first input of the Function
  * @param input2 type information about the second input if the Function is binary
  * @param input1FieldMapping additional mapping information for input1
  *   (e.g. POJO types have no deterministic field order and some input fields might not be read)
  * @param input2FieldMapping additional mapping information for input2
  *   (e.g. POJO types have no deterministic field order and some input fields might not be read)
  */
class CodeGenerator(
    config: TableConfig,
    nullableInput: Boolean,
    input1: TypeInformation[_ <: Any],
    input2: Option[TypeInformation[_ <: Any]] = None,
    input1FieldMapping: Option[Array[Int]] = None,
    input2FieldMapping: Option[Array[Int]] = None)
  extends RexVisitor[GeneratedExpression] {

  // check if nullCheck is enabled when inputs can be null
  if (nullableInput && !config.getNullCheck) {
    throw new CodeGenException("Null check must be enabled if entire rows can be null.")
  }

  // check for POJO input1 mapping
  input1 match {
    case pt: PojoTypeInfo[_] =>
      input1FieldMapping.getOrElse(
        throw new CodeGenException("No input mapping is specified for input1 of type POJO."))
    case _ => // ok
  }

  // check for POJO input2 mapping
  input2 match {
    case Some(pt: PojoTypeInfo[_]) =>
      input2FieldMapping.getOrElse(
        throw new CodeGenException("No input mapping is specified for input2 of type POJO."))
    case _ => // ok
  }

  private val input1Mapping = input1FieldMapping match {
    case Some(mapping) => mapping
    case _ => (0 until input1.getArity).toArray
  }

  private val input2Mapping = input2FieldMapping match {
    case Some(mapping) => mapping
    case _ => input2 match {
      case Some(input) => (0 until input.getArity).toArray
      case _ => Array[Int]()
    }
  }

  /**
    * A code generator for generating unary Flink
    * [[org.apache.flink.api.common.functions.Function]]s with one input.
    *
    * @param config configuration that determines runtime behavior
    * @param nullableInput input(s) can be null.
    * @param input type information about the input of the Function
    * @param inputFieldMapping additional mapping information necessary for input
    *   (e.g. POJO types have no deterministic field order and some input fields might not be read)
    */
  def this(
      config: TableConfig,
      nullableInput: Boolean,
      input: TypeInformation[Any],
      inputFieldMapping: Array[Int]) =
    this(config, nullableInput, input, None, Some(inputFieldMapping))

  /**
    * A code generator for generating Flink input formats.
    *
    * @param config configuration that determines runtime behavior
    */
  def this(config: TableConfig) =
    this(config, false, new RowTypeInfo(), None, None)

  // set of member statements that will be added only once
  // we use a LinkedHashSet to keep the insertion order
  private val reusableMemberStatements = mutable.LinkedHashSet[String]()

  // set of constructor statements that will be added only once
  // we use a LinkedHashSet to keep the insertion order
  private val reusableInitStatements = mutable.LinkedHashSet[String]()

  // set of open statements for RichFunction that will be added only once
  // we use a LinkedHashSet to keep the insertion order
  private val reusableOpenStatements = mutable.LinkedHashSet[String]()

  // set of close statements for RichFunction that will be added only once
  // we use a LinkedHashSet to keep the insertion order
  private val reusableCloseStatements = mutable.LinkedHashSet[String]()

  // set of statements that will be added only once per record
  // we use a LinkedHashSet to keep the insertion order
  private val reusablePerRecordStatements = mutable.LinkedHashSet[String]()

  // map of initial input unboxing expressions that will be added only once
  // (inputTerm, index) -> expr
  private val reusableInputUnboxingExprs = mutable.Map[(String, Int), GeneratedExpression]()

  // set of constructor statements that will be added only once
  // we use a LinkedHashSet to keep the insertion order
  private val reusableConstructorStatements = mutable.LinkedHashSet[(String, String)]()

  /**
    * @return code block of statements that need to be placed in the member area of the Function
    *         (e.g. member variables and their initialization)
    */
  def reuseMemberCode(): String = {
    reusableMemberStatements.mkString("", "\n", "\n")
  }

  /**
    * @return code block of statements that need to be placed in the constructor of the Function
    */
  def reuseInitCode(): String = {
    reusableInitStatements.mkString("", "\n", "\n")
  }

  /**
    * @return code block of statements that need to be placed in the open() method of RichFunction
    */
  def reuseOpenCode(): String = {
    reusableOpenStatements.mkString("", "\n", "\n")
  }

  /**
    * @return code block of statements that need to be placed in the close() method of RichFunction
    */
  def reuseCloseCode(): String = {
    reusableCloseStatements.mkString("", "\n", "\n")
  }

  /**
    * @return code block of statements that need to be placed in the SAM of the Function
    */
  def reusePerRecordCode(): String = {
    reusablePerRecordStatements.mkString("", "\n", "\n")
  }

  /**
    * @return code block of statements that unbox input variables to a primitive variable
    *         and a corresponding null flag variable
    */
  def reuseInputUnboxingCode(): String = {
    reusableInputUnboxingExprs.values.map(_.code).mkString("", "\n", "\n")
  }

  /**
    * @return code block of constructor statements for the Function
    */
  def reuseConstructorCode(className: String): String = {
    reusableConstructorStatements.map { case (params, body) =>
      s"""
        |public $className($params) throws Exception {
        |  this();
        |  $body
        |}
        |""".stripMargin
    }.mkString("", "\n", "\n")
  }

  /**
    * @return term of the (casted and possibly boxed) first input
    */
  var input1Term = "in1"

  /**
    * @return term of the (casted and possibly boxed) second input
    */
  var input2Term = "in2"

  /**
    * @return term of the (casted) output collector
    */
  var collectorTerm = "c"

  /**
    * @return term of the output record (possibly defined in the member area e.g. Row, Tuple)
    */
  var outRecordTerm = "out"

  /**
    * @return term of the [[ProcessFunction]]'s context
    */
  var contextTerm = "ctx"

  /**
    * @return returns if null checking is enabled
    */
  def nullCheck: Boolean = config.getNullCheck

  /**
    * Generates an expression from a RexNode. If objects or variables can be reused, they will be
    * added to reusable code sections internally.
    *
    * @param rex Calcite row expression
    * @return instance of GeneratedExpression
    */
  def generateExpression(rex: RexNode): GeneratedExpression = {
    rex.accept(this)
  }

  /**
    * Generates a [[org.apache.flink.table.runtime.aggregate.GeneratedAggregations]] that can be
    * passed to a Java compiler.
    *
    * @param name        Class name of the function.
    *                    Does not need to be unique but has to be a valid Java class identifier.
    * @param generator   The code generator instance
    * @param physicalInputTypes Physical input row types
    * @param aggregates  All aggregate functions
    * @param aggFields   Indexes of the input fields for all aggregate functions
    * @param aggMapping  The mapping of aggregates to output fields
    * @param partialResults A flag defining whether final or partial results (accumulators) are set
    *                       to the output row.
    * @param fwdMapping  The mapping of input fields to output fields
    * @param mergeMapping An optional mapping to specify the accumulators to merge. If not set, we
    *                     assume that both rows have the accumulators at the same position.
    * @param constantFlags An optional parameter to define where to set constant boolean flags in
    *                      the output row.
    * @param outputArity The number of fields in the output row.
    * @param needRetract a flag to indicate if the aggregate needs the retract method
    * @param needMerge a flag to indicate if the aggregate needs the merge method
    * @param needReset a flag to indicate if the aggregate needs the resetAccumulator method
    *
    * @return A GeneratedAggregationsFunction
    */
  def generateAggregations(
      name: String,
      generator: CodeGenerator,
      physicalInputTypes: Seq[TypeInformation[_]],
      aggregates: Array[AggregateFunction[_ <: Any, _ <: Any]],
      aggFields: Array[Array[Int]],
      aggMapping: Array[Int],
      partialResults: Boolean,
      fwdMapping: Array[Int],
      mergeMapping: Option[Array[Int]],
      constantFlags: Option[Array[(Int, Boolean)]],
      outputArity: Int,
      needRetract: Boolean,
      needMerge: Boolean,
      needReset: Boolean)
  : GeneratedAggregationsFunction = {

    // get unique function name
    val funcName = newName(name)
    // register UDAGGs
    val aggs = aggregates.map(a => generator.addReusableFunction(a))
    // get java types of accumulators
    val accTypeClasses = aggregates.map { a =>
      a.getClass.getMethod("createAccumulator").getReturnType
    }
    val accTypes = accTypeClasses.map(_.getCanonicalName)

    // get java classes of input fields
    val javaClasses = physicalInputTypes.map(t => t.getTypeClass)
    // get parameter lists for aggregation functions
    val parameters = aggFields.map { inFields =>
      val fields = for (f <- inFields) yield
        s"(${javaClasses(f).getCanonicalName}) input.getField($f)"
      fields.mkString(", ")
    }
    val methodSignaturesList = aggFields.map {
      inFields => for (f <- inFields) yield javaClasses(f)
    }

    // check and validate the needed methods
    aggregates.zipWithIndex.map {
      case (a, i) => {
        getUserDefinedMethod(a, "accumulate", Array(accTypeClasses(i)) ++ methodSignaturesList(i))
          .getOrElse(
            throw new CodeGenException(
              s"No matching accumulate method found for AggregateFunction " +
                s"'${a.getClass.getCanonicalName}'" +
                s"with parameters '${signatureToString(methodSignaturesList(i))}'.")
          )

        if (needRetract) {
          getUserDefinedMethod(a, "retract", Array(accTypeClasses(i)) ++ methodSignaturesList(i))
            .getOrElse(
              throw new CodeGenException(
                s"No matching retract method found for AggregateFunction " +
                  s"'${a.getClass.getCanonicalName}'" +
                  s"with parameters '${signatureToString(methodSignaturesList(i))}'.")
            )
        }

        if (needMerge) {
          val methods =
            getUserDefinedMethod(a, "merge", Array(accTypeClasses(i), classOf[JIterable[Any]]))
              .getOrElse(
                throw new CodeGenException(
                  s"No matching merge method found for AggregateFunction " +
                    s"${a.getClass.getCanonicalName}'.")
              )

          var iterableTypeClass = methods.getGenericParameterTypes.apply(1)
            .asInstanceOf[ParameterizedType].getActualTypeArguments.apply(0)
          // further extract iterableTypeClass if the accumulator has generic type
          iterableTypeClass match {
            case impl: ParameterizedType => iterableTypeClass = impl.getRawType
            case _ =>
          }

          if (iterableTypeClass != accTypeClasses(i)) {
            throw new CodeGenException(
              s"merge method in AggregateFunction ${a.getClass.getCanonicalName} does not have " +
                s"the correct Iterable type. Actually: ${iterableTypeClass.toString}. " +
                s"Expected: ${accTypeClasses(i).toString}")
          }
        }

        if (needReset) {
          getUserDefinedMethod(a, "resetAccumulator", Array(accTypeClasses(i)))
            .getOrElse(
              throw new CodeGenException(
                s"No matching resetAccumulator method found for " +
                  s"aggregate ${a.getClass.getCanonicalName}'.")
            )
        }
      }
    }

    def genSetAggregationResults: String = {

      val sig: String =
        j"""
           |  public final void setAggregationResults(
           |    org.apache.flink.types.Row accs,
           |    org.apache.flink.types.Row output)""".stripMargin

      val setAggs: String = {
        for (i <- aggs.indices) yield

          if (partialResults) {
            j"""
               |    output.setField(
               |      ${aggMapping(i)},
               |      (${accTypes(i)}) accs.getField($i));""".stripMargin
          } else {
            j"""
               |    org.apache.flink.table.functions.AggregateFunction baseClass$i =
               |      (org.apache.flink.table.functions.AggregateFunction) ${aggs(i)};
               |
               |    output.setField(
               |      ${aggMapping(i)},
               |      baseClass$i.getValue((${accTypes(i)}) accs.getField($i)));""".stripMargin
          }
      }.mkString("\n")

      j"""
         |$sig {
         |$setAggs
         |  }""".stripMargin
    }

    def genAccumulate: String = {

      val sig: String =
        j"""
            |  public final void accumulate(
            |    org.apache.flink.types.Row accs,
            |    org.apache.flink.types.Row input)""".stripMargin

      val accumulate: String = {
        for (i <- aggs.indices) yield
          j"""
             |    ${aggs(i)}.accumulate(
             |      ((${accTypes(i)}) accs.getField($i)),
             |      ${parameters(i)});""".stripMargin
      }.mkString("\n")

      j"""$sig {
         |$accumulate
         |  }""".stripMargin
    }

    def genRetract: String = {

      val sig: String =
        j"""
            |  public final void retract(
            |    org.apache.flink.types.Row accs,
            |    org.apache.flink.types.Row input)""".stripMargin

      val retract: String = {
        for (i <- aggs.indices) yield
          j"""
             |    ${aggs(i)}.retract(
             |      ((${accTypes(i)}) accs.getField($i)),
             |      ${parameters(i)});""".stripMargin
      }.mkString("\n")

      if (needRetract) {
        j"""
           |$sig {
           |$retract
           |  }""".stripMargin
      } else {
        j"""
           |$sig {
           |  }""".stripMargin
      }
    }

    def genCreateAccumulators: String = {

      val sig: String =
        j"""
           |  public final org.apache.flink.types.Row createAccumulators()
           |    """.stripMargin
      val init: String =
        j"""
           |      org.apache.flink.types.Row accs =
           |          new org.apache.flink.types.Row(${aggs.length});"""
          .stripMargin
      val create: String = {
        for (i <- aggs.indices) yield
          j"""
             |    accs.setField(
             |      $i,
             |      ${aggs(i)}.createAccumulator());"""
            .stripMargin
      }.mkString("\n")
      val ret: String =
        j"""
           |      return accs;"""
          .stripMargin

      j"""$sig {
         |$init
         |$create
         |$ret
         |  }""".stripMargin
    }

    def genSetForwardedFields: String = {

      val sig: String =
        j"""
           |  public final void setForwardedFields(
           |    org.apache.flink.types.Row input,
           |    org.apache.flink.types.Row output)
           |    """.stripMargin

      val forward: String = {
        for (i <- fwdMapping.indices if fwdMapping(i) >= 0) yield
          {
            j"""
               |    output.setField(
               |      $i,
               |      input.getField(${fwdMapping(i)}));"""
              .stripMargin
          }
      }.mkString("\n")

      j"""$sig {
         |$forward
         |  }""".stripMargin
    }

    def genSetConstantFlags: String = {

      val sig: String =
        j"""
           |  public final void setConstantFlags(org.apache.flink.types.Row output)
           |    """.stripMargin

      val setFlags: String = if (constantFlags.isDefined) {
        {
          for (cf <- constantFlags.get) yield {
            j"""
               |    output.setField(${cf._1}, ${if (cf._2) "true" else "false"});"""
              .stripMargin
          }
        }.mkString("\n")
      } else {
        ""
      }

      j"""$sig {
         |$setFlags
         |  }""".stripMargin
    }

    def genCreateOutputRow: String = {
      j"""
         |  public final org.apache.flink.types.Row createOutputRow() {
         |    return new org.apache.flink.types.Row($outputArity);
         |  }""".stripMargin
    }

    def genMergeAccumulatorsPair: String = {

      val mapping = mergeMapping.getOrElse(aggs.indices.toArray)

      val sig: String =
        j"""
           |  public final org.apache.flink.types.Row mergeAccumulatorsPair(
           |    org.apache.flink.types.Row a,
           |    org.apache.flink.types.Row b)
           """.stripMargin
      val merge: String = {
        for (i <- aggs.indices) yield
          j"""
             |    ${accTypes(i)} aAcc$i = (${accTypes(i)}) a.getField($i);
             |    ${accTypes(i)} bAcc$i = (${accTypes(i)}) b.getField(${mapping(i)});
             |    accIt$i.setElement(bAcc$i);
             |    ${aggs(i)}.merge(aAcc$i, accIt$i);
             |    a.setField($i, aAcc$i);
             """.stripMargin
      }.mkString("\n")
      val ret: String =
        j"""
           |      return a;
           """.stripMargin

      if (needMerge) {
        j"""
           |$sig {
           |$merge
           |$ret
           |  }""".stripMargin
      } else {
        j"""
           |$sig {
           |$ret
           |  }""".stripMargin
      }
    }

    def genMergeList: String = {
      {
        val singleIterableClass = "org.apache.flink.table.runtime.aggregate.SingleElementIterable"
        for (i <- accTypes.indices) yield
          j"""
             |    private final $singleIterableClass<${accTypes(i)}> accIt$i =
             |      new $singleIterableClass<${accTypes(i)}>();
             """.stripMargin
      }.mkString("\n")
    }

    def genResetAccumulator: String = {

      val sig: String =
        j"""
           |  public final void resetAccumulator(
           |    org.apache.flink.types.Row accs)""".stripMargin

      val reset: String = {
        for (i <- aggs.indices) yield
          j"""
             |    ${aggs(i)}.resetAccumulator(
             |      ((${accTypes(i)}) accs.getField($i)));""".stripMargin
      }.mkString("\n")

      if (needReset) {
        j"""$sig {
           |$reset
           |  }""".stripMargin
      } else {
        j"""$sig {
           |  }""".stripMargin
      }
    }

    var funcCode =
      j"""
         |public final class $funcName
         |  extends org.apache.flink.table.runtime.aggregate.GeneratedAggregations {
         |
         |  ${reuseMemberCode()}
         |  $genMergeList
         |  public $funcName() throws Exception {
         |    ${reuseInitCode()}
         |  }
         |  ${reuseConstructorCode(funcName)}
         |
         """.stripMargin

    funcCode += genSetAggregationResults + "\n"
    funcCode += genAccumulate + "\n"
    funcCode += genRetract + "\n"
    funcCode += genCreateAccumulators + "\n"
    funcCode += genSetForwardedFields + "\n"
    funcCode += genSetConstantFlags + "\n"
    funcCode += genCreateOutputRow + "\n"
    funcCode += genMergeAccumulatorsPair + "\n"
    funcCode += genResetAccumulator + "\n"
    funcCode += "}"

    GeneratedAggregationsFunction(funcName, funcCode)
  }

  /**
    * Generates a [[org.apache.flink.api.common.functions.Function]] that can be passed to Java
    * compiler.
    *
    * @param name Class name of the Function. Must not be unique but has to be a valid Java class
    *             identifier.
    * @param clazz Flink Function to be generated.
    * @param bodyCode code contents of the SAM (Single Abstract Method). Inputs, collector, or
    *                 output record can be accessed via the given term methods.
    * @param returnType expected return type
    * @tparam F Flink Function to be generated.
    * @tparam T Return type of the Flink Function.
    * @return instance of GeneratedFunction
    */
  def generateFunction[F <: Function, T <: Any](
      name: String,
      clazz: Class[F],
      bodyCode: String,
      returnType: TypeInformation[T])
    : GeneratedFunction[F, T] = {
    val funcName = newName(name)

    // Janino does not support generics, that's why we need
    // manual casting here
    val samHeader =
      // FlatMapFunction
      if (clazz == classOf[FlatMapFunction[_, _]]) {
        val baseClass = classOf[RichFlatMapFunction[_, _]]
        val inputTypeTerm = boxedTypeTermForTypeInfo(input1)
        (baseClass,
          s"void flatMap(Object _in1, org.apache.flink.util.Collector $collectorTerm)",
          List(s"$inputTypeTerm $input1Term = ($inputTypeTerm) _in1;"))
      }

      // MapFunction
      else if (clazz == classOf[MapFunction[_, _]]) {
        val baseClass = classOf[RichMapFunction[_, _]]
        val inputTypeTerm = boxedTypeTermForTypeInfo(input1)
        (baseClass,
          "Object map(Object _in1)",
          List(s"$inputTypeTerm $input1Term = ($inputTypeTerm) _in1;"))
      }

      // FlatJoinFunction
      else if (clazz == classOf[FlatJoinFunction[_, _, _]]) {
        val baseClass = classOf[RichFlatJoinFunction[_, _, _]]
        val inputTypeTerm1 = boxedTypeTermForTypeInfo(input1)
        val inputTypeTerm2 = boxedTypeTermForTypeInfo(input2.getOrElse(
          throw new CodeGenException("Input 2 for FlatJoinFunction should not be null")))
        (baseClass,
          s"void join(Object _in1, Object _in2, org.apache.flink.util.Collector $collectorTerm)",
          List(s"$inputTypeTerm1 $input1Term = ($inputTypeTerm1) _in1;",
               s"$inputTypeTerm2 $input2Term = ($inputTypeTerm2) _in2;"))
      }

      // ProcessFunction
      else if (clazz == classOf[ProcessFunction[_, _]]) {
        val baseClass = classOf[ProcessFunction[_, _]]
        val inputTypeTerm = boxedTypeTermForTypeInfo(input1)
        (baseClass,
          s"void processElement(Object _in1, " +
            s"org.apache.flink.streaming.api.functions.ProcessFunction.Context $contextTerm," +
            s"org.apache.flink.util.Collector $collectorTerm)",
          List(s"$inputTypeTerm $input1Term = ($inputTypeTerm) _in1;"))
      }
      else {
        // TODO more functions
        throw new CodeGenException("Unsupported Function.")
      }

    val funcCode = j"""
      public class $funcName
          extends ${samHeader._1.getCanonicalName} {

        ${reuseMemberCode()}

        public $funcName() throws Exception {
          ${reuseInitCode()}
        }

        ${reuseConstructorCode(funcName)}

        @Override
        public void open(${classOf[Configuration].getCanonicalName} parameters) throws Exception {
          ${reuseOpenCode()}
        }

        @Override
        public ${samHeader._2} throws Exception {
          ${samHeader._3.mkString("\n")}
          ${reusePerRecordCode()}
          ${reuseInputUnboxingCode()}
          $bodyCode
        }

        @Override
        public void close() throws Exception {
          ${reuseCloseCode()}
        }
      }
    """.stripMargin

    GeneratedFunction(funcName, returnType, funcCode)
  }

  /**
    * Generates a values input format that can be passed to Java compiler.
    *
    * @param name Class name of the input format. Must not be unique but has to be a
    *             valid Java class identifier.
    * @param records code for creating records
    * @param returnType expected return type
    * @tparam T Return type of the Flink Function.
    * @return instance of GeneratedFunction
    */
  def generateValuesInputFormat[T <: Row](
      name: String,
      records: Seq[String],
      returnType: TypeInformation[T])
    : GeneratedInput[GenericInputFormat[T], T] = {
    val funcName = newName(name)

    addReusableOutRecord(returnType)

    val funcCode = j"""
      public class $funcName extends ${classOf[GenericInputFormat[_]].getCanonicalName} {

        private int nextIdx = 0;

        ${reuseMemberCode()}

        public $funcName() throws Exception {
          ${reuseInitCode()}
        }

        @Override
        public boolean reachedEnd() throws java.io.IOException {
          return nextIdx >= ${records.length};
        }

        @Override
        public Object nextRecord(Object reuse) {
          switch (nextIdx) {
            ${records.zipWithIndex.map { case (r, i) =>
              s"""
                 |case $i:
                 |  $r
                 |break;
               """.stripMargin
            }.mkString("\n")}
          }
          nextIdx++;
          return $outRecordTerm;
        }
      }
    """.stripMargin

    GeneratedInput(funcName, returnType, funcCode)
  }

  /**
    * Generates a [[TableFunctionCollector]] that can be passed to Java compiler.
    *
    * @param name Class name of the table function collector. Must not be unique but has to be a
    *             valid Java class identifier.
    * @param bodyCode body code for the collector method
    * @param collectedType The type information of the element collected by the collector
    * @return instance of GeneratedCollector
    */
  def generateTableFunctionCollector(
      name: String,
      bodyCode: String,
      collectedType: TypeInformation[Any])
    : GeneratedCollector = {

    val className = newName(name)
    val input1TypeClass = boxedTypeTermForTypeInfo(input1)
    val input2TypeClass = boxedTypeTermForTypeInfo(collectedType)

    val funcCode = j"""
      public class $className extends ${classOf[TableFunctionCollector[_]].getCanonicalName} {

        ${reuseMemberCode()}

        public $className() throws Exception {
          ${reuseInitCode()}
        }

        @Override
        public void collect(Object record) throws Exception {
          super.collect(record);
          $input1TypeClass $input1Term = ($input1TypeClass) getInput();
          $input2TypeClass $input2Term = ($input2TypeClass) record;
          ${reuseInputUnboxingCode()}
          $bodyCode
        }

        @Override
        public void close() {
        }
      }
    """.stripMargin

    GeneratedCollector(className, funcCode)
  }

  /**
    * Generates an expression that converts the first input (and second input) into the given type.
    * If two inputs are converted, the second input is appended. If objects or variables can
    * be reused, they will be added to reusable code sections internally. The evaluation result
    * may be stored in the global result variable (see [[outRecordTerm]]).
    *
    * @param returnType conversion target type. Inputs and output must have the same arity.
    * @param resultFieldNames result field names necessary for a mapping to POJO fields.
    * @return instance of GeneratedExpression
    */
  def generateConverterResultExpression(
      returnType: TypeInformation[_ <: Any],
      resultFieldNames: Seq[String])
    : GeneratedExpression = {
    val input1AccessExprs = for (i <- 0 until input1.getArity if input1Mapping.contains(i))
      yield generateInputAccess(input1, input1Term, i, input1Mapping)

    val input2AccessExprs = input2 match {
      case Some(ti) => for (i <- 0 until ti.getArity if input2Mapping.contains(i))
        yield generateInputAccess(ti, input2Term, i, input2Mapping)
      case None => Seq() // add nothing
    }

    generateResultExpression(input1AccessExprs ++ input2AccessExprs, returnType, resultFieldNames)
  }

  /**
    * Generates an expression from the left input and the right table function.
    */
  def generateCorrelateAccessExprs: (Seq[GeneratedExpression], Seq[GeneratedExpression]) = {
    val input1AccessExprs = for (i <- 0 until input1.getArity)
      yield generateInputAccess(input1, input1Term, i, input1Mapping)

    val input2AccessExprs = input2 match {
      case Some(ti) => for (i <- 0 until ti.getArity if input2Mapping.contains(i))
        // use generateFieldAccess instead of generateInputAccess to avoid the generated table
        // function's field access code is put on the top of function body rather than
        // the while loop
        yield generateFieldAccess(ti, input2Term, i, input2Mapping)
      case None => throw new CodeGenException("Type information of input2 must not be null.")
    }
    (input1AccessExprs, input2AccessExprs)
  }

  /**
    * Generates an expression from a sequence of RexNode. If objects or variables can be reused,
    * they will be added to reusable code sections internally. The evaluation result
    * may be stored in the global result variable (see [[outRecordTerm]]).
    *
    * @param returnType conversion target type. Type must have the same arity than rexNodes.
    * @param resultFieldNames result field names necessary for a mapping to POJO fields.
    * @param rexNodes sequence of RexNode to be converted
    * @return instance of GeneratedExpression
    */
  def generateResultExpression(
      returnType: TypeInformation[_ <: Any],
      resultFieldNames: Seq[String],
      rexNodes: Seq[RexNode])
    : GeneratedExpression = {
    val fieldExprs = rexNodes.map(generateExpression)
    generateResultExpression(fieldExprs, returnType, resultFieldNames)
  }

  /**
    * Generates an expression from a sequence of other expressions. If objects or variables can
    * be reused, they will be added to reusable code sections internally. The evaluation result
    * may be stored in the global result variable (see [[outRecordTerm]]).
    *
    * @param fieldExprs field expressions to be converted
    * @param returnType conversion target type. Type must have the same arity than fieldExprs.
    * @param resultFieldNames result field names necessary for a mapping to POJO fields.
    * @return instance of GeneratedExpression
    */
  def generateResultExpression(
      fieldExprs: Seq[GeneratedExpression],
      returnType: TypeInformation[_ <: Any],
      resultFieldNames: Seq[String])
    : GeneratedExpression = {
    // initial type check
    if (returnType.getArity != fieldExprs.length) {
      throw new CodeGenException("Arity of result type does not match number of expressions.")
    }
    if (resultFieldNames.length != fieldExprs.length) {
      throw new CodeGenException("Arity of result field names does not match number of " +
        "expressions.")
    }
    // type check
    returnType match {
      case pt: PojoTypeInfo[_] =>
        fieldExprs.zipWithIndex foreach {
          case (fieldExpr, i) if fieldExpr.resultType != pt.getTypeAt(resultFieldNames(i)) =>
            throw new CodeGenException("Incompatible types of expression and result type.")

          case _ => // ok
        }

      case ct: CompositeType[_] =>
        fieldExprs.zipWithIndex foreach {
          case (fieldExpr, i) if fieldExpr.resultType != ct.getTypeAt(i) =>
            throw new CodeGenException("Incompatible types of expression and result type.")
          case _ => // ok
        }

      case at: AtomicType[_] if at != fieldExprs.head.resultType =>
        throw new CodeGenException("Incompatible types of expression and result type.")

      case _ => // ok
    }

    val returnTypeTerm = boxedTypeTermForTypeInfo(returnType)
    val boxedFieldExprs = fieldExprs.map(generateOutputFieldBoxing)

    // generate result expression
    returnType match {
      case ri: RowTypeInfo =>
        addReusableOutRecord(ri)
        val resultSetters: String = boxedFieldExprs.zipWithIndex map {
          case (fieldExpr, i) =>
            if (nullCheck) {
              s"""
              |${fieldExpr.code}
              |if (${fieldExpr.nullTerm}) {
              |  $outRecordTerm.setField($i, null);
              |}
              |else {
              |  $outRecordTerm.setField($i, ${fieldExpr.resultTerm});
              |}
              |""".stripMargin
            }
            else {
              s"""
              |${fieldExpr.code}
              |$outRecordTerm.setField($i, ${fieldExpr.resultTerm});
              |""".stripMargin
            }
        } mkString "\n"

        GeneratedExpression(outRecordTerm, "false", resultSetters, returnType)

      case pt: PojoTypeInfo[_] =>
        addReusableOutRecord(pt)
        val resultSetters: String = boxedFieldExprs.zip(resultFieldNames) map {
          case (fieldExpr, fieldName) =>
            val accessor = getFieldAccessor(pt.getTypeClass, fieldName)

            accessor match {
              // Reflective access of primitives/Objects
              case ObjectPrivateFieldAccessor(field) =>
                val fieldTerm = addReusablePrivateFieldAccess(pt.getTypeClass, fieldName)

                val defaultIfNull = if (isFieldPrimitive(field)) {
                  primitiveDefaultValue(fieldExpr.resultType)
                } else {
                  "null"
                }

                if (nullCheck) {
                  s"""
                    |${fieldExpr.code}
                    |if (${fieldExpr.nullTerm}) {
                    |  ${reflectiveFieldWriteAccess(
                          fieldTerm,
                          field,
                          outRecordTerm,
                          defaultIfNull)};
                    |}
                    |else {
                    |  ${reflectiveFieldWriteAccess(
                          fieldTerm,
                          field,
                          outRecordTerm,
                          fieldExpr.resultTerm)};
                    |}
                    |""".stripMargin
                }
                else {
                  s"""
                    |${fieldExpr.code}
                    |${reflectiveFieldWriteAccess(
                          fieldTerm,
                          field,
                          outRecordTerm,
                          fieldExpr.resultTerm)};
                    |""".stripMargin
                }

              // primitive or Object field access (implicit boxing)
              case _ =>
                if (nullCheck) {
                  s"""
                    |${fieldExpr.code}
                    |if (${fieldExpr.nullTerm}) {
                    |  $outRecordTerm.$fieldName = null;
                    |}
                    |else {
                    |  $outRecordTerm.$fieldName = ${fieldExpr.resultTerm};
                    |}
                    |""".stripMargin
                }
                else {
                  s"""
                    |${fieldExpr.code}
                    |$outRecordTerm.$fieldName = ${fieldExpr.resultTerm};
                    |""".stripMargin
                }
              }
          } mkString "\n"

        GeneratedExpression(outRecordTerm, "false", resultSetters, returnType)

      case tup: TupleTypeInfo[_] =>
        addReusableOutRecord(tup)
        val resultSetters: String = boxedFieldExprs.zipWithIndex map {
          case (fieldExpr, i) =>
            val fieldName = "f" + i
            if (nullCheck) {
              s"""
                |${fieldExpr.code}
                |if (${fieldExpr.nullTerm}) {
                |  throw new NullPointerException("Null result cannot be stored in a Tuple.");
                |}
                |else {
                |  $outRecordTerm.$fieldName = ${fieldExpr.resultTerm};
                |}
                |""".stripMargin
            }
            else {
              s"""
                |${fieldExpr.code}
                |$outRecordTerm.$fieldName = ${fieldExpr.resultTerm};
                |""".stripMargin
            }
        } mkString "\n"

        GeneratedExpression(outRecordTerm, "false", resultSetters, returnType)

      case cc: CaseClassTypeInfo[_] =>
        val fieldCodes: String = boxedFieldExprs.map(_.code).mkString("\n")
        val constructorParams: String = boxedFieldExprs.map(_.resultTerm).mkString(", ")
        val resultTerm = newName(outRecordTerm)

        val nullCheckCode = if (nullCheck) {
        boxedFieldExprs map { (fieldExpr) =>
          s"""
              |if (${fieldExpr.nullTerm}) {
              |  throw new NullPointerException("Null result cannot be stored in a Case Class.");
              |}
              |""".stripMargin
          } mkString "\n"
        } else {
          ""
        }

        val resultCode =
          s"""
            |$fieldCodes
            |$nullCheckCode
            |$returnTypeTerm $resultTerm = new $returnTypeTerm($constructorParams);
            |""".stripMargin

        GeneratedExpression(resultTerm, "false", resultCode, returnType)

      case a: AtomicType[_] =>
        val fieldExpr = boxedFieldExprs.head
        val nullCheckCode = if (nullCheck) {
          s"""
          |if (${fieldExpr.nullTerm}) {
          |  throw new NullPointerException("Null result cannot be used for atomic types.");
          |}
          |""".stripMargin
        } else {
          ""
        }
        val resultCode =
          s"""
            |${fieldExpr.code}
            |$nullCheckCode
            |""".stripMargin

        GeneratedExpression(fieldExpr.resultTerm, "false", resultCode, returnType)

      case _ =>
        throw new CodeGenException(s"Unsupported result type: $returnType")
    }
  }

  // ----------------------------------------------------------------------------------------------
  // RexVisitor methods
  // ----------------------------------------------------------------------------------------------

  override def visitInputRef(inputRef: RexInputRef): GeneratedExpression = {
    // if inputRef index is within size of input1 we work with input1, input2 otherwise
    val input = if (inputRef.getIndex < input1.getArity) {
      (input1, input1Term, input1Mapping)
    } else {
      (input2.getOrElse(throw new CodeGenException("Invalid input access.")),
        input2Term,
        input2Mapping)
    }

    val index = if (input._2 == input1Term) {
      inputRef.getIndex
    } else {
      inputRef.getIndex - input1.getArity
    }

    generateInputAccess(input._1, input._2, index, input._3)
  }

  override def visitFieldAccess(rexFieldAccess: RexFieldAccess): GeneratedExpression = {
    val refExpr = rexFieldAccess.getReferenceExpr.accept(this)
    val index = rexFieldAccess.getField.getIndex
    val fieldAccessExpr = generateFieldAccess(
      refExpr.resultType,
      refExpr.resultTerm,
      index,
      input1Mapping)

    val resultTerm = newName("result")
    val nullTerm = newName("isNull")
    val resultTypeTerm = primitiveTypeTermForTypeInfo(fieldAccessExpr.resultType)
    val defaultValue = primitiveDefaultValue(fieldAccessExpr.resultType)
    val resultCode = if (nullCheck) {
      s"""
        |${refExpr.code}
        |$resultTypeTerm $resultTerm;
        |boolean $nullTerm;
        |if (${refExpr.nullTerm}) {
        |  $resultTerm = $defaultValue;
        |  $nullTerm = true;
        |}
        |else {
        |  ${fieldAccessExpr.code}
        |  $resultTerm = ${fieldAccessExpr.resultTerm};
        |  $nullTerm = ${fieldAccessExpr.nullTerm};
        |}
        |""".stripMargin
    } else {
      s"""
        |${refExpr.code}
        |${fieldAccessExpr.code}
        |$resultTypeTerm $resultTerm = ${fieldAccessExpr.resultTerm};
        |""".stripMargin
    }

    GeneratedExpression(resultTerm, nullTerm, resultCode, fieldAccessExpr.resultType)
  }

  override def visitLiteral(literal: RexLiteral): GeneratedExpression = {
    val resultType = FlinkTypeFactory.toTypeInfo(literal.getType)
    val value = literal.getValue3
    // null value with type
    if (value == null) {
      return generateNullLiteral(resultType)
    }
    // non-null values
    literal.getType.getSqlTypeName match {

      case BOOLEAN =>
        generateNonNullLiteral(resultType, literal.getValue3.toString)

      case TINYINT =>
        val decimal = BigDecimal(value.asInstanceOf[JBigDecimal])
        if (decimal.isValidByte) {
          generateNonNullLiteral(resultType, decimal.byteValue().toString)
        }
        else {
          throw new CodeGenException("Decimal can not be converted to byte.")
        }

      case SMALLINT =>
        val decimal = BigDecimal(value.asInstanceOf[JBigDecimal])
        if (decimal.isValidShort) {
          generateNonNullLiteral(resultType, decimal.shortValue().toString)
        }
        else {
          throw new CodeGenException("Decimal can not be converted to short.")
        }

      case INTEGER =>
        val decimal = BigDecimal(value.asInstanceOf[JBigDecimal])
        if (decimal.isValidInt) {
          generateNonNullLiteral(resultType, decimal.intValue().toString)
        }
        else {
          throw new CodeGenException("Decimal can not be converted to integer.")
        }

      case BIGINT =>
        val decimal = BigDecimal(value.asInstanceOf[JBigDecimal])
        if (decimal.isValidLong) {
          generateNonNullLiteral(resultType, decimal.longValue().toString + "L")
        }
        else {
          throw new CodeGenException("Decimal can not be converted to long.")
        }

      case FLOAT =>
        val floatValue = value.asInstanceOf[JBigDecimal].floatValue()
        floatValue match {
          case Float.NaN => generateNonNullLiteral(resultType, "java.lang.Float.NaN")
          case Float.NegativeInfinity =>
            generateNonNullLiteral(resultType, "java.lang.Float.NEGATIVE_INFINITY")
          case Float.PositiveInfinity =>
            generateNonNullLiteral(resultType, "java.lang.Float.POSITIVE_INFINITY")
          case _ => generateNonNullLiteral(resultType, floatValue.toString + "f")
        }

      case DOUBLE =>
        val doubleValue = value.asInstanceOf[JBigDecimal].doubleValue()
        doubleValue match {
          case Double.NaN => generateNonNullLiteral(resultType, "java.lang.Double.NaN")
          case Double.NegativeInfinity =>
            generateNonNullLiteral(resultType, "java.lang.Double.NEGATIVE_INFINITY")
          case Double.PositiveInfinity =>
            generateNonNullLiteral(resultType, "java.lang.Double.POSITIVE_INFINITY")
          case _ => generateNonNullLiteral(resultType, doubleValue.toString + "d")
        }
      case DECIMAL =>
        val decimalField = addReusableDecimal(value.asInstanceOf[JBigDecimal])
        generateNonNullLiteral(resultType, decimalField)

      case VARCHAR | CHAR =>
        generateNonNullLiteral(resultType, "\"" + value.toString + "\"")

      case SYMBOL =>
        generateSymbol(value.asInstanceOf[Enum[_]])

      case DATE =>
        generateNonNullLiteral(resultType, value.toString)

      case TIME =>
        generateNonNullLiteral(resultType, value.toString)

      case TIMESTAMP =>
        generateNonNullLiteral(resultType, value.toString + "L")

      case typeName if YEAR_INTERVAL_TYPES.contains(typeName) =>
        val decimal = BigDecimal(value.asInstanceOf[JBigDecimal])
        if (decimal.isValidInt) {
          generateNonNullLiteral(resultType, decimal.intValue().toString)
        } else {
          throw new CodeGenException("Decimal can not be converted to interval of months.")
        }

      case typeName if DAY_INTERVAL_TYPES.contains(typeName) =>
        val decimal = BigDecimal(value.asInstanceOf[JBigDecimal])
        if (decimal.isValidLong) {
          generateNonNullLiteral(resultType, decimal.longValue().toString + "L")
        } else {
          throw new CodeGenException("Decimal can not be converted to interval of milliseconds.")
        }

      case t@_ =>
        throw new CodeGenException(s"Type not supported: $t")
    }
  }

  override def visitCorrelVariable(correlVariable: RexCorrelVariable): GeneratedExpression = {
    GeneratedExpression(input1Term, NEVER_NULL, NO_CODE, input1)
  }

  override def visitLocalRef(localRef: RexLocalRef): GeneratedExpression =
    throw new CodeGenException("Local variables are not supported yet.")

  override def visitRangeRef(rangeRef: RexRangeRef): GeneratedExpression =
    throw new CodeGenException("Range references are not supported yet.")

  override def visitDynamicParam(dynamicParam: RexDynamicParam): GeneratedExpression =
    throw new CodeGenException("Dynamic parameter references are not supported yet.")

  override def visitCall(call: RexCall): GeneratedExpression = {
    // special case: time materialization
    if (call.getOperator == TimeMaterializationSqlFunction) {
      return generateRecordTimestamp(
        FlinkTypeFactory.isRowtimeIndicatorType(call.getOperands.get(0).getType)
      )
    }

    val operands = call.getOperands.map(_.accept(this))
    val resultType = FlinkTypeFactory.toTypeInfo(call.getType)

    call.getOperator match {
      // arithmetic
      case PLUS if isNumeric(resultType) =>
        val left = operands.head
        val right = operands(1)
        requireNumeric(left)
        requireNumeric(right)
        generateArithmeticOperator("+", nullCheck, resultType, left, right)

      case PLUS | DATETIME_PLUS if isTemporal(resultType) =>
        val left = operands.head
        val right = operands(1)
        requireTemporal(left)
        requireTemporal(right)
        generateTemporalPlusMinus(plus = true, nullCheck, call.`type`.getSqlTypeName, left, right)

      case MINUS if isNumeric(resultType) =>
        val left = operands.head
        val right = operands(1)
        requireNumeric(left)
        requireNumeric(right)
        generateArithmeticOperator("-", nullCheck, resultType, left, right)

      case MINUS | MINUS_DATE if isTemporal(resultType) =>
        val left = operands.head
        val right = operands(1)
        requireTemporal(left)
        requireTemporal(right)
        generateTemporalPlusMinus(plus = false, nullCheck, call.`type`.getSqlTypeName, left, right)

      case MULTIPLY if isNumeric(resultType) =>
        val left = operands.head
        val right = operands(1)
        requireNumeric(left)
        requireNumeric(right)
        generateArithmeticOperator("*", nullCheck, resultType, left, right)

      case DIVIDE | DIVIDE_INTEGER if isNumeric(resultType) =>
        val left = operands.head
        val right = operands(1)
        requireNumeric(left)
        requireNumeric(right)
        generateArithmeticOperator("/", nullCheck, resultType, left, right)

      case MOD if isNumeric(resultType) =>
        val left = operands.head
        val right = operands(1)
        requireNumeric(left)
        requireNumeric(right)
        generateArithmeticOperator("%", nullCheck, resultType, left, right)

      case UNARY_MINUS if isNumeric(resultType) =>
        val operand = operands.head
        requireNumeric(operand)
        generateUnaryArithmeticOperator("-", nullCheck, resultType, operand)

      case UNARY_MINUS if isTimeInterval(resultType) =>
        val operand = operands.head
        requireTimeInterval(operand)
        generateUnaryIntervalPlusMinus(plus = false, nullCheck, operand)

      case UNARY_PLUS if isNumeric(resultType) =>
        val operand = operands.head
        requireNumeric(operand)
        generateUnaryArithmeticOperator("+", nullCheck, resultType, operand)

      case UNARY_PLUS if isTimeInterval(resultType) =>
        val operand = operands.head
        requireTimeInterval(operand)
        generateUnaryIntervalPlusMinus(plus = true, nullCheck, operand)

      // comparison
      case EQUALS =>
        val left = operands.head
        val right = operands(1)
        generateEquals(nullCheck, left, right)

      case NOT_EQUALS =>
        val left = operands.head
        val right = operands(1)
        generateNotEquals(nullCheck, left, right)

      case GREATER_THAN =>
        val left = operands.head
        val right = operands(1)
        requireComparable(left)
        requireComparable(right)
        generateComparison(">", nullCheck, left, right)

      case GREATER_THAN_OR_EQUAL =>
        val left = operands.head
        val right = operands(1)
        requireComparable(left)
        requireComparable(right)
        generateComparison(">=", nullCheck, left, right)

      case LESS_THAN =>
        val left = operands.head
        val right = operands(1)
        requireComparable(left)
        requireComparable(right)
        generateComparison("<", nullCheck, left, right)

      case LESS_THAN_OR_EQUAL =>
        val left = operands.head
        val right = operands(1)
        requireComparable(left)
        requireComparable(right)
        generateComparison("<=", nullCheck, left, right)

      case IS_NULL =>
        val operand = operands.head
        generateIsNull(nullCheck, operand)

      case IS_NOT_NULL =>
        val operand = operands.head
        generateIsNotNull(nullCheck, operand)

      // logic
      case AND =>
        operands.reduceLeft { (left: GeneratedExpression, right: GeneratedExpression) =>
          requireBoolean(left)
          requireBoolean(right)
          generateAnd(nullCheck, left, right)
        }

      case OR =>
        operands.reduceLeft { (left: GeneratedExpression, right: GeneratedExpression) =>
          requireBoolean(left)
          requireBoolean(right)
          generateOr(nullCheck, left, right)
        }

      case NOT =>
        val operand = operands.head
        requireBoolean(operand)
        generateNot(nullCheck, operand)

      case CASE =>
        generateIfElse(nullCheck, operands, resultType)

      case IS_TRUE =>
        val operand = operands.head
        requireBoolean(operand)
        generateIsTrue(operand)

      case IS_NOT_TRUE =>
        val operand = operands.head
        requireBoolean(operand)
        generateIsNotTrue(operand)

      case IS_FALSE =>
        val operand = operands.head
        requireBoolean(operand)
        generateIsFalse(operand)

      case IS_NOT_FALSE =>
        val operand = operands.head
        requireBoolean(operand)
        generateIsNotFalse(operand)

      // casting
      case CAST | REINTERPRET =>
        val operand = operands.head
        generateCast(nullCheck, operand, resultType)

      // as / renaming
      case AS =>
        operands.head

      // string arithmetic
      case CONCAT =>
        val left = operands.head
        val right = operands(1)
        requireString(left)
        generateArithmeticOperator("+", nullCheck, resultType, left, right)

      // arrays
      case ARRAY_VALUE_CONSTRUCTOR =>
        generateArray(this, resultType, operands)

      case ITEM =>
        operands.head.resultType match {
          case _: ObjectArrayTypeInfo[_, _] |
               _: BasicArrayTypeInfo[_, _] |
               _: PrimitiveArrayTypeInfo[_] =>
            val array = operands.head
            val index = operands(1)
            requireInteger(index)
            generateArrayElementAt(this, array, index)

          case _: MapTypeInfo[_, _] =>
            val key = operands(1)
            generateMapGet(this, operands.head, key)

          case _ => throw new CodeGenException("Expect an array or a map.")
        }

      case CARDINALITY =>
        val array = operands.head
        requireArray(array)
        generateArrayCardinality(nullCheck, array)

      case ELEMENT =>
        val array = operands.head
        requireArray(array)
        generateArrayElement(this, array)

      // advanced scalar functions
      case sqlOperator: SqlOperator =>
        val callGen = FunctionGenerator.getCallGenerator(
          sqlOperator,
          operands.map(_.resultType),
          resultType)
        callGen
          .getOrElse(throw new CodeGenException(s"Unsupported call: $sqlOperator \n" +
            s"If you think this function should be supported, " +
            s"you can create an issue and start a discussion for it."))
          .generate(this, operands)

      // unknown or invalid
      case call@_ =>
        throw new CodeGenException(s"Unsupported call: $call")
    }
  }

  override def visitOver(over: RexOver): GeneratedExpression =
    throw new CodeGenException("Aggregate functions over windows are not supported yet.")

  override def visitSubQuery(subQuery: RexSubQuery): GeneratedExpression =
    throw new CodeGenException("Subqueries are not supported yet.")

  override def visitPatternFieldRef(fieldRef: RexPatternFieldRef): GeneratedExpression =
    throw new CodeGenException("Pattern field references are not supported yet.")

  // ----------------------------------------------------------------------------------------------
  // generator helping methods
  // ----------------------------------------------------------------------------------------------

  private def generateInputAccess(
      inputType: TypeInformation[_ <: Any],
      inputTerm: String,
      index: Int,
      fieldMapping: Array[Int])
    : GeneratedExpression = {
    // if input has been used before, we can reuse the code that
    // has already been generated
    val inputExpr = reusableInputUnboxingExprs.get((inputTerm, index)) match {
      // input access and unboxing has already been generated
      case Some(expr) =>
        expr

      // generate input access and unboxing if necessary
      case None =>
        val expr = if (nullableInput) {
          generateNullableInputFieldAccess(inputType, inputTerm, index, fieldMapping)
        } else {
          generateFieldAccess(inputType, inputTerm, index, fieldMapping)
        }

        reusableInputUnboxingExprs((inputTerm, index)) = expr
        expr
    }
    // hide the generated code as it will be executed only once
    GeneratedExpression(inputExpr.resultTerm, inputExpr.nullTerm, "", inputExpr.resultType)
  }

  private def generateNullableInputFieldAccess(
      inputType: TypeInformation[_ <: Any],
      inputTerm: String,
      index: Int,
      fieldMapping: Array[Int])
    : GeneratedExpression = {
    val resultTerm = newName("result")
    val nullTerm = newName("isNull")

    val fieldType = inputType match {
      case ct: CompositeType[_] =>
        val fieldIndex = if (ct.isInstanceOf[PojoTypeInfo[_]]) {
          fieldMapping(index)
        }
        else {
          index
        }
        ct.getTypeAt(fieldIndex)
      case at: AtomicType[_] => at
      case _ => throw new CodeGenException("Unsupported type for input field access.")
    }
    val resultTypeTerm = primitiveTypeTermForTypeInfo(fieldType)
    val defaultValue = primitiveDefaultValue(fieldType)
    val fieldAccessExpr = generateFieldAccess(inputType, inputTerm, index, fieldMapping)

    val inputCheckCode =
      s"""
        |$resultTypeTerm $resultTerm;
        |boolean $nullTerm;
        |if ($inputTerm == null) {
        |  $resultTerm = $defaultValue;
        |  $nullTerm = true;
        |}
        |else {
        |  ${fieldAccessExpr.code}
        |  $resultTerm = ${fieldAccessExpr.resultTerm};
        |  $nullTerm = ${fieldAccessExpr.nullTerm};
        |}
        |""".stripMargin

    GeneratedExpression(resultTerm, nullTerm, inputCheckCode, fieldType)
  }

  private def generateFieldAccess(
      inputType: TypeInformation[_],
      inputTerm: String,
      index: Int,
      fieldMapping: Array[Int])
    : GeneratedExpression = {
    inputType match {
      case ct: CompositeType[_] =>
        val fieldIndex = if (ct.isInstanceOf[PojoTypeInfo[_]]) {
          fieldMapping(index)
        }
        else {
          index
        }
        val accessor = fieldAccessorFor(ct, fieldIndex)
        val fieldType: TypeInformation[Any] = ct.getTypeAt(fieldIndex)
        val fieldTypeTerm = boxedTypeTermForTypeInfo(fieldType)

        accessor match {
          case ObjectFieldAccessor(field) =>
            // primitive
            if (isFieldPrimitive(field)) {
              generateNonNullLiteral(fieldType, s"$inputTerm.${field.getName}")
            }
            // Object
            else {
              generateInputFieldUnboxing(
                fieldType,
                s"($fieldTypeTerm) $inputTerm.${field.getName}")
            }

          case ObjectGenericFieldAccessor(fieldName) =>
            // Object
            val inputCode = s"($fieldTypeTerm) $inputTerm.$fieldName"
            generateInputFieldUnboxing(fieldType, inputCode)

          case ObjectMethodAccessor(methodName) =>
            // Object
            val inputCode = s"($fieldTypeTerm) $inputTerm.$methodName()"
            generateInputFieldUnboxing(fieldType, inputCode)

          case ProductAccessor(i) =>
            // Object
            val inputCode = s"($fieldTypeTerm) $inputTerm.getField($i)"
            generateInputFieldUnboxing(fieldType, inputCode)

          case ObjectPrivateFieldAccessor(field) =>
            val fieldTerm = addReusablePrivateFieldAccess(ct.getTypeClass, field.getName)
            val reflectiveAccessCode = reflectiveFieldReadAccess(fieldTerm, field, inputTerm)
            // primitive
            if (isFieldPrimitive(field)) {
              generateNonNullLiteral(fieldType, reflectiveAccessCode)
            }
            // Object
            else {
              generateInputFieldUnboxing(fieldType, reflectiveAccessCode)
            }
        }

      case at: AtomicType[_] =>
        val fieldTypeTerm = boxedTypeTermForTypeInfo(at)
        val inputCode = s"($fieldTypeTerm) $inputTerm"
        generateInputFieldUnboxing(at, inputCode)

      case _ =>
        throw new CodeGenException("Unsupported type for input field access.")
    }
  }

  private def generateNullLiteral(resultType: TypeInformation[_]): GeneratedExpression = {
    val resultTerm = newName("result")
    val nullTerm = newName("isNull")
    val resultTypeTerm = primitiveTypeTermForTypeInfo(resultType)
    val defaultValue = primitiveDefaultValue(resultType)

    if (nullCheck) {
      val wrappedCode = s"""
        |$resultTypeTerm $resultTerm = $defaultValue;
        |boolean $nullTerm = true;
        |""".stripMargin
      GeneratedExpression(resultTerm, nullTerm, wrappedCode, resultType)
    } else {
      throw new CodeGenException("Null literals are not allowed if nullCheck is disabled.")
    }
  }

  private[flink] def generateNonNullLiteral(
      literalType: TypeInformation[_],
      literalCode: String)
    : GeneratedExpression = {
    val resultTerm = newName("result")
    val nullTerm = newName("isNull")
    val resultTypeTerm = primitiveTypeTermForTypeInfo(literalType)

    val resultCode = if (nullCheck) {
      s"""
        |$resultTypeTerm $resultTerm = $literalCode;
        |boolean $nullTerm = false;
        |""".stripMargin
    } else {
      s"""
        |$resultTypeTerm $resultTerm = $literalCode;
        |""".stripMargin
    }

    GeneratedExpression(resultTerm, nullTerm, resultCode, literalType)
  }

  private[flink] def generateSymbol(enum: Enum[_]): GeneratedExpression = {
    GeneratedExpression(
      qualifyEnum(enum),
      "false",
      "",
      new GenericTypeInfo(enum.getDeclaringClass))
  }

  /**
    * Converts the external boxed format to an internal mostly primitive field representation.
    * Wrapper types can autoboxed to their corresponding primitive type (Integer -> int). External
    * objects are converted to their internal representation (Timestamp -> internal timestamp
    * in long).
    *
    * @param fieldType type of field
    * @param fieldTerm expression term of field to be unboxed
    * @return internal unboxed field representation
    */
  private[flink] def generateInputFieldUnboxing(
      fieldType: TypeInformation[_],
      fieldTerm: String)
    : GeneratedExpression = {
    val tmpTerm = newName("tmp")
    val resultTerm = newName("result")
    val nullTerm = newName("isNull")
    val tmpTypeTerm = boxedTypeTermForTypeInfo(fieldType)
    val resultTypeTerm = primitiveTypeTermForTypeInfo(fieldType)
    val defaultValue = primitiveDefaultValue(fieldType)

    // explicit unboxing
    val unboxedFieldCode = if (isTimePoint(fieldType)) {
      timePointToInternalCode(fieldType, fieldTerm)
    } else {
      fieldTerm
    }

    val wrappedCode = if (nullCheck && !isReference(fieldType)) {
      s"""
        |$tmpTypeTerm $tmpTerm = $unboxedFieldCode;
        |boolean $nullTerm = $tmpTerm == null;
        |$resultTypeTerm $resultTerm;
        |if ($nullTerm) {
        |  $resultTerm = $defaultValue;
        |}
        |else {
        |  $resultTerm = $tmpTerm;
        |}
        |""".stripMargin
    } else if (nullCheck) {
      s"""
        |$resultTypeTerm $resultTerm = $unboxedFieldCode;
        |boolean $nullTerm = $fieldTerm == null;
        |""".stripMargin
    } else {
      s"""
        |$resultTypeTerm $resultTerm = $unboxedFieldCode;
        |""".stripMargin
    }

    GeneratedExpression(resultTerm, nullTerm, wrappedCode, fieldType)
  }

  /**
    * Converts the internal mostly primitive field representation to an external boxed format.
    * Primitive types can autoboxed to their corresponding object type (int -> Integer). Internal
    * representations are converted to their external objects (internal timestamp
    * in long -> Timestamp).
    *
    * @param expr expression to be boxed
    * @return external boxed field representation
    */
  private[flink] def generateOutputFieldBoxing(expr: GeneratedExpression): GeneratedExpression = {
    expr.resultType match {
      // convert internal date/time/timestamp to java.sql.* objects
      case SqlTimeTypeInfo.DATE | SqlTimeTypeInfo.TIME | SqlTimeTypeInfo.TIMESTAMP =>
        val resultTerm = newName("result")
        val resultTypeTerm = boxedTypeTermForTypeInfo(expr.resultType)
        val convMethod = internalToTimePointCode(expr.resultType, expr.resultTerm)

        val resultCode = if (nullCheck) {
          s"""
            |${expr.code}
            |$resultTypeTerm $resultTerm;
            |if (${expr.nullTerm}) {
            |  $resultTerm = null;
            |}
            |else {
            |  $resultTerm = $convMethod;
            |}
            |""".stripMargin
        } else {
          s"""
            |${expr.code}
            |$resultTypeTerm $resultTerm = $convMethod;
            |""".stripMargin
        }

        GeneratedExpression(resultTerm, expr.nullTerm, resultCode, expr.resultType)

      // other types are autoboxed or need no boxing
      case _ => expr
    }
  }

  private[flink] def generateRecordTimestamp(isEventTime: Boolean): GeneratedExpression = {
    val resultTerm = newName("result")
    val resultTypeTerm = primitiveTypeTermForTypeInfo(SqlTimeTypeInfo.TIMESTAMP)

    val resultCode = if (isEventTime) {
      s"""
        |$resultTypeTerm $resultTerm;
        |if ($contextTerm.timestamp() == null) {
        |  throw new RuntimeException("Rowtime timestamp is null. Please make sure that a proper " +
        |    "TimestampAssigner is defined and the stream environment uses the EventTime time " +
        |    "characteristic.");
        |}
        |else {
        |  $resultTerm = $contextTerm.timestamp();
        |}
        |""".stripMargin
    } else {
      s"""
        |$resultTypeTerm $resultTerm = $contextTerm.timerService().currentProcessingTime();
        |""".stripMargin
    }
    GeneratedExpression(resultTerm, NEVER_NULL, resultCode, SqlTimeTypeInfo.TIMESTAMP)
  }

  // ----------------------------------------------------------------------------------------------
  // Reusable code snippets
  // ----------------------------------------------------------------------------------------------

  /**
    * Adds a reusable output record to the member area of the generated [[Function]].
    * The passed [[TypeInformation]] defines the type class to be instantiated.
    *
    * @param ti type information of type class to be instantiated during runtime
    * @return member variable term
    */
  def addReusableOutRecord(ti: TypeInformation[_]): Unit = {
    val statement = ti match {
      case rt: RowTypeInfo =>
        s"""
          |transient ${ti.getTypeClass.getCanonicalName} $outRecordTerm =
          |    new ${ti.getTypeClass.getCanonicalName}(${rt.getArity});
          |""".stripMargin
      case _ =>
        s"""
          |${ti.getTypeClass.getCanonicalName} $outRecordTerm =
          |    new ${ti.getTypeClass.getCanonicalName}();
          |""".stripMargin
    }
    reusableMemberStatements.add(statement)
  }

  /**
    * Adds a reusable [[java.lang.reflect.Field]] to the member area of the generated [[Function]].
    * The field can be used for accessing POJO fields more efficiently during runtime, however,
    * the field does not have to be public.
    *
    * @param clazz class of containing field
    * @param fieldName name of field to be extracted and instantiated during runtime
    * @return member variable term
    */
  def addReusablePrivateFieldAccess(clazz: Class[_], fieldName: String): String = {
    val fieldTerm = s"field_${clazz.getCanonicalName.replace('.', '$')}_$fieldName"
    val fieldExtraction =
      s"""
        |transient java.lang.reflect.Field $fieldTerm =
        |    org.apache.flink.api.java.typeutils.TypeExtractor.getDeclaredField(
        |      ${clazz.getCanonicalName}.class, "$fieldName");
        |""".stripMargin
    reusableMemberStatements.add(fieldExtraction)

    val fieldAccessibility =
      s"""
        |$fieldTerm.setAccessible(true);
        |""".stripMargin
    reusableInitStatements.add(fieldAccessibility)

    fieldTerm
  }

  /**
    * Adds a reusable [[java.math.BigDecimal]] to the member area of the generated [[Function]].
    *
    * @param decimal decimal object to be instantiated during runtime
    * @return member variable term
    */
  def addReusableDecimal(decimal: JBigDecimal): String = decimal match {
    case JBigDecimal.ZERO => "java.math.BigDecimal.ZERO"
    case JBigDecimal.ONE => "java.math.BigDecimal.ONE"
    case JBigDecimal.TEN => "java.math.BigDecimal.TEN"
    case _ =>
      val fieldTerm = newName("decimal")
      val fieldDecimal =
        s"""
          |transient java.math.BigDecimal $fieldTerm =
          |    new java.math.BigDecimal("${decimal.toString}");
          |""".stripMargin
      reusableMemberStatements.add(fieldDecimal)
      fieldTerm
  }

  /**
    * Adds a reusable [[UserDefinedFunction]] to the member area of the generated [[Function]].
    *
    * @param function [[UserDefinedFunction]] object to be instantiated during runtime
    * @return member variable term
    */
  def addReusableFunction(function: UserDefinedFunction): String = {
    val classQualifier = function.getClass.getCanonicalName
    val functionSerializedData = UserDefinedFunctionUtils.serialize(function)
    val fieldTerm = s"function_${function.functionIdentifier}"

    val fieldFunction =
      s"""
        |transient $classQualifier $fieldTerm = null;
        |""".stripMargin
    reusableMemberStatements.add(fieldFunction)

    val functionDeserialization =
      s"""
         |$fieldTerm = ($classQualifier)
         |${UserDefinedFunctionUtils.getClass.getName.stripSuffix("$")}
         |.deserialize("$functionSerializedData");
       """.stripMargin

    reusableInitStatements.add(functionDeserialization)

    val openFunction =
      s"""
         |$fieldTerm.open(new ${classOf[FunctionContext].getCanonicalName}(getRuntimeContext()));
       """.stripMargin
    reusableOpenStatements.add(openFunction)

    val closeFunction =
      s"""
         |$fieldTerm.close();
       """.stripMargin
    reusableCloseStatements.add(closeFunction)

    fieldTerm
  }


  /**
    * Adds a reusable constructor statement with the given parameter types.
    *
    * @param parameterTypes The parameter types to construct the function
    * @return member variable terms
    */
  def addReusableConstructor(parameterTypes: Class[_]*): Array[String] = {
    val parameters = mutable.ListBuffer[String]()
    val fieldTerms = mutable.ListBuffer[String]()
    val body = mutable.ListBuffer[String]()

    parameterTypes.zipWithIndex.foreach { case (t, index) =>
      val classQualifier = t.getCanonicalName
      val fieldTerm = newName(s"instance_${classQualifier.replace('.', '$')}")
      val field = s"transient $classQualifier $fieldTerm = null;"
      reusableMemberStatements.add(field)
      fieldTerms += fieldTerm
      parameters += s"$classQualifier arg$index"
      body += s"$fieldTerm = arg$index;"
    }

    reusableConstructorStatements.add((parameters.mkString(","), body.mkString("", "\n", "\n")))

    fieldTerms.toArray
  }

  /**
    * Adds a reusable array to the member area of the generated [[Function]].
    */
  def addReusableArray(clazz: Class[_], size: Int): String = {
    val fieldTerm = newName("array")
    val classQualifier = clazz.getCanonicalName // works also for int[] etc.
    val initArray = classQualifier.replaceFirst("\\[", s"[$size")
    val fieldArray =
      s"""
        |transient $classQualifier $fieldTerm =
        |    new $initArray;
        |""".stripMargin
    reusableMemberStatements.add(fieldArray)
    fieldTerm
  }

  /**
    * Adds a reusable timestamp to the beginning of the SAM of the generated [[Function]].
    */
  def addReusableTimestamp(): String = {
    val fieldTerm = s"timestamp"

    val field =
      s"""
        |final long $fieldTerm = java.lang.System.currentTimeMillis();
        |""".stripMargin
    reusablePerRecordStatements.add(field)
    fieldTerm
  }

    /**
    * Adds a reusable local timestamp to the beginning of the SAM of the generated [[Function]].
    */
  def addReusableLocalTimestamp(): String = {
    val fieldTerm = s"localtimestamp"

    val timestamp = addReusableTimestamp()

    val field =
      s"""
        |final long $fieldTerm = $timestamp + java.util.TimeZone.getDefault().getOffset(timestamp);
        |""".stripMargin
    reusablePerRecordStatements.add(field)
    fieldTerm
  }

  /**
    * Adds a reusable time to the beginning of the SAM of the generated [[Function]].
    */
  def addReusableTime(): String = {
    val fieldTerm = s"time"

    val timestamp = addReusableTimestamp()

    // adopted from org.apache.calcite.runtime.SqlFunctions.currentTime()
    val field =
      s"""
        |final int $fieldTerm = (int) ($timestamp % ${DateTimeUtils.MILLIS_PER_DAY});
        |if (time < 0) {
        |  time += ${DateTimeUtils.MILLIS_PER_DAY};
        |}
        |""".stripMargin
    reusablePerRecordStatements.add(field)
    fieldTerm
  }

  /**
    * Adds a reusable local time to the beginning of the SAM of the generated [[Function]].
    */
  def addReusableLocalTime(): String = {
    val fieldTerm = s"localtime"

    val localtimestamp = addReusableLocalTimestamp()

    // adopted from org.apache.calcite.runtime.SqlFunctions.localTime()
    val field =
      s"""
        |final int $fieldTerm = (int) ($localtimestamp % ${DateTimeUtils.MILLIS_PER_DAY});
        |""".stripMargin
    reusablePerRecordStatements.add(field)
    fieldTerm
  }


  /**
    * Adds a reusable date to the beginning of the SAM of the generated [[Function]].
    */
  def addReusableDate(): String = {
    val fieldTerm = s"date"

    val timestamp = addReusableTimestamp()
    val time = addReusableTime()

    // adopted from org.apache.calcite.runtime.SqlFunctions.currentDate()
    val field =
      s"""
        |final int $fieldTerm = (int) ($timestamp / ${DateTimeUtils.MILLIS_PER_DAY});
        |if ($time < 0) {
        |  $fieldTerm -= 1;
        |}
        |""".stripMargin
    reusablePerRecordStatements.add(field)
    fieldTerm
  }
}
