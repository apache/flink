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

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.async.AsyncFunction
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.data.util.DataFormatConverters
import org.apache.flink.table.data.util.DataFormatConverters.DataFormatConverter
import org.apache.flink.table.data.{GenericRowData, JoinedRowData, RowData}
import org.apache.flink.table.functions.{AsyncTableFunction, TableFunction}
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.codegen.CodeGenUtils._
import org.apache.flink.table.planner.codegen.GenerateUtils._
import org.apache.flink.table.planner.codegen.Indenter.toISC
import org.apache.flink.table.planner.plan.utils.LookupJoinUtil.{ConstantLookupKey, FieldRefLookupKey, LookupKey}
import org.apache.flink.table.runtime.collector.{TableFunctionCollector, TableFunctionResultFuture}
import org.apache.flink.table.runtime.generated.{GeneratedCollector, GeneratedFunction, GeneratedResultFuture}
import org.apache.flink.table.runtime.operators.join.lookup.DelegatingResultFuture
import org.apache.flink.table.runtime.types.LogicalTypeDataTypeConverter.fromLogicalTypeToDataType
import org.apache.flink.table.types.logical.{LogicalType, RowType}
import org.apache.flink.table.types.utils.TypeConversions
import org.apache.flink.types.Row
import org.apache.flink.util.Collector

import org.apache.calcite.rex.{RexNode, RexProgram}

import java.util

object LookupJoinCodeGenerator {

  val ARRAY_LIST = className[util.ArrayList[_]]

  /**
    * Generates a lookup function ([[TableFunction]])
    */
  def generateLookupFunction(
      config: TableConfig,
      typeFactory: FlinkTypeFactory,
      inputType: LogicalType,
      returnType: LogicalType,
      tableReturnTypeInfo: TypeInformation[_],
      lookupKeyInOrder: Array[Int],
      // index field position -> lookup key
      allLookupFields: Map[Int, LookupKey],
      lookupFunction: TableFunction[_],
      enableObjectReuse: Boolean)
    : GeneratedFunction[FlatMapFunction[RowData, RowData]] = {

    val ctx = CodeGeneratorContext(config)
    val (prepareCode, parameters, nullInParameters) = prepareParameters(
      ctx,
      typeFactory,
      inputType,
      lookupKeyInOrder,
      allLookupFields,
      tableReturnTypeInfo.isInstanceOf[RowTypeInfo],
      enableObjectReuse)

    val lookupFunctionTerm = ctx.addReusableFunction(lookupFunction)
    val setCollectorCode = tableReturnTypeInfo match {
      case rt: RowTypeInfo =>
        val converterCollector = new RowToRowDataCollector(rt)
        val term = ctx.addReusableObject(converterCollector, "collector")
        s"""
           |$term.setCollector($DEFAULT_COLLECTOR_TERM);
           |$lookupFunctionTerm.setCollector($term);
         """.stripMargin
      case _ =>
        s"$lookupFunctionTerm.setCollector($DEFAULT_COLLECTOR_TERM);"
    }

    // TODO: filter all records when there is any nulls on the join key, because
    //  "IS NOT DISTINCT FROM" is not supported yet.
    val body =
      s"""
         |$prepareCode
         |$setCollectorCode
         |if ($nullInParameters) {
         |  return;
         |} else {
         |  $lookupFunctionTerm.eval($parameters);
         | }
      """.stripMargin

    FunctionCodeGenerator.generateFunction(
      ctx,
      "LookupFunction",
      classOf[FlatMapFunction[RowData, RowData]],
      body,
      returnType,
      inputType)
  }

  /**
    * Generates a async lookup function ([[AsyncTableFunction]])
    */
  def generateAsyncLookupFunction(
      config: TableConfig,
      typeFactory: FlinkTypeFactory,
      inputType: LogicalType,
      returnType: LogicalType,
      tableReturnTypeInfo: TypeInformation[_],
      lookupKeyInOrder: Array[Int],
      allLookupFields: Map[Int, LookupKey],
      asyncLookupFunction: AsyncTableFunction[_])
    : GeneratedFunction[AsyncFunction[RowData, AnyRef]] = {

    val ctx = CodeGeneratorContext(config)
    val (prepareCode, parameters, nullInParameters) = prepareParameters(
      ctx,
      typeFactory,
      inputType,
      lookupKeyInOrder,
      allLookupFields,
      tableReturnTypeInfo.isInstanceOf[RowTypeInfo],
      fieldCopy = true) // always copy input field because of async buffer

    val lookupFunctionTerm = ctx.addReusableFunction(asyncLookupFunction)
    val DELEGATE = className[DelegatingResultFuture[_]]

    // TODO: filter all records when there is any nulls on the join key, because
    //  "IS NOT DISTINCT FROM" is not supported yet.
    val body =
      s"""
         |$prepareCode
         |if ($nullInParameters) {
         |  $DEFAULT_COLLECTOR_TERM.complete(java.util.Collections.emptyList());
         |  return;
         |} else {
         |  $DELEGATE delegates = new $DELEGATE($DEFAULT_COLLECTOR_TERM);
         |  $lookupFunctionTerm.eval(delegates.getCompletableFuture(), $parameters);
         |}
      """.stripMargin

    FunctionCodeGenerator.generateFunction(
      ctx,
      "LookupFunction",
      classOf[AsyncFunction[RowData, AnyRef]],
      body,
      returnType,
      inputType)
  }

  /**
    * Prepares parameters and returns (code, parameters)
    */
  private def prepareParameters(
      ctx: CodeGeneratorContext,
      typeFactory: FlinkTypeFactory,
      inputType: LogicalType,
      lookupKeyInOrder: Array[Int],
      allLookupFields: Map[Int, LookupKey],
      isExternalArgs: Boolean,
      fieldCopy: Boolean): (String, String, String) = {

    val inputFieldExprs = for (i <- lookupKeyInOrder) yield {
      allLookupFields.get(i) match {
        case Some(ConstantLookupKey(dataType, literal)) =>
          generateLiteral(ctx, dataType, literal.getValue3)
        case Some(FieldRefLookupKey(index)) =>
          generateInputAccess(
            ctx,
            inputType,
            DEFAULT_INPUT1_TERM,
            index,
            nullableInput = false,
            fieldCopy)
        case None =>
          throw new CodeGenException("This should never happen!")
      }
    }
    val codeAndArg = inputFieldExprs
      .map { e =>
        val dataType = fromLogicalTypeToDataType(e.resultType)
        val bType = if (isExternalArgs) {
          typeTerm(dataType.getConversionClass)
        } else {
          boxedTypeTermForType(e.resultType)
        }
        val assign = if (isExternalArgs) {
          CodeGenUtils.genToExternal(ctx, dataType, e.resultTerm)
        } else {
          e.resultTerm
        }
        val newTerm = newName("arg")
        val code =
          s"""
             |$bType $newTerm = null;
             |if (!${e.nullTerm}) {
             |  $newTerm = $assign;
             |}
             """.stripMargin
        (code, newTerm, e.nullTerm)
      }
    (
      codeAndArg.map(_._1).mkString("\n"),
      codeAndArg.map(_._2).mkString(", "),
      codeAndArg.map(_._3).mkString("|| "))
  }

  /**
    * Generates collector for temporal join ([[Collector]])
    *
    * Differs from CommonCorrelate.generateCollector which has no real condition because of
    * FLINK-7865, here we should deal with outer join type when real conditions filtered result.
    */
  def generateCollector(
      ctx: CodeGeneratorContext,
      inputType: RowType,
      udtfTypeInfo: RowType,
      resultType: RowType,
      condition: Option[RexNode],
      pojoFieldMapping: Option[Array[Int]],
      retainHeader: Boolean = true): GeneratedCollector[TableFunctionCollector[RowData]] = {

    val inputTerm = DEFAULT_INPUT1_TERM
    val udtfInputTerm = DEFAULT_INPUT2_TERM

    val exprGenerator = new ExprCodeGenerator(ctx, nullableInput = false)
      .bindInput(udtfTypeInfo, inputTerm = udtfInputTerm, inputFieldMapping = pojoFieldMapping)

    val udtfResultExpr = exprGenerator.generateConverterResultExpression(
      udtfTypeInfo, classOf[GenericRowData])

    val joinedRowTerm = CodeGenUtils.newName("joinedRow")
    ctx.addReusableOutputRecord(resultType, classOf[JoinedRowData], joinedRowTerm)

    val header = if (retainHeader) {
      s"$joinedRowTerm.setRowKind($inputTerm.getRowKind());"
    } else {
      ""
    }

    val body =
      s"""
         |${udtfResultExpr.code}
         |$joinedRowTerm.replace($inputTerm, ${udtfResultExpr.resultTerm});
         |$header
         |outputResult($joinedRowTerm);
      """.stripMargin

    val collectorCode = if (condition.isEmpty) {
      body
    } else {

      val filterGenerator = new ExprCodeGenerator(ctx, nullableInput = false)
        .bindInput(inputType, inputTerm)
        .bindSecondInput(udtfTypeInfo, udtfInputTerm, pojoFieldMapping)
      val filterCondition = filterGenerator.generateExpression(condition.get)

      s"""
         |${filterCondition.code}
         |if (${filterCondition.resultTerm}) {
         |  $body
         |}
         |""".stripMargin
    }

    generateTableFunctionCollectorForJoinTable(
      ctx,
      "JoinTableFuncCollector",
      collectorCode,
      inputType,
      udtfTypeInfo,
      inputTerm = inputTerm,
      collectedTerm = udtfInputTerm)
  }

  /**
    * The only differences against CollectorCodeGenerator.generateTableFunctionCollector is
    * "super.collect" call is binding with collect join row in "body" code
    */
  private def generateTableFunctionCollectorForJoinTable(
      ctx: CodeGeneratorContext,
      name: String,
      bodyCode: String,
      inputType: RowType,
      collectedType: RowType,
      inputTerm: String = DEFAULT_INPUT1_TERM,
      collectedTerm: String = DEFAULT_INPUT2_TERM)
    : GeneratedCollector[TableFunctionCollector[RowData]] = {

    val funcName = newName(name)
    val input1TypeClass = boxedTypeTermForType(inputType)
    val input2TypeClass = boxedTypeTermForType(collectedType)

    val funcCode =
      s"""
      public class $funcName extends ${classOf[TableFunctionCollector[_]].getCanonicalName} {

        ${ctx.reuseMemberCode()}

        public $funcName(Object[] references) throws Exception {
          ${ctx.reuseInitCode()}
        }

        @Override
        public void open(${className[Configuration]} parameters) throws Exception {
          ${ctx.reuseOpenCode()}
        }

        @Override
        public void collect(Object record) throws Exception {
          $input1TypeClass $inputTerm = ($input1TypeClass) getInput();
          $input2TypeClass $collectedTerm = ($input2TypeClass) record;
          ${ctx.reuseLocalVariableCode()}
          ${ctx.reuseInputUnboxingCode()}
          $bodyCode
        }

        @Override
        public void close() throws Exception {
          ${ctx.reuseCloseCode()}
        }
      }
    """.stripMargin

    new GeneratedCollector(funcName, funcCode, ctx.references.toArray)
  }

  /**
    * Generates a [[TableFunctionResultFuture]] that can be passed to Java compiler.
    *
    * @param config The TableConfig
    * @param name Class name of the table function collector. Must not be unique but has to be a
    *             valid Java class identifier.
    * @param leftInputType The type information of the element being collected
    * @param collectedType The type information of the element collected by the collector
    * @param condition The filter condition before collect elements
    * @return instance of GeneratedCollector
    */
  def generateTableAsyncCollector(
      config: TableConfig,
      name: String,
      leftInputType: RowType,
      collectedType: RowType,
      condition: Option[RexNode])
    : GeneratedResultFuture[TableFunctionResultFuture[RowData]] = {

    val funcName = newName(name)
    val input1TypeClass = boxedTypeTermForType(leftInputType)
    val input2TypeClass = boxedTypeTermForType(collectedType)
    val input1Term = DEFAULT_INPUT1_TERM
    val input2Term = DEFAULT_INPUT2_TERM
    val outTerm = "resultCollection"

    val ctx = CodeGeneratorContext(config)

    val body = if (condition.isEmpty) {
      "getResultFuture().complete(records);"
    } else {
      val filterGenerator = new ExprCodeGenerator(ctx, nullableInput = false)
        .bindInput(leftInputType, input1Term)
        .bindSecondInput(collectedType, input2Term)
      val filterCondition = filterGenerator.generateExpression(condition.get)

      s"""
         |if (records == null || records.size() == 0) {
         |  getResultFuture().complete(java.util.Collections.emptyList());
         |  return;
         |}
         |try {
         |  $input1TypeClass $input1Term = ($input1TypeClass) getInput();
         |  $ARRAY_LIST $outTerm = new $ARRAY_LIST();
         |  for (Object record : records) {
         |    $input2TypeClass $input2Term = ($input2TypeClass) record;
         |    ${ctx.reuseLocalVariableCode()}
         |    ${ctx.reuseInputUnboxingCode()}
         |    ${ctx.reusePerRecordCode()}
         |    ${filterCondition.code}
         |    if (${filterCondition.resultTerm}) {
         |      $outTerm.add(record);
         |    }
         |  }
         |  getResultFuture().complete($outTerm);
         |} catch (Exception e) {
         |  getResultFuture().completeExceptionally(e);
         |}
         |""".stripMargin
    }

    val funcCode =
      j"""
      public class $funcName extends ${classOf[TableFunctionResultFuture[_]].getCanonicalName} {

        ${ctx.reuseMemberCode()}

        public $funcName(Object[] references) throws Exception {
          ${ctx.reuseInitCode()}
        }

        @Override
        public void open(${className[Configuration]} parameters) throws Exception {
          ${ctx.reuseOpenCode()}
        }

        @Override
        public void complete(java.util.Collection records) throws Exception {
          $body
        }

        public void close() throws Exception {
          ${ctx.reuseCloseCode()}
        }
      }
    """.stripMargin

    new GeneratedResultFuture(funcName, funcCode, ctx.references.toArray)
  }

  /**
    * Generates calculate flatmap function for temporal join which is used
    * to projection/filter the dimension table results
    */
  def generateCalcMapFunction(
      config: TableConfig,
      calcProgram: Option[RexProgram],
      tableSourceRowType: RowType)
    : GeneratedFunction[FlatMapFunction[RowData, RowData]] = {

    val program = calcProgram.get
    val condition = if (program.getCondition != null) {
      Some(program.expandLocalRef(program.getCondition))
    } else {
      None
    }
    CalcCodeGenerator.generateFunction(
      tableSourceRowType,
      "TableCalcMapFunction",
      FlinkTypeFactory.toLogicalRowType(program.getOutputRowType),
      classOf[GenericRowData],
      program,
      condition,
      config)
  }


  // ----------------------------------------------------------------------------------------
  //                                Utility Classes
  // ----------------------------------------------------------------------------------------

  class RowToRowDataCollector(rowTypeInfo: RowTypeInfo)
    extends TableFunctionCollector[Row] with Serializable {

    private val converter = DataFormatConverters.getConverterForDataType(
      TypeConversions.fromLegacyInfoToDataType(rowTypeInfo))
        .asInstanceOf[DataFormatConverter[RowData, Row]]

    override def collect(record: Row): Unit = {
      val result = converter.toInternal(record)
      outputResult(result)
    }
  }
}
