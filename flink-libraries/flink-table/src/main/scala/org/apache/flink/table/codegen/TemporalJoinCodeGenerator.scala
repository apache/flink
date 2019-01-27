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


import java.util.{ArrayList => JArrayList, Collection => JCollection}
import org.apache.calcite.rex.{RexLiteral, RexNode, RexProgram}
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.streaming.api.functions.async.{AsyncFunction, ResultFuture}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{TableConfig, TableConfigOptions}
import org.apache.flink.table.api.functions.{AsyncTableFunction, TableFunction}
import org.apache.flink.table.api.types.{DataTypes, InternalType, RowType}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.codegen.CodeGenUtils._
import org.apache.flink.table.dataformat.{BaseRow, GenericRow, JoinedRow}
import org.apache.flink.table.plan.schema.BaseRowSchema
import org.apache.flink.table.runtime.collector.TableFunctionCollector
import org.apache.flink.table.runtime.conversion.DataStructureConverters.RowConverter
import org.apache.flink.types.Row
import org.apache.flink.util.{Collector, Preconditions}

object TemporalJoinCodeGenerator {

  /**
    * Generates a lookup function ([[TableFunction]])
    */
  def generateLookupFunction(
    config: TableConfig,
    typeFactory: FlinkTypeFactory,
    inputType: InternalType,
    returnType: InternalType,
    tableReturnTypeInfo: TypeInformation[_],
    tableReturnClass: Class[_],
    lookupKeyInOrder: Array[Int],
    lookupKeysFromInput: Map[Int, Int], // lookup key index -> input field index
    lookupKeysFromConstant: Map[Int, RexLiteral],  // lookup key index -> constant value
    lookupFunction: TableFunction[_],
    enableObjectReuse: Boolean)
  : GeneratedFunction[FlatMapFunction[BaseRow, BaseRow], BaseRow] = {

    val ctx = CodeGeneratorContext(config)
    val (prepareCode, parameters) = prepareParameters(
      ctx,
      config,
      typeFactory,
      inputType,
      lookupKeyInOrder,
      lookupKeysFromInput,
      lookupKeysFromConstant,
      enableObjectReuse)

    val lookupFunctionTerm = ctx.addReusableFunction(lookupFunction)

    val setCollectorCode = if (tableReturnClass == classOf[Row]) {
      val converterCollector =
        new RowToBaseRowCollector(tableReturnTypeInfo.asInstanceOf[RowTypeInfo])
      val term = ctx.addReusableObject(converterCollector, "collector")
      s"""
         |$term.setCollector(${CodeGeneratorContext.DEFAULT_COLLECTOR_TERM});
         |$lookupFunctionTerm.setCollector($term);
         """.stripMargin
    } else {
      s"$lookupFunctionTerm.setCollector(${CodeGeneratorContext.DEFAULT_COLLECTOR_TERM});"
    }

    val body =
      s"""
        |$prepareCode
        |$setCollectorCode
        |$lookupFunctionTerm.eval($parameters);
      """.stripMargin

    FunctionCodeGenerator.generateFunction(
      ctx,
      "LookupFunction",
      classOf[FlatMapFunction[BaseRow, BaseRow]],
      body,
      returnType,
      inputType,
      config)
  }

  /**
    * Generates a async lookup function ([[AsyncTableFunction]])
    */
  def generateAsyncLookupFunction(
    config: TableConfig,
    typeFactory: FlinkTypeFactory,
    inputType: InternalType,
    returnType: InternalType,
    tableReturnTypeInfo: TypeInformation[_],
    tableReturnClass: Class[_],
    lookupKeyInOrder: Array[Int],
    lookupKeysFromInput: Map[Int, Int], // lookup key index -> input field index
    lookupKeysFromConstant: Map[Int, RexLiteral],
    asyncLookupFunction: AsyncTableFunction[_])
  : GeneratedFunction[AsyncFunction[BaseRow, BaseRow], BaseRow] = {

    val ctx = CodeGeneratorContext(config)
    val (prepareCode, parameters) = prepareParameters(
      ctx,
      config,
      typeFactory,
      inputType,
      lookupKeyInOrder,
      lookupKeysFromInput,
      lookupKeysFromConstant,
      fieldCopy = true) // always copy input field because of async buffer

    val lookupFunctionTerm = ctx.addReusableFunction(asyncLookupFunction)

    var futureTerm: String = null
    val setFutureCode = if (tableReturnClass == classOf[Row]) {
      val converterFuture =
        new RowToBaseRowResultFuture(tableReturnTypeInfo.asInstanceOf[RowTypeInfo])
      futureTerm = ctx.addReusableObject(converterFuture, "future")
      s"$futureTerm.setFuture(${CodeGeneratorContext.DEFAULT_COLLECTOR_TERM});"
    } else {
      futureTerm = CodeGeneratorContext.DEFAULT_COLLECTOR_TERM
      ""
    }

    val body =
      s"""
         |$prepareCode
         |$setFutureCode
         |$lookupFunctionTerm.eval($futureTerm, $parameters);
      """.stripMargin

    FunctionCodeGenerator.generateFunction(
      ctx,
      "LookupFunction",
      classOf[AsyncFunction[BaseRow, BaseRow]],
      body,
      returnType,
      inputType,
      config)
  }

  private def prepareParameters(
    ctx: CodeGeneratorContext,
    config: TableConfig,
    typeFactory: FlinkTypeFactory,
    inputType: InternalType,
    lookupKeyInOrder: Array[Int],
    lookupKeysFromInput: Map[Int, Int], // lookup key index -> input field index
    lookupKeysFromConstant: Map[Int, RexLiteral],
    fieldCopy: Boolean): (String, String) = {

    // the total number of lookupKeys should equal to fromInput plus fromConstant
    Preconditions.checkArgument(
      lookupKeyInOrder.length == lookupKeysFromInput.size + lookupKeysFromConstant.size)
    val inputFieldExprs = for (i <- lookupKeyInOrder) yield {
      if (lookupKeysFromInput.contains(i)) {
        generateInputAccess(
          ctx,
          inputType,
          CodeGeneratorContext.DEFAULT_INPUT1_TERM,
          lookupKeysFromInput(i),
          nullableInput = false,
          config.getNullCheck,
          fieldCopy)
      } else if (lookupKeysFromConstant.contains(i)) {
        val literal = lookupKeysFromConstant(i)
        val resultType = FlinkTypeFactory.toInternalType(literal.getType)
        val value = literal.getValue3
        generateLiteral(ctx, literal.getType, resultType, value, config.getNullCheck)
      } else {
        throw new CodeGenException("This should never happen!")
      }
    }
    val codeAndArg = inputFieldExprs
        .map { e =>
          val bType = boxedTypeTermForType(e.resultType)
          val newTerm = newName("arg")
          val code =
            s"""
               |$bType $newTerm = null;
               |if (!${e.nullTerm}) {
               |  $newTerm = ${e.resultTerm};
               |}
             """.stripMargin
          (code, newTerm)
        }
    (codeAndArg.map(_._1).mkString("\n"), codeAndArg.map(_._2).mkString(", "))
  }

  /**
    * Generates async collector for async temporal join ([[ResultFuture]])
    */
  def generateAsyncCollector(
    config: TableConfig,
    inputType: RowType,
    tableType: RowType,
    joinCondition: Option[RexNode]): GeneratedCollector = {

    val inputTerm = CodeGeneratorContext.DEFAULT_INPUT1_TERM
    val tableInputTerm = CodeGeneratorContext.DEFAULT_INPUT2_TERM

    val ctx = CodeGeneratorContext(config)
    val exprGenerator = new ExprCodeGenerator(ctx, false, config.getNullCheck)
      .bindInput(tableType, inputTerm = tableInputTerm)

    val tableResultExpr = exprGenerator.generateConverterResultExpression(
      tableType, classOf[GenericRow])

    val body =
      s"""
         |${tableResultExpr.code}
         |getCollector().complete(java.util.Collections.singleton(${tableResultExpr.resultTerm}));
      """.stripMargin

    val collectorCode = if (joinCondition.isEmpty) {
      body
    } else {

      val filterGenerator = new ExprCodeGenerator(ctx, false, config.getNullCheck)
        .bindInput(inputType, inputTerm)
        .bindSecondInput(tableType, tableInputTerm)
      val filterCondition = filterGenerator.generateExpression(joinCondition.get)

      s"""
         |${filterCondition.code}
         |if (${filterCondition.resultTerm}) {
         |  $body
         |} else {
         |  getCollector().complete(java.util.Collections.emptyList());
         |}
         |""".stripMargin
    }

    CollectorCodeGenerator.generateTableAsyncCollector(
      ctx,
      "TableAsyncCollector",
      collectorCode,
      inputType,
      tableType,
      config)
  }

  /**
    * Generates collector for temporal join ([[Collector]])
    *
    * Differs from CommonCorrelate.generateCollector which has no real condition because of
    * FLINK-7865, here we should deal with outer join type when real conditions filtered result.
    */
  def generateCollector(
    ctx: CodeGeneratorContext,
    config: TableConfig,
    inputType: RowType,
    udtfTypeInfo: RowType,
    resultType: RowType,
    condition: Option[RexNode],
    pojoFieldMapping: Option[Array[Int]],
    retainHeader: Boolean = true): GeneratedCollector = {

    val inputTerm = CodeGeneratorContext.DEFAULT_INPUT1_TERM
    val udtfInputTerm = CodeGeneratorContext.DEFAULT_INPUT2_TERM

    val exprGenerator = new ExprCodeGenerator(ctx, false, config.getNullCheck)
      .bindInput(udtfTypeInfo, inputTerm = udtfInputTerm, inputFieldMapping = pojoFieldMapping)

    val udtfResultExpr = exprGenerator.generateConverterResultExpression(
      udtfTypeInfo, classOf[GenericRow])

    val joinedRowTerm = CodeGenUtils.newName("joinedRow")
    ctx.addOutputRecord(resultType, classOf[JoinedRow], joinedRowTerm)

    val header = if (retainHeader) {
      s"$joinedRowTerm.setHeader($inputTerm.getHeader());"
    } else {
      ""
    }

    val body =
      s"""
         |${udtfResultExpr.code}
         |$joinedRowTerm.replace($inputTerm, ${udtfResultExpr.resultTerm});
         |$header
         |getCollector().collect($joinedRowTerm);
         |super.collect(record);
      """.stripMargin

    val collectorCode = if (condition.isEmpty) {
      body
    } else {

      val filterGenerator = new ExprCodeGenerator(ctx, false, config.getNullCheck)
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
      config,
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
    config: TableConfig,
    inputTerm: String = CodeGeneratorContext.DEFAULT_INPUT1_TERM,
    collectedTerm: String = CodeGeneratorContext.DEFAULT_INPUT2_TERM)
  : GeneratedCollector = {

    val className = newName(name)
    val input1TypeClass = boxedTypeTermForType(inputType)
    val input2TypeClass = boxedTypeTermForType(collectedType)

    val unboxingCodeSplit = generateSplitFunctionCalls(
      ctx.reusableInputUnboxingExprs.values.map(_.code).toSeq,
      config.getConf.getInteger(TableConfigOptions.SQL_CODEGEN_LENGTH_MAX),
      "inputUnbox",
      "private final void",
      ctx.reuseFieldCode().length,
      defineParams = s"$input1TypeClass $inputTerm, $input2TypeClass $collectedTerm",
      callingParams = s"$inputTerm, $collectedTerm"
    )


    val funcCode = if (unboxingCodeSplit.isSplit) {
      s"""
      public class $className extends ${classOf[TableFunctionCollector[_]].getCanonicalName} {

        ${ctx.reuseMemberCode()}
        ${ctx.reuseFieldCode()}

        public $className() throws Exception {
          ${ctx.reuseInitCode()}
        }

        @Override
        public void collect(Object record) throws Exception {
          $input1TypeClass $inputTerm = ($input1TypeClass) getInput();
          $input2TypeClass $collectedTerm = ($input2TypeClass) record;
          ${unboxingCodeSplit.callings.mkString("\n")}
          $bodyCode
        }

        ${unboxingCodeSplit.definitions.zip(unboxingCodeSplit.bodies) map {
        case (define, body) =>
          s"""
             |$define throws Exception {
             |  $body
             |}
          """.stripMargin
      } mkString "\n"
      }

        @Override
        public void close() {
        }
      }
    """.stripMargin
    } else {
      s"""
      public class $className extends ${classOf[TableFunctionCollector[_]].getCanonicalName} {

        ${ctx.reuseMemberCode()}

        public $className() throws Exception {
          ${ctx.reuseInitCode()}
        }

        @Override
        public void collect(Object record) throws Exception {
          $input1TypeClass $inputTerm = ($input1TypeClass) getInput();
          $input2TypeClass $collectedTerm = ($input2TypeClass) record;
          ${ctx.reuseFieldCode()}
          ${ctx.reuseInputUnboxingCode()}
          $bodyCode
        }

        @Override
        public void close() {
        }
      }
    """.stripMargin
    }

    GeneratedCollector(className, funcCode)
  }

  /**
    * Genrates calculate flatmap function for temporal join which is used
    * to projection/filter the dimension table results
    */
  def generateCalcMapFunction(
    config: TableConfig,
    calcProgram: Option[RexProgram],
    tableSourceSchema: BaseRowSchema)
  : GeneratedFunction[FlatMapFunction[BaseRow, BaseRow], BaseRow] = {

    val program = calcProgram.get
    val condition = if (program.getCondition != null) {
      Some(program.expandLocalRef(program.getCondition))
    } else {
      None
    }
    CalcCodeGenerator.generateFunction(
      tableSourceSchema.internalType,
      "TableCalcMapFunction",
      FlinkTypeFactory.toInternalRowType(program.getOutputRowType),
      classOf[GenericRow],
      program,
      condition,
      config,
      classOf[FlatMapFunction[BaseRow, BaseRow]])
  }


  // ----------------------------------------------------------------------------------------
  //                                Utility Classes
  // ----------------------------------------------------------------------------------------

  class RowToBaseRowCollector(rowTypeInfo: RowTypeInfo)
    extends TableFunctionCollector[Row] with Serializable {

    private val converter =
      RowConverter(rowTypeInfo.toInternalType.asInstanceOf[RowType])

    override def collect(record: Row): Unit = {
      super.collect(record)
      val result = converter.toInternalImpl(record)
      getCollector.asInstanceOf[Collector[BaseRow]].collect(result)
    }

    override def reset(): Unit = {
      super.reset()
      getCollector.asInstanceOf[TableFunctionCollector[_]].reset()
    }

    override def close(): Unit = getCollector.close()
  }

  class RowToBaseRowResultFuture(rowTypeInfo: RowTypeInfo)
    extends ResultFuture[Row] with Serializable {

    private val converter =
      RowConverter(rowTypeInfo.toInternalType.asInstanceOf[RowType])
    private var future: ResultFuture[BaseRow] = _

    def setFuture(future: ResultFuture[BaseRow]): Unit = {
      this.future = future
    }

    override def complete(result: JCollection[Row]): Unit = {
      if (result == null) {
        this.future.complete(null)
      } else {
        val baseRowResult = new JArrayList[BaseRow]
        val iter = result.iterator()
        while (iter.hasNext) {
          baseRowResult.add(converter.toInternalImpl(iter.next()))
        }
        this.future.complete(baseRowResult)
      }
    }

    override def completeExceptionally(error: Throwable): Unit = {
      this.future.completeExceptionally(error)
    }
  }
}
