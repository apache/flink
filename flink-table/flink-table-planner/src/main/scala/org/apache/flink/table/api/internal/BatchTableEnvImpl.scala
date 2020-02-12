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

package org.apache.flink.table.api.internal

import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.io.DiscardingOutputFormat
import org.apache.flink.api.java.typeutils.GenericTypeInfo
import org.apache.flink.api.java.{DataSet, ExecutionEnvironment}
import org.apache.flink.table.api._
import org.apache.flink.table.calcite.{CalciteConfig, FlinkTypeFactory}
import org.apache.flink.table.catalog.{CatalogBaseTable, CatalogManager}
import org.apache.flink.table.descriptors.{BatchTableDescriptor, ConnectTableDescriptor, ConnectorDescriptor}
import org.apache.flink.table.explain.PlanJsonParser
import org.apache.flink.table.expressions.utils.ApiExpressionDefaultVisitor
import org.apache.flink.table.expressions.{Expression, UnresolvedCallExpression}
import org.apache.flink.table.functions.BuiltInFunctionDefinitions.TIME_ATTRIBUTES
import org.apache.flink.table.module.ModuleManager
import org.apache.flink.table.operations.DataSetQueryOperation
import org.apache.flink.table.plan.BatchOptimizer
import org.apache.flink.table.plan.nodes.dataset.DataSetRel
import org.apache.flink.table.planner.Conversions
import org.apache.flink.table.runtime.MapRunner
import org.apache.flink.table.sinks._
import org.apache.flink.table.sources.{BatchTableSource, InputFormatTableSource, TableSource, TableSourceValidation}
import org.apache.flink.table.types.utils.TypeConversions
import org.apache.flink.table.types.utils.TypeConversions.fromDataTypeToLegacyInfo
import org.apache.flink.table.typeutils.FieldInfoUtils.{getFieldsInfo, validateInputTypeInfo}
import org.apache.flink.table.utils.TableConnectorUtils
import org.apache.flink.types.Row

import org.apache.calcite.plan.RelOptUtil
import org.apache.calcite.rel.RelNode

import _root_.scala.collection.JavaConverters._

/**
  * The abstract base class for the implementation of batch TableEnvironments.
  *
  * @param execEnv The [[ExecutionEnvironment]] which is wrapped in this [[BatchTableEnvImpl]].
  * @param config  The [[TableConfig]] of this [[BatchTableEnvImpl]].
  */
abstract class BatchTableEnvImpl(
    private[flink] val execEnv: ExecutionEnvironment,
    config: TableConfig,
    catalogManager: CatalogManager,
    moduleManager: ModuleManager)
  extends TableEnvImpl(config, catalogManager, moduleManager) {

  private[flink] val optimizer = new BatchOptimizer(
    () => config.getPlannerConfig.unwrap(classOf[CalciteConfig]).orElse(CalciteConfig.DEFAULT),
    planningConfigurationBuilder
  )

  /**
   * Provides necessary methods for [[ConnectTableDescriptor]].
   */
  private val registration = new Registration() {

    override def createTemporaryTable(path: String, table: CatalogBaseTable): Unit = {
      val unresolvedIdentifier = parseIdentifier(path)
      val objectIdentifier = catalogManager.qualifyIdentifier(unresolvedIdentifier)
      catalogManager.createTemporaryTable(table, objectIdentifier, false)
    }
  }

  /**
    * Registers an internal [[BatchTableSource]] in this [[TableEnvImpl]]'s catalog without
    * name checking. Registered tables can be referenced in SQL queries.
    *
    * @param tableSource The [[TableSource]] to register.
    */
  override protected def validateTableSource(tableSource: TableSource[_]): Unit = {
    TableSourceValidation.validateTableSource(tableSource, tableSource.getTableSchema)

    if (!tableSource.isInstanceOf[BatchTableSource[_]] &&
        !tableSource.isInstanceOf[InputFormatTableSource[_]]) {
      throw new TableException("Only BatchTableSource and InputFormatTableSource " +
        "can be registered in BatchTableEnvironment.")
    }
  }

  override protected  def validateTableSink(configuredSink: TableSink[_]): Unit = {
    if (!configuredSink.isInstanceOf[BatchTableSink[_]] &&
        !configuredSink.isInstanceOf[OutputFormatTableSink[_]]) {
      throw new TableException("Only BatchTableSink and OutputFormatTableSink " +
        "can be registered in BatchTableEnvironment.")
    }
  }

  def connect(connectorDescriptor: ConnectorDescriptor): BatchTableDescriptor = {
    new BatchTableDescriptor(registration, connectorDescriptor)
  }

  /**
    * Writes a [[Table]] to a [[TableSink]].
    *
    * Internally, the [[Table]] is translated into a [[DataSet]] and handed over to the
    * [[TableSink]] to write it.
    *
    * @param table The [[Table]] to write.
    * @param sink The [[TableSink]] to write the [[Table]] to.
    * @tparam T The expected type of the [[DataSet]] which represents the [[Table]].
    */
  override private[flink] def writeToSink[T](
      table: Table,
      sink: TableSink[T]): Unit = {

    sink match {
      case batchSink: BatchTableSink[T] =>
        val outputType = fromDataTypeToLegacyInfo(sink.getConsumedDataType)
          .asInstanceOf[TypeInformation[T]]
        // translate the Table into a DataSet and provide the type that the TableSink expects.
        val result: DataSet[T] = translate(table)(outputType)
        // Give the DataSet to the TableSink to emit it.
        batchSink.emitDataSet(result)
      case boundedSink: OutputFormatTableSink[T] =>
        val outputType = fromDataTypeToLegacyInfo(sink.getConsumedDataType)
          .asInstanceOf[TypeInformation[T]]
        // translate the Table into a DataSet and provide the type that the TableSink expects.
        val result: DataSet[T] = translate(table)(outputType)
        // use the OutputFormat to consume the DataSet.
        val dataSink = result.output(boundedSink.getOutputFormat)
        dataSink.name(
          TableConnectorUtils.generateRuntimeName(
            boundedSink.getClass,
            boundedSink.getTableSchema.getFieldNames))
      case _ =>
        throw new TableException(
          "BatchTableSink or OutputFormatTableSink required to emit batch Table.")
    }
  }

  /**
    * Creates a final converter that maps the internal row type to external type.
    *
    * @param physicalTypeInfo the input of the sink
    * @param schema the input schema with correct field names (esp. for POJO field mapping)
    * @param requestedTypeInfo the output type of the sink
    * @param functionName name of the map function. Must not be unique but has to be a
    *                     valid Java class identifier.
    */
  private def getConversionMapper[IN, OUT](
      physicalTypeInfo: TypeInformation[IN],
      schema: TableSchema,
      requestedTypeInfo: TypeInformation[OUT],
      functionName: String)
    : Option[MapFunction[IN, OUT]] = {

    val converterFunction = Conversions.generateRowConverterFunction[OUT](
      physicalTypeInfo.asInstanceOf[TypeInformation[Row]],
      schema,
      requestedTypeInfo,
      functionName,
      config
    )

    // add a runner if we need conversion
    converterFunction.map { func =>
      new MapRunner[IN, OUT](
          func.name,
          func.code,
          func.returnType)
    }
  }

  /**
    * Returns the AST of the specified Table API and SQL queries and the execution plan to compute
    * the result of the given [[Table]].
    *
    * @param table The table for which the AST and execution plan will be returned.
    * @param extended Flag to include detailed optimizer estimates.
    */
  private[flink] def explain(table: Table, extended: Boolean): String = {
    val ast = getRelBuilder.tableOperation(table.getQueryOperation).build()
    val optimizedPlan = optimizer.optimize(ast)
    val dataSet = translate[Row](
      optimizedPlan,
      getTableSchema(table.getQueryOperation.getTableSchema.getFieldNames, optimizedPlan))(
      new GenericTypeInfo(classOf[Row]))
    dataSet.output(new DiscardingOutputFormat[Row])
    val env = dataSet.getExecutionEnvironment
    val jasonSqlPlan = env.getExecutionPlan
    val sqlPlan = PlanJsonParser.getSqlExecutionPlan(jasonSqlPlan, extended)

    s"== Abstract Syntax Tree ==" +
        System.lineSeparator +
        s"${RelOptUtil.toString(ast)}" +
        System.lineSeparator +
        s"== Optimized Logical Plan ==" +
        System.lineSeparator +
        s"${RelOptUtil.toString(optimizedPlan)}" +
        System.lineSeparator +
        s"== Physical Execution Plan ==" +
        System.lineSeparator +
        s"$sqlPlan"
  }

  override def explain(table: Table): String = explain(table: Table, extended = false)

  override def execute(jobName: String): JobExecutionResult = execEnv.execute(jobName)

  override def explain(extended: Boolean): String = {
    throw new TableException("This method is unsupported in old planner.")
  }

  protected def asQueryOperation[T](dataSet: DataSet[T], fields: Option[Array[Expression]])
    : DataSetQueryOperation[T] = {
    val inputType = dataSet.getType

    val fieldsInfo = fields match {
      case Some(f) =>
        checkNoTimeAttributes(f)
        getFieldsInfo[T](inputType, f)

      case None => getFieldsInfo[T](inputType)
    }

    val tableOperation = new DataSetQueryOperation[T](
      dataSet,
      fieldsInfo.getIndices,
      fieldsInfo.toTableSchema)
    tableOperation
  }

  private def checkNoTimeAttributes[T](f: Array[Expression]) = {
    if (f.exists(f =>
      f.accept(new ApiExpressionDefaultVisitor[Boolean] {

        override def visit(call: UnresolvedCallExpression): Boolean = {
          TIME_ATTRIBUTES.contains(call.getFunctionDefinition) ||
            call.getChildren.asScala.exists(_.accept(this))
        }

        override protected def defaultMethod(expression: Expression): Boolean = false
      }))) {
      throw new ValidationException(
        ".rowtime and .proctime time indicators are not allowed in a batch environment.")
    }
  }

  /**
    * Translates a [[Table]] into a [[DataSet]].
    *
    * The transformation involves optimizing the relational expression tree as defined by
    * Table API calls and / or SQL queries and generating corresponding [[DataSet]] operators.
    *
    * @param table The root node of the relational expression tree.
    * @param tpe   The [[TypeInformation]] of the resulting [[DataSet]].
    * @tparam A The type of the resulting [[DataSet]].
    * @return The [[DataSet]] that corresponds to the translated [[Table]].
    */
  protected def translate[A](table: Table)(implicit tpe: TypeInformation[A]): DataSet[A] = {
    val queryOperation = table.getQueryOperation
    val relNode = getRelBuilder.tableOperation(queryOperation).build()
    val dataSetPlan = optimizer.optimize(relNode)
    translate(
      dataSetPlan,
      getTableSchema(queryOperation.getTableSchema.getFieldNames, dataSetPlan))
  }

  /**
    * Translates a logical [[RelNode]] into a [[DataSet]]. Converts to target type if necessary.
    *
    * @param logicalPlan The root node of the relational expression tree.
    * @param logicalType The row type of the result. Since the logicalPlan can lose the
    *                    field naming during optimization we pass the row type separately.
    * @param tpe         The [[TypeInformation]] of the resulting [[DataSet]].
    * @tparam A The type of the resulting [[DataSet]].
    * @return The [[DataSet]] that corresponds to the translated [[Table]].
    */
  protected def translate[A](
      logicalPlan: RelNode,
      logicalType: TableSchema)(implicit tpe: TypeInformation[A]): DataSet[A] = {
    validateInputTypeInfo(tpe)

    logicalPlan match {
      case node: DataSetRel =>
        execEnv.configure(
          config.getConfiguration,
          Thread.currentThread().getContextClassLoader)
        val plan = node.translateToPlan(this, new BatchQueryConfig)
        val conversion =
          getConversionMapper(
            plan.getType,
            logicalType,
            tpe,
            "DataSetSinkConversion")
        conversion match {
          case None => plan.asInstanceOf[DataSet[A]] // no conversion necessary
          case Some(mapFunction: MapFunction[Row, A]) =>
            plan.map(mapFunction)
              .returns(tpe)
              .name(s"to: ${tpe.getTypeClass.getSimpleName}")
              .asInstanceOf[DataSet[A]]
        }

      case _ =>
        throw new TableException("Cannot generate DataSet due to an invalid logical plan. " +
          "This is a bug and should not happen. Please file an issue.")
    }
  }

  /**
    * Returns the record type of the optimized plan with field names of the logical plan.
    */
  private def getTableSchema(originalNames: Array[String], optimizedPlan: RelNode): TableSchema = {
    val fieldTypes = optimizedPlan.getRowType.getFieldList.asScala.map(_.getType)
      .map(FlinkTypeFactory.toTypeInfo)
      .map(TypeConversions.fromLegacyInfoToDataType)
      .toArray

    TableSchema.builder().fields(originalNames, fieldTypes).build()
  }
}
