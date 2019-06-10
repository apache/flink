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

package org.apache.flink.table.api

import org.apache.calcite.plan.RelOptUtil
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.sql2rel.RelDecorrelator
import org.apache.calcite.tools.RuleSet
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.io.DiscardingOutputFormat
import org.apache.flink.api.java.typeutils.GenericTypeInfo
import org.apache.flink.api.java.{DataSet, ExecutionEnvironment}
import org.apache.flink.table.catalog.CatalogManager
import org.apache.flink.table.descriptors.{BatchTableDescriptor, ConnectorDescriptor}
import org.apache.flink.table.explain.PlanJsonParser
import org.apache.flink.table.expressions.BuiltInFunctionDefinitions.TIME_ATTRIBUTES
import org.apache.flink.table.expressions.{CallExpression, Expression}
import org.apache.flink.table.operations.DataSetQueryOperation
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.nodes.dataset.DataSetRel
import org.apache.flink.table.plan.rules.FlinkRuleSets
import org.apache.flink.table.plan.schema._
import org.apache.flink.table.runtime.MapRunner
import org.apache.flink.table.sinks._
import org.apache.flink.table.sources.{BatchTableSource, InputFormatTableSource, TableSource, TableSourceUtil}
import org.apache.flink.table.types.utils.TypeConversions.fromDataTypeToLegacyInfo
import org.apache.flink.table.typeutils.FieldInfoUtils.{calculateTableSchema, getFieldsInfo, validateInputTypeInfo}
import org.apache.flink.table.utils.TableConnectorUtils
import org.apache.flink.types.Row

/**
  * The abstract base class for the implementation of batch TableEnvironments.
  *
  * @param execEnv The [[ExecutionEnvironment]] which is wrapped in this [[BatchTableEnvImpl]].
  * @param config  The [[TableConfig]] of this [[BatchTableEnvImpl]].
  */
abstract class BatchTableEnvImpl(
    private[flink] val execEnv: ExecutionEnvironment,
    config: TableConfig,
    catalogManager: CatalogManager)
  extends TableEnvImpl(config, catalogManager) {

  override def queryConfig: BatchQueryConfig = new BatchQueryConfig

  /**
    * Registers an internal [[BatchTableSource]] in this [[TableEnvImpl]]'s catalog without
    * name checking. Registered tables can be referenced in SQL queries.
    *
    * @param tableSource The [[TableSource]] to register.
    */
  override protected def validateTableSource(tableSource: TableSource[_]): Unit = {
    TableSourceUtil.validateTableSource(tableSource)

    if (!tableSource.isInstanceOf[BatchTableSource[_]] &&
        !tableSource.isInstanceOf[InputFormatTableSource[_]]) {
      throw new TableException("Only BatchTableSource and InputFormatTableSource can be registered " +
        "in BatchTableEnvironment.")
    }
  }

  override protected  def validateTableSink(configuredSink: TableSink[_]): Unit = {
    if (!configuredSink.isInstanceOf[BatchTableSink[_]] &&
        !configuredSink.isInstanceOf[BoundedTableSink[_]]) {
      throw new TableException("Only BatchTableSink and BoundedTableSink can be registered " +
        "in BatchTableEnvironment.")
    }
  }

  def connect(connectorDescriptor: ConnectorDescriptor): BatchTableDescriptor = {
    new BatchTableDescriptor(this, connectorDescriptor)
  }

  /**
    * Writes a [[Table]] to a [[TableSink]].
    *
    * Internally, the [[Table]] is translated into a [[DataSet]] and handed over to the
    * [[TableSink]] to write it.
    *
    * @param table The [[Table]] to write.
    * @param sink The [[TableSink]] to write the [[Table]] to.
    * @param queryConfig The configuration for the query to generate.
    * @tparam T The expected type of the [[DataSet]] which represents the [[Table]].
    */
  override private[flink] def writeToSink[T](
      table: Table,
      sink: TableSink[T],
      queryConfig: QueryConfig): Unit = {

    // Check the query configuration to be a batch one.
    val batchQueryConfig = queryConfig match {
      case batchConfig: BatchQueryConfig => batchConfig
      case _ =>
        throw new TableException("BatchQueryConfig required to configure batch query.")
    }

    sink match {
      case batchSink: BatchTableSink[T] =>
        val outputType = fromDataTypeToLegacyInfo(sink.getConsumedDataType)
          .asInstanceOf[TypeInformation[T]]
        // translate the Table into a DataSet and provide the type that the TableSink expects.
        val result: DataSet[T] = translate(table, batchQueryConfig)(outputType)
        // Give the DataSet to the TableSink to emit it.
        batchSink.emitDataSet(result)
      case _ =>
        throw new TableException("BatchTableSink required to emit batch Table.")
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
  protected def getConversionMapper[IN, OUT](
      physicalTypeInfo: TypeInformation[IN],
      schema: RowSchema,
      requestedTypeInfo: TypeInformation[OUT],
      functionName: String)
    : Option[MapFunction[IN, OUT]] = {

    val converterFunction = generateRowConverterFunction[OUT](
      physicalTypeInfo.asInstanceOf[TypeInformation[Row]],
      schema,
      requestedTypeInfo,
      functionName
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
    val optimizedPlan = optimize(ast)
    val dataSet = translate[Row](optimizedPlan, ast.getRowType, queryConfig) (
      new GenericTypeInfo (classOf[Row]))
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

  def explain(table: Table): String = explain(table: Table, extended = false)

  protected def asQueryOperation[T](dataSet: DataSet[T], fields: Option[Array[Expression]])
    : DataSetQueryOperation[T] = {
    val inputType = dataSet.getType

    val fieldsInfo = fields match {
      case Some(f) =>
        if (f.exists(f =>
          f.isInstanceOf[CallExpression] &&
            TIME_ATTRIBUTES.contains(f.asInstanceOf[CallExpression].getFunctionDefinition))) {
          throw new ValidationException(
            ".rowtime and .proctime time indicators are not allowed in a batch environment.")
        }

        getFieldsInfo[T](inputType, f)
      case None => getFieldsInfo[T](inputType)
    }

    val tableOperation = new DataSetQueryOperation[T](dataSet,
      fieldsInfo.getIndices,
      calculateTableSchema(inputType, fieldsInfo.getIndices, fieldsInfo.getFieldNames))
    tableOperation
  }

  /**
    * Returns the built-in normalization rules that are defined by the environment.
    */
  protected def getBuiltInNormRuleSet: RuleSet = FlinkRuleSets.DATASET_NORM_RULES

  /**
    * Returns the built-in optimization rules that are defined by the environment.
    */
  protected def getBuiltInPhysicalOptRuleSet: RuleSet = FlinkRuleSets.DATASET_OPT_RULES

  /**
    * Generates the optimized [[RelNode]] tree from the original relational node tree.
    *
    * @param relNode The original [[RelNode]] tree
    * @return The optimized [[RelNode]] tree
    */
  private[flink] def optimize(relNode: RelNode): RelNode = {
    val convSubQueryPlan = optimizeConvertSubQueries(relNode)
    val expandedPlan = optimizeExpandPlan(convSubQueryPlan)
    val decorPlan = RelDecorrelator.decorrelateQuery(expandedPlan)
    val normalizedPlan = optimizeNormalizeLogicalPlan(decorPlan)
    val logicalPlan = optimizeLogicalPlan(normalizedPlan)
    optimizePhysicalPlan(logicalPlan, FlinkConventions.DATASET)
  }

  /**
    * Translates a [[Table]] into a [[DataSet]].
    *
    * The transformation involves optimizing the relational expression tree as defined by
    * Table API calls and / or SQL queries and generating corresponding [[DataSet]] operators.
    *
    * @param table The root node of the relational expression tree.
    * @param queryConfig The configuration for the query to generate.
    * @param tpe   The [[TypeInformation]] of the resulting [[DataSet]].
    * @tparam A The type of the resulting [[DataSet]].
    * @return The [[DataSet]] that corresponds to the translated [[Table]].
    */
  protected def translate[A](
      table: Table,
      queryConfig: BatchQueryConfig)(implicit tpe: TypeInformation[A]): DataSet[A] = {
    val relNode = getRelBuilder.tableOperation(table.getQueryOperation).build()
    val dataSetPlan = optimize(relNode)
    translate(dataSetPlan, relNode.getRowType, queryConfig)
  }

  /**
    * Translates a logical [[RelNode]] into a [[DataSet]]. Converts to target type if necessary.
    *
    * @param logicalPlan The root node of the relational expression tree.
    * @param logicalType The row type of the result. Since the logicalPlan can lose the
    *                    field naming during optimization we pass the row type separately.
    * @param queryConfig The configuration for the query to generate.
    * @param tpe         The [[TypeInformation]] of the resulting [[DataSet]].
    * @tparam A The type of the resulting [[DataSet]].
    * @return The [[DataSet]] that corresponds to the translated [[Table]].
    */
  protected def translate[A](
      logicalPlan: RelNode,
      logicalType: RelDataType,
      queryConfig: BatchQueryConfig)(implicit tpe: TypeInformation[A]): DataSet[A] = {
    validateInputTypeInfo(tpe)

    logicalPlan match {
      case node: DataSetRel =>
        val plan = node.translateToPlan(this, queryConfig)
        val conversion =
          getConversionMapper(
            plan.getType,
            new RowSchema(logicalType),
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
}
