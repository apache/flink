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

import _root_.java.util.concurrent.atomic.AtomicInteger

import org.apache.calcite.plan.RelOptUtil
import org.apache.calcite.plan.hep.HepMatchOrder
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.sql2rel.RelDecorrelator
import org.apache.calcite.tools.RuleSet
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.io.DiscardingOutputFormat
import org.apache.flink.api.java.typeutils.GenericTypeInfo
import org.apache.flink.api.java.{DataSet, ExecutionEnvironment}
import org.apache.flink.table.explain.PlanJsonParser
import org.apache.flink.table.expressions.{Expression, TimeAttribute}
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.nodes.dataset.DataSetRel
import org.apache.flink.table.plan.rules.FlinkRuleSets
import org.apache.flink.table.plan.schema.{DataSetTable, RowSchema, TableSourceTable}
import org.apache.flink.table.runtime.MapRunner
import org.apache.flink.table.sinks.{BatchTableSink, TableSink}
import org.apache.flink.table.sources.{BatchTableSource, TableSource}
import org.apache.flink.types.Row

/**
  * The abstract base class for batch TableEnvironments.
  *
  * A TableEnvironment can be used to:
  * - convert a [[DataSet]] to a [[Table]]
  * - register a [[DataSet]] in the [[TableEnvironment]]'s catalog
  * - register a [[Table]] in the [[TableEnvironment]]'s catalog
  * - scan a registered table to obtain a [[Table]]
  * - specify a SQL query on registered tables to obtain a [[Table]]
  * - convert a [[Table]] into a [[DataSet]]
  * - explain the AST and execution plan of a [[Table]]
  *
  * @param execEnv The [[ExecutionEnvironment]] which is wrapped in this [[BatchTableEnvironment]].
  * @param config The [[TableConfig]] of this [[BatchTableEnvironment]].
  */
abstract class BatchTableEnvironment(
    private[flink] val execEnv: ExecutionEnvironment,
    config: TableConfig)
  extends TableEnvironment(config) {

  // a counter for unique table names.
  private val nameCntr: AtomicInteger = new AtomicInteger(0)

  // the naming pattern for internally registered tables.
  private val internalNamePattern = "^_DataSetTable_[0-9]+$".r

  /**
    * Checks if the chosen table name is valid.
    *
    * @param name The table name to check.
    */
  override protected def checkValidTableName(name: String): Unit = {
    val m = internalNamePattern.findFirstIn(name)
    m match {
      case Some(_) =>
        throw new TableException(s"Illegal Table name. " +
          s"Please choose a name that does not contain the pattern $internalNamePattern")
      case None =>
    }
  }

  /** Returns a unique table name according to the internal naming pattern. */
  protected def createUniqueTableName(): String = "_DataSetTable_" + nameCntr.getAndIncrement()

  /**
    * Registers an external [[BatchTableSource]] in this [[TableEnvironment]]'s catalog.
    * Registered tables can be referenced in SQL queries.
    *
    * @param name        The name under which the [[TableSource]] is registered.
    * @param tableSource The [[TableSource]] to register.
    */
  override def registerTableSource(name: String, tableSource: TableSource[_]): Unit = {
    checkValidTableName(name)

    tableSource match {
      case batchTableSource: BatchTableSource[_] =>
        registerTableInternal(name, new TableSourceTable(batchTableSource))
      case _ =>
        throw new TableException("Only BatchTableSource can be registered in " +
            "BatchTableEnvironment")
    }
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

    // We do not pass the configuration on, because there is nothing to configure for batch queries.
    queryConfig match {
      case _: BatchQueryConfig =>
      case _ =>
        throw new TableException("BatchQueryConfig required to configure batch query.")
    }

    sink match {
      case batchSink: BatchTableSink[T] =>
        val outputType = sink.getOutputType
        // translate the Table into a DataSet and provide the type that the TableSink expects.
        val result: DataSet[T] = translate(table)(outputType)
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
      functionName: String):
    Option[MapFunction[IN, OUT]] = {

    if (requestedTypeInfo.getTypeClass == classOf[Row]) {
      // Row to Row, no conversion needed
      None
    } else {
      // some type that is neither Row or CRow

      val converterFunction = generateRowConverterFunction[OUT](
        physicalTypeInfo.asInstanceOf[TypeInformation[Row]],
        schema,
        requestedTypeInfo,
        functionName
      )

      val mapFunction = new MapRunner[IN, OUT](
        converterFunction.name,
        converterFunction.code,
        converterFunction.returnType)

      Some(mapFunction)
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
    val ast = table.getRelNode
    val optimizedPlan = optimize(ast)
    val dataSet = translate[Row](optimizedPlan, ast.getRowType) (new GenericTypeInfo(classOf[Row]))
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

  /**
    * Returns the AST of the specified Table API and SQL queries and the execution plan to compute
    * the result of the given [[Table]].
    *
    * @param table The table for which the AST and execution plan will be returned.
    */
  def explain(table: Table): String = explain(table: Table, extended = false)

  /**
    * Registers a [[DataSet]] as a table under a given name in the [[TableEnvironment]]'s catalog.
    *
    * @param name The name under which the table is registered in the catalog.
    * @param dataSet The [[DataSet]] to register as table in the catalog.
    * @tparam T the type of the [[DataSet]].
    */
  protected def registerDataSetInternal[T](name: String, dataSet: DataSet[T]): Unit = {

    val (fieldNames, fieldIndexes) = getFieldInfo[T](dataSet.getType)
    val dataSetTable = new DataSetTable[T](
      dataSet,
      fieldIndexes,
      fieldNames
    )
    registerTableInternal(name, dataSetTable)
  }

  /**
    * Registers a [[DataSet]] as a table under a given name with field names as specified by
    * field expressions in the [[TableEnvironment]]'s catalog.
    *
    * @param name The name under which the table is registered in the catalog.
    * @param dataSet The [[DataSet]] to register as table in the catalog.
    * @param fields The field expressions to define the field names of the table.
    * @tparam T The type of the [[DataSet]].
    */
  protected def registerDataSetInternal[T](
      name: String, dataSet: DataSet[T], fields: Array[Expression]): Unit = {

    val (fieldNames, fieldIndexes) = getFieldInfo[T](
      dataSet.getType,
      fields)

    if (fields.exists(_.isInstanceOf[TimeAttribute])) {
      throw new ValidationException(
        ".rowtime and .proctime time indicators are not allowed in a batch environment.")
    }

    val dataSetTable = new DataSetTable[T](dataSet, fieldIndexes, fieldNames)
    registerTableInternal(name, dataSetTable)
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

    // 1. decorrelate
    val decorPlan = RelDecorrelator.decorrelateQuery(relNode)

    // 2. normalize the logical plan
    val normRuleSet = getNormRuleSet
    val normalizedPlan = if (normRuleSet.iterator().hasNext) {
      runHepPlanner(HepMatchOrder.BOTTOM_UP, normRuleSet, decorPlan, decorPlan.getTraitSet)
    } else {
      decorPlan
    }

    // 3. optimize the logical Flink plan
    val logicalOptRuleSet = getLogicalOptRuleSet
    val logicalOutputProps = relNode.getTraitSet.replace(FlinkConventions.LOGICAL).simplify()
    val logicalPlan = if (logicalOptRuleSet.iterator().hasNext) {
      runVolcanoPlanner(logicalOptRuleSet, normalizedPlan, logicalOutputProps)
    } else {
      normalizedPlan
    }

    // 4. optimize the physical Flink plan
    val physicalOptRuleSet = getPhysicalOptRuleSet
    val physicalOutputProps = relNode.getTraitSet.replace(FlinkConventions.DATASET).simplify()
    val physicalPlan = if (physicalOptRuleSet.iterator().hasNext) {
      runVolcanoPlanner(physicalOptRuleSet, logicalPlan, physicalOutputProps)
    } else {
      logicalPlan
    }

    physicalPlan
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
    val relNode = table.getRelNode
    val dataSetPlan = optimize(relNode)
    translate(dataSetPlan, relNode.getRowType)
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
      logicalType: RelDataType)
      (implicit tpe: TypeInformation[A]): DataSet[A] = {
    TableEnvironment.validateType(tpe)

    logicalPlan match {
      case node: DataSetRel =>
        val plan = node.translateToPlan(this)
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
        throw TableException("Cannot generate DataSet due to an invalid logical plan. " +
          "This is a bug and should not happen. Please file an issue.")
    }
  }
}
