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

package org.apache.flink.api.table

import java.util.concurrent.atomic.AtomicInteger

import org.apache.calcite.config.Lex
import org.apache.calcite.plan.RelOptPlanner
import org.apache.calcite.schema.SchemaPlus
import org.apache.calcite.schema.impl.AbstractTable
import org.apache.calcite.sql.parser.SqlParser
import org.apache.calcite.tools.{Frameworks, FrameworkConfig, RelBuilder}
import org.apache.flink.api.common.typeinfo.{AtomicType, TypeInformation}
import org.apache.flink.api.java.{ExecutionEnvironment => JavaBatchExecEnv}
import org.apache.flink.api.java.table.{BatchTableEnvironment => JavaBatchTableEnv}
import org.apache.flink.api.java.table.{StreamTableEnvironment => JavaStreamTableEnv}
import org.apache.flink.api.java.typeutils.{PojoTypeInfo, TupleTypeInfo}
import org.apache.flink.api.scala.{ExecutionEnvironment => ScalaBatchExecEnv}
import org.apache.flink.api.scala.table.{BatchTableEnvironment => ScalaBatchTableEnv}
import org.apache.flink.api.scala.table.{StreamTableEnvironment => ScalaStreamTableEnv}
import org.apache.flink.api.scala.typeutils.CaseClassTypeInfo
import org.apache.flink.api.table.expressions.{Naming, UnresolvedFieldReference, Expression}
import org.apache.flink.api.table.plan.cost.DataSetCostFactory
import org.apache.flink.api.table.plan.schema.{TransStreamTable, TableTable}
import org.apache.flink.streaming.api.environment.{StreamExecutionEnvironment => JavaStreamExecEnv}
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment => ScalaStreamExecEnv}

/**
  * The abstract base class for batch and stream TableEnvironments.
  *
  * @param config The configuration of the TableEnvironment
  */
abstract class TableEnvironment(val config: TableConfig) {

  // configure sql parser
  // we use Java lex because back ticks are easier than double quotes in programming
  // and cases are preserved
  private val parserConfig = SqlParser
    .configBuilder()
    .setLex(Lex.JAVA)
    .build()

  // the catalog to hold all registered and translated tables
  private val tables: SchemaPlus = Frameworks.createRootSchema(true)

  // the configuration to create a Calcite planner
  private val frameworkConfig: FrameworkConfig = Frameworks
    .newConfigBuilder
    .defaultSchema(tables)
    .parserConfig(parserConfig)
    .costFactory(new DataSetCostFactory)
    .build

  // the builder for Calcite RelNodes, Calcite's representation of a relational expression tree.
  protected val relBuilder: RelBuilder = RelBuilder.create(frameworkConfig)

  // the planner instance used to optimize queries of this TableEnvironment
  private val planner: RelOptPlanner = relBuilder
    .values(Array("dummy"), new Integer(1))
    .build().getCluster.getPlanner

  // a counter for unique attribute names
  private val attrNameCntr: AtomicInteger = new AtomicInteger(0)

  /** Returns the table config to define the runtime behavior of the Table API. */
  def getConfig = config

  /**
    * Registers a [[Table]] under a unique name in the TableEnvironment's catalog.
    * Registered tables can be referenced in SQL queries.
    *
    * @param name The name under which the table is registered.
    * @param table The table to register.
    */
  def registerTable(name: String, table: Table): Unit = {

    // check that table belongs to this table environment
    if (table.tableEnv != this) {
      throw new TableException(
        "Only tables that belong to this TableEnvironment can be registered.")
    }

    checkValidTableName(name)

    table.tableEnv match {
      case e: BatchTableEnvironment =>
        val tableTable = new TableTable(table.getRelNode)
        registerTableInternal(name, tableTable)
      case e: StreamTableEnvironment =>
        val sTableTable = new TransStreamTable(table.getRelNode, true)
        tables.add(name, sTableTable)
    }

  }

  /**
    * Replaces a registered Table with another Table under the same name.
    * We use this method to replace a [[org.apache.flink.api.table.plan.schema.DataStreamTable]]
    * with a [[org.apache.calcite.schema.TranslatableTable]].
    *
    * @param name
    * @param table
    */
  protected def replaceRegisteredTable(name: String, table: AbstractTable): Unit = {

    if (isRegistered(name)) {
      tables.add(name, table)
    } else {
      throw new TableException(s"Table \'$name\' is not registered.")
    }
  }

  /**
    * Evaluates a SQL query on registered tables and retrieves the result as a [[Table]].
    *
    * All tables referenced by the query must be registered in the TableEnvironment.
    *
    * @param query The SQL query to evaluate.
    * @return The result of the query as Table.
    */
  def sql(query: String): Table

  /**
    * Registers a Calcite [[AbstractTable]] in the TableEnvironment's catalog.
    *
    * @param name The name under which the table is registered.
    * @param table The table to register in the catalog
    * @throws TableException if another table is registered under the provided name.
    */
  @throws[TableException]
  protected def registerTableInternal(name: String, table: AbstractTable): Unit = {

    if (isRegistered(name)) {
      throw new TableException(s"Table \'$name\' already exists. " +
        s"Please, choose a different name.")
    } else {
      tables.add(name, table)
    }
  }

  /**
    * Checks if the chosen table name is valid.
    *
    * @param name The table name to check.
    */
  protected def checkValidTableName(name: String): Unit

  /**
    * Checks if a table is registered under the given name.
    *
    * @param name The table name to check.
    * @return true, if a table is registered under the name, false otherwise.
    */
  protected def isRegistered(name: String): Boolean = {
    tables.getTableNames.contains(name)
  }

  /** Returns a unique temporary attribute name. */
  private[flink] def createUniqueAttributeName(): String = {
    "TMP_" + attrNameCntr.getAndIncrement()
  }

  /** Returns the [[RelBuilder]] of this TableEnvironment. */
  private[flink] def getRelBuilder: RelBuilder = {
    relBuilder
  }

  /** Returns the Calcite [[org.apache.calcite.plan.RelOptPlanner]] of this TableEnvironment. */
  protected def getPlanner: RelOptPlanner = {
    planner
  }

  /** Returns the Calcite [[FrameworkConfig]] of this TableEnvironment. */
  private[flink] def getFrameworkConfig: FrameworkConfig = {
    frameworkConfig
  }

  /**
    * Returns field names and field positions for a given [[TypeInformation]].
    *
    * Field names are automatically extracted for
    * [[org.apache.flink.api.common.typeutils.CompositeType]].
    * The method fails if inputType is not a
    * [[org.apache.flink.api.common.typeutils.CompositeType]].
    *
    * @param inputType The TypeInformation extract the field names and positions from.
    * @tparam A The type of the TypeInformation.
    * @return A tuple of two arrays holding the field names and corresponding field positions.
    */
  protected def getFieldInfo[A](inputType: TypeInformation[A]): (Array[String], Array[Int]) = {
    val fieldNames: Array[String] = inputType match {
      case t: TupleTypeInfo[A] => t.getFieldNames
      case c: CaseClassTypeInfo[A] => c.getFieldNames
      case p: PojoTypeInfo[A] => p.getFieldNames
      case tpe =>
        throw new IllegalArgumentException(
          s"Type $tpe requires explicit field naming.")
    }
    val fieldIndexes = fieldNames.indices.toArray
    (fieldNames, fieldIndexes)
  }

  /**
    * Returns field names and field positions for a given [[TypeInformation]] and [[Array]] of
    * [[Expression]].
    *
    * @param inputType The [[TypeInformation]] against which the [[Expression]]s are evaluated.
    * @param exprs The expressions that define the field names.
    * @tparam A The type of the TypeInformation.
    * @return A tuple of two arrays holding the field names and corresponding field positions.
    */
  protected def getFieldInfo[A](
    inputType: TypeInformation[A],
    exprs: Array[Expression]): (Array[String], Array[Int]) = {

    val indexedNames: Array[(Int, String)] = inputType match {
      case a: AtomicType[A] =>
        if (exprs.length != 1) {
          throw new IllegalArgumentException("Atomic type may can only have a single field.")
        }
        exprs.map {
          case UnresolvedFieldReference(name) => (0, name)
          case _ => throw new IllegalArgumentException(
            "Field reference expression expected.")
        }
      case t: TupleTypeInfo[A] =>
        exprs.zipWithIndex.map {
          case (UnresolvedFieldReference(name), idx) => (idx, name)
          case (Naming(UnresolvedFieldReference(origName), name), _) =>
            val idx = t.getFieldIndex(origName)
            if (idx < 0) {
              throw new IllegalArgumentException(s"$origName is not a field of type $t")
            }
            (idx, name)
          case _ => throw new IllegalArgumentException(
            "Field reference expression or naming expression expected.")
        }
      case c: CaseClassTypeInfo[A] =>
        exprs.zipWithIndex.map {
          case (UnresolvedFieldReference(name), idx) => (idx, name)
          case (Naming(UnresolvedFieldReference(origName), name), _) =>
            val idx = c.getFieldIndex(origName)
            if (idx < 0) {
              throw new IllegalArgumentException(s"$origName is not a field of type $c")
            }
            (idx, name)
          case _ => throw new IllegalArgumentException(
            "Field reference expression or naming expression expected.")
        }
      case p: PojoTypeInfo[A] =>
        exprs.map {
          case Naming(UnresolvedFieldReference(origName), name) =>
            val idx = p.getFieldIndex(origName)
            if (idx < 0) {
              throw new IllegalArgumentException(s"$origName is not a field of type $p")
            }
            (idx, name)
          case _ => throw new IllegalArgumentException(
            "Field naming expression expected.")
        }
      case tpe => throw new IllegalArgumentException(
        s"Type $tpe cannot be converted into Table.")
    }

    val (fieldIndexes, fieldNames) = indexedNames.unzip
    (fieldNames.toArray, fieldIndexes.toArray)
  }

}

/**
  * Object to instantiate a [[TableEnvironment]] depending on the batch or stream execution
  * environment.
  */
object TableEnvironment {

  /**
    * Returns a [[JavaBatchTableEnv]] for a Java [[JavaBatchExecEnv]].
    *
    * @param executionEnvironment The Java batch ExecutionEnvironment.
    */
  def getTableEnvironment(executionEnvironment: JavaBatchExecEnv): JavaBatchTableEnv = {
    new JavaBatchTableEnv(executionEnvironment, new TableConfig())
  }

  /**
    * Returns a [[JavaBatchTableEnv]] for a Java [[JavaBatchExecEnv]] and a given [[TableConfig]].
    *
    * @param executionEnvironment The Java batch ExecutionEnvironment.
    * @param tableConfig The TableConfig for the new TableEnvironment.
    */
  def getTableEnvironment(
    executionEnvironment: JavaBatchExecEnv,
    tableConfig: TableConfig): JavaBatchTableEnv = {

    new JavaBatchTableEnv(executionEnvironment, tableConfig)
  }

  /**
    * Returns a [[ScalaBatchTableEnv]] for a Scala [[ScalaBatchExecEnv]].
    *
    * @param executionEnvironment The Scala batch ExecutionEnvironment.
    */
  def getTableEnvironment(executionEnvironment: ScalaBatchExecEnv): ScalaBatchTableEnv = {
    new ScalaBatchTableEnv(executionEnvironment, new TableConfig())
  }

  /**
    * Returns a [[ScalaBatchTableEnv]] for a Scala [[ScalaBatchExecEnv]] and a given
    * [[TableConfig]].
    *
    * @param executionEnvironment The Scala batch ExecutionEnvironment.
    * @param tableConfig The TableConfig for the new TableEnvironment.
    */
  def getTableEnvironment(
    executionEnvironment: ScalaBatchExecEnv,
    tableConfig: TableConfig): ScalaBatchTableEnv = {

    new ScalaBatchTableEnv(executionEnvironment, tableConfig)
  }

  /**
    * Returns a [[JavaStreamTableEnv]] for a Java [[JavaStreamExecEnv]].
    *
    * @param executionEnvironment The Java StreamExecutionEnvironment.
    */
  def getTableEnvironment(executionEnvironment: JavaStreamExecEnv): JavaStreamTableEnv = {
    new JavaStreamTableEnv(executionEnvironment, new TableConfig())
  }

  /**
    * Returns a [[JavaStreamTableEnv]] for a Java [[JavaStreamExecEnv]] and a given [[TableConfig]].
    *
    * @param executionEnvironment The Java StreamExecutionEnvironment.
    * @param tableConfig The TableConfig for the new TableEnvironment.
    */
  def getTableEnvironment(
    executionEnvironment: JavaStreamExecEnv,
    tableConfig: TableConfig): JavaStreamTableEnv = {

    new JavaStreamTableEnv(executionEnvironment, tableConfig)
  }

  /**
    * Returns a [[ScalaStreamTableEnv]] for a Scala stream [[ScalaStreamExecEnv]].
    *
    * @param executionEnvironment The Scala StreamExecutionEnvironment.
    */
  def getTableEnvironment(executionEnvironment: ScalaStreamExecEnv): ScalaStreamTableEnv = {
    new ScalaStreamTableEnv(executionEnvironment, new TableConfig())
  }

  /**
    * Returns a [[ScalaStreamTableEnv]] for a Scala stream [[ScalaStreamExecEnv]].
    *
    * @param executionEnvironment The Scala StreamExecutionEnvironment.
    * @param tableConfig The TableConfig for the new TableEnvironment.
    */
  def getTableEnvironment(
    executionEnvironment: ScalaStreamExecEnv,
    tableConfig: TableConfig): ScalaStreamTableEnv = {

    new ScalaStreamTableEnv(executionEnvironment, tableConfig)
  }

}
