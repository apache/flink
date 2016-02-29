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

package org.apache.flink.api.table.plan

import java.util.concurrent.atomic.AtomicInteger
import org.apache.calcite.config.Lex
import org.apache.calcite.plan.ConventionTraitDef
import org.apache.calcite.schema.impl.AbstractTable
import org.apache.calcite.schema.SchemaPlus
import org.apache.calcite.sql.parser.SqlParser
import org.apache.calcite.tools.{FrameworkConfig, Frameworks, RelBuilder}
import org.apache.flink.api.common.typeinfo.{AtomicType, TypeInformation}
import org.apache.flink.api.java.typeutils.{PojoTypeInfo, TupleTypeInfo}
import org.apache.flink.api.scala.typeutils.CaseClassTypeInfo
import org.apache.flink.api.table.TableException
import org.apache.flink.api.table.expressions.{Naming, UnresolvedFieldReference, Expression}
import org.apache.flink.api.table.plan.cost.DataSetCostFactory
import org.apache.flink.api.table.plan.schema.DataSetTable
import org.apache.flink.api.table.plan.schema.DataStreamTable

object TranslationContext {

  private var frameworkConfig: FrameworkConfig = null
  private var relBuilder: RelBuilder = null
  private var tables: SchemaPlus = null
  private var tablesRegistry: Map[String, AbstractTable] = null
  private val nameCntr: AtomicInteger = new AtomicInteger(0)

  reset()

  def reset(): Unit = {

    // register table in Cascading schema
    tables = Frameworks.createRootSchema(true)

    // TODO move this to TableConfig when we implement full SQL support
    // configure sql parser
    // we use Java lex because back ticks are easier than double quotes in programming
    // and cases are preserved
    val parserConfig = SqlParser.configBuilder().setLex(Lex.JAVA).build()

    // initialize RelBuilder
    frameworkConfig = Frameworks
      .newConfigBuilder
      .defaultSchema(tables)
      .parserConfig(parserConfig)
      .costFactory(new DataSetCostFactory)
      .traitDefs(ConventionTraitDef.INSTANCE)
      .build

    tablesRegistry = Map[String, AbstractTable]()
    relBuilder = RelBuilder.create(frameworkConfig)
    nameCntr.set(0)

  }

  /**
   * Adds a table to the Calcite schema so it can be used by the Table API
   */
  def registerDataSetTable(newTable: DataSetTable[_]): String = {
    val tabName = "_DataSetTable_" + nameCntr.getAndIncrement()
    tables.add(tabName, newTable)
    tabName
  }

  /**
   * Adds a table to the Calcite schema and the tables registry,
   * so it can be used by both Table API and SQL statements.
   */
  @throws[TableException]
  def registerTable(table: AbstractTable, name: String): Unit = {
    val illegalPattern = "^_DataSetTable_[0-9]+$".r
    val m = illegalPattern.findFirstIn(name)
    m match {
      case Some(_) =>
        throw new TableException(s"Illegal Table name. " +
          s"Please choose a name that does not contain the pattern $illegalPattern")
      case None => {
        val existingTable = tablesRegistry.get(name)
        existingTable match {
          case Some(_) =>
            throw new TableException(s"Table \'$name\' already exists. " +
              s"Please, choose a different name.")
          case None =>
            tablesRegistry += (name -> table)
            tables.add(name, table)
        }
      }
    }
  }

  def isRegistered(name: String): Boolean = {
    val table = tablesRegistry.get(name)
    table match {
      case Some(_) =>
        true
      case None =>
        false
    }
  }

  /**
   * Adds a stream Table to the tables registry so it can be used by
   * the streaming Table API.
   */
  def addDataStream(newTable: DataStreamTable[_]): String = {
    val tabName = "_DataStreamTable_" + nameCntr.getAndIncrement()
    tables.add(tabName, newTable)
    tabName
  }

  def getUniqueName: String = {
    "TMP_" + nameCntr.getAndIncrement()
  }

  def getRelBuilder: RelBuilder = {
    relBuilder
  }

  def getFrameworkConfig: FrameworkConfig = {
    frameworkConfig
  }

  def getFieldInfo[A](inputType: TypeInformation[A]): (Array[String], Array[Int]) = {
    val fieldNames: Array[String] = inputType match {
      case t: TupleTypeInfo[A] => t.getFieldNames
      case c: CaseClassTypeInfo[A] => c.getFieldNames
      case p: PojoTypeInfo[A] => p.getFieldNames
      case tpe =>
        throw new IllegalArgumentException(
          s"Type $tpe requires explicit field naming with AS.")
    }
    val fieldIndexes = fieldNames.indices.toArray
    (fieldNames, fieldIndexes)
  }

  def getFieldInfo[A](
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


