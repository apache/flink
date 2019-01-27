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

package org.apache.flink.table.plan.rules.logical

import java.util.{ArrayList => JArrayList, HashMap => JHashMap, Map => JMap}
import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.logical._
import org.apache.calcite.rex.{RexCall, RexNode}
import org.apache.calcite.sql.parser.SqlParserPos
import org.apache.calcite.sql.{SemiJoinType, SqlLiteral}
import org.apache.calcite.tools.RelBuilder
import org.apache.calcite.util.ImmutableBitSet
import org.apache.flink.api.common.functions.InvalidTypesException
import org.apache.flink.api.java.typeutils.TypeExtractor

import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableList
import org.apache.flink.table.api.functions.TableFunction
import org.apache.flink.table.api.{Column, TableException, TableSchema}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.expressions._
import org.apache.flink.table.functions.sql.ScalarSqlFunctions
import org.apache.flink.table.functions.utils.UserDefinedFunctionUtils
import org.apache.flink.table.plan.nodes.calcite.LogicalWatermarkAssigner
import org.apache.flink.table.plan.schema._
import org.apache.flink.table.sources.TableSourceUtil
import org.apache.flink.table.api.types.{DataType, DataTypes, GenericType, TypeInfoWrappedDataType}

import scala.collection.JavaConversions._

/**
 * Catalog Table to stream table source rule.
 */
class CatalogTableToStreamTableSourceRule
    extends RelOptRule(
      operand(classOf[LogicalTableScan], any), "CatalogTableToStreamTableSource") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val rel = call.rel(0).asInstanceOf[LogicalTableScan]
    rel.getTable.unwrap(classOf[CatalogCalciteTable]) != null
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val oldRel = call.rel(0).asInstanceOf[LogicalTableScan]
    val catalogTable = oldRel.getTable.unwrap(classOf[CatalogCalciteTable])
    val tableSource = catalogTable.streamTableSource
    var table = oldRel.getTable.asInstanceOf[FlinkRelOptTable].copy(
      new StreamTableSourceTable(
        tableSource, catalogTable.getStatistic()),
      TableSourceUtil.getRelDataType(
        tableSource,
        None,
        true,
        oldRel.getCluster.getTypeFactory.asInstanceOf[FlinkTypeFactory]))

    table = if (tableSource.explainSource().isEmpty) {
      val builder = ImmutableList.builder[String]()
      builder.add("From")
      builder.addAll(oldRel.getTable.getQualifiedName)
      table.config(builder.build(), table.unwrap(classOf[TableSourceTable]))
    } else {
      table
    }

    // parser
    var newRel:RelNode = CatalogTableRules.appendParserNode(
      catalogTable,
      LogicalTableScan.create(oldRel.getCluster, table),
      call.builder())

    // computed column.
    val computedColumns = new JHashMap[String, RexNode]()

    if(catalogTable.table.getComputedColumns != null) {
      computedColumns.putAll(catalogTable.table.getComputedColumns)
    }

    newRel = CatalogTableRules.appendComputedColumns(
      call.builder(), newRel, catalogTable.table.getTableSchema, computedColumns)

    // watermark.
    if (catalogTable.table.getRowTimeField != null) {
      newRel = new LogicalWatermarkAssigner(
        newRel.getCluster,
        newRel.getTraitSet,
        newRel,
        catalogTable.table.getRowTimeField,
        catalogTable.table.getWatermarkOffset)
    }

    call.transformTo(newRel)
  }
}

/**
 * Catalog Table to batch table source rule.
 */
class CatalogTableToBatchTableSourceRule
    extends RelOptRule(
      operand(classOf[LogicalTableScan], any), "CatalogTableToBatchTableSource") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val rel = call.rel(0).asInstanceOf[LogicalTableScan]
    rel.getTable.unwrap(classOf[CatalogCalciteTable]) != null
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val oldRel = call.rel(0).asInstanceOf[LogicalTableScan]
    val catalogTable = oldRel.getTable.unwrap(classOf[CatalogCalciteTable])
    val tableSource = catalogTable.batchTableSource
    var table = oldRel.getTable.asInstanceOf[FlinkRelOptTable].copy(
      new BatchTableSourceTable(
        tableSource, catalogTable.getStatistic()),
      TableSourceUtil.getRelDataType(
        tableSource,
        None,
        false,
        oldRel.getCluster.getTypeFactory.asInstanceOf[FlinkTypeFactory]))

    table = if (tableSource.explainSource().isEmpty) {
      val builder = ImmutableList.builder[String]()
      builder.add("From")
      builder.addAll(oldRel.getTable.getQualifiedName)
      table.config(builder.build(), table.unwrap(classOf[TableSourceTable]))
    } else {
      table
    }
    // parser
    val newRel:RelNode = CatalogTableRules.appendParserNode(
      catalogTable,
      LogicalTableScan.create(oldRel.getCluster, table),
      call.builder())

    // computed columns
    val computedColumns = new JHashMap[String, RexNode]()
    if (catalogTable.table.getComputedColumns != null) {
      computedColumns.putAll(catalogTable.table.getComputedColumns)

      catalogTable.table.getComputedColumns.foreach {
        case (name: String, expr: RexCall)
            if expr.getOperator.equals(ScalarSqlFunctions.PROCTIME) =>
          computedColumns.put(
            name, Cast(CurrentTimestamp(), DataTypes.TIMESTAMP).toRexNode(call.builder()))
        case (name: String, expr: RexNode) if name.equals(catalogTable.table.getRowTimeField) =>
          computedColumns.put(
            name, oldRel.getCluster.getRexBuilder.makeCast(
              oldRel.getRowType.getField(name, true, false).getType, expr))
        case _ =>
      }
    } else if (catalogTable.table.getRowTimeField != null) {
       catalogTable.table.getTableSchema.getColumns.foreach {
         case column: Column if column.name.equals(catalogTable.table.getRowTimeField) =>
           computedColumns.put(
             column.name, oldRel.getCluster.getRexBuilder.makeCast(
               oldRel.getRowType.getField(column.name, true, false).getType,
               call.builder().push(newRel).field(column.name)))
         case column: Column =>
           computedColumns.put(column.name, call.builder().push(newRel).field(column.name))
       }
    }

    call.transformTo(CatalogTableRules.appendComputedColumns(
      call.builder(), newRel, catalogTable.table.getTableSchema, computedColumns))

  }
}

object CatalogTableRules {
  val STREAM_TABLE_SCAN_RULE = new CatalogTableToStreamTableSourceRule
  val BATCH_TABLE_SCAN_RULE = new CatalogTableToBatchTableSourceRule

  def appendParserNode(
    catalogTable: CatalogCalciteTable, inputNode: RelNode, relBuilder: RelBuilder):RelNode = {

    val parser = catalogTable.tableSourceParser

    if (parser != null) {
      val colId = inputNode.getCluster.createCorrel()
      relBuilder.push(inputNode)
      val params = parser.getParameters.map {
        case name: String => relBuilder.field(name)
      }

      val tf = parser.getParser

      val implicitResultType: DataType = try {
        new TypeInfoWrappedDataType(
          TypeExtractor.createTypeInfo(tf, classOf[TableFunction[_]], tf.getClass, 0))
      } catch {
        case _: InvalidTypesException =>
          // may be we should get type from getResultType
          new GenericType(classOf[AnyRef])
      }

      val typeFactory = inputNode.getCluster.getTypeFactory.asInstanceOf[FlinkTypeFactory]

      val parserSqlFunction = UserDefinedFunctionUtils.createTableSqlFunction(
        "parser",
        "parser",
        tf,
        implicitResultType,
        typeFactory)

      val rexCall = relBuilder.call(parserSqlFunction, params)

      val tableFunctionScan =
        LogicalTableFunctionScan.create(
          inputNode.getCluster,
          ImmutableList.of(),
          rexCall,
          parserSqlFunction.getElementType(
            typeFactory,
            params.map(_ => SqlLiteral.createNull(new SqlParserPos(0, 0)))),
          parserSqlFunction.getRowType(
            typeFactory,
            params.map(_ => SqlLiteral.createNull(new SqlParserPos(0, 0))),
            parser.getParameters.map(
              name => inputNode.getRowType.getField(name, true, false).getType)
          ),
          null)
      val outputDataType = tableFunctionScan.getRowType

      val columnSetBuilder = ImmutableBitSet.builder()
      params.foreach(param => columnSetBuilder.set(param.getIndex))
      (0 until outputDataType.getFieldCount).foreach (
        idx => columnSetBuilder.set(inputNode.getRowType.getFieldCount + idx))
      val correlate =
        LogicalCorrelate.create(
          inputNode, tableFunctionScan, colId, columnSetBuilder.build(), SemiJoinType.INNER)
      relBuilder.push(correlate)

      val projects = outputDataType.getFieldList.map(
        field => relBuilder.field(inputNode.getRowType.getFieldCount + field.getIndex))
      LogicalProject.create(
        correlate,
        projects,
        outputDataType
      )
    } else {
      inputNode
    }
  }


  def appendComputedColumns(
      relBuilder: RelBuilder,
      node: RelNode,
      schema: TableSchema,
      expression:JMap[String, RexNode]):RelNode = {
    expression match {
      case expressions: JMap[String, RexNode] if ! expressions.isEmpty =>
        relBuilder.push(node)
        val nameList = new JArrayList[String](expressions.size)
        val nodeList = new JArrayList[RexNode](expressions.size)

        schema.getColumnNames map {
          case name: String if expression.contains(name) =>
            nameList.add(name)
            nodeList.add(expression.get(name))
          case name: String =>
            throw new TableException(s"$name not existed in computed column expressions")
        }
        relBuilder.project(nodeList, nameList).build()
      case _ => node
    }
  }
}

