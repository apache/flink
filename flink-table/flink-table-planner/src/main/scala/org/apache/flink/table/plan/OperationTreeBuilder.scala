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

package org.apache.flink.table.plan

import java.util.{Optional, List => JList, Map => JMap}

import org.apache.flink.api.java.operators.join.JoinType
import org.apache.flink.table.api._
import org.apache.flink.table.expressions.{Alias, Asc, Expression, ExpressionBridge, Flattening, Ordering, PlannerExpression, UnresolvedAlias, UnresolvedFieldReference, WindowProperty}
import org.apache.flink.table.functions.utils.UserDefinedFunctionUtils
import org.apache.flink.table.operations.TableOperation
import org.apache.flink.table.plan.ProjectionTranslator.{expandProjectList, flattenExpression, resolveOverWindows}
import org.apache.flink.table.plan.logical._
import org.apache.flink.table.util.JavaScalaConversionUtil.toScala

import _root_.scala.collection.JavaConverters._

/**
  * Builder for [[[Operation]] tree.
  */
class OperationTreeBuilder(private val tableEnv: TableEnvironment) {

  private val expressionBridge: ExpressionBridge[PlannerExpression] = tableEnv.expressionBridge

  def project(
      projectList: JList[Expression],
      child: TableOperation,
      explicitAlias: Boolean = false)
    : TableOperation = {

    val childNode = child.asInstanceOf[LogicalNode]

    val convertedProjectList = projectList.asScala
      .map(expressionBridge.bridge)
      .flatMap(expr => flattenExpression(expr, childNode, tableEnv))
      .map(UnresolvedAlias).toList

    Project(convertedProjectList, childNode, explicitAlias).validate(tableEnv)
  }

  /**
    * Adds additional columns. Existing fields will be replaced if replaceIfExist is true.
    */
  def addColumns(
      replaceIfExist: Boolean,
      fieldLists: Seq[Expression],
      child: TableOperation)
  : TableOperation = {

    val childNode = child.asInstanceOf[LogicalNode]
    val childFields = childNode.output
      .map(a => UnresolvedFieldReference(a.name).asInstanceOf[PlannerExpression])

    val addFields = fieldLists.map(expressionBridge.bridge)

    if (replaceIfExist) {
      val finalFields = childFields.toBuffer

      // replace field if exist.
      addFields.foreach {
        case e@Alias(_, name, _) =>
          val index = finalFields.indexWhere(
            p => p match {
              case u: UnresolvedFieldReference => u.name.equals(name)
              case a: Alias => a.name.equals(name)
              case _ => false
            })
          if (index >= 0) {
            finalFields(index) = e
          } else {
            finalFields.append(e)
          }
        case e =>
          finalFields.append(e)
      }

      val convertedProjectList = finalFields.map(UnresolvedAlias).toList
      Project(convertedProjectList, childNode).validate(tableEnv)
    } else {
      val convertedProjectList = (childFields ++ addFields)
        .flatMap(expr => flattenExpression(expr, childNode, tableEnv))
        .map(UnresolvedAlias)
      Project(convertedProjectList, childNode).validate(tableEnv)
    }
  }

  def renameColumns(
      fieldLists: Seq[Expression],
      child: TableOperation)
  : TableOperation = {

    val childNode = child.asInstanceOf[LogicalNode]
    val finalFields = childNode.output
      .map(a => UnresolvedFieldReference(a.name).asInstanceOf[PlannerExpression]).toArray

    val renameFields = fieldLists.map(tableEnv.expressionBridge.bridge)

    // Rename existing fields
    renameFields.foreach {
      case e@Alias(child: UnresolvedFieldReference, _, _) =>
        val index = finalFields.indexWhere(
          p => p match {
            case u: UnresolvedFieldReference => u.name.equals(child.name)
            case _ => false
          })
        if (index >= 0) {
          finalFields(index) = e
        } else {
          throw new TableException(s"Rename field [${child.name}] does not exist in source table.")
        }
      case e =>
        throw new TableException(
          s"Unexpected field expression type [$e]. " +
            s"Renaming must add an alias to the original field, e.g., a as a1.")
    }

    Project(finalFields.map(UnresolvedAlias), childNode).validate(tableEnv)
  }

  def dropColumns(
      fieldLists: Seq[Expression],
      child: TableOperation)
  : TableOperation = {

    val childNode = child.asInstanceOf[LogicalNode]
    val finalFields = childNode.output.map(a => UnresolvedFieldReference(a.name)).toBuffer
    val dropFields = fieldLists.map(tableEnv.expressionBridge.bridge).distinct

    // Remove the fields which should be deleted in the final list
    dropFields.foreach {
      case UnresolvedFieldReference(name) =>
        val index = finalFields.indexWhere(
          p => p match {
            case u: UnresolvedFieldReference => u.name.equalsIgnoreCase(name)
            case _ => false
          })

        if (index >= 0) {
          finalFields.remove(index)
        } else {
          throw new TableException(s"Drop field [$name] does not exist in source table.")
        }
      case e =>
        throw new TableException(s"Unexpected field expression type [$e].")
    }
    Project(finalFields.map(UnresolvedAlias), childNode).validate(tableEnv)
  }

  def aggregate(
      groupingExpressions: JList[Expression],
      namedAggregates: JMap[Expression, String],
      child: TableOperation)
    : TableOperation = {

    val childNode = child.asInstanceOf[LogicalNode]

    val convertedGroupings = groupingExpressions.asScala
      .map(expressionBridge.bridge)

    val convertedAggregates = namedAggregates.asScala
      .map(a => Alias(expressionBridge.bridge(a._1), a._2)).toSeq

    Aggregate(convertedGroupings, convertedAggregates, childNode).validate(tableEnv)
  }

  def windowAggregate(
      groupingExpressions: JList[Expression],
      window: GroupWindow,
      namedProperties: JMap[Expression, String],
      namedAggregates: JMap[Expression, String],
      child: TableOperation)
    : TableOperation = {

    val childNode = child.asInstanceOf[LogicalNode]

    val convertedGroupings = groupingExpressions.asScala
      .map(expressionBridge.bridge)

    val convertedAggregates = namedAggregates.asScala
      .map(a => Alias(expressionBridge.bridge(a._1), a._2)).toSeq

    val convertedProperties = namedProperties.asScala
      .map(a => Alias(expressionBridge.bridge(a._1), a._2)).toSeq

    WindowAggregate(
        convertedGroupings,
        createLogicalWindow(window),
        convertedProperties,
        convertedAggregates,
        childNode)
      .validate(tableEnv)
  }

  def project(
      projectList: JList[Expression],
      child: TableOperation,
      overWindows: JList[OverWindow])
    : TableOperation = {

    val childNode = child.asInstanceOf[LogicalNode]

    val expandedFields = expandProjectList(
      projectList.asScala.map(expressionBridge.bridge),
      childNode,
      tableEnv)

    if (expandedFields.exists(_.isInstanceOf[WindowProperty])){
      throw new ValidationException(
        "Window start and end properties are not available for Over windows.")
    }

    val expandedOverFields = resolveOverWindows(
        expandedFields,
        overWindows.asScala.map(createLogicalWindow))
      .map(UnresolvedAlias)

    Project(expandedOverFields, childNode, explicitAlias = true).validate(tableEnv)
  }

  def join(
      left: TableOperation,
      right: TableOperation,
      joinType: JoinType,
      condition: Optional[Expression],
      correlated: Boolean)
    : TableOperation = {
    Join(
      left.asInstanceOf[LogicalNode],
      right.asInstanceOf[LogicalNode],
      joinType,
      toScala(condition).map(expressionBridge.bridge),
      correlated).validate(tableEnv)
  }

  def joinLateral(
      left: TableOperation,
      tableFunction: Expression,
      joinType: JoinType,
      condition: Optional[Expression])
    : TableOperation = {

    val leftNode = left.asInstanceOf[LogicalNode]

    val temporalTable = UserDefinedFunctionUtils.createLogicalFunctionCall(
      expressionBridge.bridge(tableFunction),
      leftNode).validate(tableEnv)

    join(left, temporalTable, joinType, condition, correlated = true)
  }

  def createTemporalTable(
      timeAttribute: Expression,
      primaryKey: Expression,
      underlyingOperation: TableOperation)
    : TemporalTable = {
    val underlyingOperationNode = underlyingOperation.asInstanceOf[LogicalNode]
    TemporalTable(
      expressionBridge.bridge(timeAttribute),
      expressionBridge.bridge(primaryKey),
      underlyingOperationNode)
      .validate(tableEnv)
      .asInstanceOf[TemporalTable]
  }

  def sort(
      fields: JList[Expression],
      child: TableOperation)
    : TableOperation = {
    val childNode = child.asInstanceOf[LogicalNode]

    val order: Seq[Ordering] = fields.asScala.map(expressionBridge.bridge).map {
      case o: Ordering => o
      case e => Asc(e)
    }

    Sort(order, childNode).validate(tableEnv)
  }

  def limitWithOffset(offset: Int, child: TableOperation): TableOperation = {
      limit(offset, -1, child)
  }

  def limitWithFetch(fetch: Int, child: TableOperation): TableOperation = {
      applyFetch(fetch, child)
  }

  private def applyFetch(fetch: Int, child: TableOperation): TableOperation = {
    child match {
      case Limit(o, -1, c) =>
        // replace LIMIT without FETCH by LIMIT with FETCH
        limit(o, fetch, c)
      case Limit(_, _, _) =>
        throw new ValidationException("FETCH is already defined.")
      case _ =>
        limit(0, fetch, child)
    }
  }

  def limit(offset: Int, fetch: Int, child: TableOperation): TableOperation = {
    Limit(offset, fetch, child.asInstanceOf[LogicalNode]).validate(tableEnv)
  }

  def alias(
      fields: JList[Expression],
      child: TableOperation)
    : TableOperation = {

    val childNode = child.asInstanceOf[LogicalNode]
    val convertedFields = fields.asScala.map(expressionBridge.bridge)

    AliasNode(convertedFields, childNode).validate(tableEnv)
  }

  def filter(
      condition: Expression,
      child: TableOperation)
    : TableOperation = {

    val childNode = child.asInstanceOf[LogicalNode]
    val convertedFields = expressionBridge.bridge(condition)

    Filter(convertedFields, childNode).validate(tableEnv)
  }

  def distinct(
      child: TableOperation)
    : TableOperation = {
    Distinct(child.asInstanceOf[LogicalNode]).validate(tableEnv)
  }

  def minus(
      left: TableOperation,
      right: TableOperation,
      all: Boolean)
    : TableOperation = {
    Minus(left.asInstanceOf[LogicalNode], right.asInstanceOf[LogicalNode], all).validate(tableEnv)
  }

  def intersect(
      left: TableOperation,
      right: TableOperation,
      all: Boolean)
    : TableOperation = {
    Intersect(left.asInstanceOf[LogicalNode], right.asInstanceOf[LogicalNode], all)
      .validate(tableEnv)
  }

  def union(
      left: TableOperation,
      right: TableOperation,
      all: Boolean)
    : TableOperation = {
    Union(left.asInstanceOf[LogicalNode], right.asInstanceOf[LogicalNode], all).validate(tableEnv)
  }

  def map(mapFunction: Expression, child: TableOperation): TableOperation = {
    val childNode = child.asInstanceOf[LogicalNode]
    val expandedFields = expandProjectList(
      Seq(mapFunction).map(e => Flattening(expressionBridge.bridge(e))), childNode, tableEnv)
    Project(expandedFields.map(UnresolvedAlias), childNode).validate(tableEnv)
  }

  /**
    * Converts an API class to a logical window for planning.
    */
  private def createLogicalWindow(overWindow: OverWindow): LogicalOverWindow = {
    LogicalOverWindow(
      expressionBridge.bridge(overWindow.getAlias),
      overWindow.getPartitioning.asScala.map(expressionBridge.bridge),
      expressionBridge.bridge(overWindow.getOrder),
      expressionBridge.bridge(overWindow.getPreceding),
      toScala(overWindow.getFollowing).map(expressionBridge.bridge)
    )
  }

  /**
    * Converts an API class to a logical window for planning.
    */
  private def createLogicalWindow(window: GroupWindow): LogicalWindow = window match {
    case tw: TumbleWithSizeOnTimeWithAlias =>
      TumblingGroupWindow(
        expressionBridge.bridge(tw.getAlias),
        expressionBridge.bridge(tw.getTimeField),
        expressionBridge.bridge(tw.getSize))
    case sw: SlideWithSizeAndSlideOnTimeWithAlias =>
      SlidingGroupWindow(
        expressionBridge.bridge(sw.getAlias),
        expressionBridge.bridge(sw.getTimeField),
        expressionBridge.bridge(sw.getSize),
        expressionBridge.bridge(sw.getSlide))
    case sw: SessionWithGapOnTimeWithAlias =>
      SessionGroupWindow(
        expressionBridge.bridge(sw.getAlias),
        expressionBridge.bridge(sw.getTimeField),
        expressionBridge.bridge(sw.getGap))
  }
}
