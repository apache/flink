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

package org.apache.flink.table.plan.metadata

import org.apache.flink.table.api.TableException
import org.apache.flink.table.plan.metadata.FlinkMetadata.SkewInfoMeta
import org.apache.flink.table.plan.nodes.calcite.Expand
import org.apache.flink.table.plan.schema.FlinkRelOptTable
import org.apache.flink.table.plan.stats.SkewInfoInternal
import org.apache.flink.table.plan.util.FlinkRelOptUtil.getLiteralValue

import org.apache.calcite.plan.RelOptUtil
import org.apache.calcite.plan.volcano.RelSubset
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core._
import org.apache.calcite.rel.metadata._
import org.apache.calcite.rex.{RexCall, RexInputRef, RexLiteral}
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql.SqlKind._
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.calcite.util.Util

import scala.collection.JavaConversions._
import scala.collection.mutable

class FlinkRelMdSkewInfo private extends MetadataHandler[SkewInfoMeta] {

  override def getDef: MetadataDef[SkewInfoMeta] = SkewInfoMeta.DEF

  def getSkewInfo(ts: TableScan, mq: RelMetadataQuery): SkewInfoInternal = {
    val info = ts.getTable.asInstanceOf[FlinkRelOptTable].getFlinkStatistic.getSkewInfo
    val skewMap = new mutable.HashMap[Int, Seq[AnyRef]]

    if (info != null) {
      ts.getRowType.getFieldNames.zipWithIndex.foreach {
        case (field, fieldIndex) =>
          val skewValues = info.get(field)
          if (skewValues != null && skewValues.nonEmpty) {
            skewMap.put(fieldIndex, skewValues)
          }
      }
    }
    if (skewMap.nonEmpty) {
      SkewInfoInternal(skewMap.toMap)
    } else {
      null
    }
  }

  def getSkewInfo(project: Project, mq: RelMetadataQuery): SkewInfoInternal = {
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    val skewInfo = fmq.getSkewInfo(project.getInput)
    if (skewInfo == null) {
      return null
    }

    val projects = project.getProjects
    val oldFieldIndice = project.getInput.getRowType.getFieldNames.indices
    val newFieldIndice = project.getRowType.getFieldNames.indices
    val skewMap = new mutable.HashMap[Int, Seq[AnyRef]]

    projects.zipWithIndex.foreach {
      case (inputRef: RexInputRef, i) =>
        val skewValues = skewInfo.skewInfo.getOrElse(oldFieldIndice(inputRef.getIndex), null)
        if (skewValues != null) {
          skewMap.put(newFieldIndice(i), skewValues)
        }
      case (literal: RexLiteral, i) =>
        skewMap.put(newFieldIndice(i), Seq(getLiteralValue(literal).asInstanceOf[AnyRef]))
      case _ =>
    }
    SkewInfoInternal(skewMap.toMap)
  }

  def getSkewInfo(filter: Filter, mq: RelMetadataQuery): SkewInfoInternal = {
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    val skewInfo = fmq.getSkewInfo(filter.getInput)
    if (skewInfo == null) {
      return null
    }

    val inputFieldIndice = filter.getInput.getRowType.getFieldNames.indices

    val inputRefs = RelOptUtil.InputFinder.bits(filter.getCondition).toList
    val skewMap = new mutable.HashMap[Int, Seq[AnyRef]]
    skewInfo.skewInfo.foreach { case (fieldIndex, skewValues) =>
      // if not contains this field, let keep skew values
      if (inputRefs.contains(inputFieldIndice(fieldIndex))) {

        def filterSkewValuesBySubCall(call: RexCall, kind: SqlKind) = {
          val candidate = new mutable.HashSet[AnyRef]
          candidate.addAll(skewValues)
          call.getOperands.foreach {
            case subCall: RexCall if subCall.getKind == kind &&
                subCall.operands.size() == 2 =>
              (subCall.operands.head, subCall.operands.last) match {
                case (ref: RexInputRef, literal: RexLiteral) =>
                  if (inputFieldIndice(ref.getIndex) == fieldIndex) {
                    candidate.remove(getLiteralValue(literal))
                  }
                case _ =>
              }
            case _ =>
          }
          if (candidate.nonEmpty) {
            skewMap.put(fieldIndex, candidate.toSeq)
          }
        }

        filter.getCondition match {
          // filter equal skew join
          case call: RexCall if call.getOperator == SqlStdOperatorTable.OR =>
            filterSkewValuesBySubCall(call, EQUALS)
          // filter not equal skew join
          case call: RexCall if call.getOperator == SqlStdOperatorTable.AND =>
            filterSkewValuesBySubCall(call, NOT_EQUALS)
          case call: RexCall if call.operands.size() == 2 =>
            (call.operands.head, call.operands.last) match {
              // deal with col1 = col2
              case (ref1: RexInputRef, ref2: RexInputRef) if call.getKind == EQUALS =>
                val otherFieldName = if (inputFieldIndice(ref1.getIndex) == fieldIndex) {
                  inputFieldIndice(ref2.getIndex)
                } else {
                  inputFieldIndice(ref1.getIndex)
                }
                val otherSkewValues = skewInfo.skewInfo.getOrElse(otherFieldName, Seq[AnyRef]())
                val newSkewValues = skewValues.intersect(otherSkewValues)
                if (newSkewValues.nonEmpty) {
                  skewMap.put(fieldIndex, newSkewValues)
                }
              case _ =>
                filterSingleLiteralCallSkewValues(skewMap, fieldIndex, skewValues, call)
            }
          case _ =>
            // Complex situation, keep skew values.
          skewMap.put(fieldIndex, skewValues)
        }
      } else {
        skewMap.put(fieldIndex, skewValues)
      }
    }

    SkewInfoInternal(skewMap.toMap)
  }

  private def filterSingleLiteralCallSkewValues(
      skewMap: mutable.HashMap[Int, Seq[AnyRef]],
      fieldIndex: Int,
      skewValues: Seq[AnyRef],
      call: RexCall) = {
    val (literalValue, op) = (call.operands.head, call.operands.last) match {
      case (_: RexInputRef, literal: RexLiteral) =>
        (getLiteralValue(literal), call.getKind)
      case (literal: RexLiteral, _: RexInputRef) =>
        (getLiteralValue(literal), call.getKind.reverse())
      case _ => (null, null)
    }

    if (op == null || literalValue == null) {
      skewMap.put(fieldIndex, skewValues)
    } else {

      def compareTo(a: Comparable[_], b: AnyRef): Int = {
        a.asInstanceOf[Comparable[Any]].compareTo(b.asInstanceOf[Comparable[Any]])
      }

      // filter expired skew values.
      val skewFilter = op match {
        case NOT_EQUALS =>
          (skewV: AnyRef) => literalValue != skewV
        case LESS_THAN => (skewV: AnyRef) => compareTo(literalValue, skewV) > 0
        case LESS_THAN_OR_EQUAL => (skewV: AnyRef) => compareTo(literalValue, skewV) >= 0
        case GREATER_THAN => (skewV: AnyRef) => compareTo(literalValue, skewV) < 0
        case GREATER_THAN_OR_EQUAL => (skewV: AnyRef) => compareTo(literalValue, skewV) <= 0
        case _ => (_: AnyRef) => false
      }

      val newSkews = skewValues.filter(skewFilter)
      if (newSkews.nonEmpty) {
        skewMap.put(fieldIndex, newSkews)
      }
    }
  }

  def getSkewInfo(rel: Exchange, mq: RelMetadataQuery): SkewInfoInternal = {
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    fmq.getSkewInfo(rel.getInput)
  }

  def getSkewInfo(rel: Sort, mq: RelMetadataQuery): SkewInfoInternal = {
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    fmq.getSkewInfo(rel.getInput)
  }

  def getSkewInfo(rel: Expand, mq: RelMetadataQuery): SkewInfoInternal = {
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    val skewInfoOfInput = fmq.getSkewInfo(rel.getInput)
    val skewInfoOfInputByIndex = if (skewInfoOfInput != null) {
      skewInfoOfInput.skewInfo
    } else {
      new mutable.HashMap[Int, Seq[AnyRef]]
    }
    val skewMap = new mutable.HashMap[Int, Seq[AnyRef]]
    rel.getRowType.getFieldNames.indices foreach {
      fieldIndex =>
        if (fieldIndex != rel.expandIdIndex) {
          val candidate = new mutable.HashSet[AnyRef]
          rel.projects foreach { project =>
            project.get(fieldIndex) match {
              case literal: RexLiteral if literal.isNull => candidate.add(null)
              case inputRef: RexInputRef =>
                val refIndex = inputRef.getIndex
                skewInfoOfInputByIndex.get(refIndex) match {
                  case Some(skewValue) => candidate.addAll(skewValue)
                  case _ => // ignore
                }
              case e => throw new TableException(s"Unknown expression ${e.toString}!")
            }
          }
          if (candidate.nonEmpty) {
            skewMap.put(fieldIndex, candidate.toSeq)
          }
        }
    }

    SkewInfoInternal(skewMap.toMap)
  }

  def getSkewInfo(rel: RelNode, mq: RelMetadataQuery): SkewInfoInternal = null

  def getSkewInfo(subset: RelSubset, mq: RelMetadataQuery): SkewInfoInternal = {
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    fmq.getSkewInfo(Util.first(subset.getBest, subset.getOriginal))
  }

}

object FlinkRelMdSkewInfo {

  private val INSTANCE = new FlinkRelMdSkewInfo

  val SOURCE: RelMetadataProvider = ReflectiveRelMetadataProvider.reflectiveSource(
  SkewInfoMeta.METHOD, INSTANCE)

}
