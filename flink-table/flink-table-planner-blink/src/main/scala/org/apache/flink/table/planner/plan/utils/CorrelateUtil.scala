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
package org.apache.flink.table.planner.plan.utils

import org.apache.flink.table.planner.calcite.{FlinkTypeFactory, FlinkTypeSystem}

import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeField, RelDataTypeFieldImpl}
import org.apache.calcite.rex.{RexBuilder, RexInputRef, RexNode, RexProgram, RexProgramBuilder}

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

/**
  * An utility class for optimizing and generating ExecCorrelate operators.
  */
object CorrelateUtil {

  def projectable(downsideCalc: RexProgram, correlateProgram: Option[RexProgram]): Boolean = {
    val refs = downsideCalc.getReferenceCounts
    // if the correlate output any unused column to the calc
    val calcInputFieldCnt = downsideCalc.getInputRowType.getFieldCount
    val projectable = refs.zipWithIndex.exists {
      case (refCnt: Int, index: Int) if index < calcInputFieldCnt =>
        refCnt == 0
      case _ => false
    }
    // If correlate.projectProgram.nonEmpty, that means this rule has matched before,
    // We should merge the top calc with the correlate program if we want to crop
    // the top unused projections and push used one into the correlate.
    // This is not supported yet, so we do a short-cut and return early.
    // TODO: add case for pattern that we need a RexProgram merge.
    projectable && correlateProgram.isEmpty
  }

  def getProjectableFieldSet(
      refs: Seq[Int],
      calcProgram: RexProgram,
      leftInputFieldCnt: Int): Set[Int] = {
    // calculate left/right projectable field(s)' index, only crop input refs.
    calcProgram.getExprList.zipWithIndex.filter { case (expr, index: Int) =>
      refs(index) == 0 && expr.isInstanceOf[RexInputRef]
    }.map(_._1.asInstanceOf[RexInputRef].getIndex).toSet
  }

  def projectCorrelateOutputType(
      originalType: RelDataType,
      projectableFieldSet: Set[Int]): (RelDataType, ListBuffer[Int]) = {
    val selects =  new ListBuffer[Int]
    // generate new output type that removed unused column(s) for Correlate
    val typeFactory = new FlinkTypeFactory(new FlinkTypeSystem)
    val typeBuilder = typeFactory.builder
    val reserveFieldTypes = originalType.getFieldList.zipWithIndex.filter {
      // filter unused fields
      f => !projectableFieldSet.contains(f._2)
    }.zipWithIndex.map {
      // create relField using new indexes
      case ((f: RelDataTypeField, srcIdx: Int), newIdx: Int) =>
        selects += srcIdx
        new RelDataTypeFieldImpl(
          f.getName,
          newIdx, // shift to new index
          f.getType)
    }
    if (reserveFieldTypes.size == 0) {
      // downside operator only cares records number, so we must output at least one column.
      // typical case: 'select count(*)' be pushed down here (count(0), count(1) ... as well)
      // we choose the last column to output(columns from left input more likely to be bigger).
      val reservedFieldIdx = originalType.getFieldCount - 1
      selects += reservedFieldIdx
      typeBuilder.add(
        originalType.getFieldNames.get(reservedFieldIdx),
        originalType.getFieldList.get(reservedFieldIdx).getType)
    } else {
      typeBuilder.addAll(reserveFieldTypes)
    }
    (typeBuilder.build(), selects)
  }

  def createProjectProgram(
      originalType: RelDataType,
      rexBuilder: RexBuilder,
      selects: Seq[Int]): RexProgram = {
    val rexProgBuilder = new RexProgramBuilder(originalType, rexBuilder)
    val fieldNames = originalType.getFieldNames
    selects.zipWithIndex.map {
      case (ordinal, newIdx) =>
        rexProgBuilder.addProject(newIdx, ordinal, fieldNames.get(ordinal))
    }
    rexProgBuilder.getProgram
  }

  def shiftProjectsAndCondition(
      refs: Seq[Int],
      calcProgram: RexProgram,
      projectableFieldSet: Set[Int],
      newInputType: RelDataType): (List[RexNode], RexNode) = {
    val reservedFieldsMapping = calcProgram.getExprList.zipWithIndex.filter {
      case (_, index) => refs(index) > 0
    }.zipWithIndex.map {
      case ((expr, srcIndex), newIndex) => srcIndex -> newIndex
    }.toMap

    val shiftProjects = calcProgram.getProjectList.map {
      ref => (calcProgram.expandLocalRef(ref), ref.getIndex)
    }.filter {
      case (_, idx) => !projectableFieldSet.contains(idx)
    }.map {
      case (rex, _) =>
        FlinkRexUtil.adjustInputRef(rex, reservedFieldsMapping, newInputType)
    }.toList

    val shiftCondition = if (null != calcProgram.getCondition) {
      FlinkRexUtil.adjustInputRef(
        calcProgram.expandLocalRef(calcProgram.getCondition),
        reservedFieldsMapping,
        newInputType)
    } else {
      null
    }
    (shiftProjects, shiftCondition)
  }
}
