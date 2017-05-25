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
package org.apache.flink.table.plan.rules.datastream

import org.apache.calcite.plan.volcano.RelSubset
import org.apache.calcite.plan.{ Convention, RelOptRule, RelOptRuleCall, RelTraitSet }
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.logical.{ LogicalFilter, LogicalCorrelate, LogicalTableFunctionScan }
import org.apache.calcite.rex.RexNode
import org.apache.flink.table.plan.nodes.datastream.DataStreamCorrelate
import org.apache.calcite.rel.logical.LogicalSort
import org.apache.flink.table.plan.nodes.datastream.DataStreamSort
import org.apache.calcite.rel.RelCollation
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.schema.RowSchema
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalSort
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.api.TableException
import org.apache.calcite.rel.RelFieldCollation.Direction
import org.apache.flink.table.runtime.aggregate.SortUtil

/**
 * Rule to convert a LogicalSort into a DataStreamSort.
 */
class DataStreamSortRule
    extends ConverterRule(
      classOf[FlinkLogicalSort],
      FlinkConventions.LOGICAL,
      FlinkConventions.DATASTREAM,
      "DataStreamSortRule") {

  override def matches(call: RelOptRuleCall): Boolean = {

    val result = super.matches(call)
    
    //need to identify time between others order fields. Time needs to be first sort element
    // we can safely convert the object if the match rule succeeded 
    if(result) {
      val calcSort: FlinkLogicalSort = call.rel(0).asInstanceOf[FlinkLogicalSort]
      checkTimeOrder(calcSort)
    }
    
    result
  }

  override def convert(rel: RelNode): RelNode = {
    val calcSort: FlinkLogicalSort = rel.asInstanceOf[FlinkLogicalSort]
    val traitSet: RelTraitSet = rel.getTraitSet.replace(FlinkConventions.DATASTREAM)
    val convInput: RelNode = RelOptRule.convert(calcSort.getInput(0), FlinkConventions.DATASTREAM)
    
    val inputRowType = convInput.asInstanceOf[RelSubset].getOriginal.getRowType

    new DataStreamSort(
      rel.getCluster,
      traitSet,
      convInput,
      new RowSchema(inputRowType),
      new RowSchema(rel.getRowType),
      calcSort.collation,
      calcSort.offset,
      calcSort.fetch,
      description)
    
  }
  
   
  /**
   * Function is used to check at verification time if the SQL syntax is supported
   */
  
  def checkTimeOrder(calcSort: FlinkLogicalSort) = {
    
    val rowType = calcSort.getRowType
    val sortCollation = calcSort.collation 
     //need to identify time between others order fields. Time needs to be first sort element
    val timeType = SortUtil.getTimeType(sortCollation, rowType)
    //time ordering needs to be ascending
    if (SortUtil.getTimeDirection(sortCollation) != Direction.ASCENDING) {
      throw new TableException("SQL/Table supports only ascending time ordering")
    }
    //enable to extend for other types of aggregates that will not be implemented in a window
    timeType match {
        case _ if FlinkTypeFactory.isProctimeIndicatorType(timeType) =>
        case _ if FlinkTypeFactory.isRowtimeIndicatorType(timeType) =>
        case _ =>
          throw new TableException("SQL/Table needs to have sort on time as first sort element")    

    }
  }

}

object DataStreamSortRule {
  val INSTANCE: RelOptRule = new DataStreamSortRule
}
