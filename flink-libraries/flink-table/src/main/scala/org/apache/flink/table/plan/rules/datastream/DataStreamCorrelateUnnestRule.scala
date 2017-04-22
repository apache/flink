package org.apache.flink.table.plan.rules.datastream

import java.util

import com.google.common.collect.ImmutableList
import org.apache.calcite.plan.volcano.RelSubset
import org.apache.calcite.plan.{Convention, RelOptRule, RelOptRuleCall, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.{RelDataTypeFieldImpl, RelRecordType, StructKind}
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.core.Uncollect
import org.apache.calcite.rel.logical.{LogicalCorrelate, LogicalFilter, LogicalTableFunctionScan}
import org.apache.calcite.rex.RexNode
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.functions.utils.UserDefinedFunctionUtils
import org.apache.flink.table.plan.nodes.datastream.{DataStreamConvention, DataStreamCorrelate}
import org.apache.flink.table.plan.schema.ArrayRelDataType
import org.apache.flink.table.plan.util.ExplodeFunctionUtil

/**
  * Rule to convert a LogicalCorrelate with Uncollect into a DataStreamCorrelate.
  */
class DataStreamCorrelateUnnestRule
  extends ConverterRule(
    classOf[LogicalCorrelate],
    Convention.NONE,
    DataStreamConvention.INSTANCE,
    "DataStreamCorrelateUnnestRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val join: LogicalCorrelate = call.rel(0).asInstanceOf[LogicalCorrelate]
    val right = join.getRight.asInstanceOf[RelSubset].getOriginal

    right match {
      // a filter is pushed above the table function
      case filter: LogicalFilter =>
        filter
          .getInput.asInstanceOf[RelSubset]
          .getOriginal
          .isInstanceOf[Uncollect]
      case unCollect: Uncollect => true
      case _ => false
    }
  }

  override def convert(rel: RelNode): RelNode = {
    val join: LogicalCorrelate = rel.asInstanceOf[LogicalCorrelate]
    val traitSet: RelTraitSet = rel.getTraitSet.replace(DataStreamConvention.INSTANCE)
    val convInput: RelNode = RelOptRule.convert(join.getInput(0), DataStreamConvention.INSTANCE)
    val right: RelNode = join.getInput(1)

    def convertToCorrelate(relNode: RelNode, condition: Option[RexNode]): DataStreamCorrelate = {
      relNode match {
        case rel: RelSubset =>
          convertToCorrelate(rel.getRelList.get(0), condition)

        case filter: LogicalFilter =>
          convertToCorrelate(
            filter.getInput.asInstanceOf[RelSubset].getOriginal,
            Some(filter.getCondition))

        case unCollect: Uncollect =>
          val arrayRelDataType = unCollect.getInput(0).getRowType.getFieldList.get(0).getValue.asInstanceOf[ArrayRelDataType]
          val explodeTableFunc = UserDefinedFunctionUtils.createTableSqlFunctions(
            "explode",
            ExplodeFunctionUtil.explodeTableFuncFromType(arrayRelDataType.typeInfo),
            FlinkTypeFactory.toTypeInfo(arrayRelDataType.getComponentType),
            rel.getCluster.getTypeFactory.asInstanceOf[FlinkTypeFactory])

          val rexCall = rel.getCluster.getRexBuilder.makeCall(
            explodeTableFunc.head,
            unCollect.getInput(0).asInstanceOf[RelSubset].getRelList.get(0).getChildExps);
          val func = LogicalTableFunctionScan.create(
            rel.getCluster,
            new util.ArrayList[RelNode](),
            rexCall,
            classOf[Array[Object]],
            new RelRecordType(StructKind.FULLY_QUALIFIED, ImmutableList.of(new RelDataTypeFieldImpl("f0", 0, unCollect.getInput(0).getRowType.getFieldList.get(0).getValue.getComponentType))),
            null)
          new DataStreamCorrelate(
            rel.getCluster,
            traitSet,
            convInput,
            func,
            condition,
            rel.getRowType,
            join.getRowType,
            join.getJoinType,
            description)
      }
    }
    convertToCorrelate(right, None)
  }

}

object DataStreamCorrelateUnnestRule {
  val INSTANCE: RelOptRule = new DataStreamCorrelateUnnestRule
}

