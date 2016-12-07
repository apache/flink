package org.apache.flink.api.table.expressions

import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.calcite.tools.RelBuilder

abstract sealed class GroupingFunction extends Expression {

  override def toString = s"Grouping($children)"

  override private[flink] def resultType =
    throw new UnsupportedOperationException("A grouping function has no result type.")
}

case class Grouping(children: List[Expression]) extends GroupingFunction {

  /**
    * Convert Expression to its counterpart in Calcite, i.e. RexNode
    */
  override private[flink] def toRexNode(implicit relBuilder: RelBuilder) = {
    relBuilder.call(SqlStdOperatorTable.GROUPING, children.map(_.toRexNode): _*)
  }
}

case class GroupingId(children: List[Expression]) extends GroupingFunction {

  /**
    * Convert Expression to its counterpart in Calcite, i.e. RexNode
    */
  override private[flink] def toRexNode(implicit relBuilder: RelBuilder) = {
    relBuilder.call(SqlStdOperatorTable.GROUPING_ID, children.map(_.toRexNode): _*)
  }
}

case class GroupId(children: List[Expression]) extends GroupingFunction {

  /**
    * Convert Expression to its counterpart in Calcite, i.e. RexNode
    */
  override private[flink] def toRexNode(implicit relBuilder: RelBuilder) = {
    relBuilder.call(SqlStdOperatorTable.GROUP_ID, children.map(_.toRexNode): _*)
  }
}
