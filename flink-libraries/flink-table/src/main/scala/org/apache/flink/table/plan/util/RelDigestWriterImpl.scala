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

package org.apache.flink.table.plan.util

import org.apache.flink.table.api.{TableConfig, TableConfigOptions, TableException}
import org.apache.flink.table.plan.nodes.FlinkRelNode
import org.apache.flink.table.plan.nodes.calcite.{Expand, LogicalLastRow, Rank, Sink, WatermarkAssigner}

import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core._
import org.apache.calcite.rel.externalize.RelWriterImpl
import org.apache.calcite.rel.rules.MultiJoin
import org.apache.calcite.sql.SqlExplainLevel
import org.apache.calcite.util.Pair

import java.io.{PrintWriter, StringWriter}
import java.util
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConversions._

/**
  * Row type is part of the digest for the rare occasion that similar
  * expressions have different types, e.g.
  * "WITH
  * t1 AS (SELECT CAST(a as BIGINT) AS a, SUM(b) AS b FROM x GROUP BY CAST(a as BIGINT)),
  * t2 AS (SELECT CAST(a as DOUBLE) AS a, SUM(b) AS b FROM x GROUP BY CAST(a as DOUBLE))
  * SELECT t1.*, t2.* FROM t1, t2 WHERE t1.b = t2.b"
  *
  * the physical plan is:
  * {{{
  *  HashJoin(where=[=(b, b0)], join=[a, b, a0, b0], joinType=[InnerJoin],
  *    isBroadcast=[true], build=[right])
  *  :- HashAggregate(groupBy=[a], select=[a, Final_SUM(sum$0) AS b])
  *  :  +- Exchange(distribution=[hash[a]])
  *  :     +- LocalHashAggregate(groupBy=[a], select=[a, Partial_SUM(b) AS sum$0])
  *  :        +- Calc(select=[CAST(a) AS a, b])
  *  :           +- ScanTable(table=[[builtin, default, x]], fields=[a, b, c])
  *  +- Exchange(distribution=[broadcast])
  *     +- HashAggregate(groupBy=[a], select=[a, Final_SUM(sum$0) AS b])
  *        +- Exchange(distribution=[hash[a]])
  *           +- LocalHashAggregate(groupBy=[a], select=[a, Partial_SUM(b) AS sum$0])
  *              +- Calc(select=[CAST(a) AS a, b])
  *                 +- ScanTable(table=[[builtin, default, x]], fields=[a, b, c])
  * }}}
  *
  * The sub-plan of `HashAggregate(groupBy=[a], select=[a, Final_SUM(sum$0) AS b])`
  * are different because `CAST(a) AS a` has different types, where one is BIGINT type
  * and another is DOUBLE type.
  *
  * If use the result of `RelOptUtil.toString(aggregate, SqlExplainLevel.DIGEST_ATTRIBUTES)`
  * on `HashAggregate(groupBy=[a], select=[a, Final_SUM(sum$0) AS b])` as digest,
  * we will get incorrect result. So rewrite `explain_` method of `RelWriterImpl` to
  * add row-type to digest value.
  */
class RelDigestWriterImpl(sw: StringWriter, tableConfig: TableConfig)
  extends RelWriterImpl(new PrintWriter(sw), SqlExplainLevel.DIGEST_ATTRIBUTES, false) {

  override def explain_(rel: RelNode, values: util.List[Pair[String, AnyRef]]): Unit = {
    val inputs = rel.getInputs
    val mq = rel.getCluster.getMetadataQuery
    if (!mq.isVisibleInExplain(rel, getDetailLevel)) {
      // render children in place of this, at same level
      inputs.foreach(_.explain(this))
      return
    }

    val s = new StringBuilder
    s.append(rel.getRelTypeName)
    var j = 0
    s.append("(")
    values.foreach {
      case value if value.right.isInstanceOf[RelNode] => // do nothing
      case value =>
        if (j != 0) s.append(", ")
        j += 1
        s.append(value.left).append("=[").append(value.right).append("]")
    }
    if (j > 0) {
      s.append(",")
    }
    s.append("rowType=[").append(rel.getRowType.toString).append("]")
    // if the given rel contains non-deterministic `SqlOperator`,
    // add a unique id to distinguish each other
    if (!tableConfig.getConf.getBoolean(
      TableConfigOptions.SQL_OPTIMIZER_REUSE_NONDETERMINISTIC_OPERATOR_ENABLED) &&
        !isDeterministicOperator(rel)) {
      s.append(",nonDeterministicId=[")
        .append(RelDigestWriterImpl.nonDeterministicIdCounter.incrementAndGet()).append("]")
    }
    s.append(")")

    pw.println(s)
    inputs.foreach(_.explain(this))
  }

  /**
    * Return true if the given rel does not contain non-deterministic `SqlOperator`
    * and dynamic function `SqlOperator`(e.g. op in `RexCall`, op in `SqlAggFunction`),
    * otherwise false.
    */
  private def isDeterministicOperator(rel: RelNode): Boolean = {
    rel match {
      case r: FlinkRelNode => r.isDeterministic
      case f: Filter => FlinkRexUtil.isDeterministicOperator(f.getCondition)
      case p: Project => p.getProjects.forall(p => FlinkRexUtil.isDeterministicOperator(p))
      case c: Calc => CalcUtil.isDeterministic(c.getProgram)
      case s: Sort => SortUtil.isDeterministic(s.offset, s.fetch)
      case j: Join => FlinkRexUtil.isDeterministicOperator(j.getCondition)
      case a: Aggregate => AggregateUtil.isDeterministic(a.getAggCallList)
      case w: Window => OverAggregateUtil.isDeterministic(w.groups)
      case s: TableFunctionScan => FlinkRexUtil.isDeterministicOperator(s.getCall)
      case m: Match => MatchUtil.isDeterministic(m)
      case m: MultiJoin =>
        m.getOuterJoinConditions.forall(FlinkRexUtil.isDeterministicOperator) &&
          FlinkRexUtil.isDeterministicOperator(m.getJoinFilter) &&
          FlinkRexUtil.isDeterministicOperator(m.getPostJoinFilter)
      case t: TableModify =>
        t.getSourceExpressionList != null &&
          t.getSourceExpressionList.forall(FlinkRexUtil.isDeterministicOperator)
      case r: Rank => FlinkRexUtil.isDeterministicOperator(r.rankFunction)
      case e: Expand => ExpandUtil.isDeterministic(e.projects)
      case _: Collect | _: Correlate | _: Exchange | _: SetOp | _: Sample |
           _: TableScan | _: Uncollect | _: Values | _: Sink | _: LogicalLastRow |
           _: WatermarkAssigner | _: Snapshot => true
      case o => throw new TableException(
        s"Unsupported RelNode: ${o.getRelTypeName}, which should be handled before this exception")
    }
  }

}

object RelDigestWriterImpl {
  private[flink] val nonDeterministicIdCounter = new AtomicInteger(0)

  def getDigest(rel: RelNode): String = {
    val sw = new StringWriter
    val tableConfig = FlinkRelOptUtil.getTableConfig(rel)
    rel.explain(new RelDigestWriterImpl(sw, tableConfig))
    sw.toString
  }
}
