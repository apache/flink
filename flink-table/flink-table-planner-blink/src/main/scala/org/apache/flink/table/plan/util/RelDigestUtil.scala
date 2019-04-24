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

import org.apache.flink.table.api.TableException
import org.apache.flink.table.plan.nodes.FlinkRelNode
import org.apache.flink.table.plan.nodes.calcite.{Expand, Rank, Sink, WatermarkAssigner}
import org.apache.flink.table.plan.nodes.common.CommonCalc

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
  *
  * NOTES: in some cases, non-deterministic operator can't not be reused by digest, so adds
  * unique id for non-deterministic operator into its digest to distinguish different rel with
  * same explain terms. This writer is stateless, and may get different result for same rel object.
  */
class RelDigestWriterImpl(sw: StringWriter, addUniqueIdForNonDeterministicOp: Boolean)
  extends RelWriterImpl(new PrintWriter(sw), SqlExplainLevel.DIGEST_ATTRIBUTES, false) {

  override def explain_(rel: RelNode, values: util.List[Pair[String, AnyRef]]): Unit = {
    val inputs = rel.getInputs
    val mq = rel.getCluster.getMetadataQuery
    if (!mq.isVisibleInExplain(rel, getDetailLevel)) {
      // render children in place of this, at same level
      inputs.foreach(_.explain(this))
      return
    }
    val s = explainRel(rel, values)
    pw.println(s)
    inputs.foreach(_.explain(this))
  }

  /**
    * Returns explain result for given rel.
    */
  protected def explainRel(rel: RelNode, values: util.List[Pair[String, AnyRef]]): String = {
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
    if (addUniqueIdForNonDeterministicOp && !isDeterministicOperator(rel)) {
      s.append(",nonDeterministicId=[")
        .append(RelDigestUtil.nonDeterministicIdCounter.incrementAndGet()).append("]")
    }
    s.append(")")
    s.toString()
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
      case c: Calc => CommonCalc.isDeterministic(c.getProgram)
      case s: Sort => SortUtil.isDeterministic(s.offset, s.fetch)
      case j: Join => FlinkRexUtil.isDeterministicOperator(j.getCondition)
      case a: Aggregate => AggregateUtil.isDeterministic(a.getAggCallList)
      case w: Window => OverAggregateUtil.isDeterministic(w.groups)
      case s: TableFunctionScan => FlinkRexUtil.isDeterministicOperator(s.getCall)
      case m: MultiJoin =>
        m.getOuterJoinConditions.forall(FlinkRexUtil.isDeterministicOperator) &&
          FlinkRexUtil.isDeterministicOperator(m.getJoinFilter) &&
          FlinkRexUtil.isDeterministicOperator(m.getPostJoinFilter)
      case t: TableModify =>
        t.getSourceExpressionList != null &&
          t.getSourceExpressionList.forall(FlinkRexUtil.isDeterministicOperator)
      case e: Expand => ExpandUtil.isDeterministic(e.projects)
      case _: Collect | _: Correlate | _: Exchange | _: SetOp | _: Sample |
           _: TableScan | _: Uncollect | _: Values | _: Sink | _: Rank |
           _: WatermarkAssigner => true
      case o => throw new TableException(
        s"Unsupported RelNode: ${o.getRelTypeName}, which should be handled before this exception")
    }
  }

}

/**
  * A RelDigestWriterImpl that will cache explain result for each rel in given rel tree.
  * NOTES: Different from RelDigestWriterImpl, this writer is stateful and always returns
  * same result for same rel object whether the rel tree contains non-deterministic operator.
  */
class CachedRelDigestWriterImpl(
    sw: StringWriter,
    addUniqueIdForNonDeterministicOp: Boolean,
    digestCache: util.IdentityHashMap[RelNode, String])
  extends RelDigestWriterImpl(sw, addUniqueIdForNonDeterministicOp) {

  require(digestCache != null)

  override def explain_(rel: RelNode, values: util.List[Pair[String, AnyRef]]): Unit = {
    val cachedDigest = digestCache.get(rel)
    if (cachedDigest != null) {
      pw.println(cachedDigest)
      return
    }

    val inputs = rel.getInputs
    val mq = rel.getCluster.getMetadataQuery
    if (!mq.isVisibleInExplain(rel, getDetailLevel)) {
      // render children in place of this, at same level
      inputs.foreach(_.explain(this))
      return
    }
    val s = explainRel(rel, values)
    pw.println(s)
    inputs.foreach(_.explain(this))

    val digest = if (inputs.isEmpty) s else s"$s\n${inputs.map(digestCache.get).mkString("\n")}"
    digestCache.put(rel, digest)
  }
}

object RelDigestUtil {
  private[flink] val nonDeterministicIdCounter = new AtomicInteger(0)

  /**
    * Gets the digest for a rel tree.
    *
    * @param rel rel node tree
    * @param addUniqueIdForNonDeterministicOp whether add unique id for non-deterministic operator.
    * @return The digest of given rel tree.
    */
  def getDigest(
      rel: RelNode,
      addUniqueIdForNonDeterministicOp: Boolean): String = {
    val sw = new StringWriter
    rel.explain(new RelDigestWriterImpl(sw, addUniqueIdForNonDeterministicOp))
    sw.toString
  }

  /**
    * Gets the digest for a rel tree.
    *
    * <p>NOTES: this method will cache explain result for each rel in given rel tree,
    * and always returns same result for same rel object whether the rel tree contains
    * non-deterministic operator.
    *
    * @param rel rel node tree
    * @param addUniqueIdForNonDeterministicOp whether add unique id for non-deterministic operator.
    * @param digestCache cache digest of each rel in the given tree.
    * @return The digest of given rel tree.
    */
  def getDigestWithCache(
      rel: RelNode,
      addUniqueIdForNonDeterministicOp: Boolean,
      digestCache: util.IdentityHashMap[RelNode, String]): String = {
    val sw = new StringWriter
    rel.explain(new CachedRelDigestWriterImpl(sw, addUniqueIdForNonDeterministicOp, digestCache))
    sw.toString
  }
}
