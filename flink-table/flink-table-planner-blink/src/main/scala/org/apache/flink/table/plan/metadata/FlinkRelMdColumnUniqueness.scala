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

import org.apache.flink.table.plan.nodes.FlinkRelNode

import org.apache.calcite.plan.volcano.RelSubset
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.Converter
import org.apache.calcite.rel.core._
import org.apache.calcite.rel.metadata._
import org.apache.calcite.util.{Bug, BuiltInMethod, ImmutableBitSet, Util}

import java.lang.{Boolean => JBool}

import scala.collection.JavaConversions._

/**
  * FlinkRelMdColumnUniqueness supplies a implementation of
  * [[RelMetadataQuery#areColumnsUnique]] for the standard logical algebra.
  *
  * <p>Different from Calcite's default implementation, [[FlinkRelMdColumnUniqueness]] throws
  * [[RelMdMethodNotImplementedException]] to disable the default implementation on [[RelNode]]
  * and requires to provide implementation on each kind of RelNode.
  *
  * <p>When add a new kind of RelNode, the author maybe forget to implement the logic for
  * the new rel, and get the unexpected result from the method on [[RelNode]]. So this handler
  * will force the author to implement the logic for new rel to avoid the unexpected result.
  */
class FlinkRelMdColumnUniqueness private extends MetadataHandler[BuiltInMetadata.ColumnUniqueness] {

  def getDef: MetadataDef[BuiltInMetadata.ColumnUniqueness] = BuiltInMetadata.ColumnUniqueness.DEF

  def areColumnsUnique(
      rel: TableScan,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet,
      ignoreNulls: Boolean): JBool = {
    if (columns.cardinality == 0) {
      return false
    }
    rel.getTable.isKey(columns)
  }

  /**
    * Determines whether a specified set of columns from a RelSubSet relational expression are
    * unique.
    *
    * FIX BUG in <a href="https://issues.apache.org/jira/browse/CALCITE-2134">[CALCITE-2134] </a>
    *
    * @param subset         the RelSubSet relational expression
    * @param mq          metadata query instance
    * @param columns     column mask representing the subset of columns for which
    * uniqueness will be determined
    * @param ignoreNulls if true, ignore null values when determining column
    * uniqueness
    * @return whether the columns are unique, or
    *         null if not enough information is available to make that determination
    */
  def areColumnsUnique(
      subset: RelSubset,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet,
      ignoreNulls: Boolean): JBool = {
    if (!Bug.CALCITE_1048_FIXED) {
      val rel = Util.first(subset.getBest, subset.getOriginal)
      return mq.areColumnsUnique(rel, columns, ignoreNulls)
    }
    var nullCount = 0
    for (rel <- subset.getRels) {
      rel match {
        // NOTE: If add estimation uniqueness for new RelNode type,
        // add the RelNode to pattern matching in RelSubset.
        case _: Aggregate | _: Filter | _: Values | _: TableScan | _: Project | _: Correlate |
             _: Join | _: Exchange | _: Sort | _: SetOp | _: Calc | _: Converter | _: Window |
             _: FlinkRelNode =>
          try {
            val isUnique = mq.areColumnsUnique(rel, columns, ignoreNulls)
            if (isUnique != null) {
              if (isUnique) {
                return true
              }
            } else {
              nullCount += 1
            }
          } catch {
            case _: CyclicMetadataException =>
            // Ignore this relational expression; there will be non-cyclic ones in this set.
          }
        case _ => // skip
      }
    }

    if (nullCount == 0) false else null
  }

  /**
    * Throws [[RelMdMethodNotImplementedException]] to
    * force implement [[areColumnsUnique]] logic on each kind of RelNode.
    */
  def areColumnsUnique(
      rel: RelNode,
      mq: RelMetadataQuery,
      columns: ImmutableBitSet,
      ignoreNulls: Boolean): JBool = throw RelMdMethodNotImplementedException(
    "areColumnsUnique", getClass.getSimpleName, rel.getRelTypeName)

}

object FlinkRelMdColumnUniqueness {

  private val INSTANCE = new FlinkRelMdColumnUniqueness

  val SOURCE: RelMetadataProvider = ReflectiveRelMetadataProvider.reflectiveSource(
    BuiltInMethod.COLUMN_UNIQUENESS.method, INSTANCE)

}
