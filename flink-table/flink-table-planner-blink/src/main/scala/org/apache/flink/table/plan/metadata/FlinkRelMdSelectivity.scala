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

import org.apache.calcite.plan.volcano.RelSubset
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core._
import org.apache.calcite.rel.metadata._
import org.apache.calcite.rex.RexNode
import org.apache.calcite.util.{BuiltInMethod, Util}

import java.lang.Double

/**
  * FlinkRelMdSelectivity supplies a implementation of
  * [[RelMetadataQuery#getSelectivity]] for the standard logical algebra.
  *
  * <p>Different from Calcite's default implementation, [[FlinkRelMdSelectivity]] throws
  * [[RelMdMethodNotImplementedException]] to disable the default implementation on [[RelNode]]
  * and requires to provide implementation on each kind of RelNode.
  *
  * <p>When add a new kind of RelNode, the author maybe forget to implement the logic for
  * the new rel, and get the unexpected result from the method on [[RelNode]]. So this handler
  * will force the author to implement the logic for new rel to avoid the unexpected result.
  */
class FlinkRelMdSelectivity private extends MetadataHandler[BuiltInMetadata.Selectivity] {

  def getDef: MetadataDef[BuiltInMetadata.Selectivity] = BuiltInMetadata.Selectivity.DEF

  def getSelectivity(rel: TableScan, mq: RelMetadataQuery, predicate: RexNode): Double = {
    RelMdUtil.guessSelectivity(predicate)
  }

  def getSelectivity(subset: RelSubset, mq: RelMetadataQuery, predicate: RexNode): Double = {
    val rel = Util.first(subset.getBest, subset.getOriginal)
    mq.getSelectivity(rel, predicate)
  }

  /**
    * Throws [[RelMdMethodNotImplementedException]] to
    * force implement [[getSelectivity]] logic on each kind of RelNode.
    */
  def getSelectivity(rel: RelNode, mq: RelMetadataQuery, predicate: RexNode): Double = {
    throw RelMdMethodNotImplementedException(
      "getSelectivity", getClass.getSimpleName, rel.getRelTypeName)
  }

}

object FlinkRelMdSelectivity {

  private val INSTANCE = new FlinkRelMdSelectivity

  val SOURCE: RelMetadataProvider = ReflectiveRelMetadataProvider.reflectiveSource(
    BuiltInMethod.SELECTIVITY.method, INSTANCE)

}
