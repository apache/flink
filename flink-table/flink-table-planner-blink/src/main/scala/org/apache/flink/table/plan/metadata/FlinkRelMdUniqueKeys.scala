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
import org.apache.calcite.util.{Bug, BuiltInMethod, ImmutableBitSet, Util}

import java.util

/**
  * FlinkRelMdUniqueKeys supplies a implementation of
  * [[RelMetadataQuery#getUniqueKeys]] for the standard logical algebra.
  *
  * <p>Different from Calcite's default implementation, [[FlinkRelMdUniqueKeys]] throws
  * [[RelMdMethodNotImplementedException]] to disable the default implementation on [[RelNode]]
  * and requires to provide implementation on each kind of RelNode.
  *
  * <p>When add a new kind of RelNode, the author maybe forget to implement the logic for
  * the new rel, and get the unexpected result from the method on [[RelNode]]. So this handler
  * will force the author to implement the logic for new rel to avoid the unexpected result.
  */
class FlinkRelMdUniqueKeys private extends MetadataHandler[BuiltInMetadata.UniqueKeys] {

  def getDef: MetadataDef[BuiltInMetadata.UniqueKeys] = BuiltInMetadata.UniqueKeys.DEF

  def getUniqueKeys(
      rel: TableScan,
      mq: RelMetadataQuery,
      ignoreNulls: Boolean): util.Set[ImmutableBitSet] = null

  def getUniqueKeys(
      subset: RelSubset,
      mq: RelMetadataQuery,
      ignoreNulls: Boolean): util.Set[ImmutableBitSet] = {
    if (!Bug.CALCITE_1048_FIXED) {
      //if the best node is null, so we can get the uniqueKeys based original node, due to
      //the original node is logically equivalent as the rel.
      val rel = Util.first(subset.getBest, subset.getOriginal)
      mq.getUniqueKeys(rel, ignoreNulls)
    } else {
      throw new RuntimeException("CALCITE_1048 is fixed, so check this method again!")
    }
  }

  /**
    * Throws [[RelMdMethodNotImplementedException]] to
    * force implement [[getUniqueKeys]] logic on each kind of RelNode.
    */
  def getUniqueKeys(
      rel: RelNode,
      mq: RelMetadataQuery,
      ignoreNulls: Boolean): util.Set[ImmutableBitSet] = {
    throw RelMdMethodNotImplementedException(
      "getUniqueKeys", getClass.getSimpleName, rel.getRelTypeName)
  }

}

object FlinkRelMdUniqueKeys {

  private val INSTANCE = new FlinkRelMdUniqueKeys

  val SOURCE: RelMetadataProvider = ReflectiveRelMetadataProvider.reflectiveSource(
    BuiltInMethod.UNIQUE_KEYS.method, INSTANCE)

}
