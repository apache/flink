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

package org.apache.flink.table.calcite

import org.apache.calcite.rel.core._
import org.apache.calcite.rel.{RelHomogeneousShuttle, RelNode}
import org.apache.calcite.rex._
import org.apache.flink.table.plan.logical.rel.LogicalUpsertToRetraction
import org.apache.flink.table.plan.schema.UpsertStreamTable

import scala.collection.JavaConversions._

/**
  * Traverses a [[RelNode]] tree and add [[LogicalUpsertToRetraction]] node after the upsert source.
  */
class UpsertToRetractionConverter(rexBuilder: RexBuilder) extends RelHomogeneousShuttle {

  override def visit(other: RelNode): RelNode = {
    other match {
      case scan: TableScan => {
          scan.getTable.unwrap(classOf[UpsertStreamTable[_]]) match {
          case upsertStreamTable: UpsertStreamTable[_] =>
            LogicalUpsertToRetraction.create(
              scan.getCluster,
              scan.getTraitSet,
              scan,
              upsertStreamTable.uniqueKeys)
          case _ => scan
        }
      }
      case _ => other.copy(other.getTraitSet, other.getInputs.map(_.accept(this)))
    }
  }
}

object UpsertToRetractionConverter {

  def convert(rootRel: RelNode, rexBuilder: RexBuilder): RelNode = {
    val converter = new UpsertToRetractionConverter(rexBuilder)
    rootRel.accept(converter)
  }
}
