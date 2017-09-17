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

import org.apache.flink.table.expressions.{Expression, ResolvedFieldReference}

object PartitionPredicateExtractor {

  /**
    * Extract partition predicate from filter condition.
    *
    * @param predicates          Filter condition.
    * @param partitionFieldNames Partition field names.
    * @return Partition predicates and non-partition predicates.
    */
  def extractPartitionPredicates(
    predicates: Array[Expression],
    partitionFieldNames: Array[String]): (Array[Expression], Array[Expression]) = {

    predicates.partition(postOrderVisit(_, partitionFieldNames))
  }

  private def postOrderVisit(e: Expression, partitionFieldNames: Array[String]): Boolean = {
    e.children.forall {
      case r@ResolvedFieldReference(name, _) =>
        // skip non partition field
        postOrderVisit(r, partitionFieldNames) && partitionFieldNames.contains(name)
      case o@_ => postOrderVisit(o, partitionFieldNames)
    }
  }
}


