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

package org.apache.flink.api.expressions.analysis

import org.apache.flink.api.expressions.ExpressionException
import org.apache.flink.api.expressions.tree.{Expression, Aggregation}

import scala.collection.mutable

/**
 * Rule that verifies that an expression does not contain aggregate operations
 * as children of aggregate operations.
 */
class VerifyNoNestedAggregates extends Rule {

  def apply(expr: Expression) = {
    val errors = mutable.MutableList[String]()

    val result = expr.transformPre {
      case agg: Aggregation=> {
        if (agg.child.exists(_.isInstanceOf[Aggregation])) {
          errors += s"""Found nested aggregation inside "$agg"."""
        }
        agg
      }
    }

    if (errors.length > 0) {
      throw new ExpressionException(
        s"""Invalid expression "$expr": ${errors.mkString(" ")}""")
    }

    result

  }
}
