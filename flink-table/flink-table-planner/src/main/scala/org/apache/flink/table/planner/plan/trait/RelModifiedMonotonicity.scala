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
package org.apache.flink.table.planner.plan.`trait`

import org.apache.calcite.sql.validate.SqlMonotonicity

/**
  * The RelModifiedMonotonicity is used to describe the modified monotonicity of a
  * relation expression. Every field has its own modified monotonicity which means
  * the direction of the field's value is updated.
  */
class RelModifiedMonotonicity(val fieldMonotonicities: Array[SqlMonotonicity]) {

  override def equals(obj: scala.Any): Boolean = {

    if (obj == null || getClass != obj.getClass) {
      return false
    }

    val o = obj.asInstanceOf[RelModifiedMonotonicity]
    fieldMonotonicities.deep == o.fieldMonotonicities.deep
  }

  override def toString: String = {
    s"[${fieldMonotonicities.mkString(",")}]"
  }
}
