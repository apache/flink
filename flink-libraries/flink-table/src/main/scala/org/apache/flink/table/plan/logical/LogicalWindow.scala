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

package org.apache.flink.table.plan.logical

import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.expressions.{Expression, WindowReference}
import org.apache.flink.table.validate.{ValidationFailure, ValidationResult, ValidationSuccess}

/**
  * Logical super class for group windows.
  *
  * @param aliasAttribute window alias
  * @param timeAttribute time field indicating event-time or processing-time
  */
abstract class LogicalWindow(
    val aliasAttribute: Expression,
    val timeAttribute: Expression)
  extends Resolvable[LogicalWindow] {

  def resolveExpressions(resolver: (Expression) => Expression): LogicalWindow = this

  def validate(tableEnv: TableEnvironment): ValidationResult = aliasAttribute match {
    case WindowReference(_, _) => ValidationSuccess
    case _ => ValidationFailure("Window reference for window expected.")
  }

  override def toString: String = getClass.getSimpleName

}
