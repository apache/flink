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
package org.apache.flink.api.table.validate

/**
  * Represents the result of `Expression.validateInput`.
  */
sealed trait ExprValidationResult {
  def isFailure: Boolean = !isSuccess
  def isSuccess: Boolean
}

/**
  * Represents the successful result of `Expression.checkInputDataTypes`.
  */
object ValidationSuccess extends ExprValidationResult {
  val isSuccess: Boolean = true
}

/**
  * Represents the failing result of `Expression.checkInputDataTypes`,
  * with a error message to show the reason of failure.
  */
case class ValidationFailure(message: String) extends ExprValidationResult {
  val isSuccess: Boolean = false
}
