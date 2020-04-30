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
package org.apache.flink.table.planner.validate

/**
  * Represents the result of a validation.
  */
sealed trait ValidationResult {
  def isFailure: Boolean = !isSuccess
  def isSuccess: Boolean

  /**
    * Allows constructing a cascade of validation results.
    * The first failure result will be returned.
    */
  def orElse(other: ValidationResult): ValidationResult = {
    if (isSuccess) {
      other
    } else {
      this
    }
  }
}

/**
  * Represents the successful result of a validation.
  */
object ValidationSuccess extends ValidationResult {
  val isSuccess: Boolean = true
}

/**
  * Represents the failing result of a validation,
  * with a error message to show the reason of failure.
  */
case class ValidationFailure(message: String) extends ValidationResult {
  val isSuccess: Boolean = false
}
