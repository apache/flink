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
package org.apache.flink.table.planner.codegen.agg

import org.apache.flink.table.planner.codegen.{ExprCodeGenerator, GeneratedExpression}

/**
  * The base trait for code generating aggregate operations, such as accumulate and retract.
  * The implementation including declarative and imperative.
  */
trait AggCodeGen {

  def createAccumulator(generator: ExprCodeGenerator): Seq[GeneratedExpression]

  def setAccumulator(generator: ExprCodeGenerator): String

  def getAccumulator(generator: ExprCodeGenerator): Seq[GeneratedExpression]

  def resetAccumulator(generator: ExprCodeGenerator): String

  def accumulate(generator: ExprCodeGenerator): String

  def retract(generator: ExprCodeGenerator): String

  def merge(generator: ExprCodeGenerator): String

  def getValue(generator: ExprCodeGenerator): GeneratedExpression

  def checkNeededMethods(
    needAccumulate: Boolean = false,
    needRetract: Boolean = false,
    needMerge: Boolean = false,
    needReset: Boolean = false,
    needEmitValue: Boolean = false): Unit
}
