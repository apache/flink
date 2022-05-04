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
package org.apache.flink.api.scala.extensions.base

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.extensions.data.KeyValuePair
import org.apache.flink.util.TestLogger

import org.scalatestplus.junit.JUnitSuiteLike

/** Common facilities to test the `acceptPartialFunctions` extension */
abstract private[extensions] class AcceptPFTestBase extends TestLogger with JUnitSuiteLike {

  private val env = ExecutionEnvironment.getExecutionEnvironment

  protected val tuples = env.fromElements(new Integer(1) -> "hello", new Integer(2) -> "world")
  protected val caseObjects = env.fromElements(KeyValuePair(1, "hello"), KeyValuePair(2, "world"))

  protected val groupedTuples = tuples.groupBy(_._1)
  protected val groupedCaseObjects = caseObjects.groupBy(_.id)

}
