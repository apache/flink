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
package org.apache.flink.cep.scala.pattern

import java.lang.reflect.Method

import org.apache.flink.api.scala.completeness.ScalaAPICompletenessTestBase
import org.apache.flink.cep.pattern.{Pattern => JPattern}
import org.junit.Test

import scala.language.existentials

/**
 * This checks whether the CEP Scala API is up to feature parity with the Java API.
 * Implements the [[ScalaAPICompletenessTestBase]] for CEP.
 */
class PatternScalaAPICompletenessTest extends ScalaAPICompletenessTestBase {

  override def isExcludedByName(method: Method): Boolean = {
    val name = method.getDeclaringClass.getName + "." + method.getName
    val excludedNames = Seq()
    excludedNames.contains(name)
  }

  @Test
  override def testCompleteness(): Unit = {
    checkMethods("Pattern", "Pattern", classOf[JPattern[_, _]], classOf[Pattern[_, _]])
  }
}
