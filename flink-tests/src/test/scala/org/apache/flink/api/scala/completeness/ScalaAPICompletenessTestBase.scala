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
package org.apache.flink.api.scala.completeness

import java.lang.reflect.Method

import org.junit.Assert._
import org.junit.Test

import scala.language.existentials

/**
 * Test base for checking whether the Scala API is up to feature parity with the Java API.
 * Right now is very simple, it is only checked whether a method with the same name exists.
 *
 * When adding excluded methods to the lists you should give a good reason in a comment.
 *
 * Note: This is inspired by the JavaAPICompletenessChecker from Spark.
 */
abstract class ScalaAPICompletenessTestBase {

  /**
   * Determines whether a method is excluded by name.
   */
  protected def isExcludedByName(method: Method): Boolean

  /**
   * Determines whether a method is excluded by an interface it uses.
   */
  protected def isExcludedByInterface(method: Method): Boolean = {
    val excludedInterfaces =
      Set("org.apache.spark.Logging", "org.apache.hadoop.mapreduce.HadoopMapReduceUtil")
    def toComparisionKey(method: Method) =
      (method.getReturnType, method.getName, method.getGenericReturnType)
    val interfaces = method.getDeclaringClass.getInterfaces.filter { i =>
      excludedInterfaces.contains(i.getName)
    }
    val excludedMethods = interfaces.flatMap(_.getMethods.map(toComparisionKey))
    excludedMethods.contains(toComparisionKey(method))
  }

  /**
   * Utility to be called during the test.
   */
  protected def checkMethods(
      javaClassName: String,
      scalaClassName: String,
      javaClass: Class[_],
      scalaClass: Class[_]) {
    val javaMethods = javaClass.getMethods
      .filterNot(_.isAccessible)
      .filterNot(isExcludedByName)
      .filterNot(isExcludedByInterface)
      .map(m => m.getName).toSet

    val scalaMethods = scalaClass.getMethods
      .filterNot(_.isAccessible)
      .filterNot(isExcludedByName)
      .filterNot(isExcludedByInterface)
      .map(m => m.getName).toSet

    val missingMethods = javaMethods -- scalaMethods

    for (method <- missingMethods) {
      fail("Method " + method + " from " + javaClass + " is missing from " + scalaClassName + ".")
    }
  }

  /**
   * Tests to be performed to ensure API completeness.
   */
  @Test
  protected def testCompleteness(): Unit
}
