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

import org.apache.flink.util.TestLogger
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
abstract class ScalaAPICompletenessTestBase extends TestLogger {

  /**
   * Determines whether a method is excluded by name.
   */
  protected def isExcludedByName(method: Method): Boolean

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
      .map(m => m.getName).toSet

    val scalaMethods = scalaClass.getMethods
      .filterNot(_.isAccessible)
      .filterNot(isExcludedByName)
      .map(m => m.getName).toSet

    val missingMethods = javaMethods -- scalaMethods

    for (javaMethod <- missingMethods) {
      // check if the method simply follows different getter / setter conventions in Scala / Java
      // for example Java: getFoo() should match Scala: foo()
      if (!containsScalaGetterLike(javaMethod, scalaMethods)) {
        fail(s"Method $javaMethod from $javaClass is missing from $scalaClassName.")
      }
    }
  }

  protected def containsScalaGetterLike(javaMethod: String, scalaMethods: Set[String]): Boolean = {
    if (javaMethod.startsWith("get") && javaMethod.length >= 4) {
      val scalaMethodName = Character.toLowerCase(javaMethod.charAt(3)) + javaMethod.substring(4)
      scalaMethods.contains(scalaMethodName)
    } else {
      false
    }
  }
  
  /**
   * Tests to be performed to ensure API completeness.
   */
  @Test
  protected def testCompleteness(): Unit
}
