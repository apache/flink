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
package org.apache.flink.api.scala.utils

import java.lang.reflect.Method

import org.apache.flink.api.scala.{ClosureCleaner, LambdaReturnStatementFinder}
import org.slf4j.LoggerFactory

object LambdaClosureCleaner {

  def getFlinkClassLoader: ClassLoader = getClass.getClassLoader

  def getContextOrFlinkClassLoader: ClassLoader =
    Option(Thread.currentThread().getContextClassLoader).getOrElse(getFlinkClassLoader)

  /** Preferred alternative to Class.forName(className) */
  def classForName(className: String): Class[_] = {
    Class.forName(className, true, getContextOrFlinkClassLoader)
    // scalastyle:on classforname
  }

  val LOG = LoggerFactory.getLogger(this.getClass)

  def clean(closure: AnyRef): Unit = {
    val writeReplaceMethod: Method = try {
      closure.getClass.getDeclaredMethod("writeReplace")
    } catch {
      case e: java.lang.NoSuchMethodException =>
        LOG.warn("Expected a Java lambda; got " + closure.getClass.getName)
        return
    }

    writeReplaceMethod.setAccessible(true)
    // Because we still need to support Java 7, we must use reflection here.
    val serializedLambda: AnyRef = writeReplaceMethod.invoke(closure)
    if (serializedLambda.getClass.getName != "java.lang.invoke.SerializedLambda") {
      LOG.warn("Closure's writeReplace() method " +
        s"returned ${serializedLambda.getClass.getName}, not SerializedLambda")
      return
    }

    val serializedLambdaClass = classForName("java.lang.invoke.SerializedLambda")

    val implClassName = serializedLambdaClass
      .getDeclaredMethod("getImplClass").invoke(serializedLambda).asInstanceOf[String]
    // TODO: we do not want to unconditionally strip this suffix.
    val implMethodName = {
      serializedLambdaClass
        .getDeclaredMethod("getImplMethodName").invoke(serializedLambda).asInstanceOf[String]
        .stripSuffix("$adapted")
    }
    val implMethodSignature = serializedLambdaClass
      .getDeclaredMethod("getImplMethodSignature").invoke(serializedLambda).asInstanceOf[String]
    val capturedArgCount = serializedLambdaClass
      .getDeclaredMethod("getCapturedArgCount").invoke(serializedLambda).asInstanceOf[Int]
    val capturedArgs = (0 until capturedArgCount).map { argNum: Int =>
      serializedLambdaClass
        .getDeclaredMethod("getCapturedArg", java.lang.Integer.TYPE)
        .invoke(serializedLambda, argNum.asInstanceOf[Object])
    }
    assert(capturedArgs.size == capturedArgCount)
    val implClass = classForName(implClassName.replaceAllLiterally("/", "."))

    // Fail fast if we detect return statements in closures.
    // TODO: match the impl method based on its type signature as well, not just its name.
    ClosureCleaner
      .getClassReader(implClass)
      .accept(new LambdaReturnStatementFinder(implMethodName), 0)

    // Check serializable TODO: add flag
    ClosureCleaner.ensureSerializable(closure)
    capturedArgs.foreach(ClosureCleaner.clean(_))

    // TODO: null fields to render the closure serializable?
  }
}

