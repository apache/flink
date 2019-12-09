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

package org.apache.flink.table.codegen

import org.apache.flink.api.common.InvalidProgramException
import org.codehaus.commons.compiler.CompileException
import org.codehaus.janino.SimpleCompiler

trait Compiler[T] {

  @throws(classOf[CompileException])
  def compile(cl: ClassLoader, name: String, code: String): Class[T] = {
    require(cl != null, "Classloader must not be null.")
    val compiler = new SimpleCompiler()
    compiler.setParentClassLoader(cl)
    try {
      compiler.cook(code)
    } catch {
      case t: Throwable =>
        throw new InvalidProgramException("Table program cannot be compiled. " +
          "This is a bug. Please file an issue.", t)
    }
    compiler.getClassLoader.loadClass(name).asInstanceOf[Class[T]]
  }
}
