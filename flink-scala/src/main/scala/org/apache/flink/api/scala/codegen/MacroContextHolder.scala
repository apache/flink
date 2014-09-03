/**
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


package org.apache.flink.api.scala.codegen

import scala.reflect.macros.Context

class MacroContextHolder[C <: Context](val c: C)

object MacroContextHolder {
  def newMacroHelper[C <: Context](c: C) = new MacroContextHolder[c.type](c)
    	with Loggers[c.type]
    	with UDTDescriptors[c.type]
    	with UDTAnalyzer[c.type]
    	with TreeGen[c.type]
    	with SerializerGen[c.type]
    	with SerializeMethodGen[c.type]
    	with DeserializeMethodGen[c.type]
    	with UDTGen[c.type]
    	with SelectionExtractor[c.type]
}
