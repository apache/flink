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

import org.apache.flink.api.common.functions.InvalidTypesException
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.table.sinks.{DataStreamTableSink, TableSink}

object SinkCodeGenerator {

  private[flink] def extractTableSinkTypeClass(sink: TableSink[_]): Class[_] = {
    try {
      sink match {
        // DataStreamTableSink has no generic class, so we need get the type to get type class.
        case sink: DataStreamTableSink[_] => sink.getOutputType.getTypeClass
        case _ => TypeExtractor.createTypeInfo(sink, classOf[TableSink[_]], sink.getClass, 0)
                  .getTypeClass.asInstanceOf[Class[_]]
      }
    } catch {
      case _: InvalidTypesException =>
        classOf[Object]
    }
  }

}
