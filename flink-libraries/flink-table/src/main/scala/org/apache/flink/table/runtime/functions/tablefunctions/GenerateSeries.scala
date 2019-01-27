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
package org.apache.flink.table.runtime.functions.tablefunctions

import org.apache.flink.table.api.functions.TableFunction
import org.apache.flink.table.api.types.{DataType, DataTypes}

class GenerateSeries extends TableFunction[Integer] {
  def eval(start: Integer, end: Integer): Unit = {
    (start.intValue() until end.intValue()).foreach(collect(_))
  }

  override def getResultType(arguments: Array[AnyRef], argTypes: Array[Class[_]]): DataType = {
    DataTypes.INT
  }
}
