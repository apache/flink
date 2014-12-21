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
package org.apache.flink.api.scala.streaming

import org.apache.flink.api.scala.typeutils.CaseClassTypeInfo
import java.util.ArrayList
import org.apache.flink.api.common.typeutils.CompositeType.FlatFieldDescriptor
import org.apache.flink.api.java.functions.KeySelector

class CaseClassKeySelector[T <: Product](@transient val typeInfo: CaseClassTypeInfo[T],
  val keyFields: String*) extends KeySelector[T, Seq[Any]] {

  val numOfKeys: Int = keyFields.length;

  @transient val fieldDescriptors = new ArrayList[FlatFieldDescriptor]();
  for (field <- keyFields) {
    typeInfo.getKey(field, 0, fieldDescriptors);
  }

  val logicalKeyPositions = new Array[Int](numOfKeys)
  val orders = new Array[Boolean](numOfKeys)

  for (i <- 0 to numOfKeys - 1) {
    logicalKeyPositions(i) = fieldDescriptors.get(i).getPosition();
  }

  def getKey(value: T): Seq[Any] = {
    for (i <- 0 to numOfKeys - 1) yield value.productElement(logicalKeyPositions(i))
  }
}
