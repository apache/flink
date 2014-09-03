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


package org.apache.flink.api.scala.functions

import java.util.{ Iterator => JIterator }

import org.apache.flink.api.scala.analysis.UDTSerializer

import org.apache.flink.types.Record

protected final class DeserializingIterator[T](deserializer: UDTSerializer[T]) extends Iterator[T] {

  private var source: JIterator[Record] = null
  private var first: Record = null
  private var fresh = true

  final def initialize(records: JIterator[Record]): Record = {
    source = records

    if (source.hasNext) {
      fresh = true
      first = source.next()
    } else {
      fresh = false
      first = null
    }
    
    first
  }

  final def hasNext = fresh || source.hasNext

  final def next(): T = {

    if (fresh) {
      fresh = false
      val record = deserializer.deserializeRecyclingOff(first)
      first = null
      record
    } else {
      deserializer.deserializeRecyclingOff(source.next())
    }
  }
}