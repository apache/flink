/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.scala.stubs

import java.util.{ Iterator => JIterator }

import eu.stratosphere.scala.analysis.UDTSerializer

import eu.stratosphere.types.PactRecord

protected final class DeserializingIterator[T](deserializer: UDTSerializer[T]) extends Iterator[T] {

  private var source: JIterator[PactRecord] = null
  private var first: PactRecord = null
  private var fresh = true

  final def initialize(records: JIterator[PactRecord]): PactRecord = {
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