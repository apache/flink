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
package org.apache.flink.table.runtime.aggfunctions

import java.lang.reflect.Method

import org.apache.flink.table.api.functions.AggregateFunction
import org.apache.flink.table.dataformat.{BinaryString, GenericRow}
import org.apache.flink.table.runtime.functions.aggfunctions.ConcatAggFunction

class ConcatAggFunctionTest extends AggFunctionTestBase[BinaryString, GenericRow] {

  override def inputValueSets: Seq[Seq[_]] = Seq(
    Seq(
      BinaryString.fromString("a"),
      BinaryString.fromString("b"),
      null,
      BinaryString.fromString("c"),
      null,
      BinaryString.fromString("d"),
      BinaryString.fromString("e"),
      null,
      BinaryString.fromString("f")),
    Seq(null, null, null, null, null, null)
  )

  override def expectedResults: Seq[BinaryString] = Seq(
    BinaryString.fromString("a\nb\nc\nd\ne\nf"),
    null
  )

  override def aggregator: AggregateFunction[BinaryString, GenericRow] = new ConcatAggFunction

  override def accumulateFunc: Method = {
    aggregator.getClass.getMethod("accumulate", accType, classOf[BinaryString])
  }

  override def retractFunc = {
    aggregator.getClass.getMethod("retract", accType, classOf[BinaryString])
  }
}
