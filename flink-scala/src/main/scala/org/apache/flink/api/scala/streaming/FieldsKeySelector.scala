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

import org.apache.flink.streaming.util.keys.{ FieldsKeySelector => JavaSelector }
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.java.tuple.Tuple

class FieldsKeySelector[IN](fields: Int*) extends KeySelector[IN, Tuple] {

  val t: Tuple = JavaSelector.tupleClasses(fields.length - 1).newInstance()

  override def getKey(value: IN): Tuple =

    value match {
      case prod: Product => {
        for (i <- 0 to fields.length - 1) {
          t.setField(prod.productElement(fields(i)), i)
        }
        t
      }
      case tuple: Tuple => {
        for (i <- 0 to fields.length - 1) {
          t.setField(tuple.getField(fields(i)), i)
        }
        t
      }
      case _ => throw new RuntimeException("Only tuple types are supported")
    }

}