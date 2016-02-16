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

package org.apache.flink.api.scala.table

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.CompositeType
import org.apache.flink.api.java.table.JavaStreamingTranslator
import org.apache.flink.api.table.Table
import org.apache.flink.api.table.plan._
import org.apache.flink.api.table.expressions.Expression
import org.apache.flink.streaming.api.scala.{DataStream, javaToScalaStream}

/**
 * [[PlanTranslator]] for creating [[Table]]s from Scala [[DataStream]]s and
 * translating them back to Scala [[DataStream]]s.
 *
 * This is very limited right now. Only select and filter are implemented. Also, the expression
 * operations must be extended to allow windowing operations.
 */
class ScalaStreamingTranslator extends PlanTranslator {

  private val javaTranslator = new JavaStreamingTranslator

  override type Representation[A] = DataStream[A]

  override def translate[O](op: PlanNode)(implicit tpe: TypeInformation[O]): DataStream[O] = {
    // fake it till you make it ...
    javaToScalaStream(javaTranslator.translate(op))
  }

  override def createTable[A](
      repr: Representation[A],
      inputType: CompositeType[A],
      expressions: Array[Expression],
      resultFields: Seq[(String, TypeInformation[_])]): Table = {

    val result =
      javaTranslator.createTable(repr.javaStream, inputType, expressions, resultFields)

    new Table(result.operation)
  }
}
