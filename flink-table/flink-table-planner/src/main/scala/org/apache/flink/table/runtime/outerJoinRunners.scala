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

package org.apache.flink.table.runtime

import java.lang.{Boolean => JBool}

import org.apache.flink.api.common.functions.util.FunctionUtils
import org.apache.flink.api.common.functions.{JoinFunction, RichFlatJoinFunction}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.configuration.Configuration
import org.apache.flink.table.codegen.Compiler
import org.apache.flink.table.util.Logging
import org.apache.flink.types.Row
import org.apache.flink.util.Collector

abstract class OuterJoinRunner(
    name: String,
    code: String,
    @transient var returnType: TypeInformation[Row])
  extends RichFlatJoinFunction[Row, Row, Row]
  with ResultTypeQueryable[Row]
  with Compiler[JoinFunction[Row, Row, JBool]]
  with Logging {

  protected var function: JoinFunction[Row, Row, JBool] = null

  override def open(parameters: Configuration): Unit = {
    LOG.debug(s"Compiling FlatJoinFunction: $name \n\n Code:\n$code")
    val clazz = compile(getRuntimeContext.getUserCodeClassLoader, name, code)
    LOG.debug("Instantiating FlatJoinFunction.")
    function = clazz.newInstance()
    FunctionUtils.setFunctionRuntimeContext(function, getRuntimeContext)
    FunctionUtils.openFunction(function, parameters)
  }

  override def getProducedType: TypeInformation[Row] = returnType

  override def close(): Unit = {
    FunctionUtils.closeFunction(function)
  }
}

/**
  * Emits left outer join pairs of left and right rows.
  * Left rows are always preserved if no matching right row is found (predicate evaluates to false
  * or right input row is null).
  */
class LeftOuterJoinRunner(
    name: String,
    code: String,
    returnType: TypeInformation[Row])
  extends OuterJoinRunner(name, code, returnType) {

  val outRow = new Row(3)

  override final def join(leftWithCnt: Row, right: Row, out: Collector[Row]): Unit = {

    val left: Row = leftWithCnt.getField(0).asInstanceOf[Row]
    val leftCnt = leftWithCnt.getField(1)

    outRow.setField(0, left)
    outRow.setField(2, leftCnt)

    if (right == null) {
      // right input row is null. Emit pair with null as right row
      outRow.setField(1, null)
    } else {
      // evaluate predicate.
      if (function.join(left, right)) {
        // emit pair with right row
        outRow.setField(1, right)
      } else {
        // emit pair with null as right row
        outRow.setField(1, null)
      }
    }
    out.collect(outRow)

  }
}

/**
  * Emits right outer join pairs of left and right rows.
  * Right rows are always preserved if no matching left row is found (predicate evaluates to false
  * or left input row is null).
  */
class RightOuterJoinRunner(
  name: String,
  code: String,
  returnType: TypeInformation[Row])
  extends OuterJoinRunner(name, code, returnType) {

  val outRow = new Row(3)

  override final def join(left: Row, rightWithCnt: Row, out: Collector[Row]): Unit = {

    val right: Row = rightWithCnt.getField(0).asInstanceOf[Row]
    val rightCnt = rightWithCnt.getField(1)

    outRow.setField(1, right)
    outRow.setField(2, rightCnt)

    if (left == null) {
      // left input row is null. Emit pair with null as left row
      outRow.setField(0, null)
    } else {
      // evaluate predicate.
      if (function.join(left, right)) {
        outRow.setField(0, left)
      } else {
        outRow.setField(0, null)
      }
    }
    out.collect(outRow)
  }
}

/**
  * Emits full outer join pairs of left and right rows.
  * Left and right rows are always preserved if no matching right row is found (predicate evaluates
  * to false or left or right input row is null).
  */
class FullOuterJoinRunner(
  name: String,
  code: String,
  returnType: TypeInformation[Row])
  extends OuterJoinRunner(name, code, returnType) {

  val outRow = new Row(4)

  override final def join(leftWithCnt: Row, rightWithCnt: Row, out: Collector[Row]): Unit = {

    if (leftWithCnt == null) {
      // left row is null. Emit join pair with null as left row.
      val right: Row = rightWithCnt.getField(0).asInstanceOf[Row]
      val rightCnt = rightWithCnt.getField(1)

      outRow.setField(0, null)
      outRow.setField(1, right)
      outRow.setField(2, null)
      outRow.setField(3, rightCnt)
      out.collect(outRow)
    } else if (rightWithCnt == null) {
      // right row is null. Emit join pair with null as right row.
      val left: Row = leftWithCnt.getField(0).asInstanceOf[Row]
      val leftCnt = leftWithCnt.getField(1)

      outRow.setField(0, left)
      outRow.setField(1, null)
      outRow.setField(2, leftCnt)
      outRow.setField(3, null)
      out.collect(outRow)
    } else {
      // both input rows are not null. Evaluate predicate.
      val left: Row = leftWithCnt.getField(0).asInstanceOf[Row]
      val leftCnt = leftWithCnt.getField(1)
      val right: Row = rightWithCnt.getField(0).asInstanceOf[Row]
      val rightCnt = rightWithCnt.getField(1)

      if (function.join(left, right)) {
        // predicate was true. Set rows in join pair
        outRow.setField(0, left)
        outRow.setField(1, right)
        outRow.setField(2, leftCnt)
        outRow.setField(3, rightCnt)
        out.collect(outRow)
      } else {
        // predicate was false. Emit two join pairs to preserve both input rows.
        // emit pair with left row and null as right row
        outRow.setField(0, left)
        outRow.setField(1, null)
        outRow.setField(2, leftCnt)
        outRow.setField(3, null)
        out.collect(outRow)

        // emit pair with right row and null as left row
        outRow.setField(0, null)
        outRow.setField(1, right)
        outRow.setField(2, null)
        outRow.setField(3, rightCnt)
        out.collect(outRow)
      }
    }
  }
}
