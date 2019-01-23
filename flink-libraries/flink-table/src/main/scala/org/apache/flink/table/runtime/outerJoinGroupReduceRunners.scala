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

import java.lang.Iterable

import org.apache.flink.api.common.functions.util.FunctionUtils
import org.apache.flink.api.common.functions.{JoinFunction, RichGroupReduceFunction}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.table.codegen.Compiler
import org.apache.flink.table.util.Logging
import org.apache.flink.types.Row
import org.apache.flink.util.Collector

abstract class OuterJoinGroupReduceRunner(
    name: String,
    code: String,
    @transient var returnType: TypeInformation[Row])
  extends RichGroupReduceFunction[Row, Row]
    with Compiler[JoinFunction[Row, Row, Row]] with Logging {

  protected var function: JoinFunction[Row, Row, Row] = null

  override def open(config: Configuration) {
    LOG.debug(s"Compiling JoinFunction: $name \n\n Code:\n$code")
    val clazz = compile(getRuntimeContext.getUserCodeClassLoader, name, code)
    LOG.debug("Instantiating JoinFunction.")
    function = clazz.newInstance()
    FunctionUtils.setFunctionRuntimeContext(function, getRuntimeContext)
    FunctionUtils.openFunction(function, config)
  }

  override def close(): Unit = {
    FunctionUtils.closeFunction(function)
  }
}

class LeftOuterJoinGroupReduceRunner(
    name: String,
    code: String,
    returnType: TypeInformation[Row])
  extends OuterJoinGroupReduceRunner(name, code, returnType) {

  override final def reduce(pairs: Iterable[Row], out: Collector[Row]): Unit = {

    var needsNull = true
    var left: Row = null
    var dupCnt: Int = 0

    val pairsIt = pairs.iterator()

    // go over all joined pairs
    while (pairsIt.hasNext) {

      val pair = pairsIt.next()
      left = pair.getField(0).asInstanceOf[Row]
      dupCnt = pair.getField(2).asInstanceOf[Int]
      val right = pair.getField(1).asInstanceOf[Row]

      if (right != null) {
        // we have a joining right record. Do not emit a null-padded result record.
        needsNull = false
        val result = function.join(left, right)
        // emit as many result records as the duplication count of the left record
        var i = dupCnt
        while (i > 0) {
          out.collect(result)
          i -= 1
        }
      }
    }

    // we did not find a single joining right record. Emit null-padded result records.
    if (needsNull) {
      val result = function.join(left, null)
      // emit as many null-padded result records as the duplication count of the left record.
      while (dupCnt > 0) {
        out.collect(result)
        dupCnt -= 1
      }
    }
  }
}

class RightOuterJoinGroupReduceRunner(
  name: String,
  code: String,
  returnType: TypeInformation[Row])
  extends OuterJoinGroupReduceRunner(name, code, returnType) {

  override final def reduce(pairs: Iterable[Row], out: Collector[Row]): Unit = {

    var needsNull = true
    var right: Row = null
    var dupCnt: Int = 0

    val pairsIt = pairs.iterator()

    // go over all joined pairs
    while (pairsIt.hasNext) {

      val pair = pairsIt.next()
      right = pair.getField(1).asInstanceOf[Row]
      dupCnt = pair.getField(2).asInstanceOf[Int]
      val left = pair.getField(0).asInstanceOf[Row]

      if (left != null) {
        // we have a joining left record. Do not emit a null-padded result record.
        needsNull = false
        val result = function.join(left, right)
        // emit as many result records as the duplication count of the right record
        var i = dupCnt
        while (i > 0) {
          out.collect(result)
          i -= 1
        }
      }
    }

    // we did not find a single joining left record. Emit null-padded result records.
    if (needsNull) {
      val result = function.join(null, right)
      // emit as many null-padded result records as the duplication count of the right record.
      while (dupCnt > 0) {
        out.collect(result)
        dupCnt -= 1
      }
    }
  }
}

/**
  * Emits a part of the results of a full outer join:
  *
  * - join result from matching join pairs (left + right)
  * - preserved left rows (left + null)
  *
  * Preserved right rows (null, right) are emitted by RightFullOuterJoinGroupReduceRunner.
  */
class LeftFullOuterJoinGroupReduceRunner(
  name: String,
  code: String,
  returnType: TypeInformation[Row])
  extends OuterJoinGroupReduceRunner(name, code, returnType) {

  override final def reduce(pairs: Iterable[Row], out: Collector[Row]): Unit = {

    var needsNull = true
    var left: Row = null
    var leftDupCnt: Int = 0

    val pairsIt = pairs.iterator()

    // go over all joined pairs
    while (pairsIt.hasNext) {

      val pair = pairsIt.next()
      left = pair.getField(0).asInstanceOf[Row]
      leftDupCnt = pair.getField(2).asInstanceOf[Int]
      val right = pair.getField(1).asInstanceOf[Row]

      if (right != null) {
        // we have a joining right record. Do not emit a null-padded result record.
        needsNull = false
        val rightDupCnt = pair.getField(3).asInstanceOf[Int]
        // emit as many result records as the product of the duplication counts of left and right.
        var i = leftDupCnt * rightDupCnt
        val result = function.join(left, right)
        while (i > 0) {
          out.collect(result)
          i -= 1
        }
      }
    }

    // we did not find a single joining right record. Emit null-padded result records.
    if (needsNull) {
      val result = function.join(left, null)
      // emit as many null-padded result records as the duplication count of the left record.
      while (leftDupCnt > 0) {
        out.collect(result)
        leftDupCnt -= 1
      }
    }
  }
}

/**
  * Emits a part of the results of a full outer join:
  *
  * - preserved right rows (null, right)
  *
  * Join result from matching join pairs (left + right) and preserved left rows (left + null) are
  * emitted by LeftFullOuterJoinGroupReduceRunner.
  */
class RightFullOuterJoinGroupReduceRunner(
  name: String,
  code: String,
  returnType: TypeInformation[Row])
  extends OuterJoinGroupReduceRunner(name, code, returnType) {

  override final def reduce(pairs: Iterable[Row], out: Collector[Row]): Unit = {

    var needsNull = true
    var right: Row = null
    var rightDupCnt: Int = 0

    val pairsIt = pairs.iterator()

    // go over all joined pairs
    while (pairsIt.hasNext && needsNull) {

      val pair = pairsIt.next()
      right = pair.getField(1).asInstanceOf[Row]
      rightDupCnt = pair.getField(3).asInstanceOf[Int]
      val left = pair.getField(0).asInstanceOf[Row]

      if (left != null) {
        // we have a joining left record. Do not emit a null-padded result record.
        needsNull = false
        // we do NOT emit join results here. This was done by LeftFullOuterJoinGroupReduceRunner.
      }
    }

    // we did not find a single joining left record. Emit null-padded result records.
    if (needsNull) {
      val result = function.join(null, right)
      // emit as many null-padded result records as the duplication count of the right record.
      while (rightDupCnt > 0) {
        out.collect(result)
        rightDupCnt -= 1
      }
    }
  }
}
