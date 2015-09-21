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

package org.apache.flink.streaming.api.scala

import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.functions.CrossFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.datastream.temporal.TemporalWindow
import org.apache.flink.streaming.api.datastream.{DataStream => JavaStream}
import org.apache.flink.streaming.api.functions.co.CrossWindowFunction
import org.apache.flink.streaming.api.operators.co.CoStreamWindow

import scala.reflect.ClassTag

class StreamCrossOperator[I1, I2](i1: JavaStream[I1], i2: JavaStream[I2]) extends
  TemporalOperator[I1, I2, StreamCrossOperator.CrossWindow[I1, I2]](i1, i2) {

  override def createNextWindowOperator(): StreamCrossOperator.CrossWindow[I1, I2] = {

    val crossWindowFunction = StreamCrossOperator.getCrossWindowFunction(this,
      (l: I1, r: I2) => (l, r))


    val returnType = createTuple2TypeInformation[I1, I2](input1.getType, input2.getType)
    val javaStream = input1.connect(input2).addGeneralWindowCombine(
      crossWindowFunction,
      returnType, windowSize,
      slideInterval, timeStamp1, timeStamp2)

    new StreamCrossOperator.CrossWindow[I1, I2](this, javaStream)
  }
}
object StreamCrossOperator {

  private[flink] class CrossWindow[I1, I2](op: StreamCrossOperator[I1, I2],
                                           javaStream: JavaStream[(I1, I2)]) extends
    DataStream[(I1, I2)](javaStream) with TemporalWindow[CrossWindow[I1, I2]] {

    /**
     * Sets a wrapper for the crossed elements. For each crossed pair, the result of the udf
     * call will be emitted.
     *
     */
    def apply[R: TypeInformation: ClassTag](fun: (I1, I2) => R): DataStream[R] = {

      val cleanCrossWindowFunction = clean(getCrossWindowFunction(op, fun))

      op.input1.connect(op.input2).addGeneralWindowCombine(
        cleanCrossWindowFunction,
        implicitly[TypeInformation[R]],
        op.windowSize,
        op.slideInterval,
        op.timeStamp1,
        op.timeStamp2)
    }
    
    override def every(length: Long, timeUnit: TimeUnit): CrossWindow[I1, I2] = {
      every(timeUnit.toMillis(length))
    }

    override def every(length: Long): CrossWindow[I1, I2] = {
      val graph = javaStream.getExecutionEnvironment().getStreamGraph()
      val operator = graph.getStreamNode(javaStream.getId()).getOperator()
      operator.asInstanceOf[CoStreamWindow[_,_,_]].setSlideSize(length)
      this
    }
  }

  private[flink] def getCrossWindowFunction[I1, I2, R](op: StreamCrossOperator[I1, I2],
                                                       crossFunction: (I1, I2) => R):
  CrossWindowFunction[I1, I2, R] = {
    require(crossFunction != null, "Join function must not be null.")

    val cleanFun = op.input1.clean(crossFunction)
    val crossFun = new CrossFunction[I1, I2, R] {
      override def cross(first: I1, second: I2): R = {
        cleanFun(first, second)
      }
    }

    new CrossWindowFunction[I1, I2, R](crossFun)
  }

}
