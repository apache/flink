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

import scala.reflect.ClassTag
import org.apache.commons.lang.Validate
import org.apache.flink.api.common.functions.CrossFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.scala.typeutils.CaseClassSerializer
import org.apache.flink.api.scala.typeutils.CaseClassTypeInfo
import org.apache.flink.streaming.api.datastream.{DataStream => JavaStream}
import org.apache.flink.streaming.api.function.co.CrossWindowFunction
import org.apache.flink.streaming.api.invokable.operator.co.CoWindowInvokable
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment.clean
import org.apache.flink.streaming.api.datastream.temporaloperator.TemporalWindow
import java.util.concurrent.TimeUnit

class StreamCrossOperator[I1, I2](i1: JavaStream[I1], i2: JavaStream[I2]) extends
  TemporalOperator[I1, I2, StreamCrossOperator.CrossWindow[I1, I2]](i1, i2) {

  override def createNextWindowOperator(): StreamCrossOperator.CrossWindow[I1, I2] = {

    val crossWindowFunction = StreamCrossOperator.getCrossWindowFunction(this,
      (l: I1, r: I2) => (l, r))

    val returnType = new CaseClassTypeInfo[(I1, I2)](

      classOf[(I1, I2)], Seq(input1.getType, input2.getType), Array("_1", "_2")) {

      override def createSerializer: TypeSerializer[(I1, I2)] = {
        val fieldSerializers: Array[TypeSerializer[_]] = new Array[TypeSerializer[_]](getArity)
        for (i <- 0 until getArity) {
          fieldSerializers(i) = types(i).createSerializer
        }

        new CaseClassSerializer[(I1, I2)](classOf[(I1, I2)], fieldSerializers) {
          override def createInstance(fields: Array[AnyRef]) = {
            (fields(0).asInstanceOf[I1], fields(1).asInstanceOf[I2])
          }
        }
      }
    }

    val javaStream = input1.connect(input2).addGeneralWindowCombine(
      crossWindowFunction,
      returnType, windowSize,
      slideInterval, timeStamp1, timeStamp2);

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

      val invokable = new CoWindowInvokable[I1, I2, R](
        clean(getCrossWindowFunction(op, fun)), op.windowSize, op.slideInterval, op.timeStamp1,
        op.timeStamp2)

      javaStream.getExecutionEnvironment().getJobGraphBuilder().setInvokable(javaStream.getId(),
        invokable)

      javaStream.setType(implicitly[TypeInformation[R]])
    }
    
    override def every(length: Long, timeUnit: TimeUnit): CrossWindow[I1, I2] = {
      every(timeUnit.toMillis(length))
    }

    override def every(length: Long): CrossWindow[I1, I2] = {
      val builder = javaStream.getExecutionEnvironment().getJobGraphBuilder()
      val invokable = builder.getInvokable(javaStream.getId())
      invokable.asInstanceOf[CoWindowInvokable[_,_,_]].setSlideSize(length)
      this
    }
  }

  private[flink] def getCrossWindowFunction[I1, I2, R](op: StreamCrossOperator[I1, I2],
                                                       crossFunction: (I1, I2) => R):
  CrossWindowFunction[I1, I2, R] = {
    Validate.notNull(crossFunction, "Join function must not be null.")

    val crossFun = new CrossFunction[I1, I2, R] {
      val cleanFun = op.input1.clean(crossFunction)

      override def cross(first: I1, second: I2): R = {
        cleanFun(first, second)
      }
    }

    new CrossWindowFunction[I1, I2, R](crossFun)
  }

}
