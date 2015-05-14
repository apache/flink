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

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.functions.JoinFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.java.operators.Keys
import org.apache.flink.api.scala.typeutils.{CaseClassSerializer, CaseClassTypeInfo}
import org.apache.flink.streaming.api.datastream.temporal.TemporalWindow
import org.apache.flink.streaming.api.datastream.{DataStream => JavaStream, SingleOutputStreamOperator}
import org.apache.flink.streaming.api.functions.co.JoinWindowFunction
import org.apache.flink.streaming.api.operators.co.CoStreamWindow
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment.clean
import org.apache.flink.streaming.util.keys.KeySelectorUtil

import scala.Array.canBuildFrom
import scala.reflect.ClassTag

class StreamJoinOperator[I1, I2](i1: JavaStream[I1], i2: JavaStream[I2]) extends 
TemporalOperator[I1, I2, StreamJoinOperator.JoinWindow[I1, I2]](i1, i2) {

  override def createNextWindowOperator() = {
    new StreamJoinOperator.JoinWindow[I1, I2](this)
  }
}

object StreamJoinOperator {

  class JoinWindow[I1, I2](private[flink] val op: StreamJoinOperator[I1, I2]) extends
  TemporalWindow[JoinWindow[I1, I2]] {

    private[flink] val type1 = op.input1.getType()

    /**
     * Continues a temporal Join transformation by defining
     * the fields in the first stream to be used as keys for the join.
     * The resulting incomplete join can be completed by JoinPredicate.equalTo()
     * to define the second key.
     */
    def where(fields: Int*) = {
      new JoinPredicate[I1, I2](op, KeySelectorUtil.getSelectorForKeys(
        new Keys.ExpressionKeys(fields.toArray, type1),
        type1,
        op.input1.getExecutionEnvironment.getConfig))
    }

    /**
     * Continues a temporal Join transformation by defining
     * the fields in the first stream to be used as keys for the join.
     * The resulting incomplete join can be completed by JoinPredicate.equalTo()
     * to define the second key.
     */
    def where(firstField: String, otherFields: String*) =
      new JoinPredicate[I1, I2](op, KeySelectorUtil.getSelectorForKeys(
        new Keys.ExpressionKeys(firstField +: otherFields.toArray, type1),
        type1,
        op.input1.getExecutionEnvironment.getConfig))

    /**
     * Continues a temporal Join transformation by defining
     * the keyselector function that will be used to extract keys from the first stream
     * for the join.
     * The resulting incomplete join can be completed by JoinPredicate.equalTo()
     * to define the second key.
     */
    def where[K: TypeInformation](fun: (I1) => K) = {
      val keyType = implicitly[TypeInformation[K]]
      val keyExtractor = new KeySelector[I1, K] {
        val cleanFun = op.input1.clean(fun)
        def getKey(in: I1) = cleanFun(in)
      }
      new JoinPredicate[I1, I2](op, keyExtractor)
    }

    override def every(length: Long, timeUnit: TimeUnit): JoinWindow[I1, I2] = {
      every(timeUnit.toMillis(length))
    }

    override def every(length: Long): JoinWindow[I1, I2] = {
      op.slideInterval = length
      this
    }

  }

  class JoinPredicate[I1, I2](private[flink] val op: StreamJoinOperator[I1, I2],
    private[flink] val keys1: KeySelector[I1, _]) {
    private[flink] var keys2: KeySelector[I2, _] = null
    private[flink] val type2 = op.input2.getType()

    /**
     * Creates a temporal join transformation by defining the second join key.
     * The returned transformation wrapes each joined element pair in a tuple2:
     * (first, second)
     * To define a custom wrapping, use JoinedStream.apply(...)
     */
    def equalTo(fields: Int*): JoinedStream[I1, I2] = {
      finish(KeySelectorUtil.getSelectorForKeys(
        new Keys.ExpressionKeys(fields.toArray, type2),
        type2,
        op.input1.getExecutionEnvironment.getConfig))
    }

    /**
     * Creates a temporal join transformation by defining the second join key.
     * The returned transformation wrapes each joined element pair in a tuple2:
     * (first, second)
     * To define a custom wrapping, use JoinedStream.apply(...)
     */
    def equalTo(firstField: String, otherFields: String*): JoinedStream[I1, I2] =
      finish(KeySelectorUtil.getSelectorForKeys(
        new Keys.ExpressionKeys(firstField +: otherFields.toArray, type2),
        type2,
        op.input1.getExecutionEnvironment.getConfig))

    /**
     * Creates a temporal join transformation by defining the second join key.
     * The returned transformation wrapes each joined element pair in a tuple2:
     * (first, second)
     * To define a custom wrapping, use JoinedStream.apply(...)
     */
    def equalTo[K: TypeInformation](fun: (I2) => K): JoinedStream[I1, I2] = {
      val keyType = implicitly[TypeInformation[K]]
      val keyExtractor = new KeySelector[I2, K] {
        val cleanFun = op.input1.clean(fun)
        def getKey(in: I2) = cleanFun(in)
      }
      finish(keyExtractor)
    }

    private def finish(keys2: KeySelector[I2, _]): JoinedStream[I1, I2] = {
      this.keys2 = keys2
      new JoinedStream[I1, I2](this, createJoinOperator())
    }

    private def createJoinOperator(): JavaStream[(I1, I2)] = {

      val returnType = new CaseClassTypeInfo[(I1, I2)](
        classOf[(I1, I2)],
        Array(op.input1.getType, op.input2.getType),
        Seq(op.input1.getType, op.input2.getType),
        Array("_1", "_2")) {

        override def createSerializer(
            executionConfig: ExecutionConfig): TypeSerializer[(I1, I2)] = {
          val fieldSerializers: Array[TypeSerializer[_]] = new Array[TypeSerializer[_]](getArity)
          for (i <- 0 until getArity) {
            fieldSerializers(i) = types(i).createSerializer(executionConfig)
          }

          new CaseClassSerializer[(I1, I2)](classOf[(I1, I2)], fieldSerializers) {
            override def createInstance(fields: Array[AnyRef]) = {
              (fields(0).asInstanceOf[I1], fields(1).asInstanceOf[I2])
            }
          }
        }
      }

      op.input1.groupBy(keys1).connect(op.input2.groupBy(keys2))
        .addGeneralWindowCombine(getJoinWindowFunction(this, (_, _)),
          returnType, op.windowSize, op.slideInterval, op.timeStamp1, op.timeStamp2)
    }
  }

  class JoinedStream[I1, I2](jp: JoinPredicate[I1, I2], javaStream: JavaStream[(I1, I2)]) extends 
  DataStream[(I1, I2)](javaStream) {

    private val op = jp.op

    /**
     * Sets a wrapper for the joined elements. For each joined pair, the result of the
     * udf call will be emitted.
     */
    def apply[R: TypeInformation: ClassTag](fun: (I1, I2) => R): DataStream[R] = {

      val operator = new CoStreamWindow[I1, I2, R](
        clean(getJoinWindowFunction(jp, fun)), op.windowSize, op.slideInterval, op.timeStamp1,
        op.timeStamp2)

      javaStream.getExecutionEnvironment().getStreamGraph().setOperator(javaStream.getId(),
        operator)

      val js = javaStream.asInstanceOf[SingleOutputStreamOperator[R,_]]
      js.returns(implicitly[TypeInformation[R]]).asInstanceOf[SingleOutputStreamOperator[R,_]]
    }
  }

  private[flink] def getJoinWindowFunction[I1, I2, R](jp: JoinPredicate[I1, I2],
    joinFunction: (I1, I2) => R) = {
    require(joinFunction != null, "Join function must not be null.")

    val joinFun = new JoinFunction[I1, I2, R] {

      val cleanFun = jp.op.input1.clean(joinFunction)

      override def join(first: I1, second: I2): R = {
        cleanFun(first, second)
      }
    }

    new JoinWindowFunction[I1, I2, R](jp.keys1, jp.keys2, joinFun)
  }

}
