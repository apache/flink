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

package org.apache.flink.api.scala

import org.apache.flink.annotation.Public
import org.apache.flink.api.common.functions.CoGroupFunction
import org.apache.flink.api.common.operators.Keys
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.operators._
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo
import org.apache.flink.util.Collector
import scala.collection.JavaConverters._
import scala.reflect.ClassTag


/**
 * An unfinished coGroup operation that results from [[DataSet.coGroup]] The keys for the left and
 * right side must be specified using first `where` and then `equalTo`. For example:
 *
 * {{{
 *   val left = ...
 *   val right = ...
 *   val coGroupResult = left.coGroup(right).where(...).equalTo(...)
 * }}}
 *
 * @tparam L The type of the left input of the coGroup.
 * @tparam R The type of the right input of the coGroup.
 */
@Public
class UnfinishedCoGroupOperation[L: ClassTag, R: ClassTag](
                                                            leftInput: DataSet[L],
                                                            rightInput: DataSet[R])
  extends UnfinishedKeyPairOperation[L, R, CoGroupDataSet[L, R]](leftInput, rightInput) {

  private[flink] def finish(leftKey: Keys[L], rightKey: Keys[R]) = {
    val coGrouper = new CoGroupFunction[L, R, (Array[L], Array[R])] {
      def coGroup(
                   left: java.lang.Iterable[L],
                   right: java.lang.Iterable[R],
                   out: Collector[(Array[L], Array[R])]) = {
        val leftResult = Array[Any](left.asScala.toSeq: _*).asInstanceOf[Array[L]]
        val rightResult = Array[Any](right.asScala.toSeq: _*).asInstanceOf[Array[R]]

        out.collect((leftResult, rightResult))
      }
    }

    // We have to use this hack, for some reason classOf[Array[T]] does not work.
    // Maybe because ObjectArrayTypeInfo does not accept the Scala Array as an array class.
    val leftArrayType =
      ObjectArrayTypeInfo.getInfoFor(new Array[L](0).getClass, leftInput.getType)
        .asInstanceOf[TypeInformation[Array[L]]]
    val rightArrayType =
      ObjectArrayTypeInfo.getInfoFor(new Array[R](0).getClass, rightInput.getType)
        .asInstanceOf[TypeInformation[Array[R]]]

    val returnType = createTuple2TypeInformation[Array[L], Array[R]](leftArrayType, rightArrayType)
    val coGroupOperator = new CoGroupOperator[L, R, (Array[L], Array[R])](
      leftInput.javaSet, rightInput.javaSet, leftKey, rightKey, coGrouper, returnType,
      null, // partitioner
      getCallLocationName())

    new CoGroupDataSet(coGroupOperator, leftInput, rightInput, leftKey, rightKey)
  }
}

