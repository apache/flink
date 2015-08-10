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

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.{utils => jutils}

import _root_.scala.language.implicitConversions
import _root_.scala.reflect.ClassTag

/**
 * This class provides simple utility methods for zipping elements in a data set with an index
 * or with a unique identifier.
 */

class DataSetUtils[T](val self: DataSet[T]) extends AnyVal {

  /**
   * Method that takes a set of subtask index, total number of elements mappings
   * and assigns ids to all the elements from the input data set.
   *
   * @return a data set of tuple 2 consisting of consecutive ids and initial values.
   */
  def zipWithIndex(implicit ti: TypeInformation[(Long, T)],
                   ct: ClassTag[(Long, T)]): DataSet[(Long, T)] = {
    wrap(jutils.DataSetUtils.zipWithIndex(self.javaSet))
      .map { t => (t.f0.toLong, t.f1) }
  }

  /**
   * Method that assigns a unique id to all the elements of the input data set.
   *
   * @return a data set of tuple 2 consisting of ids and initial values.
   */
  def zipWithUniqueId(implicit ti: TypeInformation[(Long, T)],
                      ct: ClassTag[(Long, T)]): DataSet[(Long, T)] = {
    wrap(jutils.DataSetUtils.zipWithUniqueId(self.javaSet))
      .map { t => (t.f0.toLong, t.f1) }
  }
}

object DataSetUtils {

  /**
   * Tie the new class to an existing Scala API class: DataSet.
   */
  implicit def utilsToDataSet[T: TypeInformation: ClassTag](dataSet: DataSet[T]) =
    new DataSetUtils[T](dataSet)
}
