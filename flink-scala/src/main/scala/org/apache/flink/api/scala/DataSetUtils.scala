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
import org.apache.flink.api.java.{Utils, utils => jutils}

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

  // --------------------------------------------------------------------------------------------
  //  Sample
  // --------------------------------------------------------------------------------------------
  /**
   * Generate a sample of DataSet by the probability fraction of each element.
   *
   * @param withReplacement Whether element can be selected more than once.
   * @param fraction        Probability that each element is chosen, should be [0,1] without
   *                        replacement, and [0, ∞) with replacement. While fraction is larger
   *                        than 1, the elements are expected to be selected multi times into
   *                        sample on average.
   * @param seed            Random number generator seed.
   * @return The sampled DataSet
   */
  def sample(withReplacement: Boolean, fraction: Double, seed: Long = Utils.RNG.nextLong())
            (implicit ti: TypeInformation[T], ct: ClassTag[T]): DataSet[T] = {

    wrap(jutils.DataSetUtils.sample(self.javaSet, withReplacement, fraction, seed))
  }

  /**
   * Generate a sample of DataSet with fixed sample size.
   * <p>
   * <strong>NOTE:</strong> Sample with fixed size is not as efficient as sample with fraction,
   * use sample with fraction unless you need exact precision.
   * <p/>
   *
   * @param withReplacement Whether element can be selected more than once.
   * @param numSample       The expected sample size.
   * @param seed            Random number generator seed.
   * @return The sampled DataSet
   */
  def sampleWithSize(withReplacement: Boolean, numSample: Int, seed: Long = Utils.RNG.nextLong())
                    (implicit ti: TypeInformation[T], ct: ClassTag[T]): DataSet[T] = {

    wrap(jutils.DataSetUtils.sampleWithSize(self.javaSet, withReplacement, numSample, seed))
  }
}

object DataSetUtils {

  /**
   * Tie the new class to an existing Scala API class: DataSet.
   */
  implicit def utilsToDataSet[T: TypeInformation: ClassTag](dataSet: DataSet[T]) =
    new DataSetUtils[T](dataSet)
}
