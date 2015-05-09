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

package org.apache.flink.ml.feature.extraction

import java.nio.charset.StandardCharsets

import org.apache.flink.api.scala._
import org.apache.flink.ml.common.{Parameter, ParameterMap, Transformer}
import org.apache.flink.ml.feature.extraction.FeatureHasher.{NonNegative, NumFeatures}
import org.apache.flink.ml.math.{Vector, SparseVector}

import scala.util.hashing.MurmurHash3


/** This transformer turns sequences of symbolic feature names (strings) into flink.ml.math.SparseVectors,
  * using a hash function to compute the matrix column corresponding to a name. Aka the hashing trick.
  * The hash function employed is the signed 32-bit version of Murmurhash3.
  *
  * By default for [[FeatureHasher]] transformer numFeatures=2#94;20 and nonNegative=false.
  *
  * This transformer takes a [[Seq]] of strings and maps it to a
  * feature [[Vector]].
  *
  * This transformer can be prepended to all [[Transformer]] and
  * [[org.apache.flink.ml.common.Learner]] implementations which expect an input of
  * [[Vector]].
  *
  * @example
  *          {{{
  *            val trainingDS: DataSet[Seq[String]] = env.fromCollection(data)
  *            val transformer = FeatureHasher().setNumFeatures(65536).setNonNegative(false)
  *
  *            transformer.transform(trainingDS)
  *          }}}
  *
  * =Parameters=
  *
  * - [[FeatureHasher.NumFeatures]]: The number of features (entries) in the output vector; by default equal to 2&#94;20
  * - [[FeatureHasher.NonNegative]]: Whether output vector should contain non-negative values only.
  * When True, output values can be interpreted as frequencies. When False, output values will have expected value zero;
  * by default equal to false
  */
class FeatureHasher extends Transformer[Seq[String], Vector] with Serializable {

  // The seed used to initialize the hasher
  val Seed = 0

  /** Sets the number of features (entries) in the output vector
    *
    * @param numFeatures the user-specified numFeatures value. In case the user gives a value less than 1,
    *                    numFeatures is set to its default value: 2&#94;20
    * @return the FeatureHasher instance with its numFeatures value set to the user-specified value
    */
  def setNumFeatures(numFeatures: Int): FeatureHasher = {
    // number of features must be greater zero
    if (numFeatures < 1)
      return this
    parameters.add(NumFeatures, numFeatures)
    this
  }

  /** Sets whether output vector should contain non-negative values only
    *
    * @param nonNegative the user-specified nonNegative value.
    * @return the FeatureHasher instance with its nonNegative value set to the user-specified value
    */
  def setNonNegative(nonNegative: Boolean): FeatureHasher = {
    parameters.add(NonNegative, nonNegative)
    this
  }

  override def transform(input: DataSet[Seq[String]], parameters: ParameterMap):
  DataSet[Vector] = {
    val resultingParameters = this.parameters ++ parameters

    val nonNegative = resultingParameters(NonNegative)
    val numFeatures = resultingParameters(NumFeatures)

    // each item of the sequence is hashed and transformed into a tuple (index, value)
    input.map {
      inputSeq => {
        val entries = inputSeq.map {
          s => {
            // unicode strings are converted to utf-8
            // bytesHash is faster than arrayHash, because it hashes 4 bytes at once
            val h = MurmurHash3.bytesHash(s.getBytes(StandardCharsets.UTF_8), Seed) % numFeatures
            val index = Math.abs(h)
            // instead of using two hash functions (Weinberger et al.), assume the sign is independent of the other bits
            val value = if (h >= 0) 1.0 else -1.0
            (index, value)
          }
        }
        val myVector = SparseVector.fromCOO(numFeatures, entries)
        // in case of non negative output, return the absolute of the vector
        if(nonNegative) {
          // maybe there's a more straightforward way of applying a function to all elements of a vector
          for(index <- myVector.indices) {
            myVector(index) = Math.abs(myVector(index))
          }
        }
        myVector
        // in case we want to return a DenseVector based on some decision metric
        // we can still return SparseVector.toDenseVector
      }
    }
  }
}

object FeatureHasher {

  case object NumFeatures extends Parameter[Int] {
    override val defaultValue: Option[Int] = Some(Math.pow(2, 20).toInt)
  }

  case object NonNegative extends Parameter[Boolean] {
    override val defaultValue: Option[Boolean] = Some(false)
  }

  def apply(): FeatureHasher = {
    new FeatureHasher()
  }
}