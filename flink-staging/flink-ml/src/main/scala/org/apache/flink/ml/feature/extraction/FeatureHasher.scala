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

import java.nio.charset.Charset

import breeze.linalg.VectorBuilder
import org.apache.flink.api.scala._
import org.apache.flink.ml.common.{Parameter, ParameterMap}
import org.apache.flink.ml.feature.extraction.FeatureHasher.{NonNegative, NumFeatures}
import org.apache.flink.ml.math.{Vector, SparseVector}
import org.apache.flink.ml.math.Breeze._
import org.apache.flink.ml.pipeline.{TransformDataSetOperation, Transformer, FitOperation}
import scala.util.hashing.MurmurHash3


/** This transformer turns iterables of symbolic feature names (strings) into
  * flink.ml.math.SparseVectors, using a hash function to compute the matrix column corresponding
  * to a name. Aka the hashing trick.
  * The hash function employed is the signed 32-bit version of Murmurhash3.
  *
  * By default for [[FeatureHasher]] transformer numFeatures=2#94;20 and nonNegative=false.
  *
  * This transformer takes a [[Iterable[String]] and maps it to a feature [[SparseVector]].
  *
  * This transformer can be prepended to all [[Transformer]] and
  * [[org.apache.flink.ml.pipeline.Predictor]] implementations which expect an input of
  * [[Vector]].
  *
  * @example
  * {{{
  *             val documents: DataSet[Seq[String]] = env.fromCollection(data)
  *             val featureHasher = FeatureHasher().setNumFeatures(65536).setNonNegative(false)
  *
  *             featureHasher.transform(documents)
  * }}}
  *
  * =Parameters=
  *
  * - [[FeatureHasher.NumFeatures]]: The number of features (entries) in the output vector;
  * by default equal to 2&#94;20
  * - [[FeatureHasher.NonNegative]]: Whether output vector should contain non-negative values only.
  * When True, output values can be interpreted as frequencies. When False, output values will have
  * expected value zero; by default equal to false
  */
class FeatureHasher extends Transformer[FeatureHasher] {

  /** Sets the number of features (entries) in the output vector
    *
    * @param numFeatures the user-specified numFeatures value. In case the user gives a value less
    *                    than 1, numFeatures is set to its default value: 2&#94;20
    * @return the FeatureHasher instance with its numFeatures value set to the user-specified value
    */
  def setNumFeatures(numFeatures: Int): FeatureHasher = {
    // number of features must be greater zero
    require(numFeatures > 0, "numFeatures must be greater than zero")
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
}

object FeatureHasher {

  // The seed used to initialize Murmurhash3
  val Seed = 0

  // ====================================== Parameters =============================================

  case object NumFeatures extends Parameter[Int] {
    override val defaultValue: Option[Int] = Some(Math.pow(2, 20).toInt)
  }

  case object NonNegative extends Parameter[Boolean] {
    override val defaultValue: Option[Boolean] = Some(false)
  }

  // ==================================== Factory methods ==========================================

  def apply(): FeatureHasher = {
    new FeatureHasher()
  }

  // ====================================== Operations =============================================

  /** The [[FeatureHasher]] transformer does not need a fitting phase.
    *
    * @tparam T The fitting works with arbitrary input types
    * @return
    */
  implicit def fitNoOp[T] = {
    new FitOperation[FeatureHasher, T]{
      override def fit(
        instance: FeatureHasher,
        fitParameters: ParameterMap,
        input: DataSet[T])
      : Unit = {}
    }
  }

  /** [[TransformDataSetOperation]] which hashes input data of subtype of [[Iterable[String]] into
    * a [[SparseVector]] of fixed size with indices determined by applying a hash function to each
    * element.
    * The size of the vector (number of features) is configurable.
    *
    * @tparam T Type of the input which has to be a subtype of [[Iterable[String]]
    * @return [[TransformDataSetOperation]] transforming subtypes of [[Iterable[String]] to feature
    *        [[SparseVector]]
    */
  implicit def transformDocumentsToVectors[T <: Iterable[String]] = {
    new TransformDataSetOperation[FeatureHasher, T, Vector] {
      override def transformDataSet(
        instance: FeatureHasher,
        transformParameters: ParameterMap,
        documents: DataSet[T])
      : DataSet[Vector] = {

        val resultingParameters = instance.parameters ++ transformParameters
        val nonNegative = resultingParameters(NonNegative)
        val numFeatures = resultingParameters(NumFeatures)

        // document is hashed into a sparse vector
        documents.map { document =>
          val vector = new VectorBuilder[Double](numFeatures)
          for (word <- document) {
            val (index, value) = murmurHash(word, 1, numFeatures)
            vector.add(index, value)
          }
          // in case of non negative output, return the absolute of the vector
          if (nonNegative) {
            absoluteVector(vector).toSparseVector.fromBreeze
          }
          else {
            vector.toSparseVector.fromBreeze
          }
        }
      }
    }
  }

  private def murmurHash(feature: String, count: Long, numFeatures: Int): (Int, Long) = {
    val hash = MurmurHash3.bytesHash(feature.getBytes(Charset.forName("UTF-8")), Seed)
    val index = scala.math.abs(hash) % numFeatures
    /* instead of using two hash functions (Weinberger et al.), assume the sign is in-
       dependent of the other bits */
    val value = if (hash >= 0) count else -1 * count
    (index, value)
  }

  private def absoluteVector(builder: VectorBuilder[Double]): VectorBuilder[Double] = {
    // only touch active entries
    for(entry <- builder.activeIterator) {
      builder(entry._1) = scala.math.abs(entry._2)
    }
    builder
  }
}
