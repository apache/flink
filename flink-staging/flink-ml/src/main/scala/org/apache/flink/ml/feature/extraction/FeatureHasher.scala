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

import org.apache.flink.api.common.functions._
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.DataSet
import org.apache.flink.ml.common.{Parameter, ParameterMap, Transformer}
import org.apache.flink.ml.math.{SparseVector, DenseVector, Vector}
import org.apache.flink.ml.feature.extraction.FeatureHasher.NumberFeatures

import scala.util.hashing.MurmurHash3

/** Apply feature hashing on observations, so that all features have a user-specified dimensionality.
  * By default for [[FeatureHasher]] transformer numberFeatures=1048576.
  *
  * This transformer takes a [[Vector]] of values and maps it to a feature hashed
  * [[Vector]] of an user-specified dimensionality.
  *
  * This transformer can be prepended to all [[Transformer]] and
  * [[org.apache.flink.ml.common.Learner]] implementations which expect an input of
  * [[Vector]].
  *
  * @example
  * {{{
  *             val trainingDS: DataSet[Vector] = env.fromCollection(data)
  *             val transformer = FeatureHasher().setNumberFeatures(1000)
  *
  *             transformer.transform(trainingDS)
  * }}}
  *
  * =Parameters=
  *
  * - [[FeatureHasher.NumberFeatures]]: The number of features in the resulting data set.
  */
class FeatureHasher extends Transformer[Seq[String], Vector] with Serializable {

	/** Sets the target number of features of the transformed data
	  *
	  * @param n the user-specified number of features.
	  * @return the FeatureHasher instance with its number of features set to the user-specified value
	  */
	def setNumberFeatures(n: Int): FeatureHasher = {
		parameters.add(NumberFeatures, n)
		this
	}

	override def transform(input: DataSet[Seq[String]], parameters: ParameterMap): DataSet[Vector] = {
		val resultingParameters = this.parameters ++ parameters
		val numberFeatures = resultingParameters(NumberFeatures)

		input.map(new RichMapFunction[Seq[String], Vector]() {

			override def map(a: Seq[String]): Vector = {
				var hashBuckets = Array.fill[Double](numberFeatures)(0.0)

				for (element <- a) {
					val h = Math.abs(MurmurHash3.arrayHash(Array(element)))

					hashBuckets(Math.abs(h) % numberFeatures) += (if (h > 0) {
						1.0
					} else {
						-1.0
					})
				}
				
				val zeroCount = hashBuckets.count(_ == 0.0)

				if (zeroCount.toFloat/numberFeatures > 0.5) {
					val size = numberFeatures - zeroCount
					val indices = new Array[(Int)](size)
					val data = new Array[(Double)](size)
					var i = 0
					for(element <- 0 until hashBuckets.size) {
						if(hashBuckets(element) != 0.0) {
							indices(i) = element
							data(i) = hashBuckets(element)
							i = i + 1
						}
					}
					new SparseVector(size,indices,data)
				} else {
					new DenseVector(hashBuckets)
				}
			}
		})
	}
}

object FeatureHasher {

	case object NumberFeatures extends Parameter[Int] {
		override val defaultValue: Option[Int] = Some(1048576)
	}

	def apply(): FeatureHasher = {
		new FeatureHasher()
	}
}
