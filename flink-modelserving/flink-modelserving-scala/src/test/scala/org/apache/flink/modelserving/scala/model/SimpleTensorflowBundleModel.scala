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

package org.apache.flink.modelserving.scala.model

import org.apache.flink.modelserving.scala.model.tensorflow.{Signature, TensorFlowBundleModel}
import org.tensorflow.{Graph, Session}

class SimpleTensorflowBundleModel(inputStream: Array[Byte])
  extends TensorFlowBundleModel[Double, Double](inputStream) {

  /**
    * Score data.
    *
    * @param input object to score.
    */
  override def score(input: Double): Double = .0

  // Getters for testing
  /**
    * Get tensorflow graph.
    *
    * @return tensorflow graph.
    */
  def getGraph: Graph = graph

  /**
    * Get tensorflow session.
    *
    * @return tensorflow session.
    */
  def getSession: Session = session

  /**
    * Get tensorflow signatures map.
    *
    * @return tensorflow signatures map.
    */
  def getSignatures: Map[String, Signature] = parsedSign

  /**
    * Get tensorflow tags.
    *
    * @return tensorflow tags.
    */
  def getTags: Seq[String] = tags
}

/**
  * Implementation of tensorflow (optimized) model factory.
  */
object SimpleTensorflowBundleModel extends  ModelFactory[Double, Double] {

  /**
    * Creates a new tensorflow (bundled) model.
    *
    * @param descriptor model to serve representation of tensorflow model.
    * @return model
    */
  override def create(input: ModelToServe): Option[Model[Double, Double]] = try
    Some(new SimpleTensorflowBundleModel(input.location.getBytes()))
  catch {
    case t: Throwable => None
  }

  /**
    * Restore tensorflow (bundled) model from binary.
    *
    * @param bytes binary representation of tensorflow model location.
    * @return model
    */
  override def restore(bytes: Array[Byte]) = new SimpleTensorflowBundleModel(bytes)
}
