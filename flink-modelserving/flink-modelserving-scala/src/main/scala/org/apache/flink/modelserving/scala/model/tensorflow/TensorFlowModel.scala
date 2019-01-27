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

package org.apache.flink.modelserving.scala.model.tensorflow

import org.apache.flink.annotation.Public
import org.apache.flink.model.modeldescriptor.ModelDescriptor
import org.apache.flink.modelserving.scala.model.Model
import org.tensorflow.{Graph, Session}

/**
  * Base class for tensorflow (optimized) model processing.
  */
@Public
abstract class TensorFlowModel[RECORD, RESULT](inputStream : Array[Byte])
  extends Model[RECORD, RESULT] {

  /**
    * Creates a new tensorflow (optimized) model.
    *
    * @param input binary representation of tensorflow(optimized) model.
    */
  // Make sure data is not empty
  if(inputStream.length < 1) throw new Exception("Empty graph data")
  // Model graph
  val graph = new Graph
  graph.importGraphDef(inputStream)
  // Create tensorflow session
  val session = new Session(graph)

  /**
    * Clean up tensorflow model.
    */
  override def cleanup(): Unit = {
    try
      session.close
    catch {
      case t: Throwable => // Swallow
    }
    try
      graph.close
    catch {
      case t: Throwable => // Swallow
    }
  }

  /**
    * Get bytes representation of tensorflow model.
    *
    * @return binary representation of the tensorflow model.
    */
  override def toBytes(): Array[Byte] = inputStream   //graph.toGraphDef

  /**
    * Get model'a type.
    *
    * @return tensorflow (optimized) model type.
    */
  override def getType: Long = ModelDescriptor.ModelType.TENSORFLOW.value

  /**
    * Compare 2 tensorflow (optimized) models. They are equal if their binary content is same.
    *
    * @param obj other model.
    * @return boolean specifying whether models are the same.
    */
  override def equals(obj: Any): Boolean = obj match {
    case tfModel: TensorFlowModel[RECORD, RESULT] =>
      tfModel.toBytes.toList == inputStream.toList
    case _ => false
  }
}
