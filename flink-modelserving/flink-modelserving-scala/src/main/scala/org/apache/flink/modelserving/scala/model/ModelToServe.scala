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

import org.apache.flink.model.modeldescriptor.ModelDescriptor

import scala.util.Try

import java.io.DataOutputStream

/**
  * Model to serve - collection of static methods for data transformation.
  */
object ModelToServe {

  // Model Factory resolver
  private var resolver: ModelFactoryResolver[_, _] = _

  /**
    * Setting Model factories converter. Has to be invoked by the user at the beginning of his code.
    *
    * @param res model factories resolver.
    */
  def setResolver[RECORD, RESULT](res: ModelFactoryResolver[RECORD, RESULT]): Unit =
    resolver = res

  /**
    * Convert byte array to ModelToServe.
    *
    * @param binary byte representation of ModelDescriptor.proto.
    * @return model to serve.
    */
  def fromByteArray(message: Array[Byte]) = Try {
    val m = ModelDescriptor.parseFrom(message)
    m.messageContent.isData match {
      case true => ModelToServe(m.name, m.description, m.modeltype,
        m.getData.toByteArray, null, m.dataType)
      case _ => ModelToServe(m.name, m.description, m.modeltype,
        Array[Byte](), m.getLocation, m.dataType)
    }
  }

  /**
    * Write model to byte stream.
    *
    * @param model  model.
    * @param output DataOutputStream to write to.
    */
  def writeModel[RECORD, RESULT](model: Model[RECORD, RESULT], output: DataOutputStream): Unit = {
    try {
      model match {
        case null => output.writeLong(0)
        case _ =>
          val bytes = model.toBytes()
          output.writeLong(bytes.length)
          output.writeLong(model.getType)
          output.write(bytes)
      }
    } catch {
      case t: Throwable =>
        System.out.println("Error Serializing model")
        t.printStackTrace()
    }
  }

  /**
    * Deep copy of the model.
    *
    * @param from model.
    * @return model's copy.
    */
  def copy[RECORD, RESULT](from: Option[Model[RECORD, RESULT]]): Option[Model[RECORD, RESULT]] = {
    validateResolver()
    from match {
      case Some(model) =>
        validateResolver()
        Some(resolver.getFactory(model.getType.asInstanceOf[Int]).get.
          restore(model.toBytes()).asInstanceOf[Model[RECORD, RESULT]])
      case _ => None
    }
  }

    /**
      * Restore model from byte array.
      *
      * @param t    model's type.
      * @param from byte array representing model.
      * @return model.
      */
    def restore[RECORD, RESULT](t: Int, content: Array[Byte]): Option[Model[RECORD, RESULT]] = {
      validateResolver()
      Some(resolver.getFactory(t).get.restore(content).asInstanceOf[Model[RECORD, RESULT]])
    }

    /**
      * Get model from model to serve.
      *
      * @param model model to server.
      * @return model.
      */
    def toModel[RECORD, RESULT](model: ModelToServe): Option[Model[RECORD, RESULT]] = {
      validateResolver()
      resolver.getFactory(model.modelType.value) match {
        case Some(factory) => factory.create(model) match {
          case Some(model) => Some(model.asInstanceOf[Model[RECORD, RESULT]])
          case _ => None
        }
        case _ => None
      }
    }

    /**
      * Validating that resolver is set.
      *
      */
    def validateResolver(): Unit = resolver match {
      case null => throw new Exception("Model factory resolver is not set")
      case _ =>
    }
}

/**
  * Internal generic representation for model to serve.
  */
case class ModelToServe(name: String, description: String,
                        modelType: ModelDescriptor.ModelType,
                        model : Array[Byte], location : String, dataType : String)

/**
  * Representation of the model serving statistics.
  */
case class ModelToServeStats(name: String = "", description: String = "",
                             modelType: ModelDescriptor.ModelType = ModelDescriptor.ModelType.PMML,
                             since : Long = 0, var usage : Long = 0, var duration : Double = .0,
                             var min : Long = Long.MaxValue, var max : Long = Long.MinValue){
  def this(m : ModelToServe) = this(m.name, m.description, m.modelType, System.currentTimeMillis())

  /**
    * Increment usage. Invoked every time serving is completed.
    *
    * @param execution Model serving time.
    * @return updated statistics.
    */
  def incrementUsage(execution : Long) : ModelToServeStats = {
    usage = usage + 1
    duration = duration + execution
    if(execution < min) min = execution
    if(execution > max) max = execution
    this
  }
}

/**
  * Representation of the model serving result.
  */
case class ServingResult[RESULT](duration : Long, result: RESULT)
