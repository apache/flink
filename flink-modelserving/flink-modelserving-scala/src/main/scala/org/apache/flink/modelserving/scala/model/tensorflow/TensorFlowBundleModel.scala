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

import java.io.File
import java.nio.file.Files

import com.google.protobuf.Descriptors

import org.apache.flink.model.modeldescriptor.ModelDescriptor
import org.apache.flink.modelserving.scala.model.Model
import org.apache.flink.annotation.Public
import org.tensorflow.SavedModelBundle

import scala.collection.mutable.{Map => MMap}
import org.tensorflow.framework.{MetaGraphDef, SavedModel, SignatureDef, TensorInfo, TensorShapeProto}

import scala.collection.JavaConverters._

/**
  * Base class for tensorflow (bundled) model processing.
  * This is experimental implementation showing how to deal with bundle. The implementation
  * leverages local file system access in both constructor and get tag method. A real
  * implementation will use some kind of shared storage, for example S3, Minio, GKS, etc.
  */
@Public
abstract class TensorFlowBundleModel[RECORD,RESULT](inputStream : Array[Byte])
  extends Model[RECORD,RESULT] {

  /**
    * Creates a new tensorflow (optimized) model.
    *
    * @param input binary representation of tensorflow(optimized) model.
    */

  // Convert input into file path
  val path = new String(inputStream)
  // get tags. We assume here that the first tag is the one we use
  val tags = getTags(path)
  // get saved model bundle
  val bundle = SavedModelBundle.load(path, tags(0))
  // get grapth
  val graph = bundle.graph
  // get metatagraph and signature
  val metaGraphDef = MetaGraphDef.parseFrom(bundle.metaGraphDef)
  val signatureMap = metaGraphDef.getSignatureDefMap.asScala
  //  parse signature, so that we can use definitions (if necessary) programmatically
  // in score method
  val parsedSign = parseSignatures(signatureMap)
  // Create tensorflow session
  val session = bundle.session

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
  override def toBytes(): Array[Byte] = inputStream

  /**
    * Get model'a type.
    *
    * @return tensorflow (bundled) model type.
    */
  override def getType: Long = ModelDescriptor.ModelType.TENSORFLOWSAVED.value

  /**
    * Compare 2 tensorflow (bundled) models. They are equal if their binary content is same.
    *
    * @param obj other model.
    * @return boolean specifying whether models are the same.
    */
  override def equals(obj: Any): Boolean = obj match {
    case tfModel: TensorFlowBundleModel[RECORD, RESULT] =>
      tfModel.toBytes.toList == inputStream.toList
    case _ => false
  }

  /**
    * Parse protobuf definition of signature map.
    *
    * @param signaturedefs map of protobuf encoded signatures.
    * @return map of signatures.
    */
  private def parseSignatures(signatures : MMap[String, SignatureDef]) : Map[String, Signature] =
    signatures.map(signature =>
      signature._1 -> Signature(parseInputOutput(signature._2.getInputsMap.asScala),
        parseInputOutput(signature._2.getOutputsMap.asScala))
  ).toMap

  /**
    * Parse protobuf definition of field map.
    *
    * @param inputOutputs map of protobuf encoded fields.
    * @return map of fields.
    */
  private def parseInputOutput(inputOutputs : MMap[String, TensorInfo]) : Map[String, Field] =
    inputOutputs.map(inputOutput => {
      var name = ""
      var dtype : Descriptors.EnumValueDescriptor = null
      var shape = Seq.empty[Int]
      inputOutput._2.getAllFields.asScala.foreach(descriptor => {
        descriptor._1.getName match {
          case n if n.contains("shape") =>
            descriptor._2.asInstanceOf[TensorShapeProto].getDimList.toArray.map(d =>
            d.asInstanceOf[TensorShapeProto.Dim].getSize).toSeq.foreach(v =>
            shape = shape :+ v.toInt)
          case n if n.contains("name") =>
            name = descriptor._2.toString.split(":")(0)
          case n if n.contains("shape") =>
            dtype = descriptor._2.asInstanceOf[Descriptors.EnumValueDescriptor]
          case _ =>
        }
    })
    inputOutput._1 -> Field(name,dtype, shape)
  }).toMap

  /**
    * Get model's tags.
    * Get tags method. If you want a known tag overwrite this method to return a list (of one) with
    * the required tag.
    *
    * @param directory Directory where definition is located.
    * @return map of fields.
    */
  protected def getTags(directory : String) : Seq[String] = {
    val d = new File(directory)
    val pbfiles = if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).filter(name => name.getName.endsWith("pb")
        || name.getName.endsWith("pbtxt")).toList
    }
    else List[File]()
    pbfiles.length match {
      case len if(len > 0) =>
        val byteArray = Files.readAllBytes(pbfiles(0).toPath)
        SavedModel.parseFrom(byteArray).getMetaGraphsList.asScala.
          flatMap(graph => graph.getMetaInfoDef.getTagsList.asByteStringList.
            asScala.map(_.toStringUtf8))
      case _ => Seq.empty
    }
  }
}

/**
  * Tensorflow bundled Field definition.
  */
case class Field(name : String, `type` : Descriptors.EnumValueDescriptor, shape : Seq[Int])

/**
  * Tensorflow bundled Signature definition.
  */
case class Signature(inputs :  Map[String, Field], outputs :  Map[String, Field])
