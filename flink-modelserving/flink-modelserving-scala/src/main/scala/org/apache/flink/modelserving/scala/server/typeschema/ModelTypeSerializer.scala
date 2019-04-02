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

package org.apache.flink.modelserving.scala.server.typeschema

import java.io.IOException

import org.apache.flink.modelserving.scala.model.{Model, ModelToServe}
import org.apache.flink.api.common.typeutils._
import org.apache.flink.core.memory.{DataInputView, DataOutputView}
import org.apache.flink.util.InstantiationUtil

/**
  * Type serializer for model - used by Flink checkpointing.
  */
class ModelTypeSerializer[RECORD, RESULT] extends TypeSerializer[Option[Model[RECORD, RESULT]]] {

  /**
    * Create model instatnce.
    *
    * @return model's instance.
    */
  override def createInstance(): Option[Model[RECORD, RESULT]] = None

  /**
    * Check if Type serializers can be equal.
    *
    * @param obj another object.
    * @return boolean specifying whether serializires can be equal.
    */
  def canEqual(obj: scala.Any): Boolean =
    obj.isInstanceOf[ModelTypeSerializer[RECORD, RESULT]]

  /**
    * Duplicate type serializer.
    *
    * @return duplicate of serializer.
    */
  override def duplicate() = new ModelTypeSerializer[RECORD, RESULT]

  /**
    * Serialize model.
    *
    * @param model  model.
    * @param target output.
    */
  override def serialize(record: Option[Model[RECORD, RESULT]], target: DataOutputView): Unit = {
    record match {
      case Some(model) =>
        target.writeBoolean(true)
        val content = model.toBytes()
        target.writeLong(model.getType)
        target.writeLong(content.length)
        target.write(content)
      case _ => target.writeBoolean(false)
    }
  }

  /**
    * Check whether type is immutable.
    *
    * @return boolean specifying whether type is immutable.
    */
  override def isImmutableType = false

  /**
    * Get model serialized length.
    *
    * @return model's serialized length.
    */
  override def getLength: Int = -1

  /**
    * Get snapshot's configuration.
    *
    * @return snapshot's configuration.
    */
  override def snapshotConfiguration() = new ModelSerializerConfigSnapshot[RECORD, RESULT]

  /**
    * Copy model.
    *
    * @param from original model.
    * @return model's copy.
    */
  override def copy(from: Option[Model[RECORD, RESULT]]): Option[Model[RECORD, RESULT]] =
    ModelToServe.copy[RECORD, RESULT](from)

  /**
    * Copy model (with reuse).
    *
    * @param from  original model.
    * @param reuse model to reuse.
    * @return model's copy.
    */
  override def copy(from: Option[Model[RECORD, RESULT]], reuse: Option[Model[RECORD, RESULT]]):
  Option[Model[RECORD, RESULT]] = ModelToServe.copy[RECORD, RESULT](from)

  override def copy(source: DataInputView, target: DataOutputView): Unit = {
    val exist = source.readBoolean()
    target.writeBoolean(exist)
    exist match {
      case true =>
        target.writeLong (source.readLong () )
        val clen = source.readLong ().asInstanceOf[Int]
        target.writeLong (clen)
        val content = new Array[Byte] (clen)
        source.read (content)
        target.write (content)
      case _ =>
    }
  }

  /**
    * Copy model using data streams.
    *
    * @param source original data stream.
    * @param target resulting data stream.
    */
  override def deserialize(source: DataInputView): Option[Model[RECORD, RESULT]] =
    source.readBoolean() match {
      case true =>
        val t = source.readLong().asInstanceOf[Int]
        val size = source.readLong().asInstanceOf[Int]
        val content = new Array[Byte] (size)
        source.read (content)
        ModelToServe.restore(t, content)
      case _ => Option.empty
    }

  /**
    * Deserialize byte array message.
    *
    * @param source Byte array message.
    * @return deserialized model.
    */
  override def deserialize(reuse: Option[Model[RECORD, RESULT]], source: DataInputView):
  Option[Model[RECORD, RESULT]] = deserialize(source)

  /**
    * Deserialize byte array message (with reuse).
    *
    * @param source Byte array message.
    * @param reuse  model to reuse.
    * @return deserialized model.
    */
  override def equals(obj: scala.Any): Boolean =
    obj.isInstanceOf[ModelTypeSerializer[RECORD, RESULT]]

  /**
    * Get hash code.
    *
    * @return hash code.
    */
  override def hashCode() = 42
}

object ModelTypeSerializer{

  def apply[RECORD, RESULT] = new ModelTypeSerializer[RECORD, RESULT]()
}

object ModelSerializerConfigSnapshot{

  val CURRENT_VERSION = 1
}

/**
  * Type serializer snapshot for model - used by Flink checkpointing.
  */
class ModelSerializerConfigSnapshot[RECORD, RESULT] extends
  TypeSerializerSnapshot[Option[Model[RECORD, RESULT]]]{

  import ModelSerializerConfigSnapshot._

  private var serializerClass = classOf[ModelTypeSerializer[RECORD, RESULT]]

  /**
    * Get current snapshot version.
    *
    * @return snapshot version.
    */
  override def getCurrentVersion: Int = CURRENT_VERSION

  /**
    * write snapshot.
    *
    * @param out output stream.
    */
  override def writeSnapshot(out: DataOutputView): Unit = out.writeUTF(serializerClass.getName)

  /**
    * Read snapshot.
    *
    * @param readVersion snapshot version.
    * @param in          input stream.
    * @param classLoader current classloader.
    */
  override def readSnapshot(readVersion: Int, in: DataInputView, classLoader: ClassLoader):
  Unit = {
    readVersion match {
      case CURRENT_VERSION =>
        val className = in.readUTF
        resolveClassName(className, classLoader, false)
      case _ =>
        throw new IOException("Unrecognized version: " + readVersion)
    }
  }

  /**
    * Restore serializer.
    *
    * @return type serializer.
    */
  override def restoreSerializer(): TypeSerializer[Option[Model[RECORD, RESULT]]] =
    InstantiationUtil.instantiate(serializerClass)

  /**
    * Resolve serializer compatibility.
    *
    * @param newSerializer serializer to compare.
    * @return compatibility resilt.
    */
  override def resolveSchemaCompatibility(newSerializer:
                                          TypeSerializer[Option[Model[RECORD, RESULT]]]):
  TypeSerializerSchemaCompatibility[Option[Model[RECORD, RESULT]]] =
    newSerializer.getClass match {
      case c if(c eq serializerClass) => TypeSerializerSchemaCompatibility.compatibleAsIs()
      case _ => TypeSerializerSchemaCompatibility.incompatible()
    }

  /**
    * Support method to resolve class name.
    *
    * @param className          class name.
    * @param cl                 class loader.
    * @param allowCanonicalName allow canonical name flag.
    * @return class.
    */
  private def resolveClassName(className: String, cl: ClassLoader, allowCanonicalName: Boolean):
  Unit =
    try
      serializerClass = cast(Class.forName(className, false, cl))
    catch {
      case e: Throwable =>
        throw new IOException("Failed to read SimpleTypeSerializerSnapshot: " +
          s"Serializer class not found: $className", e)
    }

  /**
    * Cast to required class.
    *
    * @param clazz class to cast.
    * @return class of required type.
    */
  private def cast[T](clazz: Class[_]) : Class[ModelTypeSerializer[RECORD, RESULT]] = {
    if (!classOf[ModelTypeSerializer[RECORD, RESULT]].isAssignableFrom(clazz)) {
      throw new IOException("Failed to read SimpleTypeSerializerSnapshot. " +
        "Serializer class name leads to a class that is not a TypeSerializer: " + clazz.getName)
    }
    clazz.asInstanceOf[Class[ModelTypeSerializer[RECORD, RESULT]]]
  }
}
