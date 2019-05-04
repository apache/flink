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

import org.apache.flink.api.common.typeutils._
import org.apache.flink.core.memory.{DataInputView, DataOutputView}
import org.apache.flink.util.InstantiationUtil
import org.apache.flink.modelserving.scala.model.{ModelWithType, ModelToServe}

/**
  * Type serializer for model with type - used by Flink checkpointing.
  */
class ModelWithTypeSerializer[RECORD, RESULT]
  extends TypeSerializer[ModelWithType[RECORD, RESULT]] {

  /**
    * Create model with state instatnce.
    *
    * @return model's instance.
    */
  override def createInstance(): ModelWithType[RECORD, RESULT] =
    new ModelWithType[RECORD, RESULT]("", null)

  /**
    * Check if Type serializers can be equal.
    *
    * @param obj another object.
    * @return boolean specifying whether serializires can be equal.
    */
  def canEqual(obj: scala.Any): Boolean =
    obj.isInstanceOf[ModelWithTypeSerializer[RECORD, RESULT]]

  /**
    * Duplicate type serializer.
    *
    * @return duplicate of serializer.
    */
  override def duplicate() = new ModelWithTypeSerializer[RECORD, RESULT]

  /**
    * Serialize model with state.
    *
    * @param model  model.
    * @param target output.
    */
  override def serialize(model: ModelWithType[RECORD, RESULT], target: DataOutputView): Unit = {
    target.writeUTF(model.dataType)
    model.model match {
      case Some(m) =>
        target.writeBoolean(true)
        val content = m.toBytes()
        target.writeLong(m.getType)
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
  override def snapshotConfiguration() = new ModelWithTypeSerializerConfigSnapshot[RECORD, RESULT]

  /**
    * Copy model with type.
    *
    * @param from original model with state.
    * @return model's with state copy.
    */
  override def copy(from: ModelWithType[RECORD, RESULT]): ModelWithType[RECORD, RESULT] =
    new ModelWithType[RECORD, RESULT](from.dataType, from.model)

  /**
    * Copy model with state (with reuse).
    *
    * @param from  original model with state.
    * @param reuse model with state to reuse.
    * @return model's with state copy.
    */
  override def copy(from: ModelWithType[RECORD, RESULT], reuse: ModelWithType[RECORD, RESULT]):
  ModelWithType[RECORD, RESULT] = copy(from)

  /**
    * Copy model using data streams.
    *
    * @param source original data stream.
    * @param target resulting data stream.
    */
  override def copy(source: DataInputView, target: DataOutputView): Unit = {
    target.writeUTF(source.readUTF())
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
    * Deserialize byte array message.
    *
    * @param source Byte array message.
    * @return deserialized model with state.
    */
  override def deserialize(source: DataInputView): ModelWithType[RECORD, RESULT] = {
    val dataType = source.readUTF()
    source.readBoolean() match {
      case true =>
        val t = source.readLong().asInstanceOf[Int]
        val size = source.readLong().asInstanceOf[Int]
        val content = new Array[Byte](size)
        source.read(content)
        new ModelWithType[RECORD, RESULT](
          dataType, ModelToServe.restore[RECORD, RESULT](t, content))
      case _ => new ModelWithType[RECORD, RESULT](dataType, None)
    }
  }

  /**
    * Deserialize byte array message (with reuse).
    *
    * @param source Byte array message.
    * @param reuse  model to reuse.
    * @return deserialized model.
    */
  override def deserialize(reuse: ModelWithType[RECORD, RESULT], source: DataInputView):
  ModelWithType[RECORD, RESULT] = deserialize(source)

  /**
    * Check if type serializer's are equal.
    *
    * @param obj Byte array message.
    * @return boolean specifying whether type serializers are equal.
    */
  override def equals(obj: scala.Any): Boolean =
    obj.isInstanceOf[ModelWithTypeSerializer[RECORD, RESULT]]

  /**
    * Get hash code.
    *
    * @return hash code.
    */
  override def hashCode() = 42
}

object ModelWithTypeSerializer{

  def apply[RECORD, RESULT] : ModelWithTypeSerializer[RECORD, RESULT] =
    new ModelWithTypeSerializer[RECORD, RESULT]()
}

object ModelWithTypeSerializerConfigSnapshot{

  val CURRENT_VERSION = 1
}

/**
  * Type serializer snapshot for model with state - used by Flink checkpointing.
  * */
class ModelWithTypeSerializerConfigSnapshot[RECORD, RESULT] extends
  TypeSerializerSnapshot[ModelWithType[RECORD, RESULT]]{

  import ModelWithTypeSerializerConfigSnapshot._

  private var serializerClazz = classOf[ModelWithTypeSerializer[RECORD, RESULT]]

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
  override def writeSnapshot(out: DataOutputView): Unit = {
    out.writeUTF(serializerClazz.getName)
  }

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
  override def restoreSerializer(): TypeSerializer[ModelWithType[RECORD, RESULT]] =
    InstantiationUtil.instantiate(serializerClazz)

  /**
    * Support method to resolve class name.
    *
    * @param className          class name.
    * @param cl                 class loader.
    * @param allowCanonicalName allow canonical name flag.
    * @return class.
    */
  override def resolveSchemaCompatibility(newSerializer:
                                          TypeSerializer[ModelWithType[RECORD, RESULT]]):
  TypeSerializerSchemaCompatibility[ModelWithType[RECORD, RESULT]] =
    if (newSerializer.getClass eq serializerClazz) {
      TypeSerializerSchemaCompatibility.compatibleAsIs()
    }
    else {
      TypeSerializerSchemaCompatibility.incompatible()
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
      serializerClazz = cast(Class.forName(className, false, cl))
    catch {
      case e: Throwable =>
        throw new IOException(s"Failed to read SimpleTypeSerializerSnapshot: Serializer class " +
          s"not found: $className, $e")
  }

  /**
    * Cast to required class.
    *
    * @param clazz class to cast.
    * @return class of required type.
    */
  private def cast[T](clazz: Class[_]) : Class[ModelWithTypeSerializer[RECORD, RESULT]]   = {
    if (!classOf[ModelWithTypeSerializer[RECORD, RESULT]].isAssignableFrom(clazz)) {
      throw new IOException("Failed to read SimpleTypeSerializerSnapshot. " +
        s"Serializer class name leads to a class that is not a TypeSerializer: ${clazz.getName}")
    }
    clazz.asInstanceOf[Class[ModelWithTypeSerializer[RECORD, RESULT]]]
  }
}
