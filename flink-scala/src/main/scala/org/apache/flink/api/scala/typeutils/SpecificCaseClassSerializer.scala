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

package org.apache.flink.api.scala.typeutils

import org.apache.flink.api.common.typeutils.CompositeTypeSerializerUtil.delegateCompatibilityCheckToNewSnapshot
import org.apache.flink.api.common.typeutils.TypeSerializerConfigSnapshot.SelfResolvingTypeSerializer
import org.apache.flink.api.common.typeutils._
import org.apache.flink.api.java.typeutils.runtime.TupleSerializerConfigSnapshot
import org.apache.flink.api.scala.typeutils.SpecificCaseClassSerializer.lookupConstructor

import java.io.ObjectInputStream
import java.lang.invoke.{MethodHandle, MethodHandles}

import scala.collection.JavaConverters._
import scala.reflect.runtime.universe

/**
  * This is a non macro-generated, concrete Scala case class serializer.
  *
  * <p>We need this serializer to replace the previously macro generated,
  * anonymous [[CaseClassSerializer]].
  */
@SerialVersionUID(1L)
class SpecificCaseClassSerializer[T <: Product](
  clazz: Class[T],
  scalaFieldSerializers: Array[TypeSerializer[_]]
) extends CaseClassSerializer[T](clazz, scalaFieldSerializers)
    with SelfResolvingTypeSerializer[T] {

  @transient
  private var constructor = lookupConstructor(clazz)

  override def createInstance(fields: Array[AnyRef]): T = {
    constructor.invoke(fields).asInstanceOf[T]
  }

  override def snapshotConfiguration(): TypeSerializerSnapshot[T] = {
    new SpecificCaseClassSerializerSnapshot[T](this)
  }

  override def resolveSchemaCompatibilityViaRedirectingToNewSnapshotClass(
    s: TypeSerializerConfigSnapshot[T]
  ): TypeSerializerSchemaCompatibility[T] = {

    require(s.isInstanceOf[TupleSerializerConfigSnapshot[_]])

    val configSnapshot = s.asInstanceOf[TupleSerializerConfigSnapshot[T]]
    val nestedSnapshots = configSnapshot.getNestedSerializersAndConfigs.asScala
      .map(t => t.f1)
      .toArray

    val newCompositeSnapshot =
      new SpecificCaseClassSerializerSnapshot[T](configSnapshot.getTupleClass)

    delegateCompatibilityCheckToNewSnapshot(
      this,
      newCompositeSnapshot,
      nestedSnapshots: _*
    )
  }

  private def readObject(in: ObjectInputStream): Unit = {
    // this should be removed once we make sure that serializer are no long java serialized.
    in.defaultReadObject()
    constructor = lookupConstructor(clazz)
  }

}

object SpecificCaseClassSerializer {

  def lookupConstructor[T](clazz: Class[_]): MethodHandle = {
    val types = findPrimaryConstructorParameterTypes(clazz, clazz.getClassLoader)

    val constructor = clazz.getConstructor(types: _*)

    val handle = MethodHandles
      .lookup()
      .unreflectConstructor(constructor)
      .asSpreader(classOf[Array[AnyRef]], types.length)

    handle
  }

  private def findPrimaryConstructorParameterTypes(cls: Class[_], cl: ClassLoader):
  List[Class[_]] = {
    val rootMirror = universe.runtimeMirror(cl)
    val classSymbol = rootMirror.classSymbol(cls)

    require(
      classSymbol.isStatic,
      s"""
         |The class ${cls.getSimpleName} is an instance class, meaning it is not a member of a
         |toplevel object, or of an object contained in a toplevel object,
         |therefore it requires an outer instance to be instantiated, but we don't have a
         |reference to the outer instance. Please consider changing the outer class to an object.
         |""".stripMargin
    )

    val primaryConstructorSymbol = findPrimaryConstructorMethodSymbol(classSymbol)
    val scalaTypes = getArgumentsTypes(primaryConstructorSymbol)
    scalaTypes.map(tpe => scalaTypeToJavaClass(rootMirror)(tpe))
  }

  private def findPrimaryConstructorMethodSymbol(classSymbol: universe.ClassSymbol):
  universe.MethodSymbol = {
    classSymbol.toType
      .decl(universe.termNames.CONSTRUCTOR)
      .alternatives
      .head
      .asMethod
  }

  private def getArgumentsTypes(primaryConstructorSymbol: universe.MethodSymbol):
  List[universe.Type] = {
    primaryConstructorSymbol.typeSignature
      .paramLists
      .head
      .map(symbol => symbol.typeSignature)
  }

  private def scalaTypeToJavaClass(mirror: universe.Mirror)(scalaType: universe.Type): Class[_] = {
    val erasure = scalaType.erasure
    mirror.runtimeClass(erasure)
  }
}
