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

package org.apache.flink.table.descriptors

import org.apache.flink.table.api.{TableException, ValidationException}

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer


/**
  * Validator for [[ClassTypeDescriptor]].
  */
class ClassTypeValidator extends HierarchyDescriptorValidator {
  override def validateWithPrefix(keyPrefix: String, properties: DescriptorProperties): Unit = {

    properties.validateString(s"$keyPrefix${ClassTypeValidator.CLASS}", isOptional = false)

    val constructorPrefix = s"$keyPrefix${ClassTypeValidator.CLASS_CONSTRUCTOR}"
    properties.validateString(constructorPrefix, isOptional = true)

    val constructorProps =
      properties.getVariableIndexedProperties(constructorPrefix, List())
    var i = 0
    val primitiveTypeValidator = new PrimitiveTypeValidator
    while (i < constructorProps.size()) {
      if (constructorProps(i).containsKey(PrimitiveTypeValidator.PRIMITIVE_TYPE)) {
        primitiveTypeValidator.validateWithPrefix(s"$constructorPrefix.$i.", properties)
      } else if (constructorProps(i).containsKey(ClassTypeValidator.CLASS)) {
        validateWithPrefix(s"$constructorPrefix.$i.", properties)
      } else {
        throw ValidationException("A constructor field must contain a 'class' or a 'type' key.")
      }
      i += 1
    }
  }
}

object ClassTypeValidator {

  val CLASS = "class"
  val CLASS_CONSTRUCTOR = "constructor"

  def generateInstance[T](
      prefix: String,
      properties: DescriptorProperties,
      classLoader: ClassLoader): T = {
    generateInstanceInternally(prefix, properties, classLoader)
  }

  private def generateInstanceInternally[T](
      keyPrefix: String,
      descriptorProps: DescriptorProperties,
      classLoader: ClassLoader): T = {
    val constructorPrefix = s"$keyPrefix$CLASS_CONSTRUCTOR"
    val constructorProps =
      descriptorProps.getVariableIndexedProperties(constructorPrefix, List())
    var i = 0
    val typeValueList: ArrayBuffer[(Class[_], Any)] = new ArrayBuffer
    while (i < constructorProps.size()) {
      if (constructorProps(i).containsKey(PrimitiveTypeValidator.PRIMITIVE_TYPE)) {
        val primitiveVal = PrimitiveTypeValidator
          .derivePrimitiveValue(s"$constructorPrefix.$i.", descriptorProps)
        typeValueList += ((primitiveVal.getClass, primitiveVal))
      } else if (constructorProps(i).containsKey(CLASS)) {
        val typeValuePair = (
          Class.forName(
            descriptorProps.getString(constructorProps(i).get(CLASS))),
          generateInstanceInternally(s"$constructorPrefix.$i.", descriptorProps, classLoader))
        typeValueList += typeValuePair
      }
      i += 1
    }
    val clazz = classLoader.loadClass(descriptorProps.getString(s"$keyPrefix$CLASS"))
    val constructor = clazz.getConstructor(typeValueList.map(_._1): _*)
      if (null == constructor) {
        throw TableException(s"Cannot find a constructor with parameter types " +
          s"${typeValueList.map(_._1)} for ${clazz.getName}")
      }
    constructor.newInstance(typeValueList.map(_._2.asInstanceOf[AnyRef]): _*).asInstanceOf[T]
  }
}




