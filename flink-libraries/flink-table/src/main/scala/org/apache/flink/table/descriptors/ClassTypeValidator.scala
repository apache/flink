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

import org.apache.flink.table.api.ValidationException

import scala.collection.JavaConversions._

/**
  * Validator for [[ClassType]].
  */
class ClassTypeValidator extends HierarchyDescriptorValidator {
  override def validateWithPrefix(keyPrefix: String, properties: DescriptorProperties): Unit = {

    properties.validateString(s"$keyPrefix${ClassTypeValidator.CLASS}", isOptional = false)

    val constructorPrefix = s"$keyPrefix${ClassTypeValidator.CONSTRUCTOR}"
    normalizeConstructorParams(constructorPrefix, properties)

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

  /**
    * For each constructor parameter (e.g., constructor.0 = abc), we derive its type and replace it
    * with the normalized form (e.g., constructor.0.type = VARCHAR, constructor.0.value = abc);
    *
    * @param constructorPrefix the prefix to get the constructor parameters
    * @param properties the descriptor properties
    */
  def normalizeConstructorParams(
    constructorPrefix: String,
    properties: DescriptorProperties): Unit = {
    val constructorValues = properties.getListProperties(constructorPrefix)
    constructorValues.foreach(kv => {
      properties.unsafeRemove(kv._1)
      val tp = PrimitiveTypeValidator.deriveTypeStrFromValueStr(kv._2)
      properties.putString(s"${kv._1}.${PrimitiveTypeValidator.PRIMITIVE_TYPE}", tp)
      properties.putString(s"${kv._1}.${PrimitiveTypeValidator.PRIMITIVE_VALUE}", kv._2)
    })
  }
}

object ClassTypeValidator {
  val CLASS = "class"
  val CONSTRUCTOR = "constructor"
}
