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

import org.apache.flink.table.descriptors.HierarchyDescriptorValidator.EMPTY_PREFIX

import scala.collection.JavaConversions._

/**
  * Validator for [[ClassInstance]].
  */
class ClassInstanceValidator(keyPrefix: String = EMPTY_PREFIX)
  extends HierarchyDescriptorValidator(keyPrefix) {

  override def validateWithPrefix(keyPrefix: String, properties: DescriptorProperties): Unit = {
    // check class name
    properties.validateString(s"$keyPrefix${ClassInstanceValidator.CLASS}", false, 1)

    // check constructor
    val constructorPrefix = s"$keyPrefix${ClassInstanceValidator.CONSTRUCTOR}"

    val constructorProperties = properties.getVariableIndexedProperties(constructorPrefix, List())
    var i = 0
    while (i < constructorProperties.size()) {
      // nested class instance
      if (constructorProperties(i).containsKey(ClassInstanceValidator.CLASS)) {
        val classInstanceValidator = new ClassInstanceValidator(s"$constructorPrefix.$i.")
        classInstanceValidator.validate(properties)
      }
      // literal value
      else {
        val primitiveValueValidator = new LiteralValueValidator(s"$constructorPrefix.$i.")
        primitiveValueValidator.validate(properties)
      }
      i += 1
    }
  }
}

object ClassInstanceValidator {
  val CLASS = "class"
  val CONSTRUCTOR = "constructor"
}
