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
import org.apache.flink.table.descriptors.FunctionDescriptorValidator.FROM
import org.apache.flink.table.util.JavaScalaConversionUtil.toJava

import scala.collection.JavaConverters._

/**
  * Validator for [[FunctionDescriptor]].
  */
class FunctionDescriptorValidator extends DescriptorValidator {

  override def validate(properties: DescriptorProperties): Unit = {

    val classValidation = (_: String) => {
      new ClassInstanceValidator().validate(properties)
    }

    // check for 'from'
    if (properties.containsKey(FROM)) {
      properties.validateEnum(
        FROM,
        false,
        Map(
          FunctionDescriptorValidator.FROM_VALUE_CLASS -> toJava(classValidation)
        ).asJava
      )
    } else {
      throw new ValidationException("Could not find 'from' property for function.")
    }
  }
}

object FunctionDescriptorValidator {

  val FROM = "from"
  val FROM_VALUE_CLASS = "class"
}
