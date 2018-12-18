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
import java.util

/**
  * Descriptor for describing a function.
  */
class FunctionDescriptor extends Descriptor {

  private var from: Option[String] = None
  private var classInstance: Option[ClassInstance] = None

  /**
    * Creates a function from a class description.
    */
  def fromClass(classType: ClassInstance): FunctionDescriptor = {
    from = Some(FunctionDescriptorValidator.FROM_VALUE_CLASS)
    this.classInstance = Option(classType)
    this
  }

  /**
    * Converts this descriptor into a set of properties.
    */
  override def toProperties: util.Map[String, String] = {
    val properties = new DescriptorProperties()
    from.foreach(properties.putString(FunctionDescriptorValidator.FROM, _))
    classInstance.foreach(d => properties.putProperties(d.toProperties))
    properties.asMap()
  }
}

/**
  * Descriptor for describing a function.
  */
object FunctionDescriptor {

  /**
    * Descriptor for describing a function.
    *
    * @deprecated Use `new FunctionDescriptor()`.
    */
  @deprecated
  def apply(): FunctionDescriptor = new FunctionDescriptor()
}
