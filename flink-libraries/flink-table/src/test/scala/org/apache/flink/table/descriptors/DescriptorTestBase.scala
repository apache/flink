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

import org.junit.Assert.assertEquals

abstract class DescriptorTestBase {

  /**
    * Returns a valid descriptor.
    */
  def descriptor(): Descriptor

  /**
    * Returns a validator that can validate this descriptor.
    */
  def validator(): DescriptorValidator

  def verifyProperties(descriptor: Descriptor, expected: Seq[(String, String)]): Unit = {
    val normProps = new DescriptorProperties
    descriptor.addProperties(normProps)
    assertEquals(expected.toMap, normProps.asMap)
  }

  def verifyInvalidProperty(property: String, invalidValue: String): Unit = {
    val properties = new DescriptorProperties
    descriptor().addProperties(properties)
    properties.unsafePut(property, invalidValue)
    validator().validate(properties)
  }

  def verifyMissingProperty(removeProperty: String): Unit = {
    val properties = new DescriptorProperties
    descriptor().addProperties(properties)
    properties.unsafeRemove(removeProperty)
    validator().validate(properties)
  }
}
