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

import org.apache.flink.util.Preconditions
import org.junit.Assert.assertEquals
import org.junit.Test

import scala.collection.JavaConverters._

abstract class DescriptorTestBase {

  /**
    * Returns a set of valid descriptors.
    * This method is implemented in both Scala and Java.
    */
  def descriptors(): java.util.List[Descriptor]

  /**
    * Returns a set of properties for each valid descriptor.
    * This code is implemented in both Scala and Java.
    */
  def properties(): java.util.List[java.util.Map[String, String]]

  /**
    * Returns a validator that can validate all valid descriptors.
    */
  def validator(): DescriptorValidator

  @Test
  def testValidation(): Unit = {
    val d = descriptors().asScala
    val p = properties().asScala

    Preconditions.checkArgument(d.length == p.length)

    d.zip(p).foreach { case (desc, props) =>
      verifyProperties(desc, props.asScala.toMap)
    }
  }

  def verifyProperties(descriptor: Descriptor, expected: Map[String, String]): Unit = {
    val normProps = new DescriptorProperties
    descriptor.addProperties(normProps)

    // test produced properties
    assertEquals(expected, normProps.asMap.asScala.toMap)

    // test validation logic
    validator().validate(normProps)
  }

  def addPropertyAndVerify(
      descriptor: Descriptor,
      property: String,
      invalidValue: String): Unit = {
    val properties = new DescriptorProperties
    descriptor.addProperties(properties)
    properties.unsafePut(property, invalidValue)
    validator().validate(properties)
  }

  def removePropertyAndVerify(descriptor: Descriptor, removeProperty: String): Unit = {
    val properties = new DescriptorProperties
    descriptor.addProperties(properties)
    properties.unsafeRemove(removeProperty)
    validator().validate(properties)
  }
}

class TestTableSourceDescriptor(connector: ConnectorDescriptor)
  extends TableSourceDescriptor {

  this.connectorDescriptor = Some(connector)

  def addFormat(format: FormatDescriptor): TestTableSourceDescriptor = {
    this.formatDescriptor = Some(format)
    this
  }

  def addSchema(schema: Schema): TestTableSourceDescriptor = {
    this.schemaDescriptor = Some(schema)
    this
  }
}
