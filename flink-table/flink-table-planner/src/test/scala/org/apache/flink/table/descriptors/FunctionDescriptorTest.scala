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

import java.util.{Arrays => JArrays, List => JList, Map => JMap}

import scala.collection.JavaConverters._

/**
  * Tests for [[FunctionDescriptor]].
  */
class FunctionDescriptorTest extends DescriptorTestBase {

  override def descriptors(): JList[Descriptor] = {
    val desc1 = new FunctionDescriptor()
      .fromClass(
        new ClassInstance()
          .of("my.class")
          .parameter("INT", "1")
          .parameter(
            new ClassInstance()
              .of("my.class2")
              .parameterString("true")))

    JArrays.asList(desc1)
  }

  override def validator(): DescriptorValidator = {
    new FunctionDescriptorValidator()
  }

  override def properties(): JList[JMap[String, String]] = {
    val props1 = Map(
      "from" -> "class",
      "class" -> "my.class",
      "constructor.0.type" -> "INT",
      "constructor.0.value" -> "1",
      "constructor.1.class" -> "my.class2",
      "constructor.1.constructor.0" -> "true"
    )
    JArrays.asList(props1.asJava)
  }
}
