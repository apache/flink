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

import java.util.{List => JList, Map => JMap, Arrays => JArrays}

import org.apache.flink.table.api.ValidationException
import org.junit.Test

import scala.collection.JavaConverters._

class FunctionTest extends DescriptorTestBase {

  @Test(expected = classOf[ValidationException])
  def testMissingName(): Unit = {
    removePropertyAndVerify(descriptors().get(0), "name")
  }

  override def descriptors(): JList[Descriptor] = {
    val desc1 = FunctionDescriptor("func1")
      .using(
        ClassType("another.class")
          .of("my.class")
          .param("INT", "1")
          .param(
            ClassType()
              .of("my.class2")
              .strParam("true")))

    JArrays.asList(desc1)
  }

  override def validator(): DescriptorValidator = {
    new FunctionValidator()
  }

  override def properties(): JList[JMap[String, String]] = {
    val props1 = Map(
      "name" -> "func1",
      "class" -> "my.class",
      "constructor.0.type" -> "INT",
      "constructor.0.value" -> "1",
      "constructor.1.class" -> "my.class2",
      "constructor.1.constructor.0.type" -> "BOOLEAN",
      "constructor.1.constructor.0.value" -> "true"
    )
    JArrays.asList(props1.asJava)
  }
}
