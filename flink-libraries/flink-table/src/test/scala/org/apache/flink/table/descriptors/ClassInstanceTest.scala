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

import org.apache.flink.table.api.{Types, ValidationException}
import org.junit.Test

import scala.collection.JavaConverters._

class ClassInstanceTest extends DescriptorTestBase {

  @Test(expected = classOf[ValidationException])
  def testMissingClass(): Unit = {
    removePropertyAndVerify(descriptors().get(0), ClassInstanceValidator.CLASS)
  }

  override def descriptors(): JList[Descriptor] = {
    val desc1 = ClassInstance()
      .of("class1")
      .parameter(Types.LONG, "1")
      .parameter(
        ClassInstance()
          .of("class2")
          .parameter(
            ClassInstance()
              .of("class3")
              .parameterString("StarryNight")
              .parameter(
                ClassInstance()
                    .of("class4"))))
      .parameter(2L)

    val desc2 = ClassInstance()
      .of("class2")

    val desc3 = ClassInstance()
      .of("org.example.Function")
      .parameter(42)
      .parameter(2.asInstanceOf[Byte])
      .parameter(new java.math.BigDecimal("23.22"))
      .parameter(222.2222)
      .parameter(222.2222f)

    JArrays.asList(desc1, desc2, desc3)
  }

  override def validator(): DescriptorValidator = {
    new ClassInstanceValidator()
  }

  override def properties(): JList[JMap[String, String]] = {
    val props1 = Map(
      "class" -> "class1",
      "constructor.0.type" -> "BIGINT",
      "constructor.0.value" -> "1",
      "constructor.1.class" -> "class2",
      "constructor.1.constructor.0.class" -> "class3",
      "constructor.1.constructor.0.constructor.0" -> "StarryNight",
      "constructor.1.constructor.0.constructor.1.class" -> "class4",
      "constructor.2.type" -> "BIGINT",
      "constructor.2.value" -> "2"
    )

    val props2 = Map(
      "class" -> "class2"
    )

    val props3 = Map(
      "class" -> "org.example.Function",
      "constructor.0.type" -> "INT",
      "constructor.0.value" -> "42",
      "constructor.1.type" -> "TINYINT",
      "constructor.1.value" -> "2",
      "constructor.2.type" -> "DECIMAL",
      "constructor.2.value" -> "23.22",
      "constructor.3.type" -> "DOUBLE",
      "constructor.3.value" -> "222.2222",
      "constructor.4.type" -> "FLOAT",
      "constructor.4.value" -> "222.2222"
    )

    JArrays.asList(props1.asJava, props2.asJava, props3.asJava)
  }
}
