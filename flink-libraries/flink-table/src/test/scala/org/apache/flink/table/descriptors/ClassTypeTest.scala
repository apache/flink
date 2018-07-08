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

import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.table.api.ValidationException
import org.junit.Test

import scala.collection.JavaConverters._

class ClassTypeTest extends DescriptorTestBase {

  @Test(expected = classOf[ValidationException])
  def testMissingClass(): Unit = {
    removePropertyAndVerify(descriptors().get(0), ClassTypeValidator.CLASS)
  }

  override def descriptors(): JList[Descriptor] = {
    val desc1 = ClassType("class1")
      .param(BasicTypeInfo.LONG_TYPE_INFO, "1")
      .param(
        ClassType("class2")
          .param(
            ClassType("class3")
              .param("StarryNight")
              .param(
                ClassType("class4"))))
      .param(2L)

    val desc2 = ClassType().of("class2")

    JArrays.asList(desc1, desc2)
  }

  override def validator(): DescriptorValidator = {
    new ClassTypeValidator()
  }

  override def properties(): JList[JMap[String, String]] = {
    val props1 = Map(
      "class" -> "class1",
      "constructor.0.type" -> "BIGINT",
      "constructor.0.value" -> "1",
      "constructor.1.class" -> "class2",
      "constructor.1.constructor.0.class" -> "class3",
      "constructor.1.constructor.0.constructor.0.type" -> "VARCHAR",
      "constructor.1.constructor.0.constructor.0.value" -> "StarryNight",
      "constructor.1.constructor.0.constructor.1.class" -> "class4",
      "constructor.2.type" -> "BIGINT",
      "constructor.2.value" -> "2"
    )

    val props2 = Map(
      "class" -> "class2"
    )

    JArrays.asList(props1.asJava, props2.asJava)
  }
}
