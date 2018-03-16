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

import org.apache.flink.table.api.ValidationException
import org.junit.Test

import scala.collection.JavaConverters._

class FileSystemTest extends DescriptorTestBase {

  @Test(expected = classOf[ValidationException])
  def testInvalidPath(): Unit = {
    addPropertyAndVerify(descriptors().get(0), "connector.path", "")
  }

  @Test(expected = classOf[ValidationException])
  def testMissingPath(): Unit = {
    removePropertyAndVerify(descriptors().get(0), "connector.path")
  }

  // ----------------------------------------------------------------------------------------------

  override def descriptors(): util.List[Descriptor] = {
    util.Arrays.asList(FileSystem().path("/myfile"))
  }

  override def validator(): DescriptorValidator = {
    new FileSystemValidator()
  }

  override def properties(): util.List[util.Map[String, String]] = {
    val desc = Map(
      "connector.type" -> "filesystem",
      "connector.property-version" -> "1",
      "connector.path" -> "/myfile")

    util.Arrays.asList(desc.asJava)
  }
}
