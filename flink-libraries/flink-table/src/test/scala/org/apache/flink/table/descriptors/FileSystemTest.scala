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
import org.junit.Test

class FileSystemTest extends DescriptorTestBase {

  @Test
  def testFileSystem(): Unit = {
    val desc = FileSystem().path("/myfile")
    val expected = Seq(
      "connector.type" -> "filesystem",
      "connector.version" -> "1",
      "connector.path" -> "/myfile")
    verifyProperties(desc, expected)
  }

  @Test(expected = classOf[ValidationException])
  def testInvalidPath(): Unit = {
    verifyInvalidProperty("connector.path", "")
  }

  @Test(expected = classOf[ValidationException])
  def testMissingPath(): Unit = {
    verifyMissingProperty("connector.path")
  }

  override def descriptor(): Descriptor = {
    FileSystem().path("/myfile")
  }

  override def validator(): DescriptorValidator = {
    new FileSystemValidator()
  }
}
