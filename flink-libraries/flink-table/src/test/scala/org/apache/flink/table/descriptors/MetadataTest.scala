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

class MetadataTest extends DescriptorTestBase {

  @Test
  def testMetadata(): Unit = {
    val desc = Metadata()
      .comment("Some additional comment")
      .creationTime(123L)
      .lastAccessTime(12020202L)
    val expected = Seq(
      "metadata.comment" -> "Some additional comment",
      "metadata.creation-time" -> "123",
      "metadata.last-access-time" -> "12020202"
    )
    verifyProperties(desc, expected)
  }

  @Test(expected = classOf[ValidationException])
  def testInvalidCreationTime(): Unit = {
    verifyInvalidProperty("metadata.creation-time", "dfghj")
  }

  override def descriptor(): Descriptor = {
    Metadata()
      .comment("Some additional comment")
      .creationTime(123L)
      .lastAccessTime(12020202L)
  }

  override def validator(): DescriptorValidator = {
    new MetadataValidator()
  }
}
