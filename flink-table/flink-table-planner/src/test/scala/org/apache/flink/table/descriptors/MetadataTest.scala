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

class MetadataTest extends DescriptorTestBase {

  @Test(expected = classOf[ValidationException])
  def testInvalidCreationTime(): Unit = {
    addPropertyAndVerify(descriptors().get(0), "metadata.creation-time", "dfghj")
  }

  // ----------------------------------------------------------------------------------------------

  override def descriptors(): util.List[Descriptor] = {
    val desc = Metadata()
      .comment("Some additional comment")
      .creationTime(123L)
      .lastAccessTime(12020202L)

    util.Arrays.asList(desc)
  }

  override def validator(): DescriptorValidator = {
    new MetadataValidator()
  }

  override def properties(): util.List[util.Map[String, String]] = {
    val props = Map(
      "metadata.comment" -> "Some additional comment",
      "metadata.creation-time" -> "123",
      "metadata.last-access-time" -> "12020202"
    )

    util.Arrays.asList(props.asJava)
  }
}
