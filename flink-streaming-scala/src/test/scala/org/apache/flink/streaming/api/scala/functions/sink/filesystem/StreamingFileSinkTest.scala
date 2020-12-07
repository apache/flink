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

package org.apache.flink.streaming.api.scala.functions.sink.filesystem

import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.BulkWriterTest.TestBulkWriterFactory
import org.apache.flink.streaming.api.functions.sink.filesystem.{OutputFileConfig, StreamingFileSink}
import org.junit.Test

/**
 * Tests for the [[org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink]]
 */
class StreamingFileSinkTest {

  /**
   * Tests that the StreamingFileSink builder works with the Scala APIs.
   */
  @Test
  def testStreamingFileSinkRowFormatBuilderCompiles(): Unit = {
    StreamingFileSink.forRowFormat(new Path("foobar"), new SimpleStringEncoder[String]())
      .withBucketCheckInterval(10L)
      .withOutputFileConfig(OutputFileConfig.builder().build())
      .build()
  }

  /**
   * Tests that the StreamingFileSink builder works with the Scala APIs.
   */
  @Test
  def testStreamingFileSinkBulkFormatBuilderCompiles(): Unit = {
    StreamingFileSink.forBulkFormat(new Path("foobar"), new TestBulkWriterFactory())
      .withBucketCheckInterval(10L)
      .withOutputFileConfig(OutputFileConfig.builder().build())
      .build()
  }
}
