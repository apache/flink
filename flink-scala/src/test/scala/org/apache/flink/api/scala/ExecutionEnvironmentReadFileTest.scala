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
package org.apache.flink.api.scala

import org.junit.Test
import org.apache.flink.api.common.io.DelimitedInputFormat
import org.junit.Assert._

class ExecutionEnvironmentReadFileTest {

  @Test
  def testReadFileWithMultipleFiles(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val format = new DelimitedInputFormat[String]() {
      def readRecord(reuse: String, bytes: Array[Byte], offset: Int, numBytes: Int) : String =  {
        new String(bytes, offset, numBytes)
      }
    }

    val tempFile = "/my/imaginary_file"
    val tempFile2 = "/my/imaginary_file2"

    val data = env.readFile(format, tempFile, tempFile2)
    val filePaths = format.getFilePaths
    
    assertNotNull("Should not be null", data)
    assertEquals("The number of file paths should be correct", 2, filePaths.length);
    assertEquals("File paths should be correct", tempFile, filePaths(0).toUri().toString());
    assertEquals("File paths should be correct", tempFile2, filePaths(1).toUri().toString());
  }
}
