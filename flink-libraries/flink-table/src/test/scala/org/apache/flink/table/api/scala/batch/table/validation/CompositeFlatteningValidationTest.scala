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
package org.apache.flink.table.api.scala.batch.table.validation

import org.apache.flink.table.api.ValidationException
import org.apache.flink.table.utils.TableTestBase
import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.junit.Test

class CompositeFlatteningValidationTest extends TableTestBase {

  @Test(expected = classOf[ValidationException])
  def testDuplicateFlattening(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[((Int, Long), (String, Boolean), String)]("MyTable", 'a, 'b, 'c)

    table.select('a.flatten(), 'a.flatten())
  }

}
