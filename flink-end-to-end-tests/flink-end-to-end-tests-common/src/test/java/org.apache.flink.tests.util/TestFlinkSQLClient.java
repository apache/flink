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

package org.apache.flink.tests.util;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static org.apache.flink.tests.util.FlinkSQLClient.findSQLJarPaths;

public class TestFlinkSQLClient {

	@Test
	public void testFindSQLJarPaths() throws IOException {
		List<Path> paths = findSQLJarPaths("/Users/openinx/software/flink/flink-end-to-end-tests/flink-sql-client-test/target/sql-jars", "avro");
		Assert.assertEquals(paths.size(), 1);
		System.out.println(paths);
	}
}
