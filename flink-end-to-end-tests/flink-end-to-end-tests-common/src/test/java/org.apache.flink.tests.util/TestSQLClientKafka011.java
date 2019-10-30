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

import java.nio.file.Path;

public class TestSQLClientKafka011 extends TestSQLClientKafka {
	private static final Path TEST_DATA_PATH = End2EndUtil.getTestDataDir();

	public TestSQLClientKafka011() {
		super(TEST_DATA_PATH,
			"http://archive.apache.org/dist/kafka/0.11.0.2/kafka_2.11-0.11.0.2.tgz",
			"kafka_2.11-0.11.0.2.tgz",
			"0.11",
			"kafka-0.11");
	}
}
