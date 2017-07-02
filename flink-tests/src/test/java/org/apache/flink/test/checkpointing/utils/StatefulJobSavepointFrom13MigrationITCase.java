/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.test.checkpointing.utils;

import org.junit.Ignore;
import org.junit.Test;

/**
 * This verifies that we can restore a complete job from a Flink 1.3 savepoint.
 *
 * <p>The test pipeline contains both "Checkpointed" state and keyed user state.
 */
public class StatefulJobSavepointFrom13MigrationITCase extends StatefulJobSavepointFrom12MigrationITCase {

	/**
	 * This has to be manually executed to create the savepoint on Flink 1.3.
	 */
	@Test
	@Ignore
	public void testCreateSavepointOnFlink13() throws Exception {
		testCreateSavepointOnFlink12();
	}

	/**
	 * This has to be manually executed to create the savepoint on Flink 1.3.
	 */
	@Test
	@Ignore
	public void testCreateSavepointOnFlink13WithRocksDB() throws Exception {
		testCreateSavepointOnFlink12WithRocksDB();
	}

	@Override
	protected String getSavepointPath() {
		return "stateful-udf-migration-itcase-flink1.3-savepoint";
	}

	@Override
	protected String getRocksDBSavepointPath() {
		return "stateful-udf-migration-itcase-flink1.3-rocksdb-savepoint";
	}
}
