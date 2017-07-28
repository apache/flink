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

package org.apache.flink.test.checkpointing;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;

/**
 * Integration tests for incremental RocksDB backend.
 */
public class IncrementalRocksDbBackendEventTimeWindowCheckpointingITCase extends AbstractEventTimeWindowCheckpointingITCase {

	@Rule
	public TestName name = new TestName();

	public IncrementalRocksDbBackendEventTimeWindowCheckpointingITCase() {
		super(StateBackendEnum.ROCKSDB_INCREMENTAL);
	}

	/**
	 * Prints a message when starting a test method to avoid Travis' <tt>"Maven produced no output
	 * for xxx seconds."</tt> messages.
	 */
	@Before
	public void printMethodStart() {
		System.out.println(
			"Starting " + getClass().getCanonicalName() + "#" + name.getMethodName() + ".");
	}

	/**
	 * Prints a message when finishing a test method to avoid Travis' <tt>"Maven produced no output
	 * for xxx seconds."</tt> messages.
	 */
	@After
	public void printProgress() {
		System.out.println(
			"Finished " + getClass().getCanonicalName() + "#" + name.getMethodName() + ".");
	}

	@Override
	protected int numElementsPerKey() {
		return 3000;
	}

	@Override
	protected int windowSize() {
		return 1000;
	}

	@Override
	protected int windowSlide() {
		return 100;
	}

	@Override
	protected int numKeys() {
		return 100;
	}
}
