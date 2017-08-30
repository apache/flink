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

package org.apache.flink.streaming.util.migration;

import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OperatorSnapshotUtil;

/**
 * Utility methods for testing snapshot migrations.
 */
public class MigrationTestUtil {

	/**
	 * Restore from a snapshot taken with an older Flink version.
	 *
	 * @param testHarness          the test harness to restore the snapshot to.
	 * @param snapshotPath         the absolute path to the snapshot.
	 * @param snapshotFlinkVersion the Flink version of the snapshot.
	 * @throws Exception
	 */
	public static void restoreFromSnapshot(
		AbstractStreamOperatorTestHarness<?> testHarness,
		String snapshotPath,
		MigrationVersion snapshotFlinkVersion) throws Exception {

		testHarness.initializeState(OperatorSnapshotUtil.readStateHandle(snapshotPath));
	}
}
