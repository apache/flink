/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state.heap;

import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

/**
 * Interface for the snapshots of a {@link StateTable}. Offers a way to serialize the snapshot (by key-group). All
 * snapshots should be released after usage.
 */
interface StateTableSnapshot {

	/**
	 * Writes the data for the specified key-group to the output.
	 *
	 * @param dov the output
	 * @param keyGroupId the key-group to write
	 * @throws IOException on write related problems
	 */
	void writeMappingsInKeyGroup(DataOutputView dov, int keyGroupId) throws IOException;

	/**
	 * Release the snapshot. All snapshots should be released when they are no longer used because some implementation
	 * can only release resources after a release.
	 */
	void release();
}