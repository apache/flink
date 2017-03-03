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

import org.apache.flink.core.memory.DataInputView;

import java.io.IOException;

/**
 * Interface for state de-serialization into {@link StateTable}s by key-group.
 */
interface StateTableByKeyGroupReader {

	/**
	 * Read the data for the specified key-group from the input.
	 *
	 * @param div        the input
	 * @param keyGroupId the key-group to write
	 * @throws IOException on write related problems
	 */
	void readMappingsInKeyGroup(DataInputView div, int keyGroupId) throws IOException;
}