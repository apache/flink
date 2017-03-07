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

package org.apache.flink.migration.state;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.state.KeyGroupRangeOffsets;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.util.Migration;

/**
 * This class is just a KeyGroupsStateHandle that is tagged as migration, to figure out which restore logic to apply,
 * e.g. when restoring backend data from a state handle.
 */
@Internal
@Deprecated
public class MigrationKeyGroupStateHandle extends KeyGroupsStateHandle implements Migration {

	private static final long serialVersionUID = -8554427169776881697L;

	/**
	 * @param groupRangeOffsets range of key-group ids that in the state of this handle
	 * @param streamStateHandle handle to the actual state of the key-groups
	 */
	public MigrationKeyGroupStateHandle(KeyGroupRangeOffsets groupRangeOffsets, StreamStateHandle streamStateHandle) {
		super(groupRangeOffsets, streamStateHandle);
	}
}
