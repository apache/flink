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

import static org.apache.flink.runtime.state.LocalRecoveryConfig.LocalRecoveryMode.ENABLE_FILE_BASED;
import static org.apache.flink.test.checkpointing.AbstractEventTimeWindowCheckpointingITCase.StateBackendEnum.ROCKSDB_FULLY_ASYNC;

/**
 * Tests file-based local recovery with the RocksDB state-backend.
 */
public class LocalRecoveryRocksDBFullITCase extends AbstractLocalRecoveryITCase {
	public LocalRecoveryRocksDBFullITCase() {
		super(
			ROCKSDB_FULLY_ASYNC,
			ENABLE_FILE_BASED);
	}
}
