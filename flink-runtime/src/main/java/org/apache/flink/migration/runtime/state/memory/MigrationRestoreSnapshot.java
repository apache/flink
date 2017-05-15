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

package org.apache.flink.migration.runtime.state.memory;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.state.heap.HeapKeyedStateBackend;
import org.apache.flink.runtime.state.heap.StateTable;
import org.apache.flink.util.Migration;

import java.io.IOException;

/**
 * @deprecated Internal class for savepoint backwards compatibility. Don't use for other purposes.
 */
@Deprecated
@Internal
public interface MigrationRestoreSnapshot<K, N, S> extends Migration {
	StateTable<K, N, S> deserialize(String stateName, HeapKeyedStateBackend<K> stateBackend) throws IOException;
}
