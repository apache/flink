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

package org.apache.flink.migration.runtime.state.filesystem;

import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.fs.Path;

@Deprecated
@SuppressWarnings("deprecation")
public class FsReducingState<K, N, V> {

	public static class Snapshot<K, N, V> extends AbstractFsStateSnapshot<K, N, V, ReducingState<V>, ReducingStateDescriptor<V>> {
		private static final long serialVersionUID = 1L;

		public Snapshot(TypeSerializer<K> keySerializer,
			TypeSerializer<N> namespaceSerializer,
			TypeSerializer<V> stateSerializer,
			ReducingStateDescriptor<V> stateDescs,
			Path filePath) {
			super(keySerializer, namespaceSerializer, stateSerializer, stateDescs, filePath);
		}
	}
}
