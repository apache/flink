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

import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.migration.runtime.state.KvStateSnapshot;

import java.io.IOException;

/**
 * A snapshot of a heap key/value state stored in a file.
 * 
 * @param <K> The type of the key in the snapshot state.
 * @param <N> The type of the namespace in the snapshot state.
 * @param <SV> The type of the state value.
 */
@Deprecated
public abstract class AbstractFsStateSnapshot<K, N, SV, S extends State, SD extends StateDescriptor<S, ?>> 
		extends AbstractFileStateHandle implements KvStateSnapshot<K, N, S, SD> {

	private static final long serialVersionUID = 1L;

	/** Key Serializer */
	protected final TypeSerializer<K> keySerializer;

	/** Namespace Serializer */
	protected final TypeSerializer<N> namespaceSerializer;

	/** Serializer for the state value */
	protected final TypeSerializer<SV> stateSerializer;

	/** StateDescriptor, for sanity checks */
	protected final SD stateDesc;

	public AbstractFsStateSnapshot(TypeSerializer<K> keySerializer,
		TypeSerializer<N> namespaceSerializer,
		TypeSerializer<SV> stateSerializer,
		SD stateDesc,
		Path filePath) {
		super(filePath);
		this.stateDesc = stateDesc;
		this.keySerializer = keySerializer;
		this.stateSerializer = stateSerializer;
		this.namespaceSerializer = namespaceSerializer;

	}

	@Override
	public long getStateSize() throws IOException {
		return getFileSize();
	}

	public TypeSerializer<K> getKeySerializer() {
		return keySerializer;
	}

	public TypeSerializer<N> getNamespaceSerializer() {
		return namespaceSerializer;
	}

	public TypeSerializer<SV> getStateSerializer() {
		return stateSerializer;
	}

	public SD getStateDesc() {
		return stateDesc;
	}
}
