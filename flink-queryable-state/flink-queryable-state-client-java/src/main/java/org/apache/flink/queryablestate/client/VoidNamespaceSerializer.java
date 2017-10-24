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

package org.apache.flink.queryablestate.client;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

/**
 * Serializer for {@link VoidNamespace}.
 *
 * <p><b>THIS WAS COPIED FROM RUNTIME SO THAT WE AVOID THE DEPENDENCY.</b>
 */
@Internal
public final class VoidNamespaceSerializer extends TypeSerializerSingleton<VoidNamespace> {

	private static final long serialVersionUID = 1L;

	public static final VoidNamespaceSerializer INSTANCE = new VoidNamespaceSerializer();

	@Override
	public boolean isImmutableType() {
		return true;
	}

	@Override
	public VoidNamespace createInstance() {
		return VoidNamespace.get();
	}

	@Override
	public VoidNamespace copy(VoidNamespace from) {
		return VoidNamespace.get();
	}

	@Override
	public VoidNamespace copy(VoidNamespace from, VoidNamespace reuse) {
		return VoidNamespace.get();
	}

	@Override
	public int getLength() {
		return 0;
	}

	@Override
	public void serialize(VoidNamespace record, DataOutputView target) throws IOException {
		// Make progress in the stream, write one byte.
		//
		// We could just skip writing anything here, because of the way this is
		// used with the state backends, but if it is ever used somewhere else
		// (even though it is unlikely to happen), it would be a problem.
		target.write(0);
	}

	@Override
	public VoidNamespace deserialize(DataInputView source) throws IOException {
		source.readByte();
		return VoidNamespace.get();
	}

	@Override
	public VoidNamespace deserialize(VoidNamespace reuse, DataInputView source) throws IOException {
		source.readByte();
		return VoidNamespace.get();
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		target.write(source.readByte());
	}

	@Override
	public boolean canEqual(Object obj) {
		return obj instanceof VoidNamespaceSerializer;
	}
}
