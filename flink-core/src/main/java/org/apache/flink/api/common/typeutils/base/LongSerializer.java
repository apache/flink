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

package org.apache.flink.api.common.typeutils.base;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

@Internal
public final class LongSerializer extends TypeSerializerSingleton<Long> {

	private static final long serialVersionUID = 1L;
	
	public static final LongSerializer INSTANCE = new LongSerializer();
	
	private static final Long ZERO = 0L;

	@Override
	public boolean isImmutableType() {
		return true;
	}

	@Override
	public Long createInstance() {
		return ZERO;
	}

	@Override
	public Long copy(Long from) {
		return from;
	}
	
	@Override
	public Long copy(Long from, Long reuse) {
		return from;
	}

	@Override
	public int getLength() {
		return Long.BYTES;
	}

	@Override
	public void serialize(Long record, DataOutputView target) throws IOException {
		target.writeLong(record);
	}

	@Override
	public Long deserialize(DataInputView source) throws IOException {
		return source.readLong();
	}
	
	@Override
	public Long deserialize(Long reuse, DataInputView source) throws IOException {
		return deserialize(source);
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		target.writeLong(source.readLong());
	}

	@Override
	public boolean canEqual(Object obj) {
		return obj instanceof LongSerializer;
	}

	@Override
	protected boolean isCompatibleSerializationFormatIdentifier(String identifier) {
		return super.isCompatibleSerializationFormatIdentifier(identifier)
			|| identifier.equals(LongValueSerializer.class.getCanonicalName());
	}
}
