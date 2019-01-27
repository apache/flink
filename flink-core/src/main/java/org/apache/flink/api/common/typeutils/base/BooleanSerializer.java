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

import java.io.IOException;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

@Internal
public final class BooleanSerializer extends TypeSerializerSingleton<Boolean> {

	private static final long serialVersionUID = 1L;
	
	public static final BooleanSerializer INSTANCE = new BooleanSerializer();
	
	private static final Boolean FALSE = Boolean.FALSE;

	@Override
	public boolean isImmutableType() {
		return true;
	}

	@Override
	public Boolean createInstance() {
		return FALSE;
	}

	@Override
	public Boolean copy(Boolean from) {
		return from;
	}
	
	@Override
	public Boolean copy(Boolean from, Boolean reuse) {
		return from;
	}

	@Override
	public int getLength() {
		return 1;
	}

	@Override
	public void serialize(Boolean record, DataOutputView target) throws IOException {
		target.writeBoolean(record.booleanValue());
	}

	@Override
	public Boolean deserialize(DataInputView source) throws IOException {
		return Boolean.valueOf(source.readBoolean());
	}
	
	@Override
	public Boolean deserialize(Boolean reuse, DataInputView source) throws IOException {
		return Boolean.valueOf(source.readBoolean());
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		target.writeBoolean(source.readBoolean());
	}

	@Override
	public boolean canEqual(Object obj) {
		return obj instanceof BooleanSerializer;
	}

	@Override
	protected boolean isCompatibleSerializationFormatIdentifier(String identifier) {
		return super.isCompatibleSerializationFormatIdentifier(identifier)
			|| identifier.equals(BooleanValueSerializer.class.getCanonicalName());
	}
}
