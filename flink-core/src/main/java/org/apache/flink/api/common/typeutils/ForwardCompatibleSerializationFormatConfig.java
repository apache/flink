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

package org.apache.flink.api.common.typeutils;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

/**
 * This is a special {@link TypeSerializerConfigSnapshot} that serializers can return to serve
 * as a marker to indicate that new serializers for the data written by this serializer does not
 * need to be checked for compatibility.
 */
@PublicEvolving
public final class ForwardCompatibleSerializationFormatConfig extends TypeSerializerConfigSnapshot {

	/** Singleton instance. */
	public static final ForwardCompatibleSerializationFormatConfig INSTANCE =
			new ForwardCompatibleSerializationFormatConfig();

	@Override
	public void write(DataOutputView out) throws IOException {
		// nothing to write
	}

	@Override
	public void read(DataInputView in) throws IOException {
		// nothing to read
	}

	@Override
	public int getSnapshotVersion() {
		throw new UnsupportedOperationException(
				"This is a ForwardCompatibleSerializationFormatConfig. No versioning required.");
	}

	@Override
	public int getVersion() {
		throw new UnsupportedOperationException(
				"This is a ForwardCompatibleSerializationFormatConfig. No versioning required.");
	}

	/**
	 * This special configuration type does not require the default
	 * empty nullary constructor because it will never actually be serialized.
	 */
	private ForwardCompatibleSerializationFormatConfig() {}

	@Override
	public boolean equals(Object obj) {
		return obj instanceof ForwardCompatibleSerializationFormatConfig;
	}

	@Override
	public int hashCode() {
		return getClass().hashCode();
	}
}
