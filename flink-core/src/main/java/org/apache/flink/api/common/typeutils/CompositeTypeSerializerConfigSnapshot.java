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

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.Arrays;

/**
 * A {@link TypeSerializerConfigSnapshot} for serializers that has multiple nested serializers.
 * The configuration snapshot consists of the configuration snapshots of all nested serializers.
 */
@Internal
public abstract class CompositeTypeSerializerConfigSnapshot extends TypeSerializerConfigSnapshot {

	private TypeSerializerConfigSnapshot[] nestedSerializerConfigSnapshots;

	/** This empty nullary constructor is required for deserializing the configuration. */
	public CompositeTypeSerializerConfigSnapshot() {}

	public CompositeTypeSerializerConfigSnapshot(TypeSerializerConfigSnapshot... nestedSerializerConfigSnapshots) {
		this.nestedSerializerConfigSnapshots = Preconditions.checkNotNull(nestedSerializerConfigSnapshots);
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		super.write(out);
		TypeSerializerUtil.writeSerializerConfigSnapshots(out, nestedSerializerConfigSnapshots);
	}

	@Override
	public void read(DataInputView in) throws IOException {
		super.read(in);
		nestedSerializerConfigSnapshots = TypeSerializerUtil.readSerializerConfigSnapshots(in, getUserCodeClassLoader());
	}

	public TypeSerializerConfigSnapshot[] getNestedSerializerConfigSnapshots() {
		return nestedSerializerConfigSnapshots;
	}

	public TypeSerializerConfigSnapshot getSingleNestedSerializerConfigSnapshot() {
		return nestedSerializerConfigSnapshots[0];
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		}

		if (obj == null) {
			return false;
		}

		return (obj.getClass().equals(getClass()))
				&& Arrays.equals(
					nestedSerializerConfigSnapshots,
					((CompositeTypeSerializerConfigSnapshot) obj).getNestedSerializerConfigSnapshots());
	}

	@Override
	public int hashCode() {
		return Arrays.hashCode(nestedSerializerConfigSnapshots);
	}
}
