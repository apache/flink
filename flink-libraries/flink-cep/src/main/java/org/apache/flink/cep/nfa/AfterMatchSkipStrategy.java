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

package org.apache.flink.cep.nfa;

import org.apache.flink.api.common.typeutils.CompatibilityResult;
import org.apache.flink.api.common.typeutils.CompositeTypeSerializerConfigSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerConfigSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;


/**
 * Indicate the skip strategy after a match process.
 * <p>There're four kinds of strategies:
 * SKIP_PAST_LAST_ROW,
 * SKIP_TO_NEXT_ROW,
 * SKIP_TO_FIRST_<code>RPV</code> and
 * SKIP_TO_LAST_<code>RPV</code>
 * </p>
 */
public class AfterMatchSkipStrategy implements Serializable {

	// default strategy
	SkipStrategy strategy = SkipStrategy.SKIP_TO_NEXT_ROW;

	// fields
	String rpv = null;

	public AfterMatchSkipStrategy(){
		this(SkipStrategy.SKIP_TO_NEXT_ROW, null);
	}

	public AfterMatchSkipStrategy(SkipStrategy strategy) {
		this(strategy, null);
	}

	public AfterMatchSkipStrategy(SkipStrategy strategy, String rpv) {
		if (strategy == SkipStrategy.SKIP_TO_FIRST || strategy == SkipStrategy.SKIP_TO_LAST) {
			if (rpv == null) {
				throw new IllegalArgumentException("the rpv field can not be empty when SkipStrategy is " + strategy);
			}
		}
		this.strategy = strategy;
		this.rpv = rpv;
	}

	public SkipStrategy getStrategy() {
		return strategy;
	}

	public String getRpv() {
		return rpv;
	}

	@Override
	public String toString() {
		return "AfterMatchStrategy{" +
			"strategy=" + strategy +
			", rpv=" + rpv +
			'}';
	}

	/**
	 * Skip Strategy Enum.
	 */
	public enum SkipStrategy{
		SKIP_TO_NEXT_ROW,
		SKIP_PAST_LAST_ROW,
		SKIP_TO_FIRST,
		SKIP_TO_LAST
	}

	/**
	 * The {@link TypeSerializerConfigSnapshot} serializer configuration to be stored with the managed state.
	 */
	public static class AfterMatchSkipStrategyConfigSnapshot extends CompositeTypeSerializerConfigSnapshot {

		private static final int VERSION = 1;

		/**
		 * This empty constructor is required for deserializing the configuration.
		 */
		public AfterMatchSkipStrategyConfigSnapshot() {
		}

		public AfterMatchSkipStrategyConfigSnapshot(
			TypeSerializer<SkipStrategy> enumSerializer,
			TypeSerializer<String> stringSerializer) {

			super(enumSerializer, stringSerializer);
		}

		@Override
		public int getVersion() {
			return VERSION;
		}
	}

	/**
	 *  A {@link TypeSerializer} for the {@link AfterMatchSkipStrategy}.
	 */
	public static class AfterMatchSkipStrategySerializer extends TypeSerializer<AfterMatchSkipStrategy> {

		private final TypeSerializer<SkipStrategy> enumSerializer;
		private final TypeSerializer<String> stringSerializer;

		public AfterMatchSkipStrategySerializer(TypeSerializer<SkipStrategy> enumSerializer, TypeSerializer<String> stringSerializer) {
			this.enumSerializer = enumSerializer;
			this.stringSerializer = stringSerializer;
		}

		@Override
		public boolean isImmutableType() {
			return false;
		}

		@Override
		public TypeSerializer<AfterMatchSkipStrategy> duplicate() {
			return new AfterMatchSkipStrategySerializer(enumSerializer, stringSerializer);
		}

		@Override
		public AfterMatchSkipStrategy createInstance() {
			return new AfterMatchSkipStrategy();
		}

		@Override
		public AfterMatchSkipStrategy copy(AfterMatchSkipStrategy from) {
			try {
				ByteArrayOutputStream baos = new ByteArrayOutputStream();
				ObjectOutputStream oos = new ObjectOutputStream(baos);

				serialize(from, new DataOutputViewStreamWrapper(oos));

				oos.close();
				baos.close();

				byte[] data = baos.toByteArray();

				ByteArrayInputStream bais = new ByteArrayInputStream(data);
				ObjectInputStream ois = new ObjectInputStream(bais);

				AfterMatchSkipStrategy copy = deserialize(new DataInputViewStreamWrapper(ois));
				ois.close();
				bais.close();

				return copy;
			} catch (IOException e) {
				throw new RuntimeException("Could not copy AfterMatchSkipStrategy.", e);
			}
		}

		@Override
		public AfterMatchSkipStrategy copy(AfterMatchSkipStrategy from, AfterMatchSkipStrategy reuse) {
			return copy(from);
		}

		@Override
		public int getLength() {
			return -1;
		}

		@Override
		public void serialize(AfterMatchSkipStrategy record, DataOutputView target) throws IOException {
			enumSerializer.serialize(record.getStrategy(), target);
			stringSerializer.serialize(record.getRpv(), target);
		}

		@Override
		public AfterMatchSkipStrategy deserialize(DataInputView source) throws IOException {
			SkipStrategy skipStrategy = enumSerializer.deserialize(source);
			String rpv = stringSerializer.deserialize(source);
			return new AfterMatchSkipStrategy(skipStrategy, rpv);
		}

		@Override
		public AfterMatchSkipStrategy deserialize(AfterMatchSkipStrategy reuse, DataInputView source) throws IOException {
			return deserialize(source);
		}

		@Override
		public void copy(DataInputView source, DataOutputView target) throws IOException {
			SkipStrategy skipStrategy = enumSerializer.deserialize(source);
			enumSerializer.serialize(skipStrategy, target);
			String rpv = stringSerializer.deserialize(source);
			stringSerializer.serialize(rpv, target);
		}

		@Override
		public boolean equals(Object obj) {
			return obj == this ||
				(obj != null && obj.getClass().equals(getClass()));
		}

		@Override
		public boolean canEqual(Object obj) {
			return true;
		}

		@Override
		public int hashCode() {
			return 37 * enumSerializer.hashCode() + stringSerializer.hashCode();
		}

		@Override
		public TypeSerializerConfigSnapshot snapshotConfiguration() {
			return new AfterMatchSkipStrategyConfigSnapshot(enumSerializer, stringSerializer);
		}

		@Override
		public CompatibilityResult<AfterMatchSkipStrategy> ensureCompatibility(TypeSerializerConfigSnapshot configSnapshot) {
			return CompatibilityResult.compatible();
		}
	}
}
