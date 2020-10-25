/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connectors.hive.read;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInfoFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.api.java.typeutils.runtime.DataInputViewStream;
import org.apache.flink.api.java.typeutils.runtime.DataOutputViewStream;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.streaming.api.functions.source.TimestampedInputSplit;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Type;
import java.util.Map;
import java.util.Objects;

/**
 * A {@link HiveTableInputSplit} with {@link TimestampedInputSplit}.
 * Kryo serializer can not deal with hadoop split, need specific type information factory.
 *
 * <p>Note: this class has a natural ordering that is inconsistent with equals.
 */
@TypeInfo(TimestampedHiveInputSplit.SplitTypeInfoFactory.class)
public class TimestampedHiveInputSplit extends HiveTableInputSplit implements TimestampedInputSplit {

	private static final long serialVersionUID = 1L;

	/** The modification time of the file this split belongs to. */
	private final long modificationTime;

	/**
	 * The state of the split. This information is used when
	 * restoring from a checkpoint and allows to resume reading the
	 * underlying file from the point we left off.
	 * */
	private Serializable splitState;

	public TimestampedHiveInputSplit(
			long modificationTime,
			HiveTableInputSplit split) {
		super(
				split.getSplitNumber(),
				split.getHadoopInputSplit(),
				split.getJobConf(),
				split.getHiveTablePartition());
		this.modificationTime = modificationTime;
	}

	@Override
	public void setSplitState(Serializable state) {
		this.splitState = state;
	}

	@Override
	public Serializable getSplitState() {
		return this.splitState;
	}

	@Override
	public long getModificationTime() {
		return modificationTime;
	}

	/**
	 * Note Again: this class has a natural ordering that is inconsistent with equals.
	 */
	@Override
	public int compareTo(TimestampedInputSplit o) {
		TimestampedHiveInputSplit split = (TimestampedHiveInputSplit) o;
		int modTimeComp = Long.compare(this.modificationTime, split.modificationTime);
		if (modTimeComp != 0L) {
			return modTimeComp;
		}

		int sdComp = this.hiveTablePartition.getStorageDescriptor().compareTo(
				split.hiveTablePartition.getStorageDescriptor());

		return sdComp != 0 ? sdComp :
				this.getSplitNumber() - o.getSplitNumber();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		if (!super.equals(o)) {
			return false;
		}
		TimestampedHiveInputSplit that = (TimestampedHiveInputSplit) o;
		return modificationTime == that.modificationTime;
	}

	@Override
	public int hashCode() {
		return Objects.hash(super.hashCode(), modificationTime);
	}

	@Override
	public String toString() {
		return "TimestampedHiveInputSplit{" +
				"modificationTime=" + modificationTime +
				", splitState=" + splitState +
				", hiveTablePartition=" + hiveTablePartition +
				'}';
	}

	/**
	 * {@link TypeInfoFactory} for {@link TimestampedHiveInputSplit}.
	 */
	public static class SplitTypeInfoFactory
			extends TypeInfoFactory<TimestampedHiveInputSplit>
			implements Serializable {

		private static final long serialVersionUID = 1L;

		@Override
		public TypeInformation<TimestampedHiveInputSplit> createTypeInfo(
				Type t, Map genericParameters) {
			return new BasicTypeInfo<TimestampedHiveInputSplit>(
					TimestampedHiveInputSplit.class,
					new Class<?>[]{},
					SplitTypeSerializer.INSTANCE,
					null) {};
		}
	}

	private static class SplitTypeSerializer extends TypeSerializerSingleton<TimestampedHiveInputSplit> {

		private static final SplitTypeSerializer INSTANCE = new SplitTypeSerializer();

		private static final long serialVersionUID = 1L;

		@Override
		public boolean isImmutableType() {
			return false;
		}

		@Override
		public TimestampedHiveInputSplit createInstance() {
			return null;
		}

		@Override
		public TimestampedHiveInputSplit copy(TimestampedHiveInputSplit from) {
			try {
				return InstantiationUtil.clone(from, Thread.currentThread().getContextClassLoader());
			} catch (IOException | ClassNotFoundException e) {
				throw new FlinkRuntimeException("Could not copy element via serialization: " + from, e);
			}
		}

		@Override
		public TimestampedHiveInputSplit copy(TimestampedHiveInputSplit from, TimestampedHiveInputSplit reuse) {
			return copy(from);
		}

		@Override
		public int getLength() {
			return -1;
		}

		@Override
		public void serialize(TimestampedHiveInputSplit record, DataOutputView target) throws IOException {
			try (final DataOutputViewStream outViewWrapper = new DataOutputViewStream(target)) {
				InstantiationUtil.serializeObject(outViewWrapper, record);
			}
		}

		@Override
		public TimestampedHiveInputSplit deserialize(DataInputView source) throws IOException {
			try (final DataInputViewStream inViewWrapper = new DataInputViewStream(source)) {
				return InstantiationUtil.deserializeObject(
						inViewWrapper,
						Thread.currentThread().getContextClassLoader());
			} catch (ClassNotFoundException e) {
				throw new IOException("Could not deserialize object.", e);
			}
		}

		@Override
		public TimestampedHiveInputSplit deserialize(
				TimestampedHiveInputSplit reuse, DataInputView source) throws IOException {
			return deserialize(source);
		}

		@Override
		public void copy(DataInputView source, DataOutputView target) throws IOException {
			TimestampedHiveInputSplit tmp = deserialize(source);
			serialize(tmp, target);
		}

		@Override
		public TypeSerializerSnapshot<TimestampedHiveInputSplit> snapshotConfiguration() {
			return new SplitTypeSerializer.SplitSerializerSnapshot();
		}

		/**
		 * Serializer configuration snapshot for compatibility and format evolution.
		 */
		@SuppressWarnings("WeakerAccess")
		public static final class SplitSerializerSnapshot extends
				SimpleTypeSerializerSnapshot<TimestampedHiveInputSplit> {

			public SplitSerializerSnapshot() {
				super(SplitTypeSerializer::new);
			}
		}
	}
}
