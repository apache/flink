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
package org.apache.flink.storm.wrappers;

/*
 * We do neither import
 * 		backtype.storm.tuple.Tuple;
 * nor
 * 		org.apache.flink.api.java.tuple.Tuple
 * to avoid confusion
 */

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.MessageId;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import com.google.common.base.Preconditions;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;

/**
 * {@link StormTuple} converts a Flink tuple of type {@code IN} into a Storm tuple.
 */
public class StormTuple<IN> implements backtype.storm.tuple.Tuple {

	/** The Storm representation of the original Flink tuple */
	private final Values stormTuple;
	/** The schema (ie, ordered field names) of the tuple */
	private final Fields schema;

	/** The task id where this tuple is processed */
	private final int taskId;
	/** The producer of this tuple */
	private final String producerStreamId;
	/** The producer's component id of this tuple */
	private final String producerComponentId;
	/** The message that is associated with this tuple */
	private final MessageId id;

	/**
	 * Constructor which sets defaults for producerComponentId, taskId, and componentID
	 * @param flinkTuple the Flink tuple
	 * @param schema The schema of the storm fields
	 */
	public StormTuple(final IN flinkTuple, final Fields schema) {
		this(flinkTuple, schema, -1, Utils.DEFAULT_STREAM_ID, BoltWrapper.DEFAULT_ID);
	}

	/**
	 * Create a new Storm tuple from the given Flink tuple. The provided {@code nameIndexMap} is ignored for raw input
	 * types.
	 * @param flinkTuple The Flink tuple to be converted.
	 * @param schema The schema (ie, ordered field names) of the tuple.
	 * @param producerComponentId The component id of the producer.
	 */
	public StormTuple(final IN flinkTuple, final Fields schema, int taskId, String producerStreamId, String producerComponentId) {
		if (flinkTuple instanceof org.apache.flink.api.java.tuple.Tuple) {
			this.schema = schema;
			final org.apache.flink.api.java.tuple.Tuple t = (org.apache.flink.api.java.tuple.Tuple) flinkTuple;

			final int numberOfAttributes = t.getArity();
			this.stormTuple = new Values();
			for (int i = 0; i < numberOfAttributes; ++i) {
				this.stormTuple.add(t.getField(i));
			}
		} else {
			this.schema = null;
			this.stormTuple = new Values(flinkTuple);
		}

		this.taskId = Preconditions.checkNotNull(taskId);
		this.producerStreamId = Preconditions.checkNotNull(producerStreamId);
		this.producerComponentId = Preconditions.checkNotNull(producerComponentId);
		this.id = Preconditions.checkNotNull(MessageId.makeUnanchored());
	}

	@Override
	public int size() {
		return this.stormTuple.size();
	}

	@Override
	public boolean contains(final String field) {
		if (this.schema != null) {
			return this.schema.contains(field);
		}

		try {
			this.getPublicMemberField(field);
			return true;
		} catch (NoSuchFieldException f) {
			try {
				this.getGetterMethod(field);
				return true;
			} catch (Exception g) {
				return false;
			}
		} catch (Exception e) {
			return false;
		}
	}

	@Override
	public Fields getFields() {
		return this.schema;
	}

	@Override
	public int fieldIndex(final String field) {
		return this.schema.fieldIndex(field);
	}

	@Override
	public List<Object> select(final Fields selector) {
		return this.schema.select(selector, this.stormTuple);
	}

	@Override
	public Object getValue(final int i) {
		return this.stormTuple.get(i);
	}

	@Override
	public String getString(final int i) {
		return (String) this.stormTuple.get(i);
	}

	@Override
	public Integer getInteger(final int i) {
		return (Integer) this.stormTuple.get(i);
	}

	@Override
	public Long getLong(final int i) {
		return (Long) this.stormTuple.get(i);
	}

	@Override
	public Boolean getBoolean(final int i) {
		return (Boolean) this.stormTuple.get(i);
	}

	@Override
	public Short getShort(final int i) {
		return (Short) this.stormTuple.get(i);
	}

	@Override
	public Byte getByte(final int i) {
		return (Byte) this.stormTuple.get(i);
	}

	@Override
	public Double getDouble(final int i) {
		return (Double) this.stormTuple.get(i);
	}

	@Override
	public Float getFloat(final int i) {
		return (Float) this.stormTuple.get(i);
	}

	@Override
	public byte[] getBinary(final int i) {
		return (byte[]) this.stormTuple.get(i);
	}

	private Field getPublicMemberField(final String field) throws Exception {
		assert (this.stormTuple.size() == 1);
		return this.stormTuple.get(0).getClass().getField(field);
	}

	private Method getGetterMethod(final String field) throws Exception {
		assert (this.stormTuple.size() == 1);
		return this.stormTuple
				.get(0)
				.getClass()
				.getMethod("get" + Character.toUpperCase(field.charAt(0)) + field.substring(1),
						(Class<?>[]) null);
	}

	private Object getValueByPublicMember(final String field) throws Exception {
		assert (this.stormTuple.size() == 1);
		return getPublicMemberField(field).get(this.stormTuple.get(0));
	}

	private Object getValueByGetter(final String field) throws Exception {
		assert (this.stormTuple.size() == 1);
		return getGetterMethod(field).invoke(this.stormTuple.get(0), (Object[]) null);
	}

	@SuppressWarnings("unchecked")
	public <T> T getValueByName(final String field) {
		if (this.schema != null) {
			return (T) this.getValue(this.schema.fieldIndex(field));
		}
		assert (this.stormTuple.size() == 1);

		Exception e;
		try {
			// try public member
			return (T) getValueByPublicMember(field);
		} catch (NoSuchFieldException f) {
			try {
				// try getter-method
				return (T) getValueByGetter(field);
			} catch (Exception g) {
				e = g;
			}
		} catch (Exception f) {
			e = f;
		}

		throw new RuntimeException("Could not access field <" + field + ">", e);
	}

	@Override
	public Object getValueByField(final String field) {
		return getValueByName(field);
	}

	@Override
	public String getStringByField(final String field) {
		return getValueByName(field);
	}

	@Override
	public Integer getIntegerByField(final String field) {
		return getValueByName(field);
	}

	@Override
	public Long getLongByField(final String field) {
		return getValueByName(field);
	}

	@Override
	public Boolean getBooleanByField(final String field) {
		return getValueByName(field);
	}

	@Override
	public Short getShortByField(final String field) {
		return getValueByName(field);
	}

	@Override
	public Byte getByteByField(final String field) {
		return getValueByName(field);
	}

	@Override
	public Double getDoubleByField(final String field) {
		return getValueByName(field);
	}

	@Override
	public Float getFloatByField(final String field) {
		return getValueByName(field);
	}

	@Override
	public byte[] getBinaryByField(final String field) {
		return getValueByName(field);
	}

	@Override
	public List<Object> getValues() {
		return this.stormTuple;
	}

	@Override
	public GlobalStreamId getSourceGlobalStreamid() {
		return new GlobalStreamId(getSourceComponent(), producerStreamId);
	}

	@Override
	public String getSourceComponent() {
		return producerComponentId;
	}

	@Override
	public int getSourceTask() {
		return taskId;
	}

	@Override
	public String getSourceStreamId() {
		return producerStreamId;
	}

	@Override
	public MessageId getMessageId() {
		return id;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = (prime * result) + ((this.stormTuple == null) ? 0 : this.stormTuple.hashCode());
		return result;
	}

	@Override
	public boolean equals(final Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (this.getClass() != obj.getClass()) {
			return false;
		}
		final StormTuple<?> other = (StormTuple<?>) obj;
		if (this.stormTuple == null) {
			if (other.stormTuple != null) {
				return false;
			}
		} else if (!this.stormTuple.equals(other.stormTuple)) {
			return false;
		}
		return true;
	}

	@Override
	public String toString() {
		return "StormTuple{ "+ stormTuple.toString() +" }";
	}
}
