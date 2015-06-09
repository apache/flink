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
package org.apache.flink.stormcompatibility.wrappers;

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

import java.util.List;

/**
 * {@link StormTuple} converts a Flink tuple of type {@code IN} into a Storm tuple.
 */
class StormTuple<IN> implements backtype.storm.tuple.Tuple {

	// The storm representation of the original Flink tuple
	private final Values stormTuple;

	/**
	 * Create a new Storm tuple from the given Flink tuple.
	 *
	 * @param flinkTuple
	 * 		The Flink tuple to be converted.
	 */
	public StormTuple(final IN flinkTuple) {
		if (flinkTuple instanceof org.apache.flink.api.java.tuple.Tuple) {
			final org.apache.flink.api.java.tuple.Tuple t = (org.apache.flink.api.java.tuple.Tuple) flinkTuple;

			final int numberOfAttributes = t.getArity();
			this.stormTuple = new Values();
			for (int i = 0; i < numberOfAttributes; ++i) {
				this.stormTuple.add(t.getField(i));
			}
		} else {
			this.stormTuple = new Values(flinkTuple);
		}
	}

	@Override
	public int size() {
		return this.stormTuple.size();
	}

	@Override
	public boolean contains(final String field) {
		throw new UnsupportedOperationException("Not implemented yet");
	}

	@Override
	public Fields getFields() {
		throw new UnsupportedOperationException("Not implemented yet");
	}

	@Override
	public int fieldIndex(final String field) {
		throw new UnsupportedOperationException("Not implemented yet");
	}

	@Override
	public List<Object> select(final Fields selector) {
		throw new UnsupportedOperationException("Not implemented yet");
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

	@Override
	public Object getValueByField(final String field) {
		throw new UnsupportedOperationException("Not implemented yet");
	}

	@Override
	public String getStringByField(final String field) {
		throw new UnsupportedOperationException("Not implemented yet");
	}

	@Override
	public Integer getIntegerByField(final String field) {
		throw new UnsupportedOperationException("Not implemented yet");
	}

	@Override
	public Long getLongByField(final String field) {
		throw new UnsupportedOperationException("Not implemented yet");
	}

	@Override
	public Boolean getBooleanByField(final String field) {
		throw new UnsupportedOperationException("Not implemented yet");
	}

	@Override
	public Short getShortByField(final String field) {
		throw new UnsupportedOperationException("Not implemented yet");
	}

	@Override
	public Byte getByteByField(final String field) {
		throw new UnsupportedOperationException("Not implemented yet");
	}

	@Override
	public Double getDoubleByField(final String field) {
		throw new UnsupportedOperationException("Not implemented yet");
	}

	@Override
	public Float getFloatByField(final String field) {
		throw new UnsupportedOperationException("Not implemented yet");
	}

	@Override
	public byte[] getBinaryByField(final String field) {
		throw new UnsupportedOperationException("Not implemented yet");
	}

	@Override
	public List<Object> getValues() {
		return this.stormTuple;
	}

	@Override
	public GlobalStreamId getSourceGlobalStreamid() {
		// not sure if Flink can support this
		throw new UnsupportedOperationException();
	}

	@Override
	public String getSourceComponent() {
		// not sure if Flink can support this
		throw new UnsupportedOperationException();
	}

	@Override
	public int getSourceTask() {
		// not sure if Flink can support this
		throw new UnsupportedOperationException();
	}

	@Override
	public String getSourceStreamId() {
		// not sure if Flink can support this
		throw new UnsupportedOperationException();
	}

	@Override
	public MessageId getMessageId() {
		// not sure if Flink can support this
		throw new UnsupportedOperationException();
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

}
