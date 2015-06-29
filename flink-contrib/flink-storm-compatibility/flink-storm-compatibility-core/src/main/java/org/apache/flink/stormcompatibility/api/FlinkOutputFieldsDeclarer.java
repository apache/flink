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

package org.apache.flink.stormcompatibility.api;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.TypeExtractor;

import java.util.List;

/**
 * {@link FlinkOutputFieldsDeclarer} is used to get the declared output schema of a
 * {@link backtype.storm.topology.IRichSpout spout} or {@link backtype.storm.topology.IRichBolt
 * bolt}.<br />
 * <br />
 * <strong>CAUTION: Currently, Flink does only support the default output stream. Furthermore,
 * direct emit is not supported.</strong>
 */
final class FlinkOutputFieldsDeclarer implements OutputFieldsDeclarer {

	/** the declared output schema */
	Fields outputSchema;

	@Override
	public void declare(final Fields fields) {
		this.declareStream(Utils.DEFAULT_STREAM_ID, false, fields);
	}

	/**
	 * {@inheritDoc}
	 * <p/>
	 * Direct emit is no supported by Flink. Parameter {@code direct} must be {@code false}.
	 *
	 * @throws UnsupportedOperationException
	 * 		if {@code direct} is {@code true}
	 */
	@Override
	public void declare(final boolean direct, final Fields fields) {
		this.declareStream(Utils.DEFAULT_STREAM_ID, direct, fields);
	}

	/**
	 * {@inheritDoc}
	 * <p/>
	 * Currently, Flink only supports the default output stream. Thus, parameter {@code streamId} must be equals to
	 * {@link Utils#DEFAULT_STREAM_ID}.
	 *
	 * @throws UnsupportedOperationException
	 * 		if {@code streamId} is not equal to {@link Utils#DEFAULT_STREAM_ID}
	 */
	@Override
	public void declareStream(final String streamId, final Fields fields) {
		this.declareStream(streamId, false, fields);
	}

	/**
	 * {@inheritDoc}
	 * <p/>
	 * Currently, Flink only supports the default output stream. Thus, parameter {@code streamId} must be equals to
	 * {@link Utils#DEFAULT_STREAM_ID}. Furthermore, direct emit is no supported by Flink and parameter {@code direct}
	 * must be {@code false}.
	 *
	 * @throws UnsupportedOperationException
	 * 		if {@code streamId} is not equal to {@link Utils#DEFAULT_STREAM_ID} or {@code direct} is {@code true}
	 */
	@Override
	public void declareStream(final String streamId, final boolean direct, final Fields fields) {
		if (!Utils.DEFAULT_STREAM_ID.equals(streamId)) {
			throw new UnsupportedOperationException("Currently, only the default output stream is supported by Flink");
		}
		if (direct) {
			throw new UnsupportedOperationException("Direct emit is not supported by Flink");
		}

		this.outputSchema = fields;
	}

	/**
	 * Returns {@link TypeInformation} for the declared output schema. If no or an empty output schema was declared,
	 * {@code null} is returned.
	 *
	 * @return output type information for the declared output schema; or {@code null} if no output schema was declared
	 * @throws IllegalArgumentException
	 * 		if more then 25 attributes are declared
	 */
	public TypeInformation<?> getOutputType() throws IllegalArgumentException {
		if ((this.outputSchema == null) || (this.outputSchema.size() == 0)) {
			return null;
		}

		Tuple t;
		final int numberOfAttributes = this.outputSchema.size();

		if (numberOfAttributes == 1) {
			return TypeExtractor.getForClass(Object.class);
		} else if (numberOfAttributes <= 25) {
			try {
				t = Tuple.getTupleClass(numberOfAttributes).newInstance();
			} catch (final InstantiationException e) {
				throw new RuntimeException(e);
			} catch (final IllegalAccessException e) {
				throw new RuntimeException(e);
			}
		} else {
			throw new IllegalArgumentException("Flink supports only a maximum number of 25 attributes");
		}

		// TODO: declare only key fields as DefaultComparable
		for (int i = 0; i < numberOfAttributes; ++i) {
			t.setField(new DefaultComparable(), i);
		}

		return TypeExtractor.getForObject(t);
	}

	/**
	 * {@link DefaultComparable} is a {@link Comparable} helper class that is used to get the correct {@link
	 * TypeInformation} from {@link TypeExtractor} within {@link #getOutputType()}. If key fields are not comparable,
	 * Flink cannot use them and will throw an exception.
	 */
	private static class DefaultComparable implements Comparable<DefaultComparable> {

		public DefaultComparable() {
		}

		@Override
		public int compareTo(final DefaultComparable o) {
			return 0;
		}
	}

	/**
	 * Computes the indexes within the declared output schema, for a list of given field-grouping attributes.
	 *
	 * @return array of {@code int}s that contains the index without the output schema for each attribute in the given
	 * list
	 */
	public int[] getGroupingFieldIndexes(final List<String> groupingFields) {
		final int[] fieldIndexes = new int[groupingFields.size()];

		for (int i = 0; i < fieldIndexes.length; ++i) {
			fieldIndexes[i] = this.outputSchema.fieldIndex(groupingFields.get(i));
		}

		return fieldIndexes;
	}

}
