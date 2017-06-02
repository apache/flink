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

package org.apache.flink.storm.api;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.TypeExtractor;

import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

import java.util.HashMap;
import java.util.List;

/**
 * {@link FlinkOutputFieldsDeclarer} is used to get the declared output schema of a
 * {@link org.apache.storm.topology.IRichSpout spout} or {@link org.apache.storm.topology.IRichBolt bolt}.<br />
 * <br />
 * <strong>CAUTION: Flink does not support direct emit.</strong>
 */
final class FlinkOutputFieldsDeclarer implements OutputFieldsDeclarer {

	/** The declared output streams and schemas. */
	final HashMap<String, Fields> outputStreams = new HashMap<String, Fields>();

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

	@Override
	public void declareStream(final String streamId, final Fields fields) {
		this.declareStream(streamId, false, fields);
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
	public void declareStream(final String streamId, final boolean direct, final Fields fields) {
		if (direct) {
			throw new UnsupportedOperationException("Direct emit is not supported by Flink");
		}

		this.outputStreams.put(streamId, fields);
	}

	/**
	 * Returns {@link TypeInformation} for the declared output schema for a specific stream.
	 *
	 * @param streamId
	 *            A stream ID.
	 *
	 * @return output type information for the declared output schema of the specified stream; or {@code null} if
	 *         {@code streamId == null}
	 *
	 * @throws IllegalArgumentException
	 *             If no output schema was declared for the specified stream or if more then 25 attributes got declared.
	 */
	TypeInformation<Tuple> getOutputType(final String streamId) throws IllegalArgumentException {
		if (streamId == null) {
			return null;
		}

		Fields outputSchema = this.outputStreams.get(streamId);
		if (outputSchema == null) {
			throw new IllegalArgumentException("Stream with ID '" + streamId
					+ "' was not declared.");
		}

		Tuple t;
		final int numberOfAttributes = outputSchema.size();

		if (numberOfAttributes <= 24) {
			try {
				t = Tuple.getTupleClass(numberOfAttributes + 1).newInstance();
			} catch (final InstantiationException e) {
				throw new RuntimeException(e);
			} catch (final IllegalAccessException e) {
				throw new RuntimeException(e);
			}
		} else {
			throw new IllegalArgumentException("Flink supports only a maximum number of 24 attributes");
		}

		// TODO: declare only key fields as DefaultComparable
		for (int i = 0; i < numberOfAttributes + 1; ++i) {
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
	 * Computes the indexes within the declared output schema of the specified stream, for a list of given
	 * field-grouping attributes.
	 *
	 * @param streamId
	 *            A stream ID.
	 * @param groupingFields
	 *            The names of the key fields.
	 *
	 * @return array of {@code int}s that contains the index within the output schema for each attribute in the given
	 *         list
	 */
	int[] getGroupingFieldIndexes(final String streamId, final List<String> groupingFields) {
		final int[] fieldIndexes = new int[groupingFields.size()];

		for (int i = 0; i < fieldIndexes.length; ++i) {
			fieldIndexes[i] = this.outputStreams.get(streamId).fieldIndex(groupingFields.get(i));
		}

		return fieldIndexes;
	}

}
