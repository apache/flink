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

package org.apache.flink.storm.wrappers;

import org.apache.flink.api.java.tuple.Tuple0;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple25;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.apache.storm.topology.IRichBolt;

import java.util.Collection;

import static java.util.Arrays.asList;

/**
 * A {@link MergedInputsBoltWrapper} is a {@link BoltWrapper} that expects input tuples of type {@link StormTuple}. It
 * can be used to wrap a multi-input bolt and assumes that all input stream got merged into a {@link StormTuple} stream
 * already.
 */
public final class MergedInputsBoltWrapper<IN, OUT> extends BoltWrapper<StormTuple<IN>, OUT> {
	private static final long serialVersionUID = 6399319187892878545L;

	/**
	 * Instantiates a new {@link MergedInputsBoltWrapper} that wraps the given Storm {@link IRichBolt bolt} such that it
	 * can be used within a Flink streaming program. The output type will be one of {@link Tuple0} to {@link Tuple25}
	 * depending on the bolt's declared number of attributes.
	 *
	 * @param bolt
	 *            The Storm {@link IRichBolt bolt} to be used.
	 * @throws IllegalArgumentException
	 *             If the number of declared output attributes is not with range [0;25].
	 */
	public MergedInputsBoltWrapper(final IRichBolt bolt) throws IllegalArgumentException {
		super(bolt);
	}

	/**
	 * Instantiates a new {@link MergedInputsBoltWrapper} that wraps the given Storm {@link IRichBolt bolt} such that it
	 * can be used within a Flink streaming program. The output type can be any type if parameter {@code rawOutput} is
	 * {@code true} and the bolt's number of declared output tuples is 1. If {@code rawOutput} is {@code false} the
	 * output type will be one of {@link Tuple0} to {@link Tuple25} depending on the bolt's declared number of
	 * attributes.
	 *
	 * @param bolt
	 *            The Storm {@link IRichBolt bolt} to be used.
	 * @param rawOutputs
	 *            Contains stream names if a single attribute output stream, should not be of type {@link Tuple1} but be
	 *            of a raw type.
	 * @throws IllegalArgumentException
	 *             If {@code rawOutput} is {@code true} and the number of declared output attributes is not 1 or if
	 *             {@code rawOutput} is {@code false} and the number of declared output attributes is not within range
	 *             [1;25].
	 */
	public MergedInputsBoltWrapper(final IRichBolt bolt, final String[] rawOutputs)
			throws IllegalArgumentException {
		super(bolt, asList(rawOutputs));
	}

	/**
	 * Instantiates a new {@link MergedInputsBoltWrapper} that wraps the given Storm {@link IRichBolt bolt} such that it
	 * can be used within a Flink streaming program. The output type can be any type if parameter {@code rawOutput} is
	 * {@code true} and the bolt's number of declared output tuples is 1. If {@code rawOutput} is {@code false} the
	 * output type will be one of {@link Tuple0} to {@link Tuple25} depending on the bolt's declared number of
	 * attributes.
	 *
	 * @param bolt
	 *            The Storm {@link IRichBolt bolt} to be used.
	 * @param rawOutputs
	 *            Contains stream names if a single attribute output stream, should not be of type {@link Tuple1} but be
	 *            of a raw type.
	 * @throws IllegalArgumentException
	 *             If {@code rawOutput} is {@code true} and the number of declared output attributes is not 1 or if
	 *             {@code rawOutput} is {@code false} and the number of declared output attributes is not with range
	 *             [1;25].
	 */
	public MergedInputsBoltWrapper(final IRichBolt bolt, final Collection<String> rawOutputs)
			throws IllegalArgumentException {
		super(bolt, rawOutputs);
	}

	/**
	 * Instantiates a new {@link MergedInputsBoltWrapper} that wraps the given Storm {@link IRichBolt bolt} such that it
	 * can be used within a Flink streaming program. The output type can be any type if parameter {@code rawOutput} is
	 * {@code true} and the bolt's number of declared output tuples is 1. If {@code rawOutput} is {@code false} the
	 * output type will be one of {@link Tuple0} to {@link Tuple25} depending on the bolt's declared number of
	 * attributes.
	 *
	 * @param bolt
	 *            The Storm {@link IRichBolt bolt} to be used.
	 * @param name
	 *            The name of the bolt.
	 * @param rawOutputs
	 *            Contains stream names if a single attribute output stream, should not be of type {@link Tuple1} but be
	 *            of a raw type.
	 * @throws IllegalArgumentException
	 *             If {@code rawOutput} is {@code true} and the number of declared output attributes is not 1 or if
	 *             {@code rawOutput} is {@code false} and the number of declared output attributes is not with range
	 *             [0;25].
	 */
	public MergedInputsBoltWrapper(final IRichBolt bolt, final String name, final Collection<String> rawOutputs)
			throws IllegalArgumentException {
		super(bolt, name, null, null, null, rawOutputs);
	}

	@Override
	public void processElement(final StreamRecord<StormTuple<IN>> element) throws Exception {
		this.flinkCollector.setTimestamp(element);
		this.bolt.execute(element.getValue());
	}

}
