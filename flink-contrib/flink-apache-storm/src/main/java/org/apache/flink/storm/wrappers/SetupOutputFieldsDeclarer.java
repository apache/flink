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

import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

import java.util.HashMap;

/**
 * {@link SetupOutputFieldsDeclarer} is used by {@link WrapperSetupHelper} to determine the output streams and
 * number of attributes declared by the wrapped spout's or bolt's {@code declare(...)}/{@code declareStream(...)}
 * method.
 */
class SetupOutputFieldsDeclarer implements OutputFieldsDeclarer {

	/** The declared output streams and schemas. */
	HashMap<String, Fields> outputStreams = new HashMap<String, Fields>();
	/** The number of attributes for each declared stream by the wrapped operator. */
	HashMap<String, Integer> outputSchemas = new HashMap<String, Integer>();

	@Override
	public void declare(final Fields fields) {
		this.declareStream(Utils.DEFAULT_STREAM_ID, false, fields);
	}

	@Override
	public void declare(final boolean direct, final Fields fields) {
		this.declareStream(Utils.DEFAULT_STREAM_ID, direct, fields);
	}

	@Override
	public void declareStream(final String streamId, final Fields fields) {
		this.declareStream(streamId, false, fields);
	}

	@Override
	public void declareStream(final String streamId, final boolean direct, final Fields fields) {
		if (streamId == null) {
			throw new IllegalArgumentException("Stream ID cannot be null.");
		}
		if (direct) {
			throw new UnsupportedOperationException("Direct emit is not supported by Flink");
		}

		this.outputStreams.put(streamId, fields);
		this.outputSchemas.put(streamId, fields.size());
	}

}
