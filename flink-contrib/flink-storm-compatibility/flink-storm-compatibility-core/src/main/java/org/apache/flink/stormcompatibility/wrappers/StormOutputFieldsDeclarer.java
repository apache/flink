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

package org.apache.flink.stormcompatibility.wrappers;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

/**
 * {@link StormOutputFieldsDeclarer} is used by {@link StormWrapperSetupHelper} to determine the
 * number of attributes declared by the wrapped spout's or bolt's {@code declare(...)} method.
 */
class StormOutputFieldsDeclarer implements OutputFieldsDeclarer {

	/** The output schema declared by the wrapped bolt. */
	private Fields outputSchema = null;

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
		if (!Utils.DEFAULT_STREAM_ID.equals(streamId)) {
			throw new UnsupportedOperationException("Currently, only the default output stream is supported by Flink");
		}
		if (direct) {
			throw new UnsupportedOperationException("Direct emit is not supported by Flink");
		}

		this.outputSchema = fields;
	}

	/**
	 * Returns the number of attributes of the output schema declare by the wrapped bolt. If no output schema is
	 * declared (eg, for sink bolts), {@code -1} is returned.
	 *
	 * @return the number of attributes of the output schema declare by the wrapped bolt
	 */
	public int getNumberOfAttributes() {
		if (this.outputSchema != null) {
			return this.outputSchema.size();
		}

		return -1;
	}

}
