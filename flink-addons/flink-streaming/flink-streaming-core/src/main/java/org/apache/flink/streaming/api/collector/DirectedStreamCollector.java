/**
 *
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
 *
 */

package org.apache.flink.streaming.api.collector;

import java.util.Collection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.runtime.io.network.api.RecordWriter;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.api.streamrecord.StreamRecord;
import org.apache.flink.util.StringUtils;

public class DirectedStreamCollector<T extends Tuple> extends StreamCollector<T> {

	OutputSelector<T> outputSelector;
	private static final Log log = LogFactory.getLog(DirectedStreamCollector.class);

	public DirectedStreamCollector(int channelID, SerializationDelegate<T> serializationDelegate,
			OutputSelector<T> outputSelector) {
		super(channelID, serializationDelegate);
		this.outputSelector = outputSelector;

	}

	@Override
	public void collect(T tuple) {
		streamRecord.setTuple(tuple);
		emit(streamRecord);
	}

	private void emit(StreamRecord<T> streamRecord) {
		Collection<String> outputNames = outputSelector.getOutputs(streamRecord.getTuple());
		streamRecord.setId(channelID);
		for (String outputName : outputNames) {
			try {
				for (RecordWriter<StreamRecord<T>> output : outputMap.get(outputName)) {
					output.emit(streamRecord);
				}
			} catch (Exception e) {
				if (log.isErrorEnabled()) {
					log.error(String.format("Emit to %s failed due to: %s", outputName,
							StringUtils.stringifyException(e)));
				}
			}
		}
	}
}
