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

package org.apache.flink.tez.runtime.input;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.tez.runtime.TezTaskConfig;
import org.apache.flink.tez.util.EncodingUtils;
import org.apache.flink.util.InstantiationUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.event.VertexStateUpdate;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.InputInitializer;
import org.apache.tez.runtime.api.InputInitializerContext;
import org.apache.tez.runtime.api.events.InputDataInformationEvent;
import org.apache.tez.runtime.api.events.InputInitializerEvent;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

public class FlinkInputSplitGenerator extends InputInitializer {

	private static final Log LOG = LogFactory.getLog(FlinkInputSplitGenerator.class);

	InputFormat format;

	public FlinkInputSplitGenerator(InputInitializerContext initializerContext) {
		super(initializerContext);
	}

	@Override
	public List<Event> initialize() throws Exception {

		Configuration tezConf = TezUtils.createConfFromUserPayload(this.getContext().getUserPayload());

		TezTaskConfig taskConfig = (TezTaskConfig) EncodingUtils.decodeObjectFromString(tezConf.get(TezTaskConfig.TEZ_TASK_CONFIG), getClass().getClassLoader());

		this.format = taskConfig.getInputFormat();

		int numTasks = this.getContext().getNumTasks();

		LOG.info ("Creating splits for " + numTasks + " tasks for input format " + format);
		
		InputSplit[] splits = format.createInputSplits((numTasks > 0) ? numTasks : 1 );

		LOG.info ("Created " + splits.length + " input splits" + " tasks for input format " + format);
		
		//LOG.info ("Created + " + splits.length + " input splits for input format " + format);

		LOG.info ("Sending input split events");
		LinkedList<Event> events = new LinkedList<Event>();
		for (int i = 0; i < splits.length; i++) {
			byte [] bytes = InstantiationUtil.serializeObject(splits[i]);
			ByteBuffer buf = ByteBuffer.wrap(bytes);
			InputDataInformationEvent event = InputDataInformationEvent.createWithSerializedPayload(i % numTasks, buf);
			event.setTargetIndex(i % numTasks);
			events.add(event);
			LOG.info ("Added event of index " + i + ": " + event);
		}
		return events;
	}

	@Override
	public void handleInputInitializerEvent(List<InputInitializerEvent> events) throws Exception {

	}

	@Override
	public void onVertexStateUpdated(VertexStateUpdate stateUpdate) {
		//super.onVertexStateUpdated(stateUpdate);

	}
}
