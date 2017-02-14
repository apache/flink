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

package org.apache.flink.runtime.webmonitor.handlers.checkpoints;

import com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.runtime.checkpoint.AbstractCheckpointStats;
import org.apache.flink.runtime.checkpoint.CheckpointStatsSnapshot;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.webmonitor.ExecutionGraphHolder;
import org.apache.flink.runtime.webmonitor.handlers.AbstractExecutionGraphRequestHandler;
import org.apache.flink.runtime.webmonitor.handlers.JsonFactory;
import org.apache.flink.runtime.webmonitor.utils.JsonUtils;

import java.io.StringWriter;
import java.util.Map;

/**
 * Request handler that returns checkpoint stats for a single job vertex.
 */
public class CheckpointStatsDetailsHandler extends AbstractExecutionGraphRequestHandler {

	private final CheckpointStatsCache cache;

	public CheckpointStatsDetailsHandler(ExecutionGraphHolder executionGraphHolder, CheckpointStatsCache cache) {
		super(executionGraphHolder);
		this.cache = cache;
	}

	@Override
	public String handleRequest(AccessExecutionGraph graph, Map<String, String> params) throws Exception {
		long checkpointId = parseCheckpointId(params);
		if (checkpointId == -1) {
			return "{}";
		}

		CheckpointStatsSnapshot snapshot = graph.getCheckpointStatsSnapshot();
		if (snapshot == null) {
			return "{}";
		}

		AbstractCheckpointStats checkpoint = snapshot.getHistory().getCheckpointById(checkpointId);

		if (checkpoint != null) {
			cache.tryAdd(checkpoint);
		} else {
			checkpoint = cache.tryGet(checkpointId);

			if (checkpoint == null) {
				return "{}";
			}
		}
		
		StringWriter writer = new StringWriter();
		JsonGenerator gen = JsonFactory.jacksonFactory.createGenerator(writer);

		JsonUtils.writeCheckpointDetailsAsJson(checkpoint, gen);

		gen.close();

		return writer.toString();
	}

	/**
	 * Returns the checkpoint ID parsed from the provided parameters.
	 *
	 * @param params Path parameters
	 * @return Parsed checkpoint ID or <code>-1</code> if not available.
	 */
	static long parseCheckpointId(Map<String, String> params) {
		String param = params.get("checkpointid");
		if (param == null) {
			return -1;
		}

		try {
			return Long.parseLong(param);
		} catch (NumberFormatException ignored) {
			return -1;
		}
	}
}
