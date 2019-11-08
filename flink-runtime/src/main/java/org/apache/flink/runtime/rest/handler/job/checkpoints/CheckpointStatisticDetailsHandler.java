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

package org.apache.flink.runtime.rest.handler.job.checkpoints;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.checkpoint.AbstractCheckpointStats;
import org.apache.flink.runtime.checkpoint.CheckpointStatsHistory;
import org.apache.flink.runtime.checkpoint.CheckpointStatsSnapshot;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.legacy.ExecutionGraphCache;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.rest.messages.checkpoints.CheckpointIdPathParameter;
import org.apache.flink.runtime.rest.messages.checkpoints.CheckpointMessageParameters;
import org.apache.flink.runtime.rest.messages.checkpoints.CheckpointStatistics;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.history.ArchivedJson;
import org.apache.flink.runtime.webmonitor.history.JsonArchivist;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

/**
 * REST handler which returns the details for a checkpoint.
 */
public class CheckpointStatisticDetailsHandler extends AbstractCheckpointHandler<CheckpointStatistics, CheckpointMessageParameters> implements JsonArchivist {

	public CheckpointStatisticDetailsHandler(
			GatewayRetriever<? extends RestfulGateway> leaderRetriever,
			Time timeout,
			Map<String, String> responseHeaders,
			MessageHeaders<EmptyRequestBody, CheckpointStatistics, CheckpointMessageParameters> messageHeaders,
			ExecutionGraphCache executionGraphCache,
			Executor executor,
			CheckpointStatsCache checkpointStatsCache) {
		super(
			leaderRetriever,
			timeout,
			responseHeaders,
			messageHeaders,
			executionGraphCache,
			executor,
			checkpointStatsCache);
	}

	@Override
	protected CheckpointStatistics handleCheckpointRequest(HandlerRequest<EmptyRequestBody, CheckpointMessageParameters> ignored, AbstractCheckpointStats checkpointStats) {
		return CheckpointStatistics.generateCheckpointStatistics(checkpointStats, true);
	}

	@Override
	public Collection<ArchivedJson> archiveJsonWithPath(AccessExecutionGraph graph) throws IOException {
		CheckpointStatsSnapshot stats = graph.getCheckpointStatsSnapshot();
		if (stats == null) {
			return Collections.emptyList();
		}
		CheckpointStatsHistory history = stats.getHistory();
		List<ArchivedJson> archive = new ArrayList<>(history.getCheckpoints().size());
		for (AbstractCheckpointStats checkpoint : history.getCheckpoints()) {
			ResponseBody json = CheckpointStatistics.generateCheckpointStatistics(checkpoint, true);
			String path = getMessageHeaders().getTargetRestEndpointURL()
				.replace(':' + JobIDPathParameter.KEY, graph.getJobID().toString())
				.replace(':' + CheckpointIdPathParameter.KEY, String.valueOf(checkpoint.getCheckpointId()));
			archive.add(new ArchivedJson(path, json));
		}
		return archive;
	}
}
