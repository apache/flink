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

package org.apache.flink.runtime.rest.handler.job.metrics;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.metrics.dump.MetricDump;
import org.apache.flink.runtime.metrics.dump.QueryScopeInfo;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricFetcher;
import org.apache.flink.runtime.rest.messages.job.metrics.AggregateTaskManagerMetricsParameters;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

/**
 * Tests for the {@link AggregatingTaskManagersMetricsHandler}.
 */
public class AggregatingTaskManagersMetricsHandlerTest extends AggregatingMetricsHandlerTestBase<AggregatingTaskManagersMetricsHandler, AggregateTaskManagerMetricsParameters> {

	private static final ResourceID TM_ID_1 = ResourceID.generate();
	private static final ResourceID TM_ID_2 = ResourceID.generate();
	private static final ResourceID TM_ID_3 = ResourceID.generate();

	@Override
	protected Tuple2<String, List<String>> getFilter() {
		return Tuple2.of("taskmanagers", Arrays.asList(TM_ID_1.toString(), TM_ID_3.toString()));
	}

	@Override
	protected Collection<MetricDump> getMetricDumps() {
		Collection<MetricDump> dumps = new ArrayList<>(3);
		QueryScopeInfo.TaskManagerQueryScopeInfo tm1 = new QueryScopeInfo.TaskManagerQueryScopeInfo(TM_ID_1.toString(), "abc");
		MetricDump.CounterDump cd1 = new MetricDump.CounterDump(tm1, "metric1", 1);
		dumps.add(cd1);

		QueryScopeInfo.TaskManagerQueryScopeInfo tm2 = new QueryScopeInfo.TaskManagerQueryScopeInfo(TM_ID_2.toString(), "abc");
		MetricDump.CounterDump cd2 = new MetricDump.CounterDump(tm2, "metric1", 3);
		dumps.add(cd2);

		QueryScopeInfo.TaskManagerQueryScopeInfo tm3 = new QueryScopeInfo.TaskManagerQueryScopeInfo(TM_ID_3.toString(), "abc");
		MetricDump.CounterDump cd3 = new MetricDump.CounterDump(tm3, "metric2", 5);
		dumps.add(cd3);

		return dumps;
	}

	@Override
	protected AggregatingTaskManagersMetricsHandler getHandler(GatewayRetriever<? extends RestfulGateway> leaderRetriever, Time timeout, Map<String, String> responseHeaders, Executor executor, MetricFetcher fetcher) {
		return new AggregatingTaskManagersMetricsHandler(
			leaderRetriever,
			timeout,
			responseHeaders,
			executor,
			fetcher
		);
	}
}
