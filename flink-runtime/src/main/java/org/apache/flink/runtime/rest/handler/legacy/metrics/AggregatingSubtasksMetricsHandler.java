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

package org.apache.flink.runtime.rest.handler.legacy.metrics;

import org.apache.flink.util.UnionIterator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.Executor;

import static org.apache.flink.runtime.rest.handler.legacy.metrics.JobMetricsHandler.PARAMETER_JOB_ID;
import static org.apache.flink.runtime.rest.handler.legacy.metrics.JobVertexMetricsHandler.PARAMETER_VERTEX_ID;

/**
 * Request handler that returns, aggregated across all subtasks, a list of all available metrics or the values
 * for a set of metrics.
 *
 * <p>Specific subtasks can be selected for aggregation by specifying a comma-separated list of integer ranges.
 * {@code /metrics?get=X,Y&subtasks=0-2,4-5}
 */
public class AggregatingSubtasksMetricsHandler extends AbstractAggregatingMetricsHandler {
	public AggregatingSubtasksMetricsHandler(Executor executor, MetricFetcher fetcher) {
		super(executor, fetcher);
	}

	@Override
	protected Collection<? extends MetricStore.ComponentMetricStore> getStores(MetricStore store, Map<String, String> pathParameters, Map<String, String> queryParameters) {
		String jobID = pathParameters.get(PARAMETER_JOB_ID);
		String taskID = pathParameters.get(PARAMETER_VERTEX_ID);
		if (jobID == null) {
			return Collections.emptyList();
		}
		if (taskID == null) {
			return Collections.emptyList();
		}
		String subtasksList = queryParameters.get("subtasks");
		if (subtasksList == null || subtasksList.isEmpty()) {
			return store.getTaskMetricStore(jobID, taskID).getAllSubtaskMetricStores();
		} else {
			Iterable<Integer> subtasks = getIntegerRangeFromString(subtasksList);
			Collection<MetricStore.ComponentMetricStore> subtaskStores = new ArrayList<>();
			for (int subtask : subtasks) {
				subtaskStores.add(store.getSubtaskMetricStore(jobID, taskID, subtask));
			}
			return subtaskStores;
		}
	}

	@Override
	public String[] getPaths() {
		return new String[]{"/jobs/:jobid/vertices/:vertexid/subtasks/metrics"};
	}

	private Iterable<Integer> getIntegerRangeFromString(String rangeDefinition) {
		final String[] ranges = rangeDefinition.trim().split(",");

		UnionIterator<Integer> iterators = new UnionIterator<>();

		for (String rawRange : ranges) {
			try {
				Iterator<Integer> rangeIterator;
				String range = rawRange.trim();
				int dashIdx = range.indexOf('-');
				if (dashIdx == -1) {
					// only one value in range:
					rangeIterator = Collections.singleton(Integer.valueOf(range)).iterator();
				} else {
					// evaluate range
					final int start = Integer.valueOf(range.substring(0, dashIdx));
					final int end = Integer.valueOf(range.substring(dashIdx + 1, range.length()));
					rangeIterator = new Iterator<Integer>() {
						int i = start;

						@Override
						public boolean hasNext() {
							return i <= end;
						}

						@Override
						public Integer next() {
							if (hasNext()) {
								return i++;
							} else {
								throw new NoSuchElementException();
							}
						}

						@Override
						public void remove() {
							throw new UnsupportedOperationException("Remove not supported");
						}
					};
				}
				iterators.add(rangeIterator);
			} catch (NumberFormatException nfe) {
				log.warn("Invalid value {} specified for integer range. Not a number.", rawRange, nfe);
			}
		}

		return iterators;
	}
}
