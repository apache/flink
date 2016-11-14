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
package org.apache.flink.runtime.webmonitor.metrics;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.dispatch.OnFailure;
import akka.dispatch.OnSuccess;
import akka.pattern.Patterns;
import akka.util.Timeout;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.instance.Instance;
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.messages.webmonitor.MultipleJobsDetails;
import org.apache.flink.runtime.messages.webmonitor.RequestJobDetails;
import org.apache.flink.runtime.metrics.dump.MetricQueryService;
import org.apache.flink.runtime.metrics.dump.MetricDump;
import org.apache.flink.runtime.webmonitor.JobManagerRetriever;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.concurrent.ExecutionContext;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;
import scala.concurrent.duration.FiniteDuration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.runtime.metrics.dump.MetricDumpSerialization.MetricDumpDeserializer;

/**
 * The MetricFetcher can be used to fetch metrics from the JobManager and all registered TaskManagers.
 *
 * Metrics will only be fetched when {@link MetricFetcher#update()} is called, provided that a sufficient time since
 * the last call has passed.
 */
public class MetricFetcher {
	private static final Logger LOG = LoggerFactory.getLogger(MetricFetcher.class);

	private final ActorSystem actorSystem;
	private final JobManagerRetriever retriever;
	private final ExecutionContext ctx;
	private final FiniteDuration timeout = new FiniteDuration(Duration.create(ConfigConstants.DEFAULT_AKKA_ASK_TIMEOUT).toMillis(), TimeUnit.MILLISECONDS);

	private MetricStore metrics = new MetricStore();
	private MetricDumpDeserializer deserializer = new MetricDumpDeserializer();

	private long lastUpdateTime;

	public MetricFetcher(ActorSystem actorSystem, JobManagerRetriever retriever, ExecutionContext ctx) {
		this.actorSystem = Preconditions.checkNotNull(actorSystem);
		this.retriever = Preconditions.checkNotNull(retriever);
		this.ctx = Preconditions.checkNotNull(ctx);
	}

	/**
	 * Returns the MetricStore containing all stored metrics.
	 *
	 * @return MetricStore containing all stored metrics;
	 */
	public MetricStore getMetricStore() {
		return metrics;
	}

	/**
	 * This method can be used to signal this MetricFetcher that the metrics are still in use and should be updated.
	 */
	public void update() {
		synchronized (this) {
			long currentTime = System.currentTimeMillis();
			if (currentTime - lastUpdateTime > 10000) { // 10 seconds have passed since the last update
				lastUpdateTime = currentTime;
				fetchMetrics();
			}
		}
	}

	private void fetchMetrics() {
		try {
			Option<scala.Tuple2<ActorGateway, Integer>> jobManagerGatewayAndWebPort = retriever.getJobManagerGatewayAndWebPort();
			if (jobManagerGatewayAndWebPort.isDefined()) {
				ActorGateway jobManager = jobManagerGatewayAndWebPort.get()._1();

				/**
				 * Remove all metrics that belong to a job that is not running and no longer archived.
				 */
				Future<Object> jobDetailsFuture = jobManager.ask(new RequestJobDetails(true, true), timeout);
				jobDetailsFuture
					.onSuccess(new OnSuccess<Object>() {
						@Override
						public void onSuccess(Object result) throws Throwable {
							MultipleJobsDetails details = (MultipleJobsDetails) result;
							ArrayList<String> toRetain = new ArrayList<>();
							for (JobDetails job : details.getRunningJobs()) {
								toRetain.add(job.getJobId().toString());
							}
							for (JobDetails job : details.getFinishedJobs()) {
								toRetain.add(job.getJobId().toString());
							}
							synchronized (metrics) {
								metrics.jobs.keySet().retainAll(toRetain);
							}
						}
					}, ctx);
				logErrorOnFailure(jobDetailsFuture, "Fetching of JobDetails failed.");

				String jobManagerPath = jobManager.path();
				String queryServicePath = jobManagerPath.substring(0, jobManagerPath.lastIndexOf('/') + 1) + MetricQueryService.METRIC_QUERY_SERVICE_NAME;
				ActorRef jobManagerQueryService = actorSystem.actorFor(queryServicePath);

				queryMetrics(jobManagerQueryService);

				/**
				 * We first request the list of all registered task managers from the job manager, and then
				 * request the respective metric dump from each task manager.
				 *
				 * All stored metrics that do not belong to a registered task manager will be removed.
				 */
				Future<Object> registeredTaskManagersFuture = jobManager.ask(JobManagerMessages.getRequestRegisteredTaskManagers(), timeout);
				registeredTaskManagersFuture
					.onSuccess(new OnSuccess<Object>() {
						@Override
						public void onSuccess(Object result) throws Throwable {
							Iterable<Instance> taskManagers = ((JobManagerMessages.RegisteredTaskManagers) result).asJavaIterable();
							List<String> activeTaskManagers = new ArrayList<>();
							for (Instance taskManager : taskManagers) {
								activeTaskManagers.add(taskManager.getId().toString());

								String taskManagerPath = taskManager.getTaskManagerGateway().getAddress();
								String queryServicePath = taskManagerPath.substring(0, taskManagerPath.lastIndexOf('/') + 1) + MetricQueryService.METRIC_QUERY_SERVICE_NAME + "_" + taskManager.getTaskManagerID().getResourceIdString();
								ActorRef taskManagerQueryService = actorSystem.actorFor(queryServicePath);

								queryMetrics(taskManagerQueryService);
							}
							synchronized (metrics) { // remove all metrics belonging to unregistered task managers
								metrics.taskManagers.keySet().retainAll(activeTaskManagers);
							}
						}
					}, ctx);
				logErrorOnFailure(registeredTaskManagersFuture, "Fetchin list of registered TaskManagers failed.");
			}
		} catch (Exception e) {
			LOG.warn("Exception while fetching metrics.", e);
		}
	}

	private void logErrorOnFailure(Future<Object> future, final String message) {
		future.onFailure(new OnFailure() {
			@Override
			public void onFailure(Throwable failure) throws Throwable {
				LOG.warn(message, failure);
			}
		}, ctx);
	}

	/**
	 * Requests a metric dump from the given actor.
	 *
	 * @param actor ActorRef to request the dump from
     */
	private void queryMetrics(ActorRef actor) {
		Future<Object> metricQueryFuture = new BasicGateway(actor).ask(MetricQueryService.getCreateDump(), timeout);
		metricQueryFuture
			.onSuccess(new OnSuccess<Object>() {
				@Override
				public void onSuccess(Object result) throws Throwable {
					addMetrics(result);
				}
			}, ctx);
		logErrorOnFailure(metricQueryFuture, "Fetching metrics failed.");
	}

	private void addMetrics(Object result) throws IOException {
		byte[] data = (byte[]) result;
		List<MetricDump> dumpedMetrics = deserializer.deserialize(data);
		for (MetricDump metric : dumpedMetrics) {
			metrics.add(metric);
		}
	}

	/**
	 * Helper class that allows mocking of the answer.
     */
	static class BasicGateway {
		private final ActorRef actor;

		private BasicGateway(ActorRef actor) {
			this.actor = actor;
		}

		/**
		 * Sends a message asynchronously and returns its response. The response to the message is
		 * returned as a future.
		 *
		 * @param message Message to be sent
		 * @param timeout Timeout until the Future is completed with an AskTimeoutException
		 * @return Future which contains the response to the sent message
		 */
		public Future<Object> ask(Object message, FiniteDuration timeout) {
			return Patterns.ask(actor, message, new Timeout(timeout));
		}
	}
}
