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

package org.apache.flink.table.client.gateway.local;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobSubmissionResult;
import org.apache.flink.client.deployment.ClusterDescriptor;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.table.client.gateway.SqlExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * The helper class to deploy a table program on the cluster.
 */
public class ProgramDeployer<C> implements Runnable {
	private static final Logger LOG = LoggerFactory.getLogger(ProgramDeployer.class);

	private final ExecutionContext<C> context;
	private final JobGraph jobGraph;
	private final String jobName;
	private final DynamicResult<C> result;
	private final BlockingQueue<JobExecutionResult> executionResultBucket;

	public ProgramDeployer(
			ExecutionContext<C> context,
			String jobName,
			JobGraph jobGraph,
			DynamicResult<C> result) {
		this.context = context;
		this.jobGraph = jobGraph;
		this.jobName = jobName;
		this.result = result;
		executionResultBucket = new LinkedBlockingDeque<>(1);
	}

	@Override
	public void run() {
		LOG.info("Submitting job {} for query {}`", jobGraph.getJobID(), jobName);
		if (LOG.isDebugEnabled()) {
			LOG.debug("Submitting job {} with the following environment: \n{}",
					jobGraph.getJobID(), context.getMergedEnvironment());
		}
		if (result != null) {
			executionResultBucket.add(deployJob(context, jobGraph, result));
		} else {
			deployJob(context, jobGraph, result);
		}
	}

	public JobExecutionResult fetchExecutionResult() {
		if (result != null) {
			return executionResultBucket.poll();
		} else {
			return null;
		}
	}

	/**
	 * Deploys a job. Depending on the deployment creates a new job cluster. If result is requested,
	 * it saves the cluster id in the result and blocks until job completion.
	 */
	private <T> JobExecutionResult deployJob(ExecutionContext<T> context, JobGraph jobGraph, DynamicResult<T> result) {
		final boolean retrieveResults = result != null;
		// create or retrieve cluster and deploy job
		try (final ClusterDescriptor<T> clusterDescriptor = context.createClusterDescriptor()) {
			ClusterClient<T> clusterClient = null;
			try {
				// new cluster
				if (context.getClusterId() == null) {
					// deploy job cluster, attach the job if result is requested
					clusterClient = clusterDescriptor.deployJobCluster(
							context.getClusterSpec(), jobGraph, !retrieveResults);
					if (retrieveResults) {
						// save the new cluster id
						result.setClusterId(clusterClient.getClusterId());
						// we need to hard cast for now
						return ((RestClusterClient<T>) clusterClient)
								.requestJobResult(jobGraph.getJobID())
								.get()
								.toJobExecutionResult(context.getClassLoader()); // throws exception if job fails
					} else {
						return null;
					}
				}
				// reuse existing cluster
				else {
					// retrieve existing cluster
					clusterClient = clusterDescriptor.retrieve(context.getClusterId());
					if (retrieveResults) {
						// save the cluster id
						result.setClusterId(clusterClient.getClusterId());
					}
					// submit the job
					clusterClient.setDetached(!retrieveResults);
					JobSubmissionResult submissionResult =
							clusterClient.submitJob(jobGraph, context.getClassLoader());
					if (retrieveResults) {
						return submissionResult.getJobExecutionResult();
					} else {
						return null;
					}
				}
			} catch (Exception e) {
				throw new SqlExecutionException("Could not retrieve or create a cluster.", e);
			} finally {
				try {
					if (clusterClient != null) {
						clusterClient.shutdown();
					}
				} catch (Exception e) {
					// ignore
				}
			}
		} catch (SqlExecutionException e) {
			throw e;
		} catch (Exception e) {
			throw new SqlExecutionException("Could not locate a cluster.", e);
		}
	}
}

