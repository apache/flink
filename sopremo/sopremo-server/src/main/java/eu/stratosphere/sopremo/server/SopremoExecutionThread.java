/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/
package eu.stratosphere.sopremo.server;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.client.JobClient;
import eu.stratosphere.nephele.fs.FileSystem;
import eu.stratosphere.nephele.fs.Path;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.util.StringUtils;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.compiler.DataStatistics;
import eu.stratosphere.pact.compiler.PactCompiler;
import eu.stratosphere.pact.compiler.costs.FixedSizeClusterCostEstimator;
import eu.stratosphere.pact.compiler.jobgen.JobGraphGenerator;
import eu.stratosphere.pact.compiler.plan.OptimizedPlan;
import eu.stratosphere.sopremo.execution.ExecutionResponse.ExecutionState;
import eu.stratosphere.sopremo.io.Sink;
import eu.stratosphere.sopremo.operator.Operator;
import eu.stratosphere.sopremo.operator.SopremoPlan;

/**
 * @author Arvid Heise
 */
public class SopremoExecutionThread implements Runnable {
	private SopremoJobInfo jobInfo;

	private final InetSocketAddress jobManagerAddress;

	/**
	 * The logging object used for debugging.
	 */
	private static final Log LOG = LogFactory.getLog(JobClient.class);

	public SopremoExecutionThread(SopremoJobInfo environment, InetSocketAddress jobManagerAddress) {
		this.jobInfo = environment;
		this.jobManagerAddress = jobManagerAddress;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public void run() {
		processPlan(this.jobInfo.getInitialRequest().getQuery());
	}

	private void processPlan(SopremoPlan plan) {
		try {
			switch (this.jobInfo.getInitialRequest().getMode()) {
			case RUN:
				if (executePlan(plan) != -1)
					this.jobInfo.setStatusAndDetail(ExecutionState.FINISHED, "");
				break;
			case RUN_WITH_STATISTICS:
				final long runtime = executePlan(plan);
				if (runtime != -1)
					gatherStatistics(plan, runtime);
				break;
			}
		} catch (Throwable ex) {
			this.jobInfo.setStatusAndDetail(ExecutionState.ERROR,
				"Cannot process plan: " + StringUtils.stringifyException(ex));
		}
	}

	/**
	 * @param plan
	 */
	private void gatherStatistics(SopremoPlan plan, long runtime) {
		StringBuilder statistics = new StringBuilder();
		statistics.append("Executed in ").append(runtime).append(" ms");
		for (Operator<?> op : plan.getContainedOperators())
			if (op instanceof Sink) {
				try {
					final String path = ((Sink) op).getOutputPath();
					final long length = FileSystem.get(new URI(path)).getFileStatus(new Path(path)).getLen();
					statistics.append("\n").append(path).append(": ").append(length).append(" B");
				} catch (Exception e) {
					LOG.warn(StringUtils.stringifyException(e));
				}
			}
		this.jobInfo.setStatusAndDetail(ExecutionState.FINISHED, statistics.toString());
	}

	private long executePlan(SopremoPlan plan) {
		final Plan pactPlan = plan.asPactPlan();

		JobGraph jobGraph;
		try {
			jobGraph = getJobGraph(pactPlan);
		} catch (Exception e) {
			this.jobInfo.setStatusAndDetail(ExecutionState.ERROR, "Could not generate job graph: " + e.getMessage());
			return -1;
		}

		JobClient client;
		try {
			client = new JobClient(jobGraph, this.jobInfo.getConfiguration(), this.jobManagerAddress);
		} catch (Exception e) {
			this.jobInfo.setStatusAndDetail(ExecutionState.ERROR, "Could not open job manager: " + e.getMessage());
			return -1;
		}

		try {
			this.jobInfo.setJobClient(client);
			this.jobInfo.setStatusAndDetail(ExecutionState.RUNNING, "");
			return client.submitJobAndWait();
		} catch (Exception ex) {
			this.jobInfo.setStatusAndDetail(ExecutionState.ERROR,
				"The job was not successfully submitted to the nephele job manager: "
					+ StringUtils.stringifyException(ex));
			return -1;
		}
	}

	JobGraph getJobGraph(final Plan pactPlan) {
		PactCompiler compiler =
			new PactCompiler(new DataStatistics(), new FixedSizeClusterCostEstimator(), this.jobManagerAddress);

		final OptimizedPlan optPlan = compiler.compile(pactPlan);
		JobGraphGenerator gen = new JobGraphGenerator();
		return gen.compileJobGraph(optPlan);
	}

}
