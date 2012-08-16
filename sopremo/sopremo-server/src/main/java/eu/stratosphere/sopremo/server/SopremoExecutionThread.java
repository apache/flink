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

import java.net.InetSocketAddress;
import java.net.URI;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.client.JobClient;
import eu.stratosphere.nephele.execution.librarycache.LibraryCacheManager;
import eu.stratosphere.nephele.fs.FileSystem;
import eu.stratosphere.nephele.fs.Path;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.util.StringUtils;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.compiler.DataStatistics;
import eu.stratosphere.pact.compiler.PactCompiler;
import eu.stratosphere.pact.compiler.costs.DefaultCostEstimator;
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
	private static final Log LOG = LogFactory.getLog(SopremoExecutionThread.class);

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
		processPlan();
	}

	private void processPlan() {
		try {
			LOG.info("Starting job " + this.jobInfo.getJobId());
			SopremoPlan plan = this.jobInfo.getInitialRequest().getQuery();
			final long runtime = executePlan(plan);
			if (runtime != -1) {
				switch (this.jobInfo.getInitialRequest().getMode()) {
				case RUN:
					this.jobInfo.setStatusAndDetail(ExecutionState.FINISHED, "");
					break;
				case RUN_WITH_STATISTICS:
					gatherStatistics(plan, runtime);
					break;
				}
				LOG.info(String.format("Finished job %s in %s ms", this.jobInfo.getJobId(), runtime));
			}
		} catch (Throwable ex) {
			LOG.error("Cannot process plan " + this.jobInfo.getJobId(), ex);
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
					statistics.append("\n").append("Sink ").append(path).append(": ").append(length).append(" B");
				} catch (Exception e) {
					LOG.warn("While gathering statistics", e);
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
			LOG.error("Could not generate job graph " + this.jobInfo.getJobId(), e);
			this.jobInfo.setStatusAndDetail(ExecutionState.ERROR, "Could not generate job graph: "
				+ StringUtils.stringifyException(e));
			return -1;
		}

		try {
			for (String requiredPackage : this.jobInfo.getInitialRequest().getQuery().getRequiredPackages())
				jobGraph.addJar(LibraryCacheManager.contains(requiredPackage));
		} catch (Exception e) {
			LOG.error("Could not find associated packages " + this.jobInfo.getJobId(), e);
			this.jobInfo.setStatusAndDetail(ExecutionState.ERROR, "Could not find associated packages: "
				+ StringUtils.stringifyException(e));
			return -1;
		}

		JobClient client;
		try {
			client = new JobClient(jobGraph, this.jobInfo.getConfiguration(), this.jobManagerAddress);
		} catch (Exception e) {
			LOG.error("Could not open job manager " + this.jobInfo.getJobId(), e);
			this.jobInfo.setStatusAndDetail(ExecutionState.ERROR, "Could not open job manager: "
				+ StringUtils.stringifyException(e));
			return -1;
		}

		try {
			this.jobInfo.setJobClient(client);
			this.jobInfo.setStatusAndDetail(ExecutionState.RUNNING, "");
			return client.submitJobAndWait();
		} catch (Exception e) {
			LOG.error("The job was not successfully executed " + this.jobInfo.getJobId(), e);
			this.jobInfo.setStatusAndDetail(ExecutionState.ERROR,
				"The job was not successfully executed: "
					+ StringUtils.stringifyException(e));
			return -1;
		}
	}

	JobGraph getJobGraph(final Plan pactPlan) {
		PactCompiler compiler =
			new PactCompiler(new DataStatistics(), new DefaultCostEstimator(), this.jobManagerAddress);

		final OptimizedPlan optPlan = compiler.compile(pactPlan);
		JobGraphGenerator gen = new JobGraphGenerator();
		return gen.compileJobGraph(optPlan);
	}

}
