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

package org.apache.flink.tez.client;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.PlanExecutor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.optimizer.CompilerException;
import org.apache.flink.optimizer.Optimizer;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plandump.PlanJSONDumpGenerator;
import org.apache.flink.tez.dag.TezDAGGenerator;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.tez.client.TezClient;
import org.apache.tez.client.TezClientUtils;
import org.apache.tez.common.TezCommonUtils;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;

import java.util.Map;
import java.util.TreeMap;

public class TezExecutor extends PlanExecutor {

	private static final Log LOG = LogFactory.getLog(TezExecutor.class);

	private TezConfiguration tezConf;
	private Optimizer compiler;
	
	private Path jarPath;

	private long runTime = -1; //TODO get DAG execution time from Tez
	private int parallelism;

	public TezExecutor(TezConfiguration tezConf, Optimizer compiler, int parallelism) {
		this.tezConf = tezConf;
		this.compiler = compiler;
		this.parallelism = parallelism;
	}

	public TezExecutor(Optimizer compiler, int parallelism) {
		this.tezConf = null;
		this.compiler = compiler;
		this.parallelism = parallelism;
	}

	public void setConfiguration (TezConfiguration tezConf) {
		this.tezConf = tezConf;
	}

	private JobExecutionResult executePlanWithConf (TezConfiguration tezConf, Plan plan) throws Exception {

		String jobName = plan.getJobName();

		TezClient tezClient = TezClient.create(jobName, tezConf);
		tezClient.start();
		try {
			OptimizedPlan optPlan = getOptimizedPlan(plan, parallelism);
			TezDAGGenerator dagGenerator = new TezDAGGenerator(tezConf, new Configuration());
			DAG dag = dagGenerator.createDAG(optPlan);

			if (jarPath != null) {
				addLocalResource(tezConf, jarPath, dag);
			}

			tezClient.waitTillReady();
			LOG.info("Submitting DAG to Tez Client");
			DAGClient dagClient = tezClient.submitDAG(dag);

			LOG.info("Submitted DAG to Tez Client");

			// monitoring
			DAGStatus dagStatus = dagClient.waitForCompletion();

			if (dagStatus.getState() != DAGStatus.State.SUCCEEDED) {
				LOG.error (jobName + " failed with diagnostics: " + dagStatus.getDiagnostics());
				throw new RuntimeException(jobName + " failed with diagnostics: " + dagStatus.getDiagnostics());
			}
			LOG.info(jobName + " finished successfully");

			return new JobExecutionResult(null, runTime, null);

		}
		finally {
			tezClient.stop();
		}
	}

	@Override
	public void start() throws Exception {
		throw new IllegalStateException("Session management is not supported in the TezExecutor.");
	}

	@Override
	public void stop() throws Exception {
		throw new IllegalStateException("Session management is not supported in the TezExecutor.");
	}

	@Override
	public void endSession(JobID jobID) throws Exception {
		throw new IllegalStateException("Session management is not supported in the TezExecutor.");
	}

	@Override
	public boolean isRunning() {
		return false;
	}

	@Override
	public JobExecutionResult executePlan(Plan plan) throws Exception {
		return executePlanWithConf(tezConf, plan);
	}
	
	private static void addLocalResource (TezConfiguration tezConf, Path jarPath, DAG dag) {
		
		try {
			org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.get(tezConf);

			LOG.info("Jar path received is " + jarPath.toString());

			String jarFile = jarPath.getName();

			Path remoteJarPath = null;
			
			/*
			if (tezConf.get(TezConfiguration.TEZ_AM_STAGING_DIR) == null) {
				LOG.info("Tez staging directory is null, setting it.");
				Path stagingDir = new Path(fs.getWorkingDirectory(), UUID.randomUUID().toString());
				LOG.info("Setting Tez staging directory to " + stagingDir.toString());
				tezConf.set(TezConfiguration.TEZ_AM_STAGING_DIR, stagingDir.toString());
				LOG.info("Set Tez staging directory to " + stagingDir.toString());
			}
			Path stagingDir = new Path(tezConf.get(TezConfiguration.TEZ_AM_STAGING_DIR));
			LOG.info("Ensuring that Tez staging directory exists");
			TezClientUtils.ensureStagingDirExists(tezConf, stagingDir);
			LOG.info("Tez staging directory exists and is " + stagingDir.toString());
			*/


			Path stagingDir = TezCommonUtils.getTezBaseStagingPath(tezConf);
			LOG.info("Tez staging path is " + stagingDir);
			TezClientUtils.ensureStagingDirExists(tezConf, stagingDir);
			LOG.info("Tez staging dir exists");
			
			remoteJarPath = fs.makeQualified(new Path(stagingDir, jarFile));
			LOG.info("Copying " + jarPath.toString() + " to " + remoteJarPath.toString());
			fs.copyFromLocalFile(jarPath, remoteJarPath);


			FileStatus remoteJarStatus = fs.getFileStatus(remoteJarPath);
			Credentials credentials = new Credentials();
			TokenCache.obtainTokensForNamenodes(credentials, new Path[]{remoteJarPath}, tezConf);

			Map<String, LocalResource> localResources = new TreeMap<String, LocalResource>();
			LocalResource jobJar = LocalResource.newInstance(
					ConverterUtils.getYarnUrlFromPath(remoteJarPath),
					LocalResourceType.FILE, LocalResourceVisibility.APPLICATION,
					remoteJarStatus.getLen(), remoteJarStatus.getModificationTime());
			localResources.put(jarFile.toString(), jobJar);

			dag.addTaskLocalFiles(localResources);

			LOG.info("Added job jar as local resource.");
		}
		catch (Exception e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
			System.exit(-1);
		}
	}
	
	public void setJobJar (Path jarPath) {
		this.jarPath = jarPath;
	}


	@Override
	public String getOptimizerPlanAsJSON(Plan plan) throws Exception {
		OptimizedPlan optPlan = getOptimizedPlan(plan, parallelism);
		PlanJSONDumpGenerator jsonGen = new PlanJSONDumpGenerator();
		return jsonGen.getOptimizerPlanAsJSON(optPlan);
	}

	public OptimizedPlan getOptimizedPlan(Plan p, int parallelism) throws CompilerException {
		if (parallelism > 0 && p.getDefaultParallelism() <= 0) {
			p.setDefaultParallelism(parallelism);
		}
		return this.compiler.compile(p);
	}
}
