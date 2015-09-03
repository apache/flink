/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.flink.languagebinding.api.java.python;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.Plan;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.ExecutionEnvironmentFactory;
import org.apache.flink.api.java.LocalEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import static org.apache.flink.languagebinding.api.java.python.PythonPlanBinder.ARGUMENT_PYTHON_2;
import static org.apache.flink.languagebinding.api.java.python.PythonPlanBinder.ARGUMENT_PYTHON_3;

import org.apache.flink.optimizer.DataStatistics;
import org.apache.flink.optimizer.Optimizer;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plandump.PlanJSONDumpGenerator;
import org.apache.flink.optimizer.plantranslate.JobGraphGenerator;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.minicluster.LocalFlinkMiniCluster;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PythonPlanBinderTest {
	private static final Logger LOG = LoggerFactory.getLogger(PythonPlanBinder.class);
	
	private static boolean python2Supported = true;
	private static boolean python3Supported = true;
	private static List<String> TEST_FILES;

	private static Configuration config = new Configuration();
	private static LocalFlinkMiniCluster cluster = new LocalFlinkMiniCluster(config, true);
	
	@BeforeClass
	public static void setup() throws Exception {
		findTestFiles();
		checkPythonSupport();
		cluster.start();
		new PythonTestEnvironment(cluster, 1).setAsContext();
	}
	
	private static void findTestFiles() throws Exception {
		TEST_FILES = new ArrayList();
		FileSystem fs = FileSystem.getLocalFileSystem();
		FileStatus[] status = fs.listStatus(
				new Path(fs.getWorkingDirectory().toString()
						+ "/src/test/python/org/apache/flink/languagebinding/api/python/flink/test"));
		for (FileStatus f : status) {
			String file = f.getPath().toString();
			if (file.endsWith(".py")) {
				TEST_FILES.add(file);
			}
		}
	}
	
	private static void checkPythonSupport() {	
		try {
			Runtime.getRuntime().exec("python");
		} catch (IOException ex) {
			python2Supported = false;
			LOG.info("No Python 2 runtime detected.");
		}
		try {
			Runtime.getRuntime().exec("python3");
		} catch (IOException ex) {
			python3Supported = false;
			LOG.info("No Python 3 runtime detected.");
		}
	}
	
	@Test
	public void testPython2() throws Exception {
		if (python2Supported) {
			for (String file : TEST_FILES) {
				LOG.info("testing " + file);
				PythonPlanBinder.main(new String[]{ARGUMENT_PYTHON_2, file});
			}
		}
	}
	
	@Test
	public void testPython3() throws Exception {
		if (python3Supported) {
			for (String file : TEST_FILES) {
				LOG.info("testing " + file);
				PythonPlanBinder.main(new String[]{ARGUMENT_PYTHON_3, file});
			}
		}
	}

	@AfterClass
	public static void tearDown() {
		cluster.stop();
	}

	private static final class PythonTestEnvironment extends LocalEnvironment {

		private final LocalFlinkMiniCluster executor;

		public PythonTestEnvironment(LocalFlinkMiniCluster executor, int parallelism) {
			this.executor = executor;
			setParallelism(parallelism);
		}

		@Override
		public JobExecutionResult execute(String jobName) throws Exception {
			try {
				OptimizedPlan op = compileProgram(jobName);

				JobGraphGenerator jgg = new JobGraphGenerator();
				JobGraph jobGraph = jgg.compileJobGraph(op);

				this.lastJobExecutionResult = executor.submitJobAndWait(jobGraph, false);
				return this.lastJobExecutionResult;
			}
			catch (Exception e) {
				System.err.println(e.getMessage());
				e.printStackTrace();
				Assert.fail("Job execution failed!");
				return null;
			}
		}


		@Override
		public String getExecutionPlan() throws Exception {
			OptimizedPlan op = compileProgram("unused");

			PlanJSONDumpGenerator jsonGen = new PlanJSONDumpGenerator();
			return jsonGen.getOptimizerPlanAsJSON(op);
		}

		private OptimizedPlan compileProgram(String jobName) {
			Plan p = createProgramPlan(jobName);

			Optimizer pc = new Optimizer(new DataStatistics(), this.executor.configuration());
			return pc.compile(p);
		}

		public void setAsContext() {
			ExecutionEnvironmentFactory factory = new ExecutionEnvironmentFactory() {
				@Override
				public ExecutionEnvironment createExecutionEnvironment() {
					return new PythonTestEnvironment(executor, getParallelism());
				}
			};

			initializeContextEnvironment(factory);
		}
	}
}
