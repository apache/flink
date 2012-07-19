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
package eu.stratosphere.meteor.server;

import java.io.File;

import org.apache.commons.cli.CommandLine;

import eu.stratosphere.meteor.MeteorParser;
import eu.stratosphere.meteor.QueryParser;
import eu.stratosphere.meteor.execution.ExecutionResponse.ExecutionStatus;
import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.pact.client.nephele.api.Client;
import eu.stratosphere.pact.client.nephele.api.ErrorInPlanAssemblerException;
import eu.stratosphere.pact.client.nephele.api.PactProgram;
import eu.stratosphere.pact.client.nephele.api.ProgramInvocationException;
import eu.stratosphere.sopremo.operator.SopremoPlan;
import eu.stratosphere.sopremo.query.QueryParserException;

/**
 * @author Arvid Heise
 */
public class MeteorExecutionThread implements Runnable {
	private MeteorExecutionEnvironment environment;

	public MeteorExecutionThread(MeteorExecutionEnvironment environment) {
		this.environment = environment;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public void run() {
		QueryParser parser = new QueryParser();
		try {
			final SopremoPlan plan = parser.tryParse(this.environment.getInitialRequest().getQuery());

			processPlan(plan);
		} catch (QueryParserException e) {
			e.printStackTrace();
		}
	}

	private void processPlan(SopremoPlan plan) {
		switch (this.environment.getInitialRequest().getMode()) {
		case GENERATE_PLAN:
			setStatus(ExecutionStatus.FINISHED, plan.toString());
			break;
		case RUN:
			executePlan(plan);
			break;
		case RUN_WITH_STATISTICS:
			executePlan(plan);
			gatherStatistics(plan);
			break;
		}
	}

	private void executePlan(SopremoPlan plan) {

		File jarFile = new File(jarFilePath);
		String assemblerClass = null;
		String[] programArgs = null;
		boolean wait = false;
		
		// Parse command line options
		CommandLine line = null;
		try {
			line = parser.parse(this.options.get(ACTION_RUN), args, false);
		} catch (Exception e) {
			handleError(e);
		}
		
		// Get jar file
		if (line.hasOption(JAR_OPTION.getOpt())) {
			String jarFilePath = line.getOptionValue(JAR_OPTION.getOpt());
			jarFile = 
			
			// Check if JAR file exists
			if(!jarFile.exists()) {
				System.err.println("Error: Jar file does not exist.");
				printHelp();
				System.exit(1);
			} else if(!jarFile.isFile()) {
				System.err.println("Error: Jar file is not a file.");
				printHelp();
				System.exit(1);
			}
		} else {
			System.err.println("Error: Jar file is not set.");	
			printHelp();
			System.exit(1);
		}
		
		// Get assembler class
		if(line.hasOption(CLASS_OPTION.getOpt())) {
			assemblerClass = line.getOptionValue(CLASS_OPTION.getOpt());
		}
		
		// get program arguments
		if(line.hasOption(ARGS_OPTION.getOpt())) {
			programArgs = line.getOptionValues(ARGS_OPTION.getOpt());
		}
		
		// get wait flag
		wait = line.hasOption(WAIT_OPTION.getOpt());
		
		// Try to get load plan
		PactProgram program = null;
		try {
			if (assemblerClass == null) {
				program = new PactProgram(jarFile, programArgs);
			} else {
				program = new PactProgram(jarFile, assemblerClass, programArgs);
			}
		} catch (ProgramInvocationException e) {
			handleError(e);
		}

		Configuration configuration = getConfiguration();
		Client client = new Client(configuration);
		try {
			final JobGraph jobGraph = client.getJobGraph(program, client.getOptimizedPlan(program));
			jobGraph.addJar(null);
			client.run(program,jobGraph, wait);
		} catch (ProgramInvocationException e) {
			handleError(e);
		} catch (ErrorInPlanAssemblerException e) {
			handleError(e);
		}

	}

	private void setStatus(ExecutionStatus status, String detail) {
	}

}
