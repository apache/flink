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

package eu.stratosphere.pact.client;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.cli.UnrecognizedOptionException;

import eu.stratosphere.nephele.configuration.ConfigConstants;
import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.event.job.RecentJobEvent;
import eu.stratosphere.nephele.ipc.RPC;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.jobgraph.JobStatus;
import eu.stratosphere.nephele.net.NetUtils;
import eu.stratosphere.nephele.protocols.ExtendedManagementProtocol;
import eu.stratosphere.nephele.util.StringUtils;
import eu.stratosphere.pact.client.nephele.api.Client;
import eu.stratosphere.pact.client.nephele.api.ErrorInPlanAssemblerException;
import eu.stratosphere.pact.client.nephele.api.PactProgram;
import eu.stratosphere.pact.client.nephele.api.ProgramInvocationException;
import eu.stratosphere.pact.compiler.CompilerException;

/**
 * Implementation of a simple command line fronted for executing PACT programs.
 */
public class CliFrontend {

	// actions
	private static final String ACTION_RUN = "run";
	private static final String ACTION_INFO = "info";
	private static final String ACTION_LIST = "list";
	private static final String ACTION_CANCEL = "cancel";
	
	private static final String GENERAL_OPTS = "general";
	
	// general options
	private static final Option HELP_OPTION = new Option("h", "help", false, "Show the help for the CLI Frontend.");
	private static final Option VERBOSE_OPTION = new Option("v", "verbose", false, "Print more detailed error messages.");
	
	// run options
	private static final Option JAR_OPTION = new Option("j", "jarfile", true, "Pact program JAR file");
	private static final Option CLASS_OPTION = new Option("c", "class", true, "Pact program assembler class");
	private static final Option ARGS_OPTION = new Option("a", "arguments", true, "Pact program arguments");
	private static final Option WAIT_OPTION = new Option("w", "wait", false, "Wait until program finishes");
	private static final Option ADDRESS_OPTION = new Option("m", "jobmanager", true, "Hostname:port of JobManager [optional]");
	
	// info options
	private static final Option DESCR_OPTION = new Option("d", "description", false, "Show argument description of pact program");
	private static final Option PLAN_OPTION = new Option("p", "plan", false, "Show execution plan of the pact program");
	
	// list options
	private static final Option RUNNING_OPTION = new Option("r", "running", false, "Show running jobs");
	private static final Option SCHEDULED_OPTION = new Option("s", "scheduled", false, "Show scheduled jobs");
	
	// cancel options
	private static final Option ID_OPTION = new Option("i", "jobid", true, "JobID to cancel");
	
	// config dir parameters
	private static final String DEFAULT_CONFIG_DIRECTORY = "../conf";
	private static final String ENV_CONFIG_DIRECTORY = "NEPHELE_CONF_DIR";

	private CommandLineParser parser;
	private Map<String,Options> options;
	private boolean verbose;

	/**
	 * Initializes the class
	 */
	public CliFrontend() {

		parser = new PosixParser();
		
		// init options
		options = new HashMap<String, Options>();
		options.put(GENERAL_OPTS, getGeneralOptions());
		options.put(ACTION_RUN, getRunOptions());
		options.put(ACTION_INFO, getInfoOptions());
		options.put(ACTION_LIST, getListOptions());
		options.put(ACTION_CANCEL, getCancelOptions());
	}

	private Options getGeneralOptions() {
		
		Options options = new Options();
		
		// general options
		HELP_OPTION.setRequired(false);
		options.addOption(HELP_OPTION);
		VERBOSE_OPTION.setRequired(false);
		options.addOption(VERBOSE_OPTION);
		
		return options;
	}
	
	/**
	 * Builds command line options for the run action.
	 * 
	 * @return Command line options for the run action.
	 */
	private Options getRunOptions() {
		Options options = new Options();
		// run options
		JAR_OPTION.setRequired(false);
		JAR_OPTION.setArgName("jarfile");
		options.addOption(JAR_OPTION);
		CLASS_OPTION.setRequired(false);
		CLASS_OPTION.setArgName("classname");
		options.addOption(CLASS_OPTION);
		ARGS_OPTION.setRequired(false);
		ARGS_OPTION.setArgName("programArgs");
		ARGS_OPTION.setArgs(Option.UNLIMITED_VALUES);
		options.addOption(ARGS_OPTION);
		WAIT_OPTION.setRequired(false);
		options.addOption(WAIT_OPTION);
		ADDRESS_OPTION.setRequired(false);
		options.addOption(ADDRESS_OPTION);
		
		return options;
	}
	
	/**
	 * Builds command line options for the info action.
	 * 
	 * @return Command line options for the info action.
	 */
	private Options getInfoOptions() {
		
		Options options = new Options();
		
		// info options
		DESCR_OPTION.setRequired(false);
		options.addOption(DESCR_OPTION);
		PLAN_OPTION.setRequired(false);
		options.addOption(PLAN_OPTION);
		JAR_OPTION.setRequired(false);
		JAR_OPTION.setArgName("jarfile");
		options.addOption(JAR_OPTION);
		CLASS_OPTION.setRequired(false);
		CLASS_OPTION.setArgName("classname");
		options.addOption(CLASS_OPTION);
		ARGS_OPTION.setRequired(false);
		ARGS_OPTION.setArgName("programArgs");
		ARGS_OPTION.setArgs(Option.UNLIMITED_VALUES);
		options.addOption(ARGS_OPTION);
		
		return options;
	}
	
	/**
	 * Builds command line options for the list action.
	 * 
	 * @return Command line options for the list action.
	 */
	private Options getListOptions() {
		
		Options options = new Options();
		
		// list options
		RUNNING_OPTION.setRequired(false);
		options.addOption(RUNNING_OPTION);
		SCHEDULED_OPTION.setRequired(false);
		options.addOption(SCHEDULED_OPTION);
		
		return options;
	}
	
	/**
	 * Builds command line options for the cancel action.
	 * 
	 * @return Command line options for the cancel action.
	 */
	private Options getCancelOptions() {
		
		Options options = new Options();
		
		// cancel options
		ID_OPTION.setRequired(false);
		ID_OPTION.setArgName("jobID");
		options.addOption(ID_OPTION);
		
		return options;
	}
	
	/**
	 * Executions the run action.
	 * 
	 * @param args Command line arguments for the run action.
	 */
	private void run(String[] args) {
		
		File jarFile = null;
		String assemblerClass = null;
		String[] programArgs = null;
		String address = null;
		boolean wait = false;
		
		// Parse command line options
		CommandLine line = null;
		try {
			line = parser.parse(this.options.get(ACTION_RUN), args, false);
		} catch (Exception e) {
			handleError(e);
		}
		
		if( line.hasOption(ADDRESS_OPTION.getOpt())) {
			address = line.getOptionValue(ADDRESS_OPTION.getOpt());
		} 
		
		// Get jar file
		if (line.hasOption(JAR_OPTION.getOpt())) {
			String jarFilePath = line.getOptionValue(JAR_OPTION.getOpt());
			jarFile = new File(jarFilePath);
			
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
		Client client;
		InetSocketAddress socket = null;
		if(address != null && !address.isEmpty()) {
			socket = RemoteExecutor.getInetFromHostport(address);
			client = new Client(socket, configuration);
		} else {
			client = new Client(configuration);
		}
		try {
			client.run(program.getPlanWithJars(), wait);
		} catch (ProgramInvocationException e) {
			handleError(e);
		} catch (ErrorInPlanAssemblerException e) {
			handleError(e);
		} catch (IOException e) {
			handleError(e);
		} finally {
			program.deleteExtractedLibraries();
		}
		if(address != null && !address.isEmpty()) {
			System.out.println("Job successfully submitted. Use -w (or --wait) option to track the progress here.\n"
					+ "JobManager web interface: http://"
					+ socket.getHostName()
					+":"+configuration.getInteger(ConfigConstants.JOB_MANAGER_WEB_PORT_KEY, ConfigConstants.DEFAULT_WEB_FRONTEND_PORT));
		} else {
			System.out.println("Job successfully submitted. Use -w (or --wait) option to track the progress here.\n"
				+ "JobManager web interface: http://"
				+ configuration.getString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, null)
				+":"+configuration.getInteger(ConfigConstants.JOB_MANAGER_WEB_PORT_KEY, ConfigConstants.DEFAULT_WEB_FRONTEND_PORT));
		}
	
	}
	
	/**
	 * Executes the info action.
	 * 
	 * @param args Command line arguments for the info action. 
	 */
	private void info(String[] args) {
		
		File jarFile = null;
		String assemblerClass = null;
		String[] programArgs = null;
		
		boolean description;
		boolean plan;
		
		// Parse command line options
		CommandLine line = null;
		try {
			line = parser.parse(this.options.get(ACTION_INFO), args, false);
		} catch (Exception e) {
			handleError(e);
		}
		
		// Get jar file
		if (line.hasOption(JAR_OPTION.getOpt())) {
			jarFile = new File(line.getOptionValue(JAR_OPTION.getOpt()));
			
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
		
		description = line.hasOption(DESCR_OPTION.getOpt());
		plan = line.hasOption(PLAN_OPTION.getOpt());
		
		if(!description && !plan) {
			System.err.println("ERROR: Specify the information to display.");
			printHelp();
			System.exit(1);
		}
		
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
		
		// check for description request
		if (description) {
			String descr = null;
			try {
				descr = program.getDescription();
			} catch (Exception e) {
				handleError(e);
			}
			
			if (descr != null) {
				System.out.println("------------------ PACT Program Description ------------------");
				System.out.println(descr);
				System.out.println("--------------------------------------------------------------");
			} else {
				System.err.println("No description available for this plan.");
			}
		}
		
		// check for json plan request
		if (plan) {
			String jsonPlan = null;
			
			Configuration configuration = getConfiguration();
			Client client = new Client(configuration);
			try {
				jsonPlan = client.getOptimizerPlanAsJSON(program.getPlanWithJars());
			} catch (ProgramInvocationException e) {
				handleError(e);
			} catch (ErrorInPlanAssemblerException e) {
				handleError(e);
			} catch (CompilerException e) {
				handleError(e);
			} catch (IOException e) {
				handleError(e);
			}
			
			if(jsonPlan != null) {
				System.out.println("-------------------- PACT Execution Plan ---------------------");
				System.out.println(jsonPlan);
				System.out.println("--------------------------------------------------------------");
			} else {
				System.err.println("JSON plan could not be compiled.");
			}
		}
		program.deleteExtractedLibraries();
	}

	/**
	 * Executes the list action.
	 * 
	 * @param args Command line arguments for the list action.
	 */
	private void list(String[] args) {
		
		boolean running;
		boolean scheduled;
		
		// Parse command line options
		CommandLine line = null;
		try {
			line = parser.parse(this.options.get(ACTION_LIST), args, false);
		} catch (Exception e) {
			handleError(e);
		}
		
		// get list options
		running = line.hasOption(RUNNING_OPTION.getOpt());
		scheduled = line.hasOption(SCHEDULED_OPTION.getOpt());
		
		if(!running && !scheduled) {
			System.err.println("Error: Specify the status of the jobs to list.");
			printHelp();
			System.exit(1);
		}
		
		ExtendedManagementProtocol jmConn = null;
		try {
			
			jmConn = getJMConnection();
			List<RecentJobEvent> recentJobs = jmConn.getRecentJobs();
			
			ArrayList<RecentJobEvent> runningJobs = null;
			ArrayList<RecentJobEvent> scheduledJobs = null;
			if(running) {
				runningJobs = new ArrayList<RecentJobEvent>();
			}
			if(scheduled) {
				scheduledJobs = new ArrayList<RecentJobEvent>();
			}
			
			for(RecentJobEvent rje : recentJobs) {
				
				if(running && rje.getJobStatus().equals(JobStatus.RUNNING)) {
					runningJobs.add(rje);
				}
				if(scheduled && rje.getJobStatus().equals(JobStatus.SCHEDULED)) {
					scheduledJobs.add(rje);
				}
			}
			
			SimpleDateFormat df = new SimpleDateFormat("dd.MM.yyyy HH:mm:ss");
			Comparator<RecentJobEvent> njec = new Comparator<RecentJobEvent>(){
				
				@Override
				public int compare(RecentJobEvent o1, RecentJobEvent o2) {
					return (int)(o1.getTimestamp()-o2.getTimestamp());
				}
			};
			
			if(running) {
				if(runningJobs.size() == 0) {
					System.out.println("No running jobs.");
				} else {
					Collections.sort(runningJobs, njec);
					
					System.out.println("------------------------ Running Jobs ------------------------");
					for(RecentJobEvent je : runningJobs) {
						System.out.println(df.format(new Date(je.getTimestamp()))+" : "+je.getJobID().toString()+" : "+je.getJobName());
					}
					System.out.println("--------------------------------------------------------------");
				}
			}
			if(scheduled) {
				if(scheduledJobs.size() == 0) {
					System.out.println("No scheduled jobs.");
				} else {
					Collections.sort(scheduledJobs, njec);
					
					System.out.println("----------------------- Scheduled Jobs -----------------------");
					for(RecentJobEvent je : scheduledJobs) {
						System.out.println(df.format(new Date(je.getTimestamp()))+" : "+je.getJobID().toString()+" : "+je.getJobName());
					}
					System.out.println("--------------------------------------------------------------");
				}
			}
			
		} catch (Throwable t) {
			handleError(t);
		} finally {
			if (jmConn != null) {
				try {
					RPC.stopProxy(jmConn);
				} catch (Throwable t) {
					System.err.println("Could not cleanly shut down connection from compiler to job manager");
				}
			}
			jmConn = null;
		}
		
	}
	
	/**
	 * Executes the cancel action.
	 * 
	 * @param args Command line arguments for the cancel action.
	 */
	private void cancel(String[] args) {
		
		String jobId = null;
		
		// Parse command line options
		CommandLine line = null;
		try {
			line = parser.parse(this.options.get(ACTION_CANCEL), args, false);
		} catch (Exception e) {
			handleError(e);
		}
		
		if(line.hasOption(ID_OPTION.getOpt())) {
			jobId = line.getOptionValue(ID_OPTION.getOpt());
		} else {
			System.err.println("Error: Specify a jobID to cancel a job.");
			printHelp();
			System.exit(1);
		}
		
		ExtendedManagementProtocol jmConn = null;
		try {
			
			jmConn = getJMConnection();
			jmConn.cancelJob(new JobID(StringUtils.hexStringToByte(jobId)));
		} catch (Throwable t) {
			handleError(t);
		} finally {
			if (jmConn != null) {
				try {
					RPC.stopProxy(jmConn);
				} catch (Throwable t) {
					System.err.println("Could not cleanly shut down connection from compiler to job manager");
				}
			}
			jmConn = null;
		}
		
	}
	
	/**
	 * Sets up a connection to the JobManager.
	 * 
	 * @return Connection to the JobManager.
	 * @throws IOException
	 */
	private ExtendedManagementProtocol getJMConnection() throws IOException {
		Configuration config = getConfiguration();
		String jmHost = config.getString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, null);
		String jmPort = config.getString(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, null);
		
		if(jmHost == null) {
			handleError(new Exception("JobManager address could not be determined."));
		}
		if(jmPort == null) {
			handleError(new Exception("JobManager port could not be determined."));
		}
		
		return RPC.getProxy(ExtendedManagementProtocol.class, 
			new InetSocketAddress(jmHost, Integer.parseInt(jmPort)), NetUtils.getSocketFactory());
	}
	

	/**
	 * Prints the help for the client.
	 * 
	 * @param options A map with options for actions. 
	 */
	private void printHelp() {
		HelpFormatter formatter = new HelpFormatter();
		formatter.setLeftPadding(5);
		
		System.out.println("./pact-client [ACTION] [GENERAL_OPTIONS] [ACTION_ARGUMENTS]");
		
		formatter.setSyntaxPrefix("  general options:");
		formatter.printHelp(" ",this.options.get(GENERAL_OPTS));
		
		System.out.println("\nAction \"run\" compiles and submits a PACT program.");
		formatter.setSyntaxPrefix("  \"run\" action arguments:");
		formatter.printHelp(" ", this.options.get(ACTION_RUN));
		
		System.out.println("\nAction \"info\" displays information about a PACT program.");
		formatter.setSyntaxPrefix("  \"info\" action arguments:");
		formatter.printHelp(" ", this.options.get(ACTION_INFO));
		
		System.out.println("\nAction \"list\" lists submitted PACT programs.");
		formatter.setSyntaxPrefix("  \"list\" action arguments:");
		formatter.printHelp(" ", this.options.get(ACTION_LIST));
		
		System.out.println("\nAction \"cancel\" cancels a submitted PACT program.");
		formatter.setSyntaxPrefix("  \"cancel\" action arguments:");
		formatter.printHelp(" ", this.options.get(ACTION_CANCEL));
		
	}
	
	/**
	 * Displays exceptions.
	 * 
	 * @param e the exception to display.
	 */
	@SuppressWarnings({"unchecked"})
	private void handleError(Throwable t) {
		if(t instanceof UnrecognizedOptionException) {
			
			Options generalOptions = this.options.get(GENERAL_OPTS);
			boolean generalOption = false;
			for(Option o : (Collection<Option>)generalOptions.getOptions()) {
				if(t.getMessage().startsWith("Unrecognized option: -"+o.getOpt()) || 
						t.getMessage().contains("Unrecognized option: -"+o.getLongOpt())) {
					generalOption = true;
				}
			}
			if(generalOption) {
				System.err.println("ERROR: General args must be placed directly after action.");
			} else {
				System.err.println("ERROR: "+t.getMessage());
			}
			printHelp();
		} else {
			System.err.println("ERROR: "+t.getMessage());
			if (this.verbose) {
				t.printStackTrace();
			} else {
				System.out.println("For a more detailed error message use the '-v' option");
			}
		}
		System.exit(1);
	}

	/**
	 * Reads configuration settings. The default path can be overridden
	 * by setting the ENV variable "NEPHELE_CONF_DIR".
	 * 
	 * @return Nephele's configuration
	 */
	private Configuration getConfiguration() {
		String location = null;
		if (System.getenv(ENV_CONFIG_DIRECTORY) != null) {
			location = System.getenv(ENV_CONFIG_DIRECTORY);
		} else {
			location = DEFAULT_CONFIG_DIRECTORY;
		}

		GlobalConfiguration.loadConfiguration(location);
		Configuration config = GlobalConfiguration.getConfiguration();

		return config;
	}
	
	/**
	 * Parses the general command line options and returns the verbosity mode.
	 * 
	 * @param args command line arguments of the action.
	 * @return all arguments that have not been parsed as general options.
	 * 
	 */
	private String[] parseGeneralOptions(String[] args) {

		// Parse command line options
		CommandLine line = null;
		try {
			line = parser.parse(this.options.get(GENERAL_OPTS), args, true);
		} catch (Exception e) {
			handleError(e);
		}
		
		// check help flag
		if (line.hasOption(HELP_OPTION.getOpt())) {
			printHelp();
			System.exit(0);
		}

		// check verbosity flag
		this.verbose = line.hasOption(VERBOSE_OPTION.getOpt());
		
		return line.getArgs();
	}
	
	/**
	 * Parses the command line arguments and starts the requested action.
	 * 
	 * @param args command line arguments of the client.
	 */
	private void parseParameters(String[] args) {
		
		// check for action
		if(args.length < 1) {
			System.err.println("ERROR: Please specify an action.");
			printHelp();
			System.exit(1);
		}
		
		// get action
		String action = args[0];
		
		// remove action from parameters
		String[] params = new String[args.length-1];
		System.arraycopy(args, 1, params, 0, params.length);
		
		params = parseGeneralOptions(params);
		
		// do action
		if(action.equals(ACTION_RUN)) {
			run(params);
		} else if (action.equals(ACTION_LIST)) {
			list(params);
		} else if (action.equals(ACTION_INFO)) {
			info(params);
		} else if (action.equals(ACTION_CANCEL)) {
			cancel(params);
		} else {
			System.err.println("Invalid action!");
			printHelp();
			System.exit(1);
		}
		
	}

	

	/**
	 * Submits the job based on the arguments
	 */
	public static void main(String[] args) throws ParseException {

		CliFrontend cli = new CliFrontend();
		cli.parseParameters(args);
		
	}
}
