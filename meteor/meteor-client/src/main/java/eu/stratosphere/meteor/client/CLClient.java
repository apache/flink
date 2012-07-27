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
package eu.stratosphere.meteor.client;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

import eu.stratosphere.meteor.execution.ExecutionRequest;
import eu.stratosphere.meteor.execution.ExecutionResponse;
import eu.stratosphere.meteor.execution.ExecutionResponse.ExecutionStatus;
import eu.stratosphere.meteor.execution.MeteorConstants;
import eu.stratosphere.meteor.execution.MeteorExecutor;
import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.ipc.RPC;
import eu.stratosphere.nephele.net.NetUtils;

/**
 * @author Arvid Heise
 */
public class CLClient {
	private Options options = new Options();

	private MeteorExecutor executor;

	private InetSocketAddress serverAddress;

	private ExecutionRequest request;

	private String submittedJobId;

	/**
	 * Initializes CLClient.
	 */
	public CLClient() {
		this.initOptions();
	}

	@SuppressWarnings("static-access")
	private void initOptions() {
		this.options.addOption(OptionBuilder.isRequired().
			withArgName("file").hasArg(true).
			withDescription("Executes the given script").create("f"));
		this.options.addOption(OptionBuilder.
			withArgName("config").hasArg(true).
			withDescription("Uses the given configuration").withLongOpt("conf").create());
		this.options.addOption(OptionBuilder.
			withArgName("server").hasArg(true).
			withDescription("Uses the specified server").withLongOpt("server").create());
		this.options.addOption(OptionBuilder.
			withArgName("port").hasArg(true).
			withDescription("Uses the specified port").withLongOpt("port").create());
		this.options.addOption(OptionBuilder.
			withArgName("updateTime").hasArg(true).
			withDescription("Checks with the given update time for the current status").withLongOpt("updateTime").create());
		this.options.addOption(OptionBuilder.
			hasArg(false).
			withDescription("Waits until the script terminates on the server").withLongOpt("wait").create());
	}

	public static void main(String[] args) {
		new CLClient().process(args);
	}

	private void process(String[] args) {
		CommandLine cmd = this.parse(args);
		this.loadConfig(cmd);
		this.initConnection(cmd);
		this.loadScript(cmd);
		final ExecutionResponse response = this.sendScript();
		if (response.getStatus() == ExecutionStatus.ERROR)
			this.dealWithError(null, "Script contains errors: " + response.getDetails());
		else if (cmd.hasOption("wait"))
			this.waitForCompletion(cmd, response);
		else
			System.out.println("Successfully submitted script");
	}

	private void loadConfig(CommandLine cmd) {
		String configDir = cmd.getOptionValue("config");
		GlobalConfiguration.loadConfiguration(configDir);
	}

	private void waitForCompletion(CommandLine cmd, ExecutionResponse submissionResponse) {
		ExecutionResponse lastResponse = submissionResponse;
		int updateTime = 5000;

		if (cmd.hasOption("updateTime"))
			updateTime = Integer.parseInt(cmd.getOptionValue("updateTime"));

		try {
			System.out.print("Submitted script");
			while (lastResponse.getStatus() == ExecutionStatus.ENQUEUED) {
				lastResponse = this.executor.getStatus(this.submittedJobId);
				sleepSafely(updateTime);
				System.out.print('.');
			}
			System.out.println();

			System.out.print("Running script");
			while (lastResponse.getStatus() == ExecutionStatus.RUNNING) {
				lastResponse = this.executor.getStatus(this.submittedJobId);
				sleepSafely(updateTime);
				System.out.print('.');
			}
			System.out.println();

			if (lastResponse.getStatus() == ExecutionStatus.ERROR)
				this.dealWithError(null, "Execution of the script yielded errors: " + lastResponse.getDetails());
			else
				System.out.println("Successfully executed script: " + lastResponse.getDetails());
		} catch (Exception e) {
			this.dealWithError(e, "Error while waiting for job execution");
		}
	}

	protected void sleepSafely(int updateTime) {
		try {
			Thread.sleep(updateTime);
		} catch (InterruptedException e) {
		}
	}

	protected void dealWithError(Exception e, final String message) {
		System.err.print(message);
		if (e != null) {
			System.err.print(": ");
			System.err.print(e);
		}
		System.err.println();
		System.exit(1);
	}

	private void loadScript(CommandLine cmd) {
		File file = new File(cmd.getOptionValue("file"));
		if (!file.exists())
			this.dealWithError(null, "Given file not found");

		StringBuilder builder;
		try {
			FileReader reader = new FileReader(file);
			builder = new StringBuilder();
			char[] buffer = new char[512];
			int read;
			while ((read = reader.read(buffer)) > 0)
				builder.append(buffer, 0, read);
			this.request = new ExecutionRequest(builder.toString());
		} catch (IOException e) {
			this.dealWithError(e, "Error while reading script");
		}
	}

	private ExecutionResponse sendScript() {
		try {
			ExecutionResponse response = this.executor.execute(this.request);
			this.submittedJobId = response.getJobId();
			return response;
		} catch (Exception e) {
			this.dealWithError(e, "Error while sending the query to the server");
			return null;
		}
	}

	private void initConnection(CommandLine cmd) {
		String address = cmd.getOptionValue("server");
		if (address == null)
			address = GlobalConfiguration.getString(MeteorConstants.METEOR_SERVER_IPC_ADDRESS_KEY, null);
		final int port = cmd.getOptionValues("port") != null ? Integer.parseInt(cmd.getOptionValue("port")) :
			GlobalConfiguration.getInteger(MeteorConstants.METEOR_SERVER_IPC_PORT_KEY,
				MeteorConstants.DEFAULT_METEOR_SERVER_IPC_PORT);

		this.serverAddress = new InetSocketAddress(address, port);

		try {
			this.executor =
				(MeteorExecutor) RPC.getProxy(MeteorExecutor.class, this.serverAddress, NetUtils.getSocketFactory());
		} catch (IOException e) {
			this.dealWithError(e, "Error while connecting to the server");
		}
	}

	protected CommandLine parse(String[] args) {
		CommandLineParser parser = new PosixParser();
		try {
			return parser.parse(this.options, args);
		} catch (ParseException e) {
			System.err.println("Cannot process the given arguments: " + e);
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("", this.options);
			System.exit(1);
			return null;
		}
	}
}
