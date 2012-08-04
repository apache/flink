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
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

import eu.stratosphere.meteor.QueryParser;
import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.sopremo.client.DefaultClient;
import eu.stratosphere.sopremo.client.StateListener;
import eu.stratosphere.sopremo.execution.ExecutionRequest.ExecutionMode;
import eu.stratosphere.sopremo.execution.ExecutionResponse.ExecutionState;
import eu.stratosphere.sopremo.execution.SopremoConstants;
import eu.stratosphere.sopremo.operator.SopremoPlan;

/**
 * @author Arvid Heise
 */
public class CLClient {
	private Options options = new Options();

	private DefaultClient sopremoClient;

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
		CommandLine cmd = this.parseOptions(args);
		final SopremoPlan plan = this.parseScript(cmd);
		this.configureClient(cmd);

		this.sopremoClient.submit(plan, new StateListener() {
			@Override
			public void stateChanged(ExecutionState executionState, String detail) {
				switch (executionState) {
				case ENQUEUED:
					System.out.print("Submitted script");
					break;
				case RUNNING:
					System.out.print("\nExecuting script");
					break;
				case FINISHED:
					System.out.println("\n" + detail);
					break;
				case ERROR:
					System.err.println("\n" + detail);
					break;
				}
			}

			/*
			 * (non-Javadoc)
			 * @see eu.stratosphere.sopremo.client.StateListener#progressUpdate(eu.stratosphere.sopremo.execution.
			 * ExecutionResponse.ExecutionState, java.lang.String)
			 */
			@Override
			public void progressUpdate(ExecutionState status, String detail) {
				super.progressUpdate(status, detail);
				System.out.print(".");
			}
		}, cmd.hasOption("wait"));

		this.sopremoClient.close();
	}

	private void configureClient(CommandLine cmd) {
		String configDir = cmd.getOptionValue("config");
		GlobalConfiguration.loadConfiguration(configDir);
		this.sopremoClient = new DefaultClient();

		int updateTime = 1000;
		if (cmd.hasOption("updateTime"))
			updateTime = Integer.parseInt(cmd.getOptionValue("updateTime"));
		this.sopremoClient.setUpdateTime(updateTime);

		String address = cmd.getOptionValue("server"), port = cmd.getOptionValue("port");
		if (address != null || port != null) {
			this.sopremoClient.setServerAddress(new InetSocketAddress(
				address == null ? "localhost" : address,
				port == null ? SopremoConstants.DEFAULT_SOPREMO_SERVER_IPC_PORT : Integer.parseInt(port)));
		}

		this.sopremoClient.setExecutionMode(ExecutionMode.RUN_WITH_STATISTICS);
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

	private SopremoPlan parseScript(CommandLine cmd) {
		File file = new File(cmd.getOptionValue("file"));
		if (!file.exists())
			this.dealWithError(null, "Given file not found");

		try {
			return new QueryParser().tryParse(new FileInputStream(file));
		} catch (IOException e) {
			this.dealWithError(e, "Error while parsing script");
			return null;
		}
	}

	protected CommandLine parseOptions(String[] args) {
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
