/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.nephele.visualization.swt;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Shell;

import eu.stratosphere.nephele.configuration.ConfigConstants;
import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.protocols.ExtendedManagementProtocol;
import eu.stratosphere.nephele.rpc.ManagementTypeUtils;
import eu.stratosphere.nephele.rpc.RPCService;
import eu.stratosphere.nephele.util.StringUtils;
import eu.stratosphere.nephele.visualization.swt.SWTVisualizationGUI;

public class SWTVisualization {

	private static final Log LOG = LogFactory.getLog(SWTVisualization.class);

	private final static String CONFIG_DIR_PARAMETER = "-configDir";

	public static void main(String[] args) {

		// First, look for -configDir parameter
		String configDir = null;
		for (int i = 0; i < (args.length - 1); i++) {
			if (CONFIG_DIR_PARAMETER.equals(args[i])) {
				configDir = args[i + 1];
				break;
			}
		}

		if (configDir == null) {
			LOG.error("Please specify Nephele configuration directory with " + CONFIG_DIR_PARAMETER);
			System.exit(1);
			return;
		}

		// Try to load global configuration
		GlobalConfiguration.loadConfiguration(configDir);

		final String address = GlobalConfiguration.getString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, null);
		if (address == null) {
			LOG.error("Cannot find address to job manager's RPC service in configuration");
			System.exit(1);
			return;
		}

		final int port = GlobalConfiguration.getInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, -1);

		if (port < 0) {
			LOG.error("Cannot find port to job manager's RPC service in configuration");
			System.exit(1);
			return;
		}

		RPCService rpcService = null;
		try {
			rpcService = new RPCService(ManagementTypeUtils.getRPCTypesToRegister());
		} catch (IOException ioe) {
			LOG.error("Error initializing the RPC service: " + StringUtils.stringifyException(ioe));
			System.exit(1);
			return;
		}

		final InetSocketAddress inetaddr = new InetSocketAddress(address, port);
		ExtendedManagementProtocol jobManager = null;
		int queryInterval = -1;
		try {
			jobManager = rpcService.getProxy(inetaddr, ExtendedManagementProtocol.class);

			// Get the query interval
			queryInterval = jobManager.getRecommendedPollingInterval();

		} catch (Exception e) {
			e.printStackTrace();
			rpcService.shutDown();
			System.exit(1);
			return;
		}

		final SWTVisualizationGUI swtVisualizationGUI = new SWTVisualizationGUI(jobManager, queryInterval);
		final Shell shell = swtVisualizationGUI.getShell();
		final Display display = swtVisualizationGUI.getDisplay();
		shell.open();

		while (!shell.isDisposed()) {
			if (!display.readAndDispatch()) {
				display.sleep();
			}
		}
		display.dispose();

		rpcService.shutDown();
	}
}
