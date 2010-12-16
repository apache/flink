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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.pact.client.web.WebInterfaceServer;
import eu.stratosphere.pact.common.util.PactConfigConstants;

/**
 * Main entry point for the web frontend. Creates a web server according to the configuration
 * in the given directory.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class WebFrontend {
	/**
	 * The log for this class.
	 */
	private static final Log LOG = LogFactory.getLog(WebFrontend.class);

	/**
	 * Main method. accepts a single parameter, which is the config directory.
	 * 
	 * @param args
	 *        The parameters to the entry point.
	 */
	public static void main(String[] args) {
		try {
			// get the config directory first
			String configDir = null;

			if (args.length >= 2 && args[0].equals("-configDir")) {
				configDir = args[1];
			}

			if (configDir == null) {
				System.err
					.println("Error: Configuration directory must be specified.\nWebFrontend -configDir <directory>\n");
				System.exit(1);
				return;
			}

			// load the global configuration
			GlobalConfiguration.loadConfiguration(configDir);
			Configuration config = GlobalConfiguration.getConfiguration();

			// get the listening port
			int port = config.getInteger(PactConfigConstants.WEB_FRONTEND_PORT_KEY,
				PactConfigConstants.DEFAULT_WEB_FRONTEND_PORT);

			// start the server
			WebInterfaceServer server = new WebInterfaceServer(config, port);
			LOG.info("Starting web frontend server on port " + port + '.');
			server.start();
			server.join();
		} catch (Throwable t) {
			LOG.error("Unexpected exception: " + t.getMessage(), t);
		}
	}
}
