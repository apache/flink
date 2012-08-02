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
package eu.stratosphere.sopremo.execution;

/**
 * @author Arvid Heise
 */
public class SopremoConstants {

	/**
	 * The key for the config parameter defining the network address to connect to
	 * for communication with the sopremo server.
	 */
	public static final String SOPREMO_SERVER_IPC_ADDRESS_KEY = "sopremo.rpc.address";

	/**
	 * The key for the config parameter defining the network port to connect to
	 * for communication with the sopremo server.
	 */
	public static final String SOPREMO_SERVER_IPC_PORT_KEY = "sopremo.rpc.port";

	/**
	 * The default network port to connect to for communication with the sopremo server.
	 */
	public static final int DEFAULT_SOPREMO_SERVER_IPC_PORT = 6201;

	public static final String SOPREMO_SERVER_HANDLER_COUNT_KEY = "sopremo.rpc.numhandler";
}
