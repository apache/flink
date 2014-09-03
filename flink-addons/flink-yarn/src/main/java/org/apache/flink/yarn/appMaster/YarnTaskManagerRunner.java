/**
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

package org.apache.flink.yarn.appMaster;

import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.Arrays;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.runtime.taskmanager.TaskManager;
import org.apache.flink.yarn.Client;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;


public class YarnTaskManagerRunner {

	private static final Log LOG = LogFactory.getLog(YarnTaskManagerRunner.class);

	public static void main(final String[] args) throws IOException {
		Map<String, String> envs = System.getenv();
		final String yarnClientUsername = envs.get(Client.ENV_CLIENT_USERNAME);
		final String localDirs = envs.get(Environment.LOCAL_DIRS.key());

		// configure local directory
		final String[] newArgs = Arrays.copyOf(args, args.length + 2);
		newArgs[newArgs.length-2] = "-"+TaskManager.ARG_CONF_DIR;
		newArgs[newArgs.length-1] = localDirs;
		LOG.info("Setting log path "+localDirs);
		LOG.info("YARN daemon runs as '"+UserGroupInformation.getCurrentUser().getShortUserName()+"' setting"
				+ " user to execute Flink TaskManager to '"+yarnClientUsername+"'");
		UserGroupInformation ugi = UserGroupInformation.createRemoteUser(yarnClientUsername);
		for(Token<? extends TokenIdentifier> toks : UserGroupInformation.getCurrentUser().getTokens()) {
			ugi.addToken(toks);
		}
		ugi.doAs(new PrivilegedAction<Object>() {
			@Override
			public Object run() {
				try {
					TaskManager.main(newArgs);
				} catch (Exception e) {
					LOG.fatal("Error while running the TaskManager", e);
				}
				return null;
			}
		});
	}
}
