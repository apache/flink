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


package org.apache.flink.yarn;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;

public class ClientCallableExTesting implements Callable<Exception>{
	List<String> clientArgs;

	public ClientCallableExTesting(List<String> args) {
		clientArgs = args;
	}

	public ClientCallableExTesting() {
		clientArgs = new LinkedList<String>();
	}

	public void addParameter(String name, String value) {
		clientArgs.add(name);
		clientArgs.add(value);
	}

	@Override
	public Exception call() throws Exception {
		Client client = new Client();
		try {
			client.run(clientArgs.toArray(new String[clientArgs.size()]));
		} catch (Exception e) {
			return e;
		}
		return null;
	}
}
