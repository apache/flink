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

package org.apache.flink.mesos;

import java.util.HashMap;

public class MesosConfiguration {

	private HashMap<ConfKeys, String> configuration = new HashMap<ConfKeys, String>();

	public static enum ConfKeys {
		VERBOSE, FLINK_CONF_DIR, FLINK_JAR, JM_MEMORY, TM_MEMORY, TM_CORES, SLOTS, MASTER, MESOS_LIB, MAX_TM_INSTANCES, USE_WEB
	}

	public void set(ConfKeys key, String value) {
		configuration.put(key, value);
	}

	public String get(ConfKeys key) {
		return configuration.get(key);
	}

}
