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

package org.apache.flink.runtime.instance;

import java.util.Map;

/**
 * Simple manager that keeps track of which TaskManager are available and alive.
 */
public interface InstanceManager {

	InstanceID registerTaskManager(InstanceConnectionInfo instanceConnectionInfo, HardwareDescription resources, int numberOfTaskSlots);
	
	boolean reportHeartBeat(InstanceID instance);

	void shutdown();

	Map<InstanceID, Instance> getAllRegisteredInstances();

	int getNumberOfRegisteredTaskManagers();

	int getTotalNumberOfSlots();
}
