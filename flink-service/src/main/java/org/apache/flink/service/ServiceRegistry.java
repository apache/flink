/*
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

package org.apache.flink.service;

import org.apache.flink.annotation.PublicEvolving;

import java.util.List;

/**
 * Used for service registration, unregistration and query registered instance.
 */
@PublicEvolving
public interface ServiceRegistry extends LifeCycleAware {

	/**
	 * Register a service instance.
	 * One service can have multiple instances.
	 *
	 * @param serviceName To identify the service.
	 * @param instanceId  To identify one instance of a service.
	 * @param serviceIp   Ip address of one instance, such as '10.101.1.1'.
	 * @param port        Port of one instance.
	 * @param customData  Custom data of one instance, can be null.
	 */
	void addInstance(String serviceName, String instanceId, String serviceIp, int port, byte[] customData);

	/**
	 * Unregister a service instance via service name and instance id.
	 *
	 * @param serviceName To identify the service.
	 * @param instanceId To identify one instance of a service.
	 */
	void removeInstance(String serviceName, String instanceId);

	/**
	 * Get all registered instances via service name.
	 *
	 * @param serviceName To identify the service.
	 * @return list of {@link ServiceInstance}.
	 */
	List<ServiceInstance> getAllInstances(String serviceName);

}
