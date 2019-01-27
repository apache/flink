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

import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;

/**
 * A simple Pojo keeps record of one service instance.
 */
public class ServiceInstance implements Serializable {

	private final String serviceName;

	private final String instanceId;

	private String serviceIp;

	private int servicePort;

	private byte[] customData;

	public ServiceInstance(String serviceName, String instanceId) {
		this.serviceName = serviceName;
		this.instanceId = instanceId;
	}

	public String getServiceName() {
		return serviceName;
	}

	public String getInstanceId() {
		return instanceId;
	}

	public String getServiceIp() {
		return serviceIp;
	}

	public ServiceInstance setServiceIp(String serviceIp) {
		this.serviceIp = serviceIp;
		return this;
	}

	public int getServicePort() {
		return servicePort;
	}

	public ServiceInstance setServicePort(int servicePort) {
		this.servicePort = servicePort;
		return this;
	}

	public byte[] getCustomData() {
		return customData;
	}

	public ServiceInstance setCustomData(byte[] customData) {
		this.customData = customData;
		return this;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		ServiceInstance that = (ServiceInstance) o;
		return Objects.equals(serviceName, that.serviceName) &&
			Objects.equals(instanceId, that.instanceId);
	}

	@Override
	public int hashCode() {
		return Objects.hash(serviceName, instanceId);
	}

	@Override
	public String toString() {
		return "ServiceInstance{" +
			"serviceName='" + serviceName + '\'' +
			", instanceId='" + instanceId + '\'' +
			", serviceIp='" + serviceIp + '\'' +
			", servicePort=" + servicePort +
			", customData=" + Arrays.toString(customData) +
			'}';
	}
}
