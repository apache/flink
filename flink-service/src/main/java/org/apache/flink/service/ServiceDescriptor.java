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

import org.apache.flink.configuration.Configuration;

import java.io.Serializable;
import java.util.Objects;

/**
 * Describe configuration and resource for UserDefined Service.
 */
public class ServiceDescriptor implements Serializable {

	/**
	 * Service class must have a default constructor.
	 */
	private String serviceClassName;

	/**
	 * Number of service instance.
	 */
	private int serviceParallelism;

	/**
	 * Heap memory for one instance.
	 */
	private int serviceHeapMemoryMb;

	/**
	 * Direct memory for one instance.
	 */
	private int serviceDirectMemoryMb;

	/**
	 * Native memory for one instance.
	 */
	private int serviceNativeMemoryMb;

	/**
	 * Vcore for one instance.
	 */
	private double serviceCpuCores;

	/**
	 * configuration can be accessed in {@link LifeCycleAware}'s open method.
	 */
	private Configuration configuration = new Configuration();

	public ServiceDescriptor setServiceClassName(String serviceClassName) {
		this.serviceClassName = serviceClassName;
		return this;
	}

	public String getServiceClassName() {
		return serviceClassName;
	}

	public int getServiceParallelism() {
		return serviceParallelism;
	}

	public ServiceDescriptor setServiceParallelism(int serviceParallelism) {
		this.serviceParallelism = serviceParallelism;
		return this;
	}

	public int getServiceHeapMemoryMb() {
		return serviceHeapMemoryMb;
	}

	public ServiceDescriptor setServiceHeapMemoryMb(int serviceHeapMemoryMb) {
		this.serviceHeapMemoryMb = serviceHeapMemoryMb;
		return this;
	}

	public int getServiceDirectMemoryMb() {
		return serviceDirectMemoryMb;
	}

	public ServiceDescriptor setServiceDirectMemoryMb(int serviceDirectMemoryMb) {
		this.serviceDirectMemoryMb = serviceDirectMemoryMb;
		return this;
	}

	public int getServiceNativeMemoryMb() {
		return serviceNativeMemoryMb;
	}

	public ServiceDescriptor setServiceNativeMemoryMb(int serviceNativeMemoryMb) {
		this.serviceNativeMemoryMb = serviceNativeMemoryMb;
		return this;
	}

	public double getServiceCpuCores() {
		return serviceCpuCores;
	}

	public ServiceDescriptor setServiceCpuCores(double serviceCpuCores) {
		this.serviceCpuCores = serviceCpuCores;
		return this;
	}

	public Configuration getConfiguration() {
		return configuration;
	}

	public ServiceDescriptor setConfiguration(Configuration configuration) {
		this.configuration = configuration;
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
		ServiceDescriptor that = (ServiceDescriptor) o;
		return Objects.equals(serviceClassName, that.serviceClassName);
	}

	@Override
	public int hashCode() {
		return Objects.hash(serviceClassName);
	}
}
