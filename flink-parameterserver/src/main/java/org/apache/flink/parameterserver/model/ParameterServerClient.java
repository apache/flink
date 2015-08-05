/*
 * Copyright 2015 EURA NOVA.
 *
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

package org.apache.flink.parameterserver.model;

/**
 *  This interface defines a client accessing a parameter server.
 *  The client has access to two cache levels:
 *  The partitioned parameter cache: for parameters subject to writes at
 *  	each iteration. Furthermore, the new parameter is written only if
 *  	it is associated with a greater convergence degree.
 *  The shared parameter cache: for parameters more often read than written.
 */
public interface ParameterServerClient {
	/**
	 * Update an element in the partitioned parameter cache if the convergence
	 * associated with the current element is better than the one currently
	 * present on the server.
	 * @param id the id of the parameter element
	 * @param value the parameter element
	 * @param opt the convergence associated with the current parameter element
	 */
	public void updateParameter(String id, ParameterElement value, ParameterElement<Double> opt);

	/**
	 * Update an element in the shared parameter cache.
 	 * @param id the id of the shared parameter element
	 * @param value the parameter element
	 */
	public void updateShared(String id, ParameterElement value);

	/**
	 * Retrieve a parameter element from the partitioned parameter cache
	 * @param id the id of the parameter element
	 * @return the parameter element or null is it does not exist on the server
	 */
	public ParameterElement getParameter(String id);

	/**
	 * Retrieve a parameter element from the shared parameter cache
	 * @param id the id of the parameter element
	 * @return the parameter element or null is it does not exist on the server
	 */
	public ParameterElement getSharedParameter(String id);
}
