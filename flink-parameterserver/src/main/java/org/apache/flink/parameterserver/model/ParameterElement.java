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
package org.apache.flink.parameterserver.model;

import java.io.Serializable;

/**
 * This interface defines a parameter element to be stored and accessed on the {@link ParameterServer}
 * in Stale Synchronous Parallel iterations
 * @param <T> the type of the parameter
 */
public interface ParameterElement<T> extends Serializable{
	/**
	 * Returns the cluster-wide clock associated with the parameter element
	 * @return the cluster-wide clock of the parameter element
	 */
	int getClock();

	/**
	 * Returns the value of the parameter
	 * @return the value of the parameter element
	 */
	T getValue();
}
