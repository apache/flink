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

package org.apache.flink.runtime.state;


import java.io.Serializable;

/**
 * StateHandle is a general handle interface meant to abstract operator state fetching. 
 * A StateHandle implementation can for example include the state itself in cases where the state 
 * is lightweight or fetching it lazily from some external storage when the state is too large.
 */
public interface StateHandle<T> extends Serializable {

	/**
	 * This retrieves and return the state represented by the handle.
	 *
	 * @param userCodeClassLoader Class loader for deserializing user code specific classes
	 *
	 * @return The state represented by the handle.
	 * @throws java.lang.Exception Thrown, if the state cannot be fetched.
	 */
	T getState(ClassLoader userCodeClassLoader) throws Exception;
	
	/**
	 * Discards the state referred to by this handle, to free up resources in
	 * the persistent storage. This method is called when the handle will not be
	 * used any more.
	 */
	void discardState() throws Exception;

	/**
	 * Returns the size of the state in bytes.
	 *
	 * <p>If the the size is not known, return <code>0</code>.
	 *
	 * @return Size of the state in bytes.
	 * @throws Exception If the operation fails during size retrieval.
	 */
	long getStateSize() throws Exception;
}
