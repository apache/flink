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


package org.apache.flink.api.common.server;

import java.io.Serializable;
import java.util.Collection;

/**
 * Parameter represents the base class of everything that can be stored on the ParameterServer
 */
public abstract class Parameter implements Serializable{

	private int clock;

	/**
	 * This function will be called by the Parameter Server to update the Parameter based on Updates
	 * received from the clients.
	 *
	 * @param update Update to be made to the Parameter.
	 */
	public void update(Update update) throws Exception{}

	/**
	 * This function must operate on a list of updates and update the parameter.
	 *
	 * @param list List of updates received from various clients
	 */
	public void reduce(Collection<Update> list) throws Exception{}

	/**
	 * This function returns the clock at the server corresponding to this parameter
	 */
	public final int getClock(){
		return clock;
	}

	/**
	 * This function sets the clock for this parameter.
	 *
	 * @param clock Clock at the server
	 */
	public final void setClock(int clock){
		this.clock = clock;
	}

}
