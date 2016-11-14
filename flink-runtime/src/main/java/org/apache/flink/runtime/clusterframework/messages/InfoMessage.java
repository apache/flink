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

package org.apache.flink.runtime.clusterframework.messages;

import java.util.Date;

import static java.util.Objects.requireNonNull;

/**
 * A simple informational message sent by the resource master to the client.
 */
public class InfoMessage implements java.io.Serializable {

	private static final long serialVersionUID = 5534993035539629765L;
	
	private final String message;
	
	private final Date date;
	
	public InfoMessage(String message) {
		this.message = message;
		this.date = new Date();
	}
	
	public InfoMessage(String message, Date date) {
		this.message = requireNonNull(message);
		this.date = requireNonNull(date);
	}
	
	public String message() {
		return message;
	}
	
	public Date date() {
		return date;
	}
	
	@Override
	public String toString() {
		return "InfoMessage { message='" + message + "', date=" + date + " }";
	}
}
