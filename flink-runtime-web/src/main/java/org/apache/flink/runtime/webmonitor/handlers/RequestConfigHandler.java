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

package org.apache.flink.runtime.webmonitor.handlers;

import org.apache.flink.runtime.webmonitor.JsonFactory;

import java.util.Map;
import java.util.TimeZone;

/**
 * Responder that returns the parameters that define how the asynchronous requests
 * against this web server should behave. It defines for example the refresh interval,
 * and time zone of the server timestamps.
 */
public class RequestConfigHandler implements RequestHandler, RequestHandler.JsonResponse {
	
	private final String configString;
	
	public RequestConfigHandler(long refreshInterval) {
		TimeZone timeZome = TimeZone.getDefault();
		String timeZoneName = timeZome.getDisplayName();
		long timeZoneOffset= timeZome.getRawOffset();
		
		this.configString = JsonFactory.generateConfigJSON(refreshInterval, timeZoneOffset, timeZoneName);
	}
	
	@Override
	public String handleRequest(Map<String, String> params) {
		return this.configString;
	}
}
