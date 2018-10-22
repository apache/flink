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

package org.apache.flink.table.client.gateway;

import org.apache.flink.table.client.config.Environment;
import org.apache.flink.table.client.config.entries.ViewEntry;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Context describing a session.
 */
public class SessionContext {

	private final String name;

	private final Environment defaultEnvironment;

	private final Map<String, String> sessionProperties;

	private final Map<String, ViewEntry> views;

	public SessionContext(String name, Environment defaultEnvironment) {
		this.name = name;
		this.defaultEnvironment = defaultEnvironment;
		this.sessionProperties = new HashMap<>();
		// the order of how views are registered matters because
		// they might reference each other
		this.views = new LinkedHashMap<>();
	}

	public void setSessionProperty(String key, String value) {
		sessionProperties.put(key, value);
	}

	public void resetSessionProperties() {
		sessionProperties.clear();
	}

	public void addView(ViewEntry viewEntry) {
		views.put(viewEntry.getName(), viewEntry);
	}

	public void removeView(String name) {
		views.remove(name);
	}

	public Map<String, ViewEntry> getViews() {
		return Collections.unmodifiableMap(views);
	}

	public String getName() {
		return name;
	}

	public Environment getEnvironment() {
		return Environment.enrich(
			defaultEnvironment,
			sessionProperties,
			views);
	}

	public SessionContext copy() {
		final SessionContext session = new SessionContext(name, defaultEnvironment);
		session.sessionProperties.putAll(sessionProperties);
		session.views.putAll(views);
		return session;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (!(o instanceof SessionContext)) {
			return false;
		}
		SessionContext context = (SessionContext) o;
		return Objects.equals(name, context.name) &&
			Objects.equals(defaultEnvironment, context.defaultEnvironment) &&
			Objects.equals(sessionProperties, context.sessionProperties) &&
			Objects.equals(views, context.views);
	}

	@Override
	public int hashCode() {
		return Objects.hash(
			name,
			defaultEnvironment,
			sessionProperties,
			views);
	}
}
