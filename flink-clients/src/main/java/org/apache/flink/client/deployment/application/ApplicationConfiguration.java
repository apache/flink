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

package org.apache.flink.client.deployment.application;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ConfigUtils;
import org.apache.flink.configuration.Configuration;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Application-specific configurations.
 */
@Internal
public class ApplicationConfiguration {

	public static final ConfigOption<List<String>> APPLICATION_ARGS = ConfigOptions
			.key("$internal.application.program-args")
			.stringType()
			.asList()
			.noDefaultValue();

	public static final ConfigOption<String> APPLICATION_MAIN_CLASS = ConfigOptions
			.key("$internal.application.main")
			.stringType()
			.noDefaultValue();

	private final String[] programArguments;

	@Nullable
	private final String applicationClassName;

	public ApplicationConfiguration(
			final String[] programArguments,
			@Nullable final String applicationClassName) {
		this.programArguments = checkNotNull(programArguments);
		this.applicationClassName = applicationClassName;
	}

	public String[] getProgramArguments() {
		return programArguments;
	}

	@Nullable
	public String getApplicationClassName() {
		return applicationClassName;
	}

	public void applyToConfiguration(final Configuration configuration) {
		checkNotNull(configuration);

		ConfigUtils.encodeArrayToConfig(configuration, APPLICATION_ARGS, programArguments, Objects::toString);
		if (applicationClassName != null) {
			configuration.set(APPLICATION_MAIN_CLASS, applicationClassName);
		}
	}

	public static ApplicationConfiguration fromConfiguration(final Configuration configuration) {
		checkNotNull(configuration);

		final List<String> programArgsList = ConfigUtils.decodeListFromConfig(configuration, APPLICATION_ARGS, String::new);

		final String[] programArgs = programArgsList.toArray(new String[0]);
		final String applicationClassName = configuration.get(APPLICATION_MAIN_CLASS);

		return new ApplicationConfiguration(programArgs, applicationClassName);
	}
}
