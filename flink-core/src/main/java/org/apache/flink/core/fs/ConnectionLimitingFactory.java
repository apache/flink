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

package org.apache.flink.core.fs;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.LimitedConnectionsFileSystem.ConnectionLimitingSettings;

import java.io.IOException;
import java.net.URI;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A wrapping factory that adds a {@link LimitedConnectionsFileSystem} to a file system.
 */
@Internal
public class ConnectionLimitingFactory implements FileSystemFactory {

	private final FileSystemFactory factory;

	private final ConnectionLimitingSettings settings;

	private ConnectionLimitingFactory(
			FileSystemFactory factory,
			ConnectionLimitingSettings settings) {

		this.factory = checkNotNull(factory);
		this.settings = checkNotNull(settings);
	}

	// ------------------------------------------------------------------------

	@Override
	public String getScheme() {
		return factory.getScheme();
	}

	@Override
	public void configure(Configuration config) {
		factory.configure(config);
	}

	@Override
	public FileSystem create(URI fsUri) throws IOException {
		FileSystem original = factory.create(fsUri);
		return new LimitedConnectionsFileSystem(original,
				settings.limitTotal, settings.limitOutput, settings.limitInput,
				settings.streamOpenTimeout, settings.streamInactivityTimeout);
	}

	// ------------------------------------------------------------------------

	/**
	 * Decorates the given factory for a {@code ConnectionLimitingFactory}, if the given
	 * configuration configured connection limiting for the given file system scheme.
	 * Otherwise, it returns the given factory as is.
	 *
	 * @param factory The factory to potentially decorate.
	 * @param scheme The file scheme for which to check the configuration.
	 * @param config The configuration
	 *
	 * @return The decorated factors, if connection limiting is configured, the original factory otherwise.
	 */
	public static FileSystemFactory decorateIfLimited(FileSystemFactory factory, String scheme, Configuration config) {
		checkNotNull(factory, "factory");

		final ConnectionLimitingSettings settings = ConnectionLimitingSettings.fromConfig(config, scheme);

		// decorate only if any limit is configured
		if (settings == null) {
			// no limit configured
			return factory;
		}
		else {
			return new ConnectionLimitingFactory(factory, settings);
		}
	}
}
