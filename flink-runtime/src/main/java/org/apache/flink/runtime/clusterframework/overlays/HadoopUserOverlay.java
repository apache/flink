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

package org.apache.flink.runtime.clusterframework.overlays;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.clusterframework.ContainerSpecification;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;

/**
 * Overlays a Hadoop user context into a container.
 *
 * The overlay essentially configures Hadoop's {@link UserGroupInformation} class,
 * establishing the effective username for filesystem calls to HDFS in non-secure clusters.
 *
 * In secure clusters, the configured keytab establishes the effective user.
 *
 * The following environment variables are set in the container:
 *  - HADOOP_USER_NAME
 */
public class HadoopUserOverlay implements ContainerOverlay {

	private static final Logger LOG = LoggerFactory.getLogger(HadoopUserOverlay.class);

	private final UserGroupInformation ugi;

	public HadoopUserOverlay(@Nullable UserGroupInformation ugi) {
		this.ugi = ugi;
	}

	@Override
	public void configure(ContainerSpecification container) throws IOException {
		if(ugi != null) {
			// overlay the Hadoop user identity (w/ tokens)
			container.getEnvironmentVariables().put("HADOOP_USER_NAME", ugi.getUserName());
		}
	}

	public static Builder newBuilder() {
		// First check if we have Hadoop specific dependencies in the ClassPath. If not, we simply don't do anything.
		try {
			Class.forName(
				"org.apache.hadoop.security.UserGroupInformation",
				false,
				HadoopUserOverlay.class.getClassLoader());
		} catch (ClassNotFoundException e) {
			LOG.info("Cannot create Hadoop User Overlay because Hadoop specific dependencies cannot be found in the Classpath.");
			return new EmptyHadoopUserOverlayBuilder();
		}

		return new HadoopUserOverlayBuilder();
	}

	/**
	 * A builder for the {@link HadoopUserOverlay}.
	 */
	public interface Builder {
		/**
		 * Configures the overlay using the current Hadoop user information (from {@link UserGroupInformation}).
		 */
		Builder fromEnvironment(Configuration globalConfiguration) throws IOException;

		HadoopUserOverlay build();
	}

	/**
	 * A builder for {@link HadoopUserOverlay} when hadoop doesn't exist on the classpath
	 */
	public static class EmptyHadoopUserOverlayBuilder implements Builder {
		@Override
		public Builder fromEnvironment(Configuration globalConfiguration) throws IOException {
			return this;
		}

		@Override
		public HadoopUserOverlay build() {
			return new HadoopUserOverlay(null);
		}
	}

	public static class HadoopUserOverlayBuilder implements Builder {

		UserGroupInformation ugi;

		@Override
		public Builder fromEnvironment(Configuration globalConfiguration) throws IOException {
			ugi = UserGroupInformation.getCurrentUser();
			return this;
		}

		@Override
		public HadoopUserOverlay build() {
			return new HadoopUserOverlay(ugi);
		}
	}
}

