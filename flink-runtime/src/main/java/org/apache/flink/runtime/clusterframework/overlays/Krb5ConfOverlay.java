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
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.clusterframework.ContainerSpecification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;


/**
 * Overlays a Kerberos configuration file into a container.
 *
 * The following files are copied to the container:
 *  - krb5.conf
 *
 * The following Java system properties are set in the container:
 *  - java.security.krb5.conf
 */
public class Krb5ConfOverlay extends AbstractContainerOverlay {

	private static final Logger LOG = LoggerFactory.getLogger(Krb5ConfOverlay.class);

	static final String JAVA_SECURITY_KRB5_CONF = "java.security.krb5.conf";

	static final Path TARGET_PATH = new Path("krb5.conf");
	final Path krb5Conf;

	public Krb5ConfOverlay(@Nullable File krb5Conf) {
		this.krb5Conf = krb5Conf != null ? new Path(krb5Conf.toURI()) : null;
	}

	public Krb5ConfOverlay(@Nullable Path krb5Conf) {
		this.krb5Conf = krb5Conf;
	}

	@Override
	public void configure(ContainerSpecification container) throws IOException {
		if(krb5Conf != null) {
			container.getArtifacts().add(ContainerSpecification.Artifact.newBuilder()
				.setSource(krb5Conf)
				.setDest(TARGET_PATH)
				.setCachable(true)
				.build());
			container.getSystemProperties().setString(JAVA_SECURITY_KRB5_CONF, TARGET_PATH.getPath());
		}
	}

	public static Builder newBuilder() {
		return new Builder();
	}

	/**
	 * A builder for the {@link Krb5ConfOverlay}.
	 */
	public static class Builder {

		File krb5ConfPath;

		/**
		 * Configures the overlay using the current environment.
		 *
		 * Locates the krb5.conf configuration file as per
		 * <a href="https://docs.oracle.com/javase/8/docs/technotes/guides/security/jgss/tutorials/KerberosReq.html">Java documentation</a>.
		 * Note that the JRE doesn't support the KRB5_CONFIG environment variable (JDK-7045913).
		 */
		public Builder fromEnvironment(Configuration globalConfiguration) {

			// check the system property
			String krb5Config = System.getProperty(JAVA_SECURITY_KRB5_CONF);
			if(krb5Config != null && krb5Config.length() != 0) {
				krb5ConfPath = new File(krb5Config);
				if(!krb5ConfPath.exists()) {
					throw new IllegalStateException("java.security.krb5.conf refers to a non-existent file");
				}
			}

			// FUTURE: check the well-known paths
			// - $JAVA_HOME/lib/security
			// - %WINDIR%\krb5.ini (Windows)
			// - /etc/krb5.conf (Linux)

			return this;
		}

		public Krb5ConfOverlay build() {
			return new Krb5ConfOverlay(krb5ConfPath);
		}
	}
}
