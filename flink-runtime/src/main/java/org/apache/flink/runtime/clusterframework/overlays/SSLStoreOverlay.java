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
import org.apache.flink.configuration.SecurityOptions;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.clusterframework.ContainerSpecification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;


/**
 * Overlays an SSL keystore/truststore into a container.
 *
 * The following files are placed into the container:
 *  - keystore.jks
 *  - truststore.jks
 *
 * The following Flink configuration entries are set:
 *  - security.ssl.keystore
 *  - security.ssl.truststore
 */
public class SSLStoreOverlay extends AbstractContainerOverlay {

	private static final Logger LOG = LoggerFactory.getLogger(SSLStoreOverlay.class);

	static final Path TARGET_KEYSTORE_PATH = new Path("keystore.jks");
	static final Path TARGET_TRUSTSTORE_PATH = new Path("truststore.jks");

	final Path keystore;
	final Path truststore;

	public SSLStoreOverlay(@Nullable File keystoreFile, @Nullable File truststoreFile) {
		this.keystore = keystoreFile != null ? new Path(keystoreFile.toURI()) : null;
		this.truststore = truststoreFile != null ? new Path(truststoreFile.toURI()) : null;
	}

	@Override
	public void configure(ContainerSpecification container) throws IOException {
		if(keystore != null) {
			container.getArtifacts().add(ContainerSpecification.Artifact.newBuilder()
				.setSource(keystore)
				.setDest(TARGET_KEYSTORE_PATH)
				.setCachable(false)
				.build());
			container.getDynamicConfiguration().setString(SecurityOptions.SSL_KEYSTORE, TARGET_KEYSTORE_PATH.getPath());
		}
		if(truststore != null) {
			container.getArtifacts().add(ContainerSpecification.Artifact.newBuilder()
				.setSource(truststore)
				.setDest(TARGET_TRUSTSTORE_PATH)
				.setCachable(false)
				.build());
			container.getDynamicConfiguration().setString(SecurityOptions.SSL_TRUSTSTORE, TARGET_TRUSTSTORE_PATH.getPath());
		}
	}

	public static Builder newBuilder() {
		return new Builder();
	}

	/**
	 * A builder for the {@link Krb5ConfOverlay}.
	 */
	public static class Builder {

		File keystorePath;

		File truststorePath;

		/**
		 * Configures the overlay using the current environment (and global configuration).
		 *
		 * The following Flink configuration settings are used to source the keystore and truststore:
		 *  - security.ssl.keystore
		 *  - security.ssl.truststore
		 */
		public Builder fromEnvironment(Configuration globalConfiguration)  {

			String keystore = globalConfiguration.getString(SecurityOptions.SSL_KEYSTORE);
			if(keystore != null) {
				keystorePath = new File(keystore);
				if(!keystorePath.exists()) {
					throw new IllegalStateException("Invalid configuration for " + SecurityOptions.SSL_KEYSTORE.key());
				}
			}

			String truststore = globalConfiguration.getString(SecurityOptions.SSL_TRUSTSTORE);
			if(truststore != null) {
				truststorePath = new File(truststore);
				if(!truststorePath.exists()) {
					throw new IllegalStateException("Invalid configuration for " + SecurityOptions.SSL_TRUSTSTORE.key());
				}
			}

			return this;
		}

		public SSLStoreOverlay build() {
			return new SSLStoreOverlay(keystorePath, truststorePath);
		}
	}
}
