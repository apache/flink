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
 * Overlays cluster-level Kerberos credentials (i.e. keytab) into a container.
 *
 * <p>The following Flink configuration entries are updated: - security.kerberos.login.keytab
 */
public class KeytabOverlay extends AbstractContainerOverlay {

    private static final Logger LOG = LoggerFactory.getLogger(KeytabOverlay.class);

    static final Path TARGET_PATH = new Path("krb5.keytab");

    final Path keytab;

    public KeytabOverlay(@Nullable File keytab) {
        this.keytab = keytab != null ? new Path(keytab.toURI()) : null;
    }

    public KeytabOverlay(@Nullable Path keytab) {
        this.keytab = keytab;
    }

    @Override
    public void configure(ContainerSpecification container) throws IOException {
        if (keytab != null) {
            container
                    .getArtifacts()
                    .add(
                            ContainerSpecification.Artifact.newBuilder()
                                    .setSource(keytab)
                                    .setDest(TARGET_PATH)
                                    .setCachable(false)
                                    .build());
            container
                    .getFlinkConfiguration()
                    .setString(SecurityOptions.KERBEROS_LOGIN_KEYTAB, TARGET_PATH.getPath());
        }
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /** A builder for the {@link KeytabOverlay}. */
    public static class Builder {

        File keytabPath;

        /**
         * Configures the overlay using the current environment (and global configuration).
         *
         * <p>The following Flink configuration settings are checked for a keytab: -
         * security.kerberos.login.keytab
         */
        public Builder fromEnvironment(Configuration globalConfiguration) {
            String keytab = globalConfiguration.getString(SecurityOptions.KERBEROS_LOGIN_KEYTAB);
            if (keytab != null) {
                keytabPath = new File(keytab);
                if (!keytabPath.exists()) {
                    throw new IllegalStateException(
                            "Invalid configuration for "
                                    + SecurityOptions.KERBEROS_LOGIN_KEYTAB
                                    + "; '"
                                    + keytab
                                    + "' not found.");
                }
            }

            return this;
        }

        public KeytabOverlay build() {
            return new KeytabOverlay(keytabPath);
        }
    }
}
