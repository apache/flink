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
 * <p>The overlay essentially configures Hadoop's {@link UserGroupInformation} class, establishing
 * the effective username for filesystem calls to HDFS in non-secure clusters.
 *
 * <p>In secure clusters, the configured keytab establishes the effective user.
 *
 * <p>The following environment variables are set in the container: - HADOOP_USER_NAME
 */
public class HadoopUserOverlay implements ContainerOverlay {

    private static final Logger LOG = LoggerFactory.getLogger(HadoopUserOverlay.class);

    private final UserGroupInformation ugi;

    public HadoopUserOverlay(@Nullable UserGroupInformation ugi) {
        this.ugi = ugi;
    }

    @Override
    public void configure(ContainerSpecification container) throws IOException {
        if (ugi != null) {
            // overlay the Hadoop user identity (w/ tokens)
            container.getEnvironmentVariables().put("HADOOP_USER_NAME", ugi.getUserName());
        }
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /** A builder for the {@link HadoopUserOverlay}. */
    public static class Builder {

        UserGroupInformation ugi;

        /**
         * Configures the overlay using the current Hadoop user information (from {@link
         * UserGroupInformation}).
         */
        public Builder fromEnvironment(Configuration globalConfiguration) throws IOException {
            ugi = UserGroupInformation.getCurrentUser();
            return this;
        }

        public HadoopUserOverlay build() {
            return new HadoopUserOverlay(ugi);
        }
    }
}
