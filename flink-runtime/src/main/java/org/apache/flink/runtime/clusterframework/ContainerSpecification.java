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

package org.apache.flink.runtime.clusterframework;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Encapsulates a container specification, including artifacts, environment variables, system
 * properties, and Flink configuration settings.
 *
 * <p>The specification is mutable.
 *
 * <p>Note that the Flink configuration settings are considered dynamic overrides of whatever static
 * configuration file is present in the container. For example, a container might be based on a
 * Docker image with a normal Flink installation with customized settings, which these settings
 * would (partially) override.
 *
 * <p>Artifacts are copied into a sandbox directory within the container, which any Flink process
 * launched in the container is assumed to use as a working directory. This assumption allows for
 * relative paths to be used in certain environment variables.
 */
public class ContainerSpecification implements java.io.Serializable {

    private static final long serialVersionUID = 1L;

    private final Configuration systemProperties;

    private final List<Artifact> artifacts;

    private final Map<String, String> environmentVariables;

    private final Configuration flinkConfiguration;

    public ContainerSpecification() {
        this.artifacts = new LinkedList<>();
        this.environmentVariables = new HashMap<String, String>();
        this.systemProperties = new Configuration();
        this.flinkConfiguration = new Configuration();
    }

    /** Get the container artifacts. */
    public List<Artifact> getArtifacts() {
        return artifacts;
    }

    /** Get the environment variables. */
    public Map<String, String> getEnvironmentVariables() {
        return environmentVariables;
    }

    /** Get the dynamic configuration. */
    public Configuration getFlinkConfiguration() {
        return flinkConfiguration;
    }

    /** Get the system properties. */
    public Configuration getSystemProperties() {
        return systemProperties;
    }

    @Override
    public String toString() {
        return "ContainerSpecification{"
                + "environmentVariables="
                + environmentVariables
                + ", systemProperties="
                + systemProperties
                + ", dynamicConfiguration="
                + flinkConfiguration
                + ", artifacts="
                + artifacts
                + '}';
    }

    /** An artifact to be copied into the container. */
    public static class Artifact {

        public Artifact(
                Path source, Path dest, boolean executable, boolean cachable, boolean extract) {
            checkArgument(source.isAbsolute(), "source must be absolute");
            checkArgument(!dest.isAbsolute(), "destination must be relative");
            this.source = source;
            this.dest = dest;
            this.executable = executable;
            this.cachable = cachable;
            this.extract = extract;
        }

        public final Path source;
        public final Path dest;
        public final boolean executable;
        public final boolean cachable;
        public final boolean extract;

        @Override
        public String toString() {
            return "Artifact{"
                    + "source="
                    + source
                    + ", dest="
                    + dest
                    + ", executable="
                    + executable
                    + ", cachable="
                    + cachable
                    + ", extract="
                    + extract
                    + '}';
        }

        public static Builder newBuilder() {
            return new Builder();
        }

        public static class Builder {

            public Path source;
            public Path dest;
            public boolean executable = false;
            public boolean cachable = true;
            public boolean extract = false;

            public Builder setSource(Path source) {
                this.source = source;
                return this;
            }

            public Builder setDest(Path dest) {
                this.dest = dest;
                return this;
            }

            public Builder setCachable(boolean cachable) {
                this.cachable = cachable;
                return this;
            }

            public Builder setExtract(boolean extract) {
                this.extract = extract;
                return this;
            }

            public Builder setExecutable(boolean executable) {
                this.executable = executable;
                return this;
            }

            public Artifact build() {
                return new Artifact(source, dest, executable, cachable, extract);
            }
        }
    }

    public static ContainerSpecification from(Configuration flinkConfiguration) {
        final ContainerSpecification containerSpecification = new ContainerSpecification();
        containerSpecification.getFlinkConfiguration().addAll(flinkConfiguration);
        return containerSpecification;
    }

    /** Format the system properties as a shell-compatible command-line argument. */
    public static String formatSystemProperties(Configuration jvmArgs) {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, String> entry : jvmArgs.toMap().entrySet()) {
            if (sb.length() > 0) {
                sb.append(" ");
            }
            final String dynamicProperty = createDynamicProperty(entry.getKey(), entry.getValue());
            sb.append(dynamicProperty);
        }
        return sb.toString();
    }

    /**
     * Create a dynamic property from the given key and value of the format {@code -Dkey=value}.
     *
     * @param key of the dynamic property
     * @param value of the dynamic property
     * @return dynamic property
     */
    public static String createDynamicProperty(String key, String value) {
        final String keyPart = "-D" + key + '=';
        final String valuePart;

        if (value.contains(" ")) {
            valuePart = "\"" + value + "\"";
        } else {
            valuePart = value;
        }

        return keyPart + valuePart;
    }
}
