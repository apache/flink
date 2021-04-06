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
import org.apache.flink.core.plugin.PluginConfig;
import org.apache.flink.runtime.clusterframework.ContainerSpecification;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;

import static org.apache.flink.configuration.ConfigConstants.ENV_FLINK_BIN_DIR;
import static org.apache.flink.configuration.ConfigConstants.ENV_FLINK_CONF_DIR;
import static org.apache.flink.configuration.ConfigConstants.ENV_FLINK_HOME_DIR;
import static org.apache.flink.configuration.ConfigConstants.ENV_FLINK_LIB_DIR;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Overlays Flink into a container, based on supplied bin/conf/lib directories.
 *
 * <p>The overlayed Flink is indistinguishable from (and interchangeable with) a normal installation
 * of Flink. For a docker image-based container, it should be possible to bypass this overlay and
 * rely on the normal installation method.
 *
 * <p>The following files are copied to the container: - bin/ - conf/ - lib/ - plugins/
 */
public class FlinkDistributionOverlay extends AbstractContainerOverlay {

    static final String TARGET_ROOT_STR = Path.CUR_DIR;

    static final Path TARGET_ROOT = new Path(TARGET_ROOT_STR);

    private final File flinkBinPath;
    private final File flinkConfPath;
    private final File flinkLibPath;
    @Nullable private final File flinkPluginsPath;

    FlinkDistributionOverlay(
            File flinkBinPath,
            File flinkConfPath,
            File flinkLibPath,
            @Nullable File flinkPluginsPath) {
        this.flinkBinPath = checkNotNull(flinkBinPath);
        this.flinkConfPath = checkNotNull(flinkConfPath);
        this.flinkLibPath = checkNotNull(flinkLibPath);
        this.flinkPluginsPath = flinkPluginsPath;
    }

    @Override
    public void configure(ContainerSpecification container) throws IOException {

        container.getEnvironmentVariables().put(ENV_FLINK_HOME_DIR, TARGET_ROOT_STR);

        // add the paths to the container specification.
        addPathRecursively(flinkBinPath, TARGET_ROOT, container);
        addPathRecursively(flinkConfPath, TARGET_ROOT, container);
        addPathRecursively(flinkLibPath, TARGET_ROOT, container);
        if (flinkPluginsPath != null) {
            addPathRecursively(flinkPluginsPath, TARGET_ROOT, container);
        }
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /** A builder for the {@link FlinkDistributionOverlay}. */
    public static class Builder {
        File flinkBinPath;
        File flinkConfPath;
        File flinkLibPath;
        @Nullable File flinkPluginsPath;

        /**
         * Configures the overlay using the current environment.
         *
         * <p>Locates Flink using FLINK_???_DIR environment variables as provided to all Flink
         * processes by config.sh.
         *
         * @param globalConfiguration the current configuration.
         */
        public Builder fromEnvironment(Configuration globalConfiguration) {
            flinkBinPath = getObligatoryFileFromEnvironment(ENV_FLINK_BIN_DIR);
            flinkConfPath = getObligatoryFileFromEnvironment(ENV_FLINK_CONF_DIR);
            flinkLibPath = getObligatoryFileFromEnvironment(ENV_FLINK_LIB_DIR);
            flinkPluginsPath = PluginConfig.getPluginsDir().orElse(null);

            return this;
        }

        public FlinkDistributionOverlay build() {
            return new FlinkDistributionOverlay(
                    flinkBinPath, flinkConfPath, flinkLibPath, flinkPluginsPath);
        }

        private static File getObligatoryFileFromEnvironment(String envVariableName) {
            String directory = System.getenv(envVariableName);
            checkState(
                    directory != null, "the %s environment variable must be set", envVariableName);
            return new File(directory);
        }
    }
}
