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
 * limitations under the License
 */

package org.apache.flink.runtime.rest.messages;

import org.apache.flink.runtime.util.EnvironmentInformation;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** The response of environment info. */
public class EnvironmentInfo implements ResponseBody {

    private static final String FIELD_NAME_ENVIRONMENT_INFO = "environment";

    private static final String FIELD_NAME_JVM_INFO = "jvm";

    private static final String FIELD_NAME_CLASSPATH = "classpath";

    @JsonProperty(FIELD_NAME_ENVIRONMENT_INFO)
    private final List<EnvironmentVariableItem> environmentVariables;

    @JsonProperty(FIELD_NAME_JVM_INFO)
    private final JVMInfo jvmInfo;

    @JsonProperty(FIELD_NAME_CLASSPATH)
    private final List<String> classpath;

    @JsonCreator
    public EnvironmentInfo(
            @JsonProperty(FIELD_NAME_ENVIRONMENT_INFO)
                    List<EnvironmentVariableItem> environmentVariables,
            @JsonProperty(FIELD_NAME_JVM_INFO) JVMInfo jvmInfo,
            @JsonProperty(FIELD_NAME_CLASSPATH) List<String> classpath) {
        this.environmentVariables = environmentVariables;
        this.jvmInfo = jvmInfo;
        this.classpath = classpath;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        EnvironmentInfo that = (EnvironmentInfo) o;
        return environmentVariables.equals(that.environmentVariables)
                && jvmInfo.equals(that.jvmInfo)
                && classpath.equals(that.classpath);
    }

    @Override
    public int hashCode() {
        return Objects.hash(environmentVariables, jvmInfo, classpath);
    }

    public static EnvironmentInfo create() {
        List<EnvironmentVariableItem> environmentVariableItems = new ArrayList<>();
        System.getenv()
                .forEach(
                        (key, value) ->
                                environmentVariableItems.add(
                                        new EnvironmentVariableItem(key, value)));

        return new EnvironmentInfo(
                environmentVariableItems,
                JVMInfo.create(),
                Arrays.asList(System.getProperty("java.class.path").split(":")));
    }

    /** A single key-value pair entry in the {@link EnvironmentInfo} response. */
    private static class EnvironmentVariableItem {
        private static final String FIELD_NAME_KEY = "key";

        private static final String FIELD_NAME_VALUE = "value";

        @JsonProperty(FIELD_NAME_KEY)
        private final String key;

        @JsonProperty(FIELD_NAME_VALUE)
        private final String value;

        @JsonCreator
        public EnvironmentVariableItem(
                @JsonProperty(FIELD_NAME_KEY) String key,
                @JsonProperty(FIELD_NAME_VALUE) String value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            EnvironmentVariableItem that = (EnvironmentVariableItem) o;
            return key.equals(that.key) && value.equals(that.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(key, value);
        }
    }

    /** JVM information. */
    private static class JVMInfo {
        private static final String FIELD_NAME_VERSION = "version";

        private static final String FIELD_NAME_ARCH = "arch";

        private static final String FIELD_NAME_OPTIONS = "options";

        @JsonProperty(FIELD_NAME_VERSION)
        private final String version;

        @JsonProperty(FIELD_NAME_ARCH)
        private final String arch;

        @JsonProperty(FIELD_NAME_OPTIONS)
        private final List<String> options;

        @JsonCreator
        public JVMInfo(
                @JsonProperty(FIELD_NAME_VERSION) String version,
                @JsonProperty(FIELD_NAME_ARCH) String arch,
                @JsonProperty(FIELD_NAME_OPTIONS) List<String> options) {
            this.version = version;
            this.arch = arch;
            this.options = options;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            JVMInfo that = (JVMInfo) o;
            return version.equals(that.version)
                    && arch.equals(that.arch)
                    && options.equals(that.options);
        }

        @Override
        public int hashCode() {
            return Objects.hash(version, arch, options);
        }

        private static JVMInfo create() {
            return new JVMInfo(
                    EnvironmentInformation.getJvmVersion(),
                    System.getProperty("os.arch"),
                    Arrays.asList(EnvironmentInformation.getJvmStartupOptionsArray()));
        }
    }
}
