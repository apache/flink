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

package org.apache.flink.runtime.webmonitor.handlers;

import org.apache.flink.runtime.rest.messages.ResponseBody;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Response type of the {@link JarListHandler}. */
public class JarListInfo implements ResponseBody {
    public static final String JAR_LIST_FIELD_ADDRESS = "address";
    public static final String JAR_LIST_FIELD_FILES = "files";

    @JsonProperty(JAR_LIST_FIELD_ADDRESS)
    private String address;

    @JsonProperty(JAR_LIST_FIELD_FILES)
    public List<JarFileInfo> jarFileList;

    @JsonCreator
    public JarListInfo(
            @JsonProperty(JAR_LIST_FIELD_ADDRESS) String address,
            @JsonProperty(JAR_LIST_FIELD_FILES) List<JarFileInfo> jarFileList) {
        this.address = checkNotNull(address);
        this.jarFileList = checkNotNull(jarFileList);
    }

    @Override
    public int hashCode() {
        return Objects.hash(address, jarFileList);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (null == o || this.getClass() != o.getClass()) {
            return false;
        }

        JarListInfo that = (JarListInfo) o;
        return Objects.equals(address, that.address)
                && Objects.equals(jarFileList, that.jarFileList);
    }

    // ---------------------------------------------------------------------------------
    // Static helper classes
    // ---------------------------------------------------------------------------------

    /** Nested class to encapsulate the jar file info. */
    public static class JarFileInfo {
        public static final String JAR_FILE_FIELD_ID = "id";
        public static final String JAR_FILE_FIELD_NAME = "name";
        public static final String JAR_FILE_FIELD_UPLOADED = "uploaded";
        public static final String JAR_FILE_FIELD_ENTRY = "entry";

        @JsonProperty(JAR_FILE_FIELD_ID)
        public String id;

        @JsonProperty(JAR_FILE_FIELD_NAME)
        public String name;

        @JsonProperty(JAR_FILE_FIELD_UPLOADED)
        private long uploaded;

        @JsonProperty(JAR_FILE_FIELD_ENTRY)
        private List<JarEntryInfo> jarEntryList;

        @JsonCreator
        public JarFileInfo(
                @JsonProperty(JAR_FILE_FIELD_ID) String id,
                @JsonProperty(JAR_FILE_FIELD_NAME) String name,
                @JsonProperty(JAR_FILE_FIELD_UPLOADED) long uploaded,
                @JsonProperty(JAR_FILE_FIELD_ENTRY) List<JarEntryInfo> jarEntryList) {
            this.id = checkNotNull(id);
            this.name = checkNotNull(name);
            this.uploaded = uploaded;
            this.jarEntryList = checkNotNull(jarEntryList);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, name, uploaded, jarEntryList);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (null == o || this.getClass() != o.getClass()) {
                return false;
            }

            JarFileInfo that = (JarFileInfo) o;
            return Objects.equals(id, that.id)
                    && Objects.equals(name, that.name)
                    && uploaded == that.uploaded
                    && Objects.equals(jarEntryList, that.jarEntryList);
        }
    }

    /** Nested class to encapsulate the jar entry info. */
    public static class JarEntryInfo {
        public static final String JAR_ENTRY_FIELD_NAME = "name";
        public static final String JAR_ENTRY_FIELD_DESC = "description";

        @JsonProperty(JAR_ENTRY_FIELD_NAME)
        private String name;

        @JsonProperty(JAR_ENTRY_FIELD_DESC)
        @Nullable
        private String description;

        @JsonCreator
        public JarEntryInfo(
                @JsonProperty(JAR_ENTRY_FIELD_NAME) String name,
                @JsonProperty(JAR_ENTRY_FIELD_DESC) @Nullable String description) {
            this.name = checkNotNull(name);
            this.description = description;
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, description);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (null == o || this.getClass() != o.getClass()) {
                return false;
            }

            JarEntryInfo that = (JarEntryInfo) o;
            return Objects.equals(name, that.name) && Objects.equals(description, that.description);
        }
    }
}
