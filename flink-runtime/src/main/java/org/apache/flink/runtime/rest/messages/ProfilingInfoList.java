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

package org.apache.flink.runtime.rest.messages;

import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;

/** Class containing a collection of {@link ProfilingInfo}. */
public class ProfilingInfoList implements ResponseBody, Serializable {

    public static final String FIELD_NAME_FLAME_GRAPHS = "profilingList";

    private static final long serialVersionUID = 1L;

    @JsonProperty(FIELD_NAME_FLAME_GRAPHS)
    private final Collection<ProfilingInfo> profilingInfos;

    @JsonCreator
    public ProfilingInfoList(
            @JsonProperty(FIELD_NAME_FLAME_GRAPHS) Collection<ProfilingInfo> profilingInfos) {
        this.profilingInfos = Preconditions.checkNotNull(profilingInfos);
    }

    public Collection<ProfilingInfo> getProfilingInfos() {
        return profilingInfos;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ProfilingInfoList that = (ProfilingInfoList) o;
        return Objects.equals(profilingInfos, that.profilingInfos);
    }

    @Override
    public int hashCode() {
        return Objects.hash(profilingInfos);
    }

    public static ProfilingInfoList empty() {
        return new ProfilingInfoList(Collections.emptyList());
    }

    public static ProfilingInfoList createNullable(Collection<ProfilingInfo> profilingInfos) {
        if (CollectionUtil.isNullOrEmpty(profilingInfos)) {
            return empty();
        }
        return new ProfilingInfoList(profilingInfos);
    }
}
