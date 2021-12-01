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

package org.apache.flink.connector.elasticsearch.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;

import org.apache.http.HttpHost;

import java.io.Serializable;
import java.time.Duration;
import java.util.List;

/** Configuration class for the {@link ElasticsearchSource}. */
@Internal
public class ElasticsearchSourceConfiguration implements Serializable {
    private final List<HttpHost> hosts;
    private final String index;
    private final int numberOfSlices;
    private final Duration pitKeepAlive;

    @VisibleForTesting
    public ElasticsearchSourceConfiguration(
            List<HttpHost> hosts, String index, int numberOfSlices, Duration pitKeepAlive) {
        this.hosts = hosts;
        this.index = index;
        this.numberOfSlices = numberOfSlices;
        this.pitKeepAlive = pitKeepAlive;
    }

    public List<HttpHost> getHosts() {
        return hosts;
    }

    public String getIndex() {
        return index;
    }

    public int getNumberOfSlices() {
        return numberOfSlices;
    }

    public Duration getPitKeepAlive() {
        return pitKeepAlive;
    }
}
