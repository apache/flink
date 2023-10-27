/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.metrics.datadog;

/** The data center to connect to. */
enum DataCenter {
    US1("https://app.datadoghq.com"),
    US3("https://us3.datadoghq.com"),
    US5("https://us5.datadoghq.com"),
    EU1("https://app.datadoghq.eu"),
    US1_FED("https://app.ddog-gov.com"),
    AP1("https://ap1.datadoghq.com");
    private final String url;

    DataCenter(String url) {
        this.url = url;
    }

    public String getUrl() {
        return url;
    }
}
