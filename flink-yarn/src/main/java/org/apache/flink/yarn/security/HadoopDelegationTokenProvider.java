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

package org.apache.flink.yarn.security;

import org.apache.flink.configuration.Configuration;

import org.apache.hadoop.security.Credentials;

/** Hadoop delegation token provider. */
public interface HadoopDelegationTokenProvider {

    /** Name of the service to provide delegation tokens. This name should be unique. */
    String serviceName();

    /**
     * Return true if delegation tokens are required for this service.
     *
     * @param flinkConf Flink configuration
     * @param hadoopConf Hadoop configuration
     * @return true if delegation tokens are required
     */
    boolean delegationTokensRequired(
            Configuration flinkConf, org.apache.hadoop.conf.Configuration hadoopConf);

    /**
     * Obtain delegation tokens for this service
     *
     * @param flinkConf Flink configuration
     * @param hadoopConf Hadoop configuration
     * @param credentials Credentials to add tokens and security keys to.
     */
    void obtainDelegationTokens(
            Configuration flinkConf,
            org.apache.hadoop.conf.Configuration hadoopConf,
            Credentials credentials);
}
