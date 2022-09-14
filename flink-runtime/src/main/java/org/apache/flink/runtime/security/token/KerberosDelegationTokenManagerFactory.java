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

package org.apache.flink.runtime.security.token;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.SecurityOptions;
import org.apache.flink.runtime.hadoop.HadoopDependency;
import org.apache.flink.util.concurrent.ScheduledExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.concurrent.ExecutorService;

/** A factory for {@link KerberosDelegationTokenManager}. */
public class KerberosDelegationTokenManagerFactory {

    private static final Logger LOG =
            LoggerFactory.getLogger(KerberosDelegationTokenManagerFactory.class);

    public static DelegationTokenManager create(
            ClassLoader classLoader,
            Configuration configuration,
            @Nullable ScheduledExecutor scheduledExecutor,
            @Nullable ExecutorService ioExecutor) {

        if (configuration.getBoolean(SecurityOptions.KERBEROS_FETCH_DELEGATION_TOKEN)) {
            if (HadoopDependency.isHadoopCommonOnClasspath(classLoader)) {
                return new KerberosDelegationTokenManager(
                        configuration, scheduledExecutor, ioExecutor);
            } else {
                LOG.info(
                        "Cannot use kerberos delegation token manager because Hadoop cannot be found in the Classpath.");
                return new NoOpDelegationTokenManager();
            }
        } else {
            return new NoOpDelegationTokenManager();
        }
    }
}
