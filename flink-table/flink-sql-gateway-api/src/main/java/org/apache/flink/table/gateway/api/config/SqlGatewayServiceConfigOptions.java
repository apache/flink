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

package org.apache.flink.table.gateway.api.config;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.gateway.api.SqlGatewayService;

import java.time.Duration;

import static org.apache.flink.configuration.ConfigOptions.key;

/** Config options of the {@link SqlGatewayService}. */
@PublicEvolving
public class SqlGatewayServiceConfigOptions {

    public static final ConfigOption<Duration> SQL_GATEWAY_SESSION_IDLE_TIMEOUT =
            key("sql-gateway.session.idle-timeout")
                    .durationType()
                    .defaultValue(Duration.ofMinutes(10))
                    .withDescription(
                            "Timeout interval for closing the session when the session hasn't been accessed during the interval. "
                                    + "If setting to zero, the session will not be closed.");

    public static final ConfigOption<Duration> SQL_GATEWAY_SESSION_CHECK_INTERVAL =
            key("sql-gateway.session.check-interval")
                    .durationType()
                    .defaultValue(Duration.ofMinutes(1))
                    .withDescription(
                            "The check interval for idle session timeout, which can be disabled by setting to zero.");

    public static final ConfigOption<Integer> SQL_GATEWAY_SESSION_MAX_NUM =
            key("sql-gateway.session.max-num")
                    .intType()
                    .defaultValue(1000000)
                    .withDescription(
                            "The maximum number of the active session for sql gateway service.");

    public static final ConfigOption<Integer> SQL_GATEWAY_WORKER_THREADS_MAX =
            key("sql-gateway.worker.threads.max")
                    .intType()
                    .defaultValue(500)
                    .withDescription(
                            "The maximum number of worker threads for sql gateway service.");

    public static final ConfigOption<Integer> SQL_GATEWAY_WORKER_THREADS_MIN =
            key("sql-gateway.worker.threads.min")
                    .intType()
                    .defaultValue(5)
                    .withDescription(
                            "The minimum number of worker threads for sql gateway service.");

    public static final ConfigOption<Duration> SQL_GATEWAY_WORKER_KEEPALIVE_TIME =
            key("sql-gateway.worker.keepalive-time")
                    .durationType()
                    .defaultValue(Duration.ofMinutes(5))
                    .withDescription(
                            "Keepalive time for an idle worker thread. When the number of workers exceeds min workers, "
                                    + "excessive threads are killed after this time interval.");
}
