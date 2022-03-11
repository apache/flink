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

package org.apache.flink.client.program.rest.retry;

import org.apache.flink.configuration.Configuration;

import static org.apache.flink.configuration.RestOptions.EXP_WAIT_STRATEGY_INIT_WAIT;
import static org.apache.flink.configuration.RestOptions.EXP_WAIT_STRATEGY_MAX_WAIT;

/** Utilities to create {@link WaitStrategy}. */
public class WaitStrategyUtils {
    public static WaitStrategy createWaitStrategyWithConfig(Configuration config) {
        return new ExponentialWaitStrategy(
                config.get(EXP_WAIT_STRATEGY_INIT_WAIT).toMillis(),
                config.get(EXP_WAIT_STRATEGY_MAX_WAIT).toMillis());
    }
}
