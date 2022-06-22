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

package org.apache.flink.table.runtime.util;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.table.api.config.ExecutionConfigOptions;

import org.slf4j.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** Utility to handle processing errors. */
@Internal
public class ErrorHandlingUtil {

    /**
     * Message to indicate the state is expired because exceeds the state ttl. The message could be
     * used to output to log.
     */
    public static final String STATE_EXPIRED_WARN_MSG =
            "The state is cleared because of state ttl. "
                    + "This will result in incorrect result. You can increase the state ttl to avoid this.";

    /**
     * Message to indicate an unexpected state error when state ttl is disabled. The message could
     * be used to raise an error.
     */
    public static final String STATE_ERROR_MSG =
            "The state is unexpectedly cleared while state ttl is not enabled, this is a bug, please fire an issue.";

    /** Utility method to handle state staled error. */
    public static void handleStateStaledError(
            @Nonnull StateTtlConfig stateTtlConfig,
            @Nonnull ExecutionConfigOptions.StateStaledErrorHandling errorHandling,
            @Nullable Logger logger) {
        handleStateStaledError(stateTtlConfig, errorHandling, STATE_EXPIRED_WARN_MSG, logger);
    }

    /**
     * Handle the state staled error by state ttl and error handling configuration. If
     * stateTtlConfig is enabled and errorHandling is not set to ERROR otherwise a {@link
     * RuntimeException} will be thrown.
     */
    public static void handleStateStaledError(
            @Nonnull StateTtlConfig stateTtlConfig,
            @Nonnull ExecutionConfigOptions.StateStaledErrorHandling errorHandling,
            @Nonnull String errorMessage,
            @Nullable Logger logger) {
        if (stateTtlConfig.isEnabled()) {
            switch (errorHandling) {
                case ERROR:
                    throw new RuntimeException(errorMessage);
                case CONTINUE_WITH_LOGGING:
                    if (null != logger) {
                        logger.warn(errorMessage);
                    }
                    break;
                case CONTINUE_WITHOUT_LOGGING:
                    break;
            }
        } else {
            throw new RuntimeException(STATE_ERROR_MSG);
        }
    }
}
