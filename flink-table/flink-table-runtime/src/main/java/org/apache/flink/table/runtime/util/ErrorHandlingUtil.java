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

/** Utility to handle state stale errors. */
@Internal
public class ErrorHandlingUtil {

    /**
     * Message to indicate the state is expired because exceeds the state ttl. The message could be
     * used to output to log.
     */
    public static final String STATE_STALE_WARN_MSG =
            "The state is cleared because of state ttl. "
                    + "This will result in incorrect result. You can increase the state ttl to avoid this.";

    /**
     * Message to indicate an unexpected state error when state ttl is disabled. The message could
     * be used to raise an error.
     */
    public static final String UNEXPECTED_STATE_ERROR =
            "The state is unexpectedly cleared while state ttl is not enabled, this is a bug, please fire an issue.";

    /**
     * Handle the state stale error by given {@link StateTtlConfig} and {@link
     * ExecutionConfigOptions.StateStaleErrorHandling}.
     *
     * <p>The action mapping to the StateTtlConfig and StateStaleErrorHandling:
     *
     * <table border="1" align="left">
     *   <tr>
     *     <td> Action </td> <td> StateTtlConfig </td> <td> StateStaleErrorHandling </td>
     *   </tr>
     *   <tr>
     *     <td> Raise Error </td> <td> Disabled </td> <td> Any </td>
     *   </tr>
     *   <tr>
     *     <td> Raise Error </td> <td> Enabled </td> <td> ERROR </td>
     *   </tr>
     *   <tr>
     *     <td> Log Only </td> <td> Enabled </td> <td> CONTINUE_WITH_LOGGING </td>
     *   </tr>
     *   <tr>
     *     <td> Do Nothing </td> <td> Enabled </td> <td> CONTINUE_WITHOUT_LOGGING </td>
     *   </tr>
     * </table>
     *
     * <p>Note: StateTtlConfig enabled means state will be expired after configured duration which
     * should be greater than zero.
     */
    public static void handleStateStaleError(
            @Nonnull StateTtlConfig stateTtlConfig,
            @Nonnull ExecutionConfigOptions.StateStaleErrorHandling errorHandling,
            @Nonnull String stateStaleErrorMessage,
            @Nullable Logger logger) {
        if (stateTtlConfig.isEnabled() && stateTtlConfig.getTtl().toMilliseconds() > 0) {
            switch (errorHandling) {
                case ERROR:
                    throw new RuntimeException(stateStaleErrorMessage);
                case CONTINUE_WITH_LOGGING:
                    if (null != logger) {
                        logger.warn(stateStaleErrorMessage);
                    }
                    break;
                case CONTINUE_WITHOUT_LOGGING:
                    break;
            }
        } else {
            throw new RuntimeException(UNEXPECTED_STATE_ERROR);
        }
    }

    /** HandleStateStaleError using default STATE_EXPIRED_WARN_MSG. */
    public static void handleStateStaleError(
            @Nonnull StateTtlConfig stateTtlConfig,
            @Nonnull ExecutionConfigOptions.StateStaleErrorHandling errorHandling,
            @Nullable Logger logger) {
        handleStateStaleError(stateTtlConfig, errorHandling, STATE_STALE_WARN_MSG, logger);
    }
}
