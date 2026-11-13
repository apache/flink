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

package org.apache.flink.util;

import org.apache.flink.api.common.errors.FlinkError;
import org.apache.flink.api.common.errors.FlinkError.Parameter;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Shared by {@link FlinkRuntimeException} and {@link FlinkException} for exceptions of errors. */
final class FlinkThrowableUtils {

    /**
     * Returns the formatted template of the error.
     *
     * @throws IllegalArgumentException with the given cause if the error isn't an enum constant or
     *     the arguments don't match the parameters of the error
     */
    static String formatMessage(
            FlinkError error, @Nullable Throwable cause, @Nullable Object[] arguments) {
        Preconditions.checkNotNull(error, "Error must not be null.");
        if (!(error instanceof Enum)) {
            throw new IllegalArgumentException(
                    String.format(
                            "Error '%s' must be an enum constant, but is a %s.",
                            error.name(), error.getClass().getName()),
                    cause);
        }
        if (arguments == null) {
            throw new IllegalArgumentException(
                    String.format("Error '%s' expects arguments, but got null.", error.name()),
                    cause);
        }
        try {
            return error.format(Arrays.asList(arguments));
        } catch (IllegalArgumentException e) {
            // A wrong throw site must not hide the cause, which is often the actual problem.
            throw new IllegalArgumentException(e.getMessage(), cause);
        }
    }

    /** Returns the arguments by parameter name, for arguments that match the parameters. */
    static Map<String, Object> toArgumentMap(FlinkError error, Object[] arguments) {
        final List<Parameter> parameters = error.getParameters();
        final Map<String, Object> map = new LinkedHashMap<>();
        for (int i = 0; i < parameters.size(); i++) {
            map.put(parameters.get(i).getName(), arguments[i]);
        }
        return Collections.unmodifiableMap(map);
    }

    private FlinkThrowableUtils() {}
}
