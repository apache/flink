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

package org.apache.flink.runtime.rpc;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.TimeUtils;

import java.lang.annotation.Annotation;
import java.time.Duration;

/** Utils for {@link RpcGateway} implementations. */
public class RpcGatewayUtils {

    /**
     * Extracts the {@link RpcTimeout} annotated rpc timeout value from the list of given method
     * arguments. If no {@link RpcTimeout} annotated parameter could be found, then the default
     * timeout is returned.
     *
     * @param parameterAnnotations Parameter annotations
     * @param args Array of arguments
     * @param defaultTimeout Default timeout to return if no {@link RpcTimeout} annotated parameter
     *     has been found
     * @return Timeout extracted from the array of arguments or the default timeout
     */
    public static Duration extractRpcTimeout(
            Annotation[][] parameterAnnotations, Object[] args, Duration defaultTimeout) {
        if (args != null) {
            Preconditions.checkArgument(parameterAnnotations.length == args.length);

            for (int i = 0; i < parameterAnnotations.length; i++) {
                if (isRpcTimeout(parameterAnnotations[i])) {
                    if (args[i] instanceof Time) {
                        return TimeUtils.toDuration((Time) args[i]);
                    } else if (args[i] instanceof Duration) {
                        return (Duration) args[i];
                    } else {
                        throw new RuntimeException(
                                "The rpc timeout parameter must be of type "
                                        + Time.class.getName()
                                        + " or "
                                        + Duration.class.getName()
                                        + ". The type "
                                        + args[i].getClass().getName()
                                        + " is not supported.");
                    }
                }
            }
        }

        return defaultTimeout;
    }

    /**
     * Checks whether any of the annotations is of type {@link RpcTimeout}.
     *
     * @param annotations Array of annotations
     * @return True if {@link RpcTimeout} was found; otherwise false
     */
    private static boolean isRpcTimeout(Annotation[] annotations) {
        for (Annotation annotation : annotations) {
            if (annotation.annotationType().equals(RpcTimeout.class)) {
                return true;
            }
        }

        return false;
    }

    private RpcGatewayUtils() {}
}
