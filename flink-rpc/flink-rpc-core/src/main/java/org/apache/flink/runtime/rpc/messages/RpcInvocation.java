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

package org.apache.flink.runtime.rpc.messages;

import java.io.IOException;

/**
 * Interface for rpc invocation messages. The interface allows to request all necessary information
 * to lookup a method and call it with the corresponding arguments.
 */
public interface RpcInvocation extends Message {

    /**
     * Returns the method's name.
     *
     * @return Method name
     * @throws IOException if the rpc invocation message is a remote message and could not be
     *     deserialized
     * @throws ClassNotFoundException if the rpc invocation message is a remote message and contains
     *     serialized classes which cannot be found on the receiving side
     */
    String getMethodName() throws IOException, ClassNotFoundException;

    /**
     * Returns the method's parameter types.
     *
     * @return Method's parameter types
     * @throws IOException if the rpc invocation message is a remote message and could not be
     *     deserialized
     * @throws ClassNotFoundException if the rpc invocation message is a remote message and contains
     *     serialized classes which cannot be found on the receiving side
     */
    Class<?>[] getParameterTypes() throws IOException, ClassNotFoundException;

    /**
     * Returns the arguments of the remote procedure call.
     *
     * @return Arguments of the remote procedure call
     * @throws IOException if the rpc invocation message is a remote message and could not be
     *     deserialized
     * @throws ClassNotFoundException if the rpc invocation message is a remote message and contains
     *     serialized classes which cannot be found on the receiving side
     */
    Object[] getArgs() throws IOException, ClassNotFoundException;

    /**
     * Converts a rpc call into its string representation.
     *
     * @param declaringClassName declaringClassName declares the specified rpc
     * @param methodName methodName of the rpc
     * @param parameterTypes parameterTypes of the rpc
     * @return string representation of the rpc
     */
    static String convertRpcToString(
            String declaringClassName, String methodName, Class<?>[] parameterTypes) {
        final StringBuilder paramTypeStringBuilder = new StringBuilder(parameterTypes.length * 5);

        if (parameterTypes.length > 0) {
            paramTypeStringBuilder.append(parameterTypes[0].getSimpleName());

            for (int i = 1; i < parameterTypes.length; i++) {
                paramTypeStringBuilder.append(", ").append(parameterTypes[i].getSimpleName());
            }
        }

        return declaringClassName + '.' + methodName + '(' + paramTypeStringBuilder + ')';
    }
}
