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

package org.apache.flink.runtime.rest.messages.cluster;

import org.apache.flink.runtime.rest.messages.MessageParameters;
import org.apache.flink.runtime.rest.messages.MessagePathParameter;

import java.util.Arrays;
import java.util.Collection;

/** Message parameters for removing a node from the blocklist. */
public class BlocklistRemoveMessageParameters extends MessageParameters {

    public final NodeIdPathParameter nodeIdPathParameter = new NodeIdPathParameter();

    @Override
    public Collection<MessagePathParameter<?>> getPathParameters() {
        return Arrays.asList(nodeIdPathParameter);
    }

    /** Path parameter for the node ID. */
    public static class NodeIdPathParameter extends MessagePathParameter<String> {

        public static final String KEY = "nodeId";

        public NodeIdPathParameter() {
            super(KEY);
        }

        @Override
        protected String convertFromString(String value) {
            return value;
        }

        @Override
        protected String convertToString(String value) {
            return value;
        }

        @Override
        public String getDescription() {
            return "The ID of the node to remove from the blocklist.";
        }
    }
}
