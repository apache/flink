/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.cassandra;

import org.apache.flink.annotation.Internal;

import java.io.IOException;

/** A {@link CassandraFailureHandler} that simply fails the sink on any failures. */
@Internal
public class NoOpCassandraFailureHandler implements CassandraFailureHandler {

    private static final long serialVersionUID = 737941343410827885L;

    @Override
    public void onFailure(Throwable failure) throws IOException {
        // simply fail the sink
        throw new IOException("Error while sending value.", failure);
    }
}
