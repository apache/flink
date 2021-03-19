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

package org.apache.flink.table.client.cli;

import org.junit.rules.ExternalResource;

/**
 * Enables {@link org.apache.flink.table.client.SqlClient} to create a default terminal using {@link
 * System#in} and {@link System#out} as the input and output stream. This can allows tests to easily
 * mock input stream of the SqlClient by hijacking the standard stream.
 */
public class TerminalStreamsResource extends ExternalResource {

    public static final TerminalStreamsResource INSTANCE = new TerminalStreamsResource();

    private TerminalStreamsResource() {
        // singleton
    }

    @Override
    protected void before() throws Throwable {
        CliClient.useSystemInOutStream = true;
    }

    @Override
    protected void after() {
        CliClient.useSystemInOutStream = false;
    }
}
