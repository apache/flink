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

package org.apache.flink.streaming.kinesis.test.containers;

import org.testcontainers.containers.GenericContainer;

/** A test Kinesis Data Streams container using Kinesalite. */
public class KinesaliteContainer extends GenericContainer<KinesaliteContainer> {
    public static final String INTER_CONTAINER_KINESALITE_ALIAS = "kinesalite";

    private static final String IMAGE = "instructure/kinesalite:latest";
    private static final int PORT = 4567;

    public KinesaliteContainer() {
        super(IMAGE);
    }

    @Override
    protected void configure() {
        withExposedPorts(PORT);
        withNetworkAliases(INTER_CONTAINER_KINESALITE_ALIAS);
        withCreateContainerCmdModifier(
                cmd ->
                        cmd.withEntrypoint(
                                "/tini",
                                "--",
                                "/usr/src/app/node_modules/kinesalite/cli.js",
                                "--path",
                                "/var/lib/kinesalite",
                                "--ssl"));
    }

    public String getEndpointUrl() {
        return "https://" + getHost() + ":" + getMappedPort(PORT);
    }
}
