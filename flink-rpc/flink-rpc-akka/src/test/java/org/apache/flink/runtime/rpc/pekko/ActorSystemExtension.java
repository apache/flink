/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.rpc.pekko;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Preconditions;

import org.apache.pekko.actor.ActorSystem;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

/** External resource which starts an {@link ActorSystem}. */
public class ActorSystemExtension implements BeforeEachCallback, AfterEachCallback {

    private final Configuration configuration;

    private ActorSystem actorSystem;

    private ActorSystemExtension(Configuration configuration) {
        this.configuration = configuration;
    }

    @Override
    public void beforeEach(ExtensionContext context) throws Exception {
        Preconditions.checkState(
                actorSystem == null, "ActorSystem must not be initialized when calling before.");
        actorSystem = PekkoUtils.createLocalActorSystem(configuration);
    }

    @Override
    public void afterEach(ExtensionContext context) throws Exception {
        Preconditions.checkState(
                actorSystem != null, "ActorSystem must be initialized when calling after.");
        PekkoUtils.terminateActorSystem(actorSystem).join();
    }

    public ActorSystem getActorSystem() {
        return actorSystem;
    }

    public static ActorSystemExtension defaultConfiguration() {
        return new ActorSystemExtension(new Configuration());
    }

    public static ActorSystemExtension withConfiguration(Configuration configuration) {
        return new ActorSystemExtension(configuration);
    }
}
