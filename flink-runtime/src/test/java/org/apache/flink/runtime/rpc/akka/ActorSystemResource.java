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

package org.apache.flink.runtime.rpc.akka;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.util.Preconditions;

import akka.actor.ActorSystem;
import org.junit.rules.ExternalResource;

/** External resource which starts an {@link akka.actor.ActorSystem}. */
public class ActorSystemResource extends ExternalResource {

    private final Configuration configuration;

    private ActorSystem actorSystem;

    private ActorSystemResource(Configuration configuration) {
        this.configuration = configuration;
    }

    @Override
    protected void before() throws Throwable {
        Preconditions.checkState(
                actorSystem == null, "ActorSystem must not be initialized when calling before.");
        actorSystem = AkkaUtils.createLocalActorSystem(configuration);
    }

    @Override
    protected void after() {
        Preconditions.checkState(
                actorSystem != null, "ActorSystem must be initialized when calling after.");
        AkkaUtils.terminateActorSystem(actorSystem).join();
    }

    public ActorSystem getActorSystem() {
        return actorSystem;
    }

    public static ActorSystemResource defaultConfiguration() {
        return new ActorSystemResource(new Configuration());
    }

    public static ActorSystemResource withConfiguration(Configuration configuration) {
        return new ActorSystemResource(configuration);
    }
}
