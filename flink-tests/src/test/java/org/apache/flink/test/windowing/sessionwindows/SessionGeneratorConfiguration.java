/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.test.windowing.sessionwindows;

import org.apache.flink.util.Preconditions;

/**
 * Configuration for a session event generator, consisting of the generator configuration and the
 * session configuration.
 *
 * @param <K> type of session key
 * @param <E> type of session event
 */
public final class SessionGeneratorConfiguration<K, E> {

    private final SessionConfiguration<K, E> sessionConfiguration;
    private final GeneratorConfiguration generatorConfiguration;

    public SessionGeneratorConfiguration(
            SessionConfiguration<K, E> sessionConfiguration,
            GeneratorConfiguration generatorConfiguration) {

        Preconditions.checkNotNull(sessionConfiguration);
        Preconditions.checkNotNull(generatorConfiguration);

        this.sessionConfiguration = sessionConfiguration;
        this.generatorConfiguration = generatorConfiguration;
    }

    public SessionConfiguration<K, E> getSessionConfiguration() {
        return sessionConfiguration;
    }

    public GeneratorConfiguration getGeneratorConfiguration() {
        return generatorConfiguration;
    }

    @Override
    public String toString() {
        return "SessionGeneratorConfiguration{"
                + "sessionConfiguration="
                + sessionConfiguration
                + ", generatorConfiguration="
                + generatorConfiguration
                + '}';
    }
}
