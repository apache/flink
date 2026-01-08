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

package org.apache.flink.table.secret;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.factories.Factory;
import org.apache.flink.table.secret.exceptions.SecretException;

import java.util.Map;

/** Factory for creating SecretStore instances. */
@PublicEvolving
public interface SecretStoreFactory extends Factory {

    /** Creates a SecretStore instance. */
    SecretStore createSecretStore();

    /** Initialize secret store. */
    void open(Context context) throws SecretException;

    /** Close secret store. */
    void close() throws CatalogException;

    interface Context {
        /**
         * Returns the options with which the secret store is created.
         *
         * <p>An implementation should perform validation of these options.
         */
        Map<String, String> getOptions();

        /** Gives read-only access to the configuration of the current session. */
        ReadableConfig getConfiguration();

        /**
         * Returns the class loader of the current session.
         *
         * <p>The class loader is in particular useful for discovering further (nested) factories.
         */
        ClassLoader getClassLoader();
    }
}
