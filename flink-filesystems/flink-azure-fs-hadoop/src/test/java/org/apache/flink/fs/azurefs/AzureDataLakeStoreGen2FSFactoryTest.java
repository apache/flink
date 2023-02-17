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

package org.apache.flink.fs.azurefs;

import org.apache.flink.configuration.Configuration;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.URI;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the ABFSAzureFSFactory. */
class AzureDataLakeStoreGen2FSFactoryTest {

    private AbstractAzureFSFactory getFactory(String scheme) {
        return scheme.equals("abfs")
                ? new AzureDataLakeStoreGen2FSFactory()
                : new SecureAzureDataLakeStoreGen2FSFactory();
    }

    @ParameterizedTest
    @ValueSource(strings = {"abfs", "abfss"})
    void testNullFsURI(String scheme) throws Exception {
        URI uri = null;
        AbstractAzureFSFactory factory = getFactory(scheme);

        assertThatThrownBy(() -> factory.create(uri))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("passed file system URI object should not be null");
    }

    @ParameterizedTest
    @ValueSource(strings = {"abfs", "abfss"})
    void testCreateFsWithMissingAuthority(String scheme) throws Exception {
        String uriString = String.format("%s:///my/path", scheme);
        final URI uri = URI.create(uriString);

        AbstractAzureFSFactory factory = getFactory(scheme);
        factory.configure(new Configuration());

        assertThatThrownBy(() -> factory.create(uri))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(String.format("%s has invalid authority.", uriString));
    }
}
