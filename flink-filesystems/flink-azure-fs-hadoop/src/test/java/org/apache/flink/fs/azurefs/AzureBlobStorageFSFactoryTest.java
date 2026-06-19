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

import org.apache.hadoop.fs.azure.AzureException;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.net.URI;
import java.util.stream.Stream;

import static org.apache.flink.configuration.ConfigurationUtils.getIntConfigOption;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the AzureFSFactory. */
class AzureBlobStorageFSFactoryTest {

    @ParameterizedTest(name = "Factory = {0}")
    @MethodSource("getFactories")
    @Retention(value = RetentionPolicy.RUNTIME)
    private @interface TestAllFsImpl {}

    @SuppressWarnings("unused")
    private static Stream<AbstractAzureFSFactory> getFactories() {
        return Stream.of(new AzureBlobStorageFSFactory(), new SecureAzureBlobStorageFSFactory());
    }

    @TestAllFsImpl
    void testNullFsURI(AbstractAzureFSFactory factory) throws Exception {
        URI uri = null;

        assertThatThrownBy(() -> factory.create(uri))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("passed file system URI object should not be null");
    }

    // missing credentials
    @TestAllFsImpl
    void testCreateFsWithAuthorityMissingCreds(AbstractAzureFSFactory factory) throws Exception {
        String uriString =
                String.format(
                        "%s://yourcontainer@youraccount.blob.core.windows.net/testDir",
                        factory.getScheme());
        final URI uri = URI.create(uriString);

        Configuration config = new Configuration();
        config.set(getIntConfigOption("fs.azure.io.retry.max.retries"), 0);
        factory.configure(config);

        assertThatThrownBy(() -> factory.create(uri)).isInstanceOf(AzureException.class);
    }

    @TestAllFsImpl
    void testCreateFsWithMissingAuthority(AbstractAzureFSFactory factory) throws Exception {
        String uriString = String.format("%s:///my/path", factory.getScheme());
        final URI uri = URI.create(uriString);

        factory.configure(new Configuration());

        assertThatThrownBy(() -> factory.create(uri))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Cannot initialize WASB file system, URI authority not recognized.");
    }
}
