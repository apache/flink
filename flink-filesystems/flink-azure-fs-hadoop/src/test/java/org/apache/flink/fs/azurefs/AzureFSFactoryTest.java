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
import org.apache.flink.util.TestLogger;

import org.apache.hadoop.fs.azure.AzureException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.net.URI;
import java.util.Arrays;
import java.util.List;

/** Tests for the AzureFSFactory. */
@RunWith(Parameterized.class)
public class AzureFSFactoryTest extends TestLogger {

    @Parameterized.Parameter public String scheme;

    @Parameterized.Parameters(name = "Scheme = {0}")
    public static List<String> parameters() {
        return Arrays.asList("wasb", "wasbs");
    }

    @Rule public final ExpectedException exception = ExpectedException.none();

    private AbstractAzureFSFactory getFactory(String scheme) {
        return scheme.equals("wasb") ? new AzureFSFactory() : new SecureAzureFSFactory();
    }

    @Test
    public void testNullFsURI() throws Exception {
        URI uri = null;
        AbstractAzureFSFactory factory = getFactory(scheme);

        exception.expect(NullPointerException.class);
        exception.expectMessage("passed file system URI object should not be null");

        factory.create(uri);
    }

    // missing credentials
    @Test
    public void testCreateFsWithAuthorityMissingCreds() throws Exception {
        String uriString =
                String.format(
                        "%s://yourcontainer@youraccount.blob.core.windows.net/testDir", scheme);
        final URI uri = URI.create(uriString);

        exception.expect(AzureException.class);

        AbstractAzureFSFactory factory = getFactory(scheme);
        Configuration config = new Configuration();
        config.setInteger("fs.azure.io.retry.max.retries", 0);
        factory.configure(config);
        factory.create(uri);
    }

    @Test
    public void testCreateFsWithMissingAuthority() throws Exception {
        String uriString = String.format("%s:///my/path", scheme);
        final URI uri = URI.create(uriString);

        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(
                "Cannot initialize WASB file system, URI authority not recognized.");

        AbstractAzureFSFactory factory = getFactory(scheme);
        factory.configure(new Configuration());
        factory.create(uri);
    }
}
