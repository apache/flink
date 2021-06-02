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

package org.apache.flink.runtime.io.network.netty;

import org.apache.flink.runtime.rest.handler.ServerBasicHttpAuthenticator;
import org.apache.flink.util.ConfigurationException;

import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableMap;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.stream.Stream;

public class ServerBasicAuthHandlerFactory {

    private final ImmutableMap<String, String> credentials;

    public ServerBasicAuthHandlerFactory(String pwdFile) throws ConfigurationException {
        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        try (Stream<String> stream = Files.lines(Paths.get(pwdFile))) {
            stream.forEach(
                    line -> {
                        if (!line.isEmpty()) {
                            String[] split = line.split(":", 2);
                            builder.put(split[0], split[1]);
                        }
                    });
        } catch (IOException ioe) {
            throw new ConfigurationException(ioe);
        }
        this.credentials = builder.build();
    }

    public ServerBasicHttpAuthenticator getAuthHandler(Map<String, String> responseHeaders) {
        return new ServerBasicHttpAuthenticator(credentials, responseHeaders);
    }
}
