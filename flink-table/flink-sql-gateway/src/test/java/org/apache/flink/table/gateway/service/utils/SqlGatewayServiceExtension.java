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

package org.apache.flink.table.gateway.service.utils;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.table.gateway.api.SqlGatewayService;
import org.apache.flink.table.gateway.service.SqlGatewayServiceImpl;
import org.apache.flink.table.gateway.service.context.DefaultContext;
import org.apache.flink.table.gateway.service.session.SessionManager;

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.apache.flink.configuration.ConfigConstants.ENV_FLINK_CONF_DIR;

/** A simple {@link Extension} to be used by tests that require a {@link SqlGatewayService}. */
public class SqlGatewayServiceExtension implements BeforeAllCallback, AfterAllCallback {

    private SqlGatewayService service;
    private SessionManager sessionManager;
    private TemporaryFolder temporaryFolder;
    private final Supplier<Configuration> configSupplier;
    private final Function<DefaultContext, SessionManager> sessionManagerCreator;

    public SqlGatewayServiceExtension(Supplier<Configuration> configSupplier) {
        this(configSupplier, SessionManager::create);
    }

    public SqlGatewayServiceExtension(
            Supplier<Configuration> configSupplier,
            Function<DefaultContext, SessionManager> sessionManagerCreator) {
        this.configSupplier = configSupplier;
        this.sessionManagerCreator = sessionManagerCreator;
    }

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        final Map<String, String> originalEnv = System.getenv();
        try {
            // prepare conf dir
            temporaryFolder = new TemporaryFolder();
            temporaryFolder.create();
            File confFolder = temporaryFolder.newFolder("conf");
            File confYaml = new File(confFolder, "flink-conf.yaml");
            if (!confYaml.createNewFile()) {
                throw new IOException("Can't create testing flink-conf.yaml file.");
            }

            FileUtils.write(
                    confYaml,
                    getFlinkConfContent(configSupplier.get().toMap()),
                    StandardCharsets.UTF_8);

            // adjust the test environment for the purposes of this test
            Map<String, String> map = new HashMap<>(System.getenv());
            map.put(ENV_FLINK_CONF_DIR, confFolder.getAbsolutePath());
            CommonTestUtils.setEnv(map);

            sessionManager =
                    sessionManagerCreator.apply(
                            DefaultContext.load(
                                    new Configuration(), Collections.emptyList(), true, false));
        } finally {
            CommonTestUtils.setEnv(originalEnv);
        }

        service = new SqlGatewayServiceImpl(sessionManager);
        sessionManager.start();
    }

    @Override
    public void afterAll(ExtensionContext context) throws Exception {
        if (sessionManager != null) {
            sessionManager.stop();
        }
        temporaryFolder.delete();
    }

    public SqlGatewayService getService() {
        return service;
    }

    public SessionManager getSessionManager() {
        return sessionManager;
    }

    private String getFlinkConfContent(Map<String, String> flinkConf) {
        StringBuilder sb = new StringBuilder();
        flinkConf.forEach((k, v) -> sb.append(k).append(": ").append(v).append("\n"));
        return sb.toString();
    }
}
