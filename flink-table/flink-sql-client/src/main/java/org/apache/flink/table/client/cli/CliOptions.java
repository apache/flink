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

package org.apache.flink.table.client.cli;

import org.apache.flink.configuration.Configuration;

import javax.annotation.Nullable;

import java.net.URL;
import java.util.List;

/**
 * Command line options to configure the SQL client. Arguments that have not been specified by the
 * user are null.
 */
public class CliOptions {

    private final boolean isPrintHelp;
    private final String sessionId;
    private final URL initFile;
    private final URL sqlFile;
    private final List<URL> jars;
    private final List<URL> libraryDirs;
    private final String updateStatement;
    private final String historyFilePath;
    private final Configuration pythonConfiguration;

    public CliOptions(
            boolean isPrintHelp,
            String sessionId,
            URL initFile,
            URL sqlFile,
            List<URL> jars,
            List<URL> libraryDirs,
            String updateStatement,
            String historyFilePath,
            Configuration pythonConfiguration) {
        this.isPrintHelp = isPrintHelp;
        this.sessionId = sessionId;
        this.initFile = initFile;
        this.sqlFile = sqlFile;
        this.jars = jars;
        this.libraryDirs = libraryDirs;
        this.updateStatement = updateStatement;
        this.historyFilePath = historyFilePath;
        this.pythonConfiguration = pythonConfiguration;
    }

    public boolean isPrintHelp() {
        return isPrintHelp;
    }

    public String getSessionId() {
        return sessionId;
    }

    public @Nullable URL getInitFile() {
        return initFile;
    }

    public @Nullable URL getSqlFile() {
        return sqlFile;
    }

    public List<URL> getJars() {
        return jars;
    }

    public List<URL> getLibraryDirs() {
        return libraryDirs;
    }

    public String getUpdateStatement() {
        return updateStatement;
    }

    public String getHistoryFilePath() {
        return historyFilePath;
    }

    public Configuration getPythonConfiguration() {
        return pythonConfiguration;
    }
}
