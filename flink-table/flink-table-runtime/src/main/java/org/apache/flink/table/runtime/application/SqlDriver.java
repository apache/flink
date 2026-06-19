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

package org.apache.flink.table.runtime.application;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.MutableURLClassLoader;
import org.apache.flink.util.Preconditions;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;

/** Driver to load the {@code ScriptRunner} and execute the script. */
public class SqlDriver {

    private static final Logger LOG = LoggerFactory.getLogger(SqlDriver.class);

    public static final Option OPTION_SQL_FILE =
            Option.builder()
                    .longOpt("scriptUri")
                    .numberOfArgs(1)
                    .desc("SQL script file URI. It supports to fetch files from the DFS or HTTP.")
                    .build();

    public static final Option OPTION_SQL_SCRIPT =
            Option.builder().longOpt("script").numberOfArgs(1).desc("Script content.").build();

    private static final String RUNNER_CLASS_NAME =
            "org.apache.flink.table.gateway.service.application.ScriptRunner";
    private static boolean testMode = false;
    private static OutputStream testOutputStream;

    public static void main(String[] args) throws Exception {
        String sql = parseOptions(args);
        if (testMode) {
            getClassLoader()
                    .loadClass(RUNNER_CLASS_NAME)
                    .getMethod("run", String.class, OutputStream.class)
                    .invoke(null, sql, Preconditions.checkNotNull(testOutputStream));
        } else {
            getClassLoader()
                    .loadClass(RUNNER_CLASS_NAME)
                    .getMethod("run", String.class)
                    .invoke(null, sql);
        }
    }

    public static void enableTestMode(OutputStream outputStream) {
        testOutputStream = outputStream;
        testMode = true;
    }

    public static void disableTestMode() {
        testOutputStream = null;
        SqlDriver.testMode = false;
    }

    private static ClassLoader getClassLoader() throws Exception {
        MutableURLClassLoader sqlGatewayClassloader =
                (MutableURLClassLoader) Thread.currentThread().getContextClassLoader();
        try {
            sqlGatewayClassloader.loadClass(RUNNER_CLASS_NAME);
            LOG.info("Load {} from the classpath.", RUNNER_CLASS_NAME);
        } catch (ClassNotFoundException e) {
            LOG.info("{} is not in the classpath. Finding...", RUNNER_CLASS_NAME);
            sqlGatewayClassloader.addURL(findExecutor().toUri().toURL());
        }
        return sqlGatewayClassloader;
    }

    private static Path findExecutor() {
        String flinkOptPath = System.getenv(ConfigConstants.ENV_FLINK_OPT_DIR);
        final List<Path> sqlJarPaths = new ArrayList<>();
        try {
            Files.walkFileTree(
                    FileSystems.getDefault().getPath(flinkOptPath),
                    new SimpleFileVisitor<>() {
                        @Override
                        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
                                throws IOException {
                            FileVisitResult result = super.visitFile(file, attrs);
                            if (file.getFileName().toString().startsWith("flink-sql-gateway")) {
                                sqlJarPaths.add(file);
                            }
                            return result;
                        }
                    });
        } catch (IOException e) {
            throw new RuntimeException(
                    "Exception encountered during finding the flink-sql-gateway jar. This should not happen.",
                    e);
        }

        if (sqlJarPaths.size() != 1) {
            throw new RuntimeException("Found " + sqlJarPaths.size() + " flink-sql-gateway jar.");
        }

        return sqlJarPaths.get(0);
    }

    static Options getSqlDriverOptions() {
        Options options = new Options();
        options.addOption(OPTION_SQL_FILE);
        options.addOption(OPTION_SQL_SCRIPT);
        return options;
    }

    // --------------------------------------------------------------------------------------------
    //  Line Parsing
    // --------------------------------------------------------------------------------------------

    public static String parseOptions(String[] args) {
        try {
            DefaultParser parser = new DefaultParser();
            CommandLine line = parser.parse(getSqlDriverOptions(), args, true);
            String content = getContent(line.getOptionValue(OPTION_SQL_FILE.getLongOpt()));
            if (content == null) {
                return Preconditions.checkNotNull(
                        line.getOptionValue(OPTION_SQL_SCRIPT.getLongOpt()),
                        "Please use \"--script\" or \"--scriptUri\" to specify script either.");
            } else {
                Preconditions.checkArgument(
                        line.getOptionValue(OPTION_SQL_SCRIPT.getLongOpt()) == null,
                        "Don't set \"--script\" or \"--scriptUri\" together.");
                return content;
            }
        } catch (ParseException | URISyntaxException e) {
            throw new IllegalArgumentException("Failed to parse args. It should never happens.", e);
        } catch (IOException e) {
            throw new IllegalArgumentException("Can not read files to execute.", e);
        }
    }

    private static @Nullable String getContent(@Nullable String filePath)
            throws IOException, URISyntaxException {
        if (filePath == null) {
            return null;
        }

        URI resolvedUri = resolveURI(filePath);
        if (resolvedUri.getScheme().equals("http") || resolvedUri.getScheme().equals("https")) {
            return readFromHttp(resolvedUri);
        } else {
            return readFileUtf8(resolvedUri);
        }
    }

    private static URI resolveURI(String path) throws URISyntaxException {
        final URI uri = new URI(path);
        if (uri.getScheme() != null) {
            return uri;
        }
        return new File(path).getAbsoluteFile().toURI();
    }

    private static String readFromHttp(URI uri) throws IOException {
        URL url = uri.toURL();
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();

        conn.setRequestMethod("GET");

        try (InputStream inputStream = conn.getInputStream();
                ByteArrayOutputStream targetFile = new ByteArrayOutputStream()) {
            IOUtils.copy(inputStream, targetFile);
            return targetFile.toString(StandardCharsets.UTF_8);
        }
    }

    private static String readFileUtf8(URI uri) throws IOException {
        org.apache.flink.core.fs.Path path = new org.apache.flink.core.fs.Path(uri);
        FileSystem fs = path.getFileSystem();
        try (FSDataInputStream inputStream = fs.open(path)) {
            return new String(
                    FileUtils.read(inputStream, (int) fs.getFileStatus(path).getLen()),
                    StandardCharsets.UTF_8);
        }
    }
}
