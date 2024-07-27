/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.client.program.artifact;

import org.apache.flink.client.cli.ArtifactFetchOptions;
import org.apache.flink.configuration.Configuration;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;

/** Downloads artifact from an HTTP resource. */
class HttpArtifactFetcher extends ArtifactFetcher {

    public static final Logger LOG = LoggerFactory.getLogger(HttpArtifactFetcher.class);

    @Override
    File fetch(String uri, Configuration flinkConf, File targetDir) throws IOException {
        long start = System.currentTimeMillis();
        URL url = new URL(uri);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        Map<String, String> headers = flinkConf.get(ArtifactFetchOptions.HTTP_HEADERS);

        if (headers != null) {
            headers.forEach(conn::setRequestProperty);
        }

        conn.setRequestMethod("GET");

        String fileName = FilenameUtils.getName(url.getPath());
        File targetFile = new File(targetDir, fileName);
        try (InputStream inputStream = conn.getInputStream()) {
            FileUtils.copyToFile(inputStream, targetFile);
        }
        LOG.debug(
                "Copied file from {} to {}, cost {} ms",
                uri,
                targetFile,
                System.currentTimeMillis() - start);
        return targetFile;
    }
}
