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

package org.apache.flink.tests.util.cache;

import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.io.IOException;
import java.nio.file.Path;

/** JUnit 5 extension that wraps a {@link DownloadCache} and manages its lifecycle. */
public class DownloadCacheExtension implements BeforeEachCallback, AfterEachCallback {

    private final DownloadCache delegate;

    public DownloadCacheExtension() {
        this(DownloadCache.get());
    }

    DownloadCacheExtension(DownloadCache delegate) {
        this.delegate = delegate;
    }

    public Path getOrDownload(String url, Path targetDir) throws IOException {
        return delegate.getOrDownload(url, targetDir);
    }

    @Override
    public void beforeEach(ExtensionContext context) throws Exception {
        delegate.before();
    }

    @Override
    public void afterEach(ExtensionContext context) {
        if (context.getExecutionException().isPresent()) {
            delegate.afterTestFailure();
        } else {
            delegate.afterTestSuccess();
        }
    }
}
