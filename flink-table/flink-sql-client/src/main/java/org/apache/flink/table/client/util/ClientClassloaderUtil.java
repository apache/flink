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

package org.apache.flink.table.client.util;

import org.apache.flink.client.ClientUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.util.FlinkUserCodeClassLoader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.util.Collections;
import java.util.List;

/** Utilities for {@link ClientWrapperClassLoader}. */
public class ClientClassloaderUtil {

    private static final Logger LOG = LoggerFactory.getLogger(ClientClassloaderUtil.class);

    public static FlinkUserCodeClassLoader buildUserClassLoader(
            List<URL> jarUrls, ClassLoader parentClassLoader, Configuration conf) {
        LOG.debug(
                String.format(
                        "Set option '%s' to 'false' to use %s.",
                        CoreOptions.CHECK_LEAKED_CLASSLOADER.key(),
                        ClientWrapperClassLoader.class.getSimpleName()));
        conf.set(CoreOptions.CHECK_LEAKED_CLASSLOADER, false);
        return (FlinkUserCodeClassLoader)
                ClientUtils.buildUserCodeClassLoader(
                        jarUrls, Collections.emptyList(), parentClassLoader, conf);
    }

    private ClientClassloaderUtil() {}
}
