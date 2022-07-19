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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.util.FlinkUserCodeClassLoaders.SafetyNetWrapperClassLoader;

/** Utilities for {@link SafetyNetWrapperClassLoader}. */
public class ClassloaderUtil {

    private static final Logger LOG = LoggerFactory.getLogger(ClassloaderUtil.class);

    public static SafetyNetWrapperClassLoader buildClassLoader(
            List<URL> jarUrls, ClassLoader parentClassLoader, Configuration conf) {
        // override to use SafetyNetWrapperClassLoader
        LOG.info(
                "Set option 'classloader.check-leaked-classloader' value to 'true' to use SafetyNetWrapperClassLoader.");
        conf.set(CoreOptions.CHECK_LEAKED_CLASSLOADER, true);
        return (SafetyNetWrapperClassLoader)
                ClientUtils.buildUserCodeClassLoader(
                        jarUrls, Collections.emptyList(), parentClassLoader, conf);
    }

    /** Private constructor to prevent instantiation. */
    private ClassloaderUtil() {}
}
