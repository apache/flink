/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.core.plugin;

import java.net.URL;
import java.util.Arrays;

/** Descriptive meta information for a plugin. */
public class PluginDescriptor {

    /** Unique identifier of the plugin. */
    private final String pluginId;

    /**
     * URLs to the plugin resources code. Usually this contains URLs of the jars that will be loaded
     * for the plugin.
     */
    private final URL[] pluginResourceURLs;

    /**
     * String patterns of classes that should be excluded from loading out of the plugin resources.
     * See {@link org.apache.flink.util.ChildFirstClassLoader}'s field alwaysParentFirstPatterns.
     */
    private final String[] loaderExcludePatterns;

    public PluginDescriptor(
            String pluginId, URL[] pluginResourceURLs, String[] loaderExcludePatterns) {
        this.pluginId = pluginId;
        this.pluginResourceURLs = pluginResourceURLs;
        this.loaderExcludePatterns = loaderExcludePatterns;
    }

    public String getPluginId() {
        return pluginId;
    }

    public URL[] getPluginResourceURLs() {
        return pluginResourceURLs;
    }

    public String[] getLoaderExcludePatterns() {
        return loaderExcludePatterns;
    }

    @Override
    public String toString() {
        return "PluginDescriptor{"
                + "pluginId='"
                + pluginId
                + '\''
                + ", pluginResourceURLs="
                + Arrays.toString(pluginResourceURLs)
                + ", loaderExcludePatterns="
                + Arrays.toString(loaderExcludePatterns)
                + '}';
    }
}
