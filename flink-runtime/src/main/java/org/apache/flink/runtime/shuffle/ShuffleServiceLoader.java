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

package org.apache.flink.runtime.shuffle;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ShuffleServiceOptions;
import org.apache.flink.core.plugin.PluginManager;
import org.apache.flink.core.plugin.PluginUtils;
import org.apache.flink.runtime.io.network.NettyShuffleServiceFactory;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.InstantiationUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/** Utility to load the pluggable {@link ShuffleServiceFactory} implementations. */
public enum ShuffleServiceLoader {
    ;

    private static final Logger LOG = LoggerFactory.getLogger(ShuffleServiceLoader.class);

    public static Map<String, ShuffleServiceFactory<?, ?, ?>> loadShuffleServiceFactories(
            Configuration configuration) throws FlinkException {
        Map<String, ShuffleServiceFactory<?, ?, ?>> factories = new HashMap<>();

        // this should never throw any exception
        ShuffleServiceFactory<?, ?, ?> nettyFactory = loadNettyShuffleServiceFactory();
        factories.put(NettyShuffleServiceFactory.class.getName(), nettyFactory);

        String defaultFactoryName =
                configuration.getString(ShuffleServiceOptions.SHUFFLE_SERVICE_FACTORY_CLASS);
        try {
            // this keeps compatibility with the previous behavior: users can put the shuffle plugin
            // in the lib folder instead of the plugins folder
            ShuffleServiceFactory<?, ?, ?> defaultFactory =
                    loadShuffleServiceFactory(defaultFactoryName);
            factories.putIfAbsent(defaultFactoryName, defaultFactory);
        } catch (Exception e) {
            LOG.debug(
                    String.format(
                            "Failed to load the default shuffle service factory %s configured by %s"
                                    + " from Flink parent classloader.",
                            defaultFactoryName,
                            ShuffleServiceOptions.SHUFFLE_SERVICE_FACTORY_CLASS.key()));
        }

        PluginManager pluginManager = PluginUtils.createPluginManagerFromRootFolder(configuration);
        Iterator<ShuffleServiceFactory> it = pluginManager.load(ShuffleServiceFactory.class);
        while (it.hasNext()) {
            ShuffleServiceFactory<?, ?, ?> factory = it.next();
            factories.putIfAbsent(factory.getClass().getName(), factory);
        }

        if (!factories.containsKey(defaultFactoryName)) {
            throw new FlinkException(
                    String.format(
                            "Failed to load the default shuffle service factory %s configured by: "
                                    + "%s, please make sure to put the shuffle service plugin jar "
                                    + "to either the lib folder or the plugins folder.",
                            defaultFactoryName,
                            ShuffleServiceOptions.SHUFFLE_SERVICE_FACTORY_CLASS.key()));
        }
        return factories;
    }

    public static ShuffleServiceFactory<?, ?, ?> loadNettyShuffleServiceFactory()
            throws FlinkException {
        return loadShuffleServiceFactory(NettyShuffleServiceFactory.class.getName());
    }

    public static ShuffleServiceFactory<?, ?, ?> loadShuffleServiceFactory(String factoryName)
            throws FlinkException {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        return InstantiationUtil.instantiate(factoryName, ShuffleServiceFactory.class, classLoader);
    }
}
