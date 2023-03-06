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

package org.apache.flink.kubernetes.kubeclient.decorators;

import org.apache.flink.core.plugin.PluginManager;
import org.apache.flink.core.plugin.PluginUtils;
import org.apache.flink.kubernetes.kubeclient.decorators.extended.ExtPluginDecorator;
import org.apache.flink.kubernetes.kubeclient.parameters.AbstractKubernetesParameters;

import org.apache.flink.shaded.guava30.com.google.common.collect.Iterators;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;

/** The flink-kubernetes decorator plugins loading class. */
public class ExtStepDecoratorUtils {
    private static final Logger LOG = LoggerFactory.getLogger(ExtStepDecoratorUtils.class);

    public static List<ExtPluginDecorator> loadExtStepDecorators(
            AbstractKubernetesParameters kubernetesComponentConf) {
        List<ExtPluginDecorator> extendedStepDecorators = new ArrayList<>();
        Map<String, ExtPluginDecorator> decoratorFactories = new HashMap<>();
        PluginManager pluginManager =
                PluginUtils.createPluginManagerFromRootFolder(
                        kubernetesComponentConf.getFlinkConfiguration());

        // get all factories
        Iterator<ExtPluginDecorator> factoryIteratorSPI =
                ServiceLoader.load(ExtPluginDecorator.class).iterator();
        Iterator<ExtPluginDecorator> factoryIteratorPlugins =
                pluginManager != null
                        ? pluginManager.load(ExtPluginDecorator.class)
                        : Collections.emptyIterator();
        Iterator<ExtPluginDecorator> factoryIterator =
                Iterators.concat(factoryIteratorPlugins, factoryIteratorSPI);
        while (factoryIterator.hasNext()) {
            try {
                ExtPluginDecorator factory = factoryIterator.next();
                factory.configure(kubernetesComponentConf);
                String factoryClassName = factory.getClass().getName();
                ExtPluginDecorator existingFactory = decoratorFactories.get(factoryClassName);
                if (existingFactory == null) {
                    decoratorFactories.put(factoryClassName, factory);
                    Collections.addAll(extendedStepDecorators, factory);
                    LOG.info("Load ext plugin step decorator: {}.", factoryClassName);
                }
            } catch (Exception e) {
                LOG.error("Loading ext plugin step decorator Failed: {}", e.getStackTrace());
                return Collections.emptyList();
            }
        }
        return extendedStepDecorators;
    }
}
