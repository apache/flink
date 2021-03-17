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

package org.apache.flink.kubernetes.kubeclient.resources;

import org.apache.flink.kubernetes.kubeclient.FlinkKubeClient;
import org.apache.flink.kubernetes.utils.KubernetesUtils;

import io.fabric8.kubernetes.api.model.ConfigMap;

import java.util.Collections;
import java.util.List;

/** Watcher for {@link ConfigMap ConfigMaps} in Kubernetes. */
public class KubernetesConfigMapWatcher
        extends AbstractKubernetesWatcher<ConfigMap, KubernetesConfigMap> {

    public KubernetesConfigMapWatcher(
            FlinkKubeClient.WatchCallbackHandler<KubernetesConfigMap> callbackHandler) {
        super(callbackHandler);
    }

    @Override
    public void eventReceived(Action action, ConfigMap configMap) {
        logger.debug(
                "Received {} event for configMap {}, details: {}{}",
                action,
                configMap.getMetadata().getName(),
                System.lineSeparator(),
                KubernetesUtils.tryToGetPrettyPrintYaml(configMap));
        final List<KubernetesConfigMap> configMaps =
                Collections.singletonList(new KubernetesConfigMap(configMap));
        switch (action) {
            case ADDED:
                callbackHandler.onAdded(configMaps);
                break;
            case MODIFIED:
                callbackHandler.onModified(configMaps);
                break;
            case ERROR:
                callbackHandler.onError(configMaps);
                break;
            case DELETED:
                callbackHandler.onDeleted(configMaps);
                break;
            default:
                logger.debug(
                        "Ignore handling {} event for configMap {}",
                        action,
                        configMap.getMetadata().getName());
                break;
        }
    }
}
