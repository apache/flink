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

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Watcher for resources in Kubernetes. */
public abstract class AbstractKubernetesWatcher<
                T extends HasMetadata, K extends KubernetesResource<T>>
        implements Watcher<T> {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    protected final FlinkKubeClient.WatchCallbackHandler<K> callbackHandler;

    AbstractKubernetesWatcher(FlinkKubeClient.WatchCallbackHandler<K> callbackHandler) {
        this.callbackHandler = callbackHandler;
    }

    @Override
    public void onClose(KubernetesClientException cause) {
        // null means the watcher is closed normally.
        if (cause == null) {
            logger.info("The watcher is closing.");
        } else {
            callbackHandler.handleFatalError(cause);
        }
    }
}
