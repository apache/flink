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

package org.apache.flink.kubernetes.kubeclient;

import org.apache.flink.kubernetes.kubeclient.FlinkKubeClient.WatchCallbackHandler;

import javax.annotation.Nullable;

import java.util.concurrent.ExecutorService;

/** The interface for the Kubernetes shared watcher. */
public interface KubernetesSharedWatcher<T> extends AutoCloseable {

    /** Close the shared watcher without Exception. */
    @Override
    void close();

    /**
     * Watch the Kubernetes resource with specified name and do the {@link WatchCallbackHandler}.
     *
     * @param name name to filter the resource to watch
     * @param callbackHandler callbackHandler which reacts to resource events
     * @param executorService to run callback
     * @return Return a watch for the Kubernetes resource. It needs to be closed after use.
     */
    Watch watch(
            String name,
            WatchCallbackHandler<T> callbackHandler,
            @Nullable ExecutorService executorService);

    /** The Watch returned after creating watching, which can be used to close the watching. */
    interface Watch extends AutoCloseable {
        @Override
        void close();
    }
}
