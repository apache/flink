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

import org.apache.flink.kubernetes.kubeclient.FlinkKubeClient.WatchCallbackHandler;
import org.apache.flink.kubernetes.kubeclient.KubernetesSharedWatcher;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.client.NamespacedKubernetesClient;
import io.fabric8.kubernetes.client.informers.ResourceEventHandler;
import io.fabric8.kubernetes.client.informers.SharedIndexInformer;
import io.fabric8.kubernetes.client.informers.SharedInformerEventListener;
import io.fabric8.kubernetes.client.informers.SharedInformerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Function;

/** Base class for shared watcher based on {@link SharedIndexInformer}. */
public abstract class KubernetesSharedInformer<
                T extends HasMetadata, TList extends KubernetesResourceList<T>, R>
        implements KubernetesSharedWatcher<R> {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    private final NamespacedKubernetesClient client;
    private final SharedInformerFactory sharedInformerFactory;
    private final SharedIndexInformer<T> sharedIndexInformer;
    private final Function<T, R> eventWrapper;

    private final AggregatedEventHandler aggregatedEventHandler;

    public KubernetesSharedInformer(
            NamespacedKubernetesClient client,
            Class<T> apiTypeClass,
            Class<TList> apiListTypeClass,
            Map<String, String> labels,
            Function<T, R> eventWrapper) {
        Preconditions.checkArgument(
                !CollectionUtil.isNullOrEmpty(labels), "Labels must not be null or empty");
        this.client = client;
        final ExecutorService executorService =
                Executors.newSingleThreadExecutor(
                        new ExecutorThreadFactory("KubernetesClient-Informer"));
        this.sharedInformerFactory = client.informers(executorService);
        this.sharedInformerFactory.withLabels(labels);
        // Using Long.MAX_VALUE as resync period to disable the internal periodical resync. Zero
        // value does not work exactly here. It could be fixed after we bump the fabric8 Kubernetes
        // client version to 5.0.0+. For more details, see
        // https://github.com/fabric8io/kubernetes-client/issues/2651.
        this.sharedIndexInformer =
                sharedInformerFactory.sharedIndexInformerFor(
                        apiTypeClass, apiListTypeClass, Long.MAX_VALUE);
        this.aggregatedEventHandler = new AggregatedEventHandler(executorService);
        this.sharedIndexInformer.addEventHandler(aggregatedEventHandler);
        this.sharedInformerFactory.addSharedInformerEventListener(aggregatedEventHandler);

        this.eventWrapper = eventWrapper;
    }

    @Override
    public Watch watch(
            String name,
            WatchCallbackHandler<R> handler,
            @Nullable ExecutorService executorService) {
        return aggregatedEventHandler.watch(name, new WatchCallback<>(handler, executorService));
    }

    @Override
    public void run() {
        sharedInformerFactory.startAllRegisteredInformers();
    }

    @Override
    public void close() {
        sharedInformerFactory.stopAllRegisteredInformers();
    }

    private String getResourceKey(String name) {
        return client.getNamespace() + "/" + name;
    }

    private class AggregatedEventHandler
            implements ResourceEventHandler<T>, SharedInformerEventListener {
        private final Map<String, EventHandler> handlers = new HashMap<>();
        private final ExecutorService executorService;

        private AggregatedEventHandler(ExecutorService executorService) {
            this.executorService = executorService;
        }

        @Override
        public void onAdd(T obj) {
            executorService.submit(
                    () -> findHandler(obj).ifPresent(EventHandler::handleResourceEvent));
        }

        @Override
        public void onUpdate(T oldObj, T newObj) {
            executorService.submit(
                    () -> findHandler(newObj).ifPresent(EventHandler::handleResourceEvent));
        }

        @Override
        public void onDelete(T obj, boolean deletedFinalStateUnknown) {
            executorService.submit(
                    () -> findHandler(obj).ifPresent(EventHandler::handleResourceEvent));
        }

        @Override
        public void onException(Exception exception) {
            handlers.forEach((k, h) -> h.handleExceptionEvent(exception));
        }

        private Watch watch(String name, WatchCallback<R> watch) {
            final String resourceKey = getResourceKey(name);
            final String watchId = UUID.randomUUID().toString();
            final CompletableFuture<Void> closeFuture = new CompletableFuture<>();
            executorService.submit(
                    () -> {
                        final EventHandler eventHandler =
                                handlers.computeIfAbsent(
                                        resourceKey, key -> new EventHandler(resourceKey));
                        eventHandler.addWatch(watchId, watch);
                    });
            closeFuture.whenCompleteAsync(
                    (ignored, error) -> {
                        if (error != null) {
                            log.error("Unhandled error while closing watcher.", error);
                        }
                        final boolean removeHandler =
                                handlers.get(resourceKey).removeWatch(watchId);
                        if (removeHandler) {
                            handlers.remove(resourceKey);
                        }
                    },
                    executorService);
            return () -> closeFuture.complete(null);
        }

        private Optional<EventHandler> findHandler(T obj) {
            final String resourceKey = getResourceKey(obj.getMetadata().getName());
            return Optional.ofNullable(handlers.get(resourceKey));
        }
    }

    private class EventHandler {
        private final String resourceKey;
        private final Map<String, WatchCallback<R>> callbacks = new HashMap<>();

        private T resource;

        private EventHandler(String resourceKey) {
            this.resourceKey = resourceKey;
            this.resource = sharedIndexInformer.getIndexer().getByKey(resourceKey);
        }

        private void addWatch(String id, WatchCallback<R> callback) {
            log.info("Starting to watch for {}, watching id:{}", resourceKey, id);
            callbacks.put(id, callback);
            if (resource != null) {
                final List<R> resources = wrapEvent(resource);
                callback.run(h -> h.onAdded(resources));
            }
        }

        private boolean removeWatch(String id) {
            callbacks.remove(id);
            log.info("Stopped to watch for {}, watching id:{}", resourceKey, id);
            return callbacks.isEmpty();
        }

        private void handleResourceEvent() {
            T newResource = sharedIndexInformer.getIndexer().getByKey(resourceKey);
            T oldResource = this.resource;
            if (newResource == null) {
                if (oldResource != null) {
                    onDeleted(oldResource);
                }
            } else {
                if (oldResource == null) {
                    onAdded(newResource);
                } else if (!oldResource
                        .getMetadata()
                        .getResourceVersion()
                        .equals(newResource.getMetadata().getResourceVersion())) {
                    onModified(newResource);
                }
            }
            this.resource = newResource;
        }

        private void onAdded(T obj) {
            this.callbacks.forEach((id, callback) -> callback.run(h -> h.onAdded(wrapEvent(obj))));
        }

        private void onModified(T obj) {
            this.callbacks.forEach(
                    (id, callback) -> callback.run(h -> h.onModified(wrapEvent(obj))));
        }

        private void onDeleted(T obj) {
            this.callbacks.forEach(
                    (id, callback) -> callback.run(h -> h.onDeleted(wrapEvent(obj))));
        }

        private List<R> wrapEvent(T obj) {
            return Collections.singletonList(eventWrapper.apply(obj));
        }

        private void handleExceptionEvent(Exception e) {
            this.callbacks.forEach((id, callback) -> callback.run(h -> h.handleError(e)));
        }
    }

    private static final class WatchCallback<T> {
        private final ReentrantLock callbackLock;
        private final WatchCallbackHandler<T> handler;
        private final ExecutorService executorService;

        private WatchCallback(
                WatchCallbackHandler<T> handler, @Nullable ExecutorService executorService) {
            this.callbackLock = new ReentrantLock(true);
            this.handler = handler;
            this.executorService = executorService;
        }

        private void run(Consumer<WatchCallbackHandler<T>> handlerConsumer) {
            if (this.executorService == null) {
                handlerConsumer.accept(handler);
            } else {
                this.executorService.submit(
                        () -> {
                            callbackLock.lock();
                            try {
                                handlerConsumer.accept(handler);
                            } finally {
                                callbackLock.unlock();
                            }
                        });
            }
        }
    }
}
