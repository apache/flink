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

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.KubernetesClientTimeoutException;
import io.fabric8.kubernetes.client.Watcher;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Watch a specific action.
 */
public class ActionWatcher<T extends HasMetadata> implements Watcher<T> {
	private final CountDownLatch latch = new CountDownLatch(1);
	private final AtomicReference<T> reference = new AtomicReference<>();
	private final T resource;
	private final Action expectedAction;

	public ActionWatcher(Action expectedAction, T resource) {
		this.resource = resource;
		this.expectedAction = expectedAction;
	}

	@Override
	public void eventReceived(Action action, T resource) {
		if (action == this.expectedAction) {
			this.reference.set(resource);
			this.latch.countDown();
		}
	}

	@Override
	public void onClose(KubernetesClientException e) {
	}

	public T await(long amount, TimeUnit timeUnit) {
		try {
			if (this.latch.await(amount, timeUnit)) {
				return this.reference.get();
			} else {
				throw new KubernetesClientTimeoutException(this.resource, amount, timeUnit);
			}
		} catch (InterruptedException var5) {
			throw new KubernetesClientTimeoutException(this.resource, amount, timeUnit);
		}
	}
}
