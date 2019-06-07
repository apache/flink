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

package org.apache.flink.kubernetes.kubeclient.fabric8.decorators;

import org.apache.flink.kubernetes.FlinkKubernetesOptions;
import org.apache.flink.kubernetes.kubeclient.fabric8.FlinkPod;

import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.api.model.Pod;

import java.util.Arrays;
import java.util.UUID;

/**
 * for GC.
 * */
public class OwnerReferenceDecorator extends Decorator<Pod, FlinkPod> {

	@Override
	protected Pod doDecorate(Pod resource, FlinkKubernetesOptions flinkKubernetesOptions) {

		if (flinkKubernetesOptions.getServiceUUID() == null) {
			flinkKubernetesOptions.setServiceUUID(UUID.randomUUID().toString());
		}

		if (resource.getMetadata() == null) {
			resource.setMetadata(new ObjectMeta());
		}

		resource.getMetadata().setOwnerReferences(Arrays.asList(
			new OwnerReferenceBuilder()
				.withName(flinkKubernetesOptions.getClusterId())
				.withController(true)
				.withBlockOwnerDeletion(true)
				.withKind("service")
				.withApiVersion(resource.getApiVersion())
				.withUid(flinkKubernetesOptions.getServiceUUID())
				.build()
		));

		return resource;
	}
}
