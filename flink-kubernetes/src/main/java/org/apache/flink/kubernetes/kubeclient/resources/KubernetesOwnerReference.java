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

import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Represent Toleration resource in kubernetes.
 */
public class KubernetesOwnerReference extends KubernetesResource<OwnerReference> {

	static final String API_VERSION = "apiversion";
	static final String DELETION = "blockownerdeletion";
	static final String CONTROLLER = "controller";
	static final String KIND = "kind";
	static final String NAME = "name";
	static final String UUID = "uid";

	private static final Logger LOG = LoggerFactory.getLogger(KubernetesOwnerReference.class);

	private KubernetesOwnerReference(OwnerReference ownerReference) {
		super(ownerReference);
	}

	public static KubernetesOwnerReference fromMap(Map<String, String> stringMap) {
		final OwnerReferenceBuilder ownerReferenceBuilder = new OwnerReferenceBuilder();
		stringMap.forEach((k, v) -> {
			switch (k.toLowerCase()) {
				case API_VERSION:
					ownerReferenceBuilder.withApiVersion(v);
					break;
				case DELETION:
					ownerReferenceBuilder.withBlockOwnerDeletion(Boolean.valueOf(v));
					break;
				case CONTROLLER:
					ownerReferenceBuilder.withController(Boolean.valueOf(v));
					break;
				case KIND:
					ownerReferenceBuilder.withKind(v);
					break;
				case NAME:
					ownerReferenceBuilder.withName(v);
					break;
				case UUID:
					ownerReferenceBuilder.withUid(v);
					break;
				default:
					LOG.warn("Unrecognized key({}) of owner reference, will ignore.", k);
					break;
			}
		});
		return new KubernetesOwnerReference(ownerReferenceBuilder.build());
	}
}
