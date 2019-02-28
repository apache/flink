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
import org.apache.flink.kubernetes.kubeclient.fabric8.Resource;

/**
 * Abstract decorator for add features to resource such as pod/service.
 * */
public abstract class Decorator<R, T extends Resource<R>> {

	public static final String API_VERSION = "v1";

	protected Boolean isEnabled(FlinkKubernetesOptions flinkKubernetesOptions) {
		return true;
	}

	/**
	 * do decorate the real resource.
	 */
	protected abstract R doDecorate(R resource, FlinkKubernetesOptions flinkKubernetesOptions);

	/**
	 * extract real resource from resource, decorate and put it back.
	 */
	public T decorate(T resource) {

		//skip if it was disabled
		if (!this.isEnabled(resource.getFlinkOptions())) {
			return resource;
		}

		R realResource = resource.getInternalResource();
		realResource = doDecorate(realResource, resource.getFlinkOptions());
		resource.setInternalResource(realResource);

		return resource;
	}
}
