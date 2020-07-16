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

package org.apache.flink.kubernetes.configuration.volume;

/**
 * A class contains all specification for mount volume.
 */
public class KubernetesVolumeSpecification {

	private final String volumeName;
	private final String mountPath;
	private final String mountSubPath;
	private final Boolean mountReadOnly;
	private final KubernetesVolumeConfig volumeConfig;

	public KubernetesVolumeSpecification(String volumeName, String path, String subPath, Boolean readOnly, KubernetesVolumeConfig volumeConfig) {
		this.volumeName = volumeName;
		this.mountPath = path;
		this.mountSubPath = subPath;
		this.mountReadOnly = readOnly;
		this.volumeConfig = volumeConfig;
	}

	public String getVolumeName() {
		return volumeName;
	}

	public String getMountPath() {
		return mountPath;
	}

	public String getMountSubPath() {
		return mountSubPath;
	}

	public Boolean getMountReadOnly() {
		return mountReadOnly;
	}

	public KubernetesVolumeConfig getVolumeConfig() {
		return volumeConfig;
	}
}
