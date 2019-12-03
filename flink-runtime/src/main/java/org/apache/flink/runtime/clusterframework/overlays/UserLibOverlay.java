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

package org.apache.flink.runtime.clusterframework.overlays;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.runtime.clusterframework.ContainerSpecification;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;

/**
 * Overlays the user library into a container.
 * The following directory and files in the directory are copied to the container if it exists:
 *  - {@link ConfigConstants#DEFAULT_FLINK_USR_LIB_DIR}/
 */
public class UserLibOverlay extends AbstractContainerOverlay {

	@Nullable
	private final File usrLibDirectory;

	private UserLibOverlay(@Nullable File usrLibDirectory) {
		this.usrLibDirectory = usrLibDirectory;
	}

	@Override
	public void configure(ContainerSpecification container) throws IOException {
		if (usrLibDirectory != null) {
			addPathRecursively(usrLibDirectory, FlinkDistributionOverlay.TARGET_ROOT, container);
		}
	}

	public static UserLibOverlay.Builder newBuilder() {
		return new UserLibOverlay.Builder();
	}

	/**
	 * A builder for the {@link UserLibOverlay}.
	 */
	public static class Builder {

		@Nullable
		private File usrLibDirectory;

		public UserLibOverlay.Builder setUsrLibDirectory(@Nullable File usrLibDirectory) {
			this.usrLibDirectory = usrLibDirectory;
			return this;
		}

		public UserLibOverlay build() {
			return new UserLibOverlay(usrLibDirectory);
		}

	}
}
