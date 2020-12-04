/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.gcp.pubsub.emulator;

import com.google.api.gax.core.CredentialsProvider;
import com.google.auth.Credentials;

/**
 * A CredentialsProvider that simply provides the right credentials that are to be used for connecting to an emulator.
 * NOTE: The Google provided NoCredentials and NoCredentialsProvider do not behave as expected.
 *       See https://github.com/googleapis/gax-java/issues/1148
 */
public final class EmulatorCredentialsProvider implements CredentialsProvider {
	@Override
	public Credentials getCredentials() {
		return EmulatorCredentials.getInstance();
	}

	public static EmulatorCredentialsProvider create() {
		return new EmulatorCredentialsProvider();
	}
}
