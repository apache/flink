/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.formats.avro.registry.confluent;

import org.apache.flink.annotation.Internal;
import org.apache.flink.formats.avro.SchemaCoder;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;

import javax.annotation.Nullable;

import java.util.Objects;

/** A {@link SchemaCoder.SchemaCoderProvider} that uses a cached schema registry
 * client underlying. **/
@Internal
class CachedSchemaCoderProvider implements SchemaCoder.SchemaCoderProvider {

	private static final long serialVersionUID = 8610401613495438381L;
	private final String subject;
	private final String url;
	private final int identityMapCapacity;

	CachedSchemaCoderProvider(String url, int identityMapCapacity) {
		this(null, url, identityMapCapacity);
	}

	CachedSchemaCoderProvider(@Nullable String subject, String url, int identityMapCapacity) {
		this.subject = subject;
		this.url = Objects.requireNonNull(url);
		this.identityMapCapacity = identityMapCapacity;
	}

	@Override
	public SchemaCoder get() {
		return new ConfluentSchemaRegistryCoder(this.subject,
				new CachedSchemaRegistryClient(
						url,
						identityMapCapacity));
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		CachedSchemaCoderProvider that = (CachedSchemaCoderProvider) o;
		return identityMapCapacity == that.identityMapCapacity &&
				Objects.equals(subject, that.subject) &&
				url.equals(that.url);
	}

	@Override
	public int hashCode() {
		return Objects.hash(subject, url, identityMapCapacity);
	}
}
