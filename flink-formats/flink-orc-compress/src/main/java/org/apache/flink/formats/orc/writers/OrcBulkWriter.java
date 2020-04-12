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

package org.apache.flink.formats.orc.writers;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.formats.orc.vectorizer.Vectorizer;

import org.apache.orc.OrcProto;
import org.apache.orc.Writer;

import java.io.IOException;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link BulkWriter} implementation that writes data in ORC format.
 *
 * @param <T> The type of element written.
 */
@PublicEvolving
public class OrcBulkWriter<T> implements BulkWriter<T> {

	private final Writer writer;
	private final Vectorizer<T> vectorizer;

	private transient List<OrcProto.UserMetadataItem> userMetadata;

	OrcBulkWriter(Vectorizer<T> vectorizer, List<OrcProto.UserMetadataItem> userMetadata, Writer writer) {
		this.vectorizer = checkNotNull(vectorizer);
		this.writer = checkNotNull(writer);
		this.userMetadata = userMetadata;

		this.vectorizer.initBatch();
	}

	@Override
	public void addElement(T element) throws IOException {
		vectorizer.vectorize(element);
	}

	@Override
	public void flush() throws IOException {
		writer.addRowBatch(vectorizer.getRowBatch());
		writer.appendUserMetadata(userMetadata);
	}

	@Override
	public void finish() throws IOException {
		vectorizer.reset();
		writer.close();
	}
}
