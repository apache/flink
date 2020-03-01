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

package org.apache.flink.streaming.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Utility class to simulate in memory file like writes, flushes and closing.
 */
public class ContentDump {
	private boolean writable = true;
	private Map<String, List<String>> filesContent = new HashMap<>();

	public Set<String> listFiles() {
		return new HashSet<>(filesContent.keySet());
	}

	public void setWritable(boolean writable) {
		this.writable = writable;
	}

	/**
	 * Creates an empty file.
	 */
	public ContentWriter createWriter(String name) {
		checkArgument(!filesContent.containsKey(name), "File [%s] already exists", name);
		filesContent.put(name, new ArrayList<>());
		return new ContentWriter(name, this);
	}

	public static void move(String name, ContentDump source, ContentDump target) {
		Collection<String> content = source.read(name);
		try (ContentWriter contentWriter = target.createWriter(name)) {
			contentWriter.write(content).flush();
		}
		source.delete(name);
	}

	public void delete(String name) {
		filesContent.remove(name);
	}

	public Collection<String> read(String name) {
		List<String> content = filesContent.get(name);
		checkState(content != null, "Unknown file [%s]", name);
		List<String> result = new ArrayList<>(content);
		return result;
	}

	private void putContent(String name, List<String> values) {
		List<String> content = filesContent.get(name);
		checkState(content != null, "Unknown file [%s]", name);
		if (!writable) {
			throw new NotWritableException(name);
		}
		content.addAll(values);
	}

	/**
	 * {@link ContentWriter} represents an abstraction that allows to putContent to the {@link ContentDump}.
	 */
	public static class ContentWriter implements AutoCloseable {
		private final ContentDump contentDump;
		private final String name;
		private final List<String> buffer = new ArrayList<>();
		private boolean closed = false;

		private ContentWriter(String name, ContentDump contentDump) {
			this.name = checkNotNull(name);
			this.contentDump = checkNotNull(contentDump);
		}

		public String getName() {
			return name;
		}

		public ContentWriter write(String value) {
			checkState(!closed);
			buffer.add(value);
			return this;
		}

		public ContentWriter write(Collection<String> values) {
			values.forEach(this::write);
			return this;
		}

		public ContentWriter flush() {
			contentDump.putContent(name, buffer);
			return this;
		}

		public void close() {
			buffer.clear();
			closed = true;
		}
	}

	/**
	 * Exception thrown for an attempt to write into read-only {@link ContentDump}.
	 */
	public static class NotWritableException extends RuntimeException {
		public NotWritableException(String name) {
			super(String.format("File [%s] is not writable", name));
		}
	}
}
