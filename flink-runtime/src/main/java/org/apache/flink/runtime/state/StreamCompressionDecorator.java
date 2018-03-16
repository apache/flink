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

package org.apache.flink.runtime.state;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.util.NonClosingInputStreamDecorator;
import org.apache.flink.runtime.util.NonClosingOutpusStreamDecorator;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;

/**
 * Implementations of this interface decorate streams with a compression scheme. Subclasses should be stateless.
 */
@Internal
public abstract class StreamCompressionDecorator implements Serializable {

	private static final long serialVersionUID = 1L;

	/**
	 * Decorates the stream by wrapping it into a stream that applies a compression.
	 *
	 * IMPORTANT: For streams returned by this method, {@link OutputStream#close()} is not propagated to the inner
	 * stream. The inner stream must be closed separately.
	 *
	 * @param stream the stream to decorate.
	 * @return an output stream that is decorated by the compression scheme.
	 */
	public final OutputStream decorateWithCompression(OutputStream stream) throws IOException {
		return decorateWithCompression(new NonClosingOutpusStreamDecorator(stream));
	}

	/**
	 * IMPORTANT: For streams returned by this method, {@link InputStream#close()} is not propagated to the inner
	 * stream. The inner stream must be closed separately.
	 *
	 * @param stream the stream to decorate.
	 * @return an input stream that is decorated by the compression scheme.
	 */
	public final InputStream decorateWithCompression(InputStream stream) throws IOException {
		return decorateWithCompression(new NonClosingInputStreamDecorator(stream));
	}

	/**
	 * @param stream the stream to decorate
	 * @return an output stream that is decorated by the compression scheme.
	 */
	protected abstract OutputStream decorateWithCompression(NonClosingOutpusStreamDecorator stream) throws IOException;

	/**
	 * @param stream the stream to decorate.
	 * @return an input stream that is decorated by the compression scheme.
	 */
	protected abstract InputStream decorateWithCompression(NonClosingInputStreamDecorator stream) throws IOException;
}
