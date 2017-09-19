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
package org.apache.flink.api.common.io.compression;

import org.xerial.snappy.SnappyInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Collections;

/**
 * Factory for input streams that decompress the Java (Xerial) Snappy compression format.
 */
public class SnappyXerialInputStreamFactory implements InflaterInputStreamFactory<SnappyInputStream> {

	private static SnappyXerialInputStreamFactory INSTANCE = null;

	public static SnappyXerialInputStreamFactory getInstance() {
		if (INSTANCE == null) {
			INSTANCE = new SnappyXerialInputStreamFactory();
		}
		return INSTANCE;
	}

	@Override
	public SnappyInputStream create(InputStream in) throws IOException {
		return new SnappyInputStream(in);
	}

	@Override
	public Collection<String> getCommonFileExtensions() {
		return Collections.singleton("snappy");
	}
}
