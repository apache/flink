/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.nephele.fs.s3;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import eu.stratosphere.nephele.fs.Path;

public final class S3DirectoryStructure {

	private final String basePath;

	private final Map<Path, S3BucketObjectPair> cache = new HashMap<Path, S3BucketObjectPair>();

	S3DirectoryStructure(final String basePath) {
		this.basePath = basePath;
	}

	S3BucketObjectPair toBucketObjectPair(final Path path) {

		S3BucketObjectPair bop = this.cache.get(path);
		if (bop != null) {
			return bop;
		}

		final URI uri = path.toUri();
		String p = uri.getPath();
		if (!this.basePath.isEmpty() && !p.contains(this.basePath)) {
			throw new IllegalArgumentException(path + " is not a valid path for the file system");
		}

		// Extract the base path
		if (!this.basePath.isEmpty()) {
			final int pos = p.indexOf(this.basePath);
			p = p.substring(pos + this.basePath.length());
		}

		// Remove leading SEPARATOR
		if (!p.isEmpty()) {
			if (p.charAt(0) == Path.SEPARATOR_CHAR) {
				p = p.substring(1);
			}
		}

		if (p.isEmpty()) {
			bop = new S3BucketObjectPair(null, null);
			this.cache.put(path, bop);
			return bop;
		}

		final int objectPos = p.indexOf(Path.SEPARATOR_CHAR);
		if (objectPos < 0) {
			bop = new S3BucketObjectPair(p, null);
		} else {
			final String bucket = p.substring(0, objectPos);
			final String object = p.substring(objectPos + 1);
			bop = new S3BucketObjectPair(bucket, object);
		}

		this.cache.put(path, bop);

		return bop;
	}
}
