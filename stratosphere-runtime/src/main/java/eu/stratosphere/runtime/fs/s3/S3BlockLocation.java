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

package eu.stratosphere.runtime.fs.s3;

import java.io.IOException;

import eu.stratosphere.core.fs.BlockLocation;

public final class S3BlockLocation implements BlockLocation {

	private final String[] hosts;

	private final long length;

	S3BlockLocation(final String host, final long length) {

		this.hosts = new String[1];
		this.hosts[0] = host;
		this.length = length;
	}

	@Override
	public int compareTo(final BlockLocation arg0) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String[] getHosts() throws IOException {

		return this.hosts;
	}

	@Override
	public long getOffset() {

		return 0;
	}

	@Override
	public long getLength() {

		return this.length;
	}

}
