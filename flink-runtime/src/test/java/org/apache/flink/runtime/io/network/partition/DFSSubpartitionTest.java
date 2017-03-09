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

package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

public class DFSSubpartitionTest extends SubpartitionTestBase {

	@Override
	DFSSubpartition createSubpartition() {
		final ResultPartition parent = mock(ResultPartition.class);

		DFSSubpartition sp = new DFSSubpartition(0, parent, "abc");
		sp.setDFSWriter(mock(DFSFileWriter.class));
		return sp;
	}

	@Test
	public void testReleaseParent() throws Exception {
	}

	@Test
	public void testReleaseParentAfterSpilled() throws Exception {
	}

	@Test
	public void testCreateReadView() throws Exception {
		final DFSSubpartition subpartition = createSubpartition();

		try {
			subpartition.createReadView(null);

			fail("Did not throw expected exception when create read view.");
		}
		catch (UnsupportedOperationException expected) {
		}
	}

}
