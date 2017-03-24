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

package org.apache.flink.core.memory;

import org.junit.Assert;
import org.junit.Test;

public class ByteArrayOutputStreamWithPosTest {
	@Test
	public void setPositionWhenBufferIsFull() throws Exception {

		int initBufferSize = 32;

		ByteArrayOutputStreamWithPos stream = new ByteArrayOutputStreamWithPos(initBufferSize);

		stream.write(new byte[initBufferSize]);

		// check whether the buffer is filled fully
		Assert.assertEquals(initBufferSize, stream.getBuf().length);

		// check current position is the end of the buffer
		Assert.assertEquals(initBufferSize, stream.getPosition());

		stream.setPosition(initBufferSize);

		// confirm current position is at where we expect.
		Assert.assertEquals(initBufferSize, stream.getPosition());

	}

}
