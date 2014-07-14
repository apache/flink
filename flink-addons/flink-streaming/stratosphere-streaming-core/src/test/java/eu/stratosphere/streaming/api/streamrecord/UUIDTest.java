/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2014 by the Stratosphere project (http://stratosphere.eu)
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
package eu.stratosphere.streaming.api.streamrecord;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.junit.Test;

public class UUIDTest {

	@Test
	public void test() throws IOException {
		ByteArrayOutputStream buff = new ByteArrayOutputStream();
		DataOutputStream out = new DataOutputStream(buff);
		
		UID id = new UID(3);
		id.write(out);
		
		DataInputStream in = new DataInputStream(new ByteArrayInputStream(buff.toByteArray()));
		
		UID id2 = new UID();
		id2.read(in);

		assertEquals(id.getChannelId(), id2.getChannelId());
		assertArrayEquals(id.getGeneratedId(), id2.getGeneratedId());
		assertArrayEquals(id.getId(), id2.getId());
	}

}
