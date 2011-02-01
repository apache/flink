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

package eu.stratosphere.nephele.configuration;

import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.junit.Test;

/**
 * This class contains test for the configuraiton package. Tested will be serialization of Configuration Objects
 * @author casp
 *
 */
public class ConfigurationTest {

	/**
	 * This test tests the serialization/deserialization of config information
	 */
	@Test
	public void testConfigurationSerialization(){
		
		//first, create inital configuration object with some parameters
		Configuration conf = new Configuration();
		conf.setString("mykey", "myvalue");
		conf.setBoolean("shouldbetrue", true);
		conf.setInteger("mynumber", 100);
		conf.setClass("myclass", this.getClass());
		
		//now, create streams to serialize configuration to byte array
		ByteArrayOutputStream bo = new ByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream(bo);
		
		try {
			//do serialize...
			conf.write(dos);
			dos.close();
			byte[] bytes = bo.toByteArray();
			bo.close();
			
			//create input streams, re-read byte array...
			ByteArrayInputStream bi = new ByteArrayInputStream(bytes);
			DataInputStream di = new DataInputStream(bi);
			
			Configuration conf2 = new Configuration();
			//de-serialize
			conf2.read(di);
			
			assertEquals(conf2.getString("mykey", "null"), "myvalue");
			assertEquals(conf2.getBoolean("shouldbetrue", false), true);
			assertEquals(conf2.getInteger("mynumber", 0) , 100);			
			assertEquals(conf2.getClass("myclass", null).toString(), this.getClass().toString());
			assertTrue(conf.equals(conf2));
			assertTrue(conf.keySet().equals(conf2.keySet()));
			
		} catch (IOException e) {
			fail(e.getMessage());
		}
		
	}
}
