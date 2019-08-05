/**
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

package org.apache.flink.formats.avro.generated;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;
import java.io.IOException;

public class AvorTest {
	public static void main(String[] args) throws IOException {
		String path = "/tmp/aaa.avro"; // avro文件存放目录
		DatumWriter<Address> userDatumWriter = new SpecificDatumWriter<Address>(Address.class);
		DataFileWriter<Address> dataFileWriter = new DataFileWriter<Address>(userDatumWriter);

		Address a = new Address();
		a.setNum(1);
		a.setCity("city");
		a.setState("state");
		a.setZip("zip001");
		a.setStreet("street");


		dataFileWriter.create(a.getSchema(),  new File(path));
		dataFileWriter.append(a);
		dataFileWriter.flush();
		dataFileWriter.close();



		DatumReader<Address> reader = new SpecificDatumReader<Address>(Address.class);
		DataFileReader<Address> dataFileReader = new DataFileReader<Address>(new File("/tmp/flink-data/avro/2019-05-27--16/part-3-0"), reader);
		Address user = null;
		while (dataFileReader.hasNext()) {
			user = dataFileReader.next();
			System.out.println(user);
		}

	}
}
