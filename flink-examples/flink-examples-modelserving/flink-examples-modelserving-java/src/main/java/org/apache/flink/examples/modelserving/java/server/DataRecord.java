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

package org.apache.flink.examples.modelserving.java.server;

import org.apache.flink.modelserving.java.model.DataToServe;
import org.apache.flink.modelserving.wine.Winerecord;

import java.util.Optional;

/**
 * Implementation of container for wine data.
 */
public class DataRecord implements DataToServe<Winerecord.WineRecord> {

	private Winerecord.WineRecord record;


	/**
	 * Creates a new wine data container.
	 *
	 * @param record wine data record.
	 */
	public DataRecord(Winerecord.WineRecord record){
		this.record = record;
	}

	/**
	 * Get record type.
	 *
	 * @return record type.
	 */
	@Override
	public String getType() {
		return record.getDataType();
	}

	/**
	 * Get wine record.
	 *
	 * @return wine record.
	 */
	@Override
	public Winerecord.WineRecord getRecord() {
		return record;
	}

	/**
	 * Convert byte array to wine record.
	 *
	 * @param binary byte array.
	 * @return wine record.
	 */
	public static Optional<DataToServe<Winerecord.WineRecord>> convertData(byte[] binary){
		try {
            // Unmarshall record
			return Optional.of(new DataRecord(Winerecord.WineRecord.parseFrom(binary)));
		} catch (Throwable t) {
            // Oops
			System.out.println("Exception parsing input record" + new String(binary));
			t.printStackTrace();
			return Optional.empty();
		}
	}
}
