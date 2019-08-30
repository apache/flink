/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.ml.common.nlp;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.ml.common.mapper.SISOMapper;
import org.apache.flink.ml.params.nlp.NGramParams;
import org.apache.flink.table.api.TableSchema;

/**
 * Transform a document into a new document composed of all its nGrams. The document is split into an array of words by
 * a word delimiter(default space). Through sliding the word array, we get all nGrams and each nGram is connected with a
 * "_" character. All the nGrams are joined together with outputDelimiter in the new document.
 */
public class NGramMapper extends SISOMapper {
	private static final String CONNECTOR_CHARACTER = "_";

	private final int n;

	public NGramMapper(TableSchema dataSchema, Params params) {
		super(dataSchema, params);
		this.n = this.params.get(NGramParams.N);
		if (this.n <= 0) {
			throw new IllegalArgumentException("N must be positive!");
		}
	}

	@Override
	public TypeInformation initOutputColType() {
		return Types.STRING;
	}

	@Override
	public Object map(Object input) {
		if (null == input) {
			return null;
		}
		String content = (String) input;
		String[] tokens = content.split(NLPConstant.WORD_DELIMITER);
		StringBuilder sbd = new StringBuilder();
		for (int i = 0; i < 1 + tokens.length - n; i++) {
			for (int j = 0; j < n; j++) {
				if (j > 0) {
					sbd.append(CONNECTOR_CHARACTER);
				}
				sbd.append(tokens[i + j]);
			}
			sbd.append(NLPConstant.WORD_DELIMITER);
		}
		return sbd.toString().trim();
	}
}
