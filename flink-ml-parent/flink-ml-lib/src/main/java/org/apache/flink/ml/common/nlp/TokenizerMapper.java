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
import org.apache.flink.table.api.TableSchema;

/**
 * Transform all words into lower case, and split it by white space.
 */
public class TokenizerMapper extends SISOMapper {
	private static final String SPLIT_DELIMITER = "\\s";

	public TokenizerMapper(TableSchema dataSchema, Params params) {
		super(dataSchema, params);
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
		String content = ((String) input).toLowerCase();
		StringBuilder builder = new StringBuilder();
		String[] tokens = content.split(SPLIT_DELIMITER);
		for (String token : tokens) {
			builder.append(token).append(NLPConstant.WORD_DELIMITER);
		}
		return builder.toString().trim();
	}
}
