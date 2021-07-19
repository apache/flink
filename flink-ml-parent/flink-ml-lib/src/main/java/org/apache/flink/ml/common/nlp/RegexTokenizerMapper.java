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
import org.apache.flink.ml.params.nlp.RegexTokenizerParams;
import org.apache.flink.table.api.TableSchema;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Extract tokens by using the given pattern to split the document if gaps is True or repeatedly match the pattern if
 * gaps is False. It can also filter tokens below the minimal length.
 */
public class RegexTokenizerMapper extends SISOMapper {
	private Pattern pattern;
	private final String patternStr;
	private final boolean toLowerCase, gaps;
	private final int minTokenLength;

	public RegexTokenizerMapper(TableSchema dataSchema, Params params) {
		super(dataSchema, params);
		this.patternStr = this.params.get(RegexTokenizerParams.PATTERN);
		this.minTokenLength = this.params.get(RegexTokenizerParams.MIN_TOKEN_LENGTH);
		this.toLowerCase = this.params.get(RegexTokenizerParams.TO_LOWER_CASE);
		this.gaps = this.params.get(RegexTokenizerParams.GAPS);
		if (!gaps) {
			pattern = Pattern.compile(patternStr);
		}
	}

	@Override
	public TypeInformation initOutputColType() {
		return Types.STRING;
	}

	/**
	 * Split the document if gaps is True or extract the tokens if gaps is False.
	 *
	 * @param input document input
	 * @return the tokens
	 */
	@Override
	public Object map(Object input) {
		if (null == input) {
			return null;
		}
		String content = (String) input;
		if (toLowerCase) {
			content = content.toLowerCase();
		}
		boolean first = true;
		StringBuilder builder = new StringBuilder();
		if (gaps) {
			String[] tokens = content.split(patternStr);
			for (String token : tokens) {
				if (token.length() >= minTokenLength) {
					if (first) {
						builder.append(token);
						first = false;
					} else {
						builder.append(NLPConstant.WORD_DELIMITER).append(token);
					}
				}
			}
		} else {
			Matcher match = pattern.matcher(content);
			while (match.find()) {
				String token = match.group();
				if (token.length() >= minTokenLength) {
					if (first) {
						builder.append(token);
						first = false;
					} else {
						builder.append(NLPConstant.WORD_DELIMITER).append(token);
					}
				}
			}
		}
		return builder.toString();
	}
}
