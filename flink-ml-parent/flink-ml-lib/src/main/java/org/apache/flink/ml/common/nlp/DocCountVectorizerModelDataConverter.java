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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.ml.common.model.SimpleModelDataConverter;

import java.util.ArrayList;
import java.util.List;

/**
 * ModelDataConverter for DocCountVectorizer.
 *
 * <p>Record the document frequency(DF), word count(WC) and inverse document frequency(IDF) of every word.
 */
public class DocCountVectorizerModelDataConverter extends SimpleModelDataConverter<List<String>, List<String>> {
	public DocCountVectorizerModelDataConverter() {
	}

	@Override
	public Tuple2<Params, Iterable<String>> serializeModel(List<String> data) {
		return Tuple2.of(new Params(), data);
	}

	@Override
	public List<String> deserializeModel(Params meta, Iterable<String> modelData) {
		List<String> ret = new ArrayList<>();
		modelData.forEach(ret::add);
		return ret;
	}
}
