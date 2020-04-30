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

package org.apache.flink.ml.api.core;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.util.InstantiationUtil;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A pipeline is a linear workflow which chains {@link Estimator}s and {@link Transformer}s to
 * execute an algorithm.
 *
 * <p>A pipeline itself can either act as an Estimator or a Transformer, depending on the stages it
 * includes. More specifically:
 * <ul>
 * <li>
 * If a Pipeline has an {@link Estimator}, one needs to call {@link Pipeline#fit(TableEnvironment,
 * Table)} before use the pipeline as a {@link Transformer} . In this case the Pipeline is an {@link
 * Estimator} and can produce a Pipeline as a {@link Model}.
 * </li>
 * <li>
 * If a Pipeline has no {@link Estimator}, it is a {@link Transformer} and can be applied to a Table
 * directly. In this case, {@link Pipeline#fit(TableEnvironment, Table)} will simply return the
 * pipeline itself.
 * </li>
 * </ul>
 *
 * <p>In addition, a pipeline can also be used as a {@link PipelineStage} in another pipeline, just
 * like an ordinary {@link Estimator} or {@link Transformer} as describe above.
 */
@PublicEvolving
public final class Pipeline implements Estimator<Pipeline, Pipeline>, Transformer<Pipeline>,
	Model<Pipeline> {
	private static final long serialVersionUID = 1L;
	private final List<PipelineStage> stages = new ArrayList<>();
	private final Params params = new Params();

	private int lastEstimatorIndex = -1;

	public Pipeline() {
	}

	public Pipeline(String pipelineJson) {
		this.loadJson(pipelineJson);
	}

	public Pipeline(List<PipelineStage> stages) {
		for (PipelineStage s : stages) {
			appendStage(s);
		}
	}

	//is the stage a simple Estimator or pipeline with Estimator
	private static boolean isStageNeedFit(PipelineStage stage) {
		return (stage instanceof Pipeline && ((Pipeline) stage).needFit()) ||
			(!(stage instanceof Pipeline) && stage instanceof Estimator);
	}

	/**
	 * Appends a PipelineStage to the tail of this pipeline. Pipeline is editable only via this
	 * method. The PipelineStage must be Estimator, Transformer, Model or Pipeline.
	 *
	 * @param stage the stage to be appended
	 */
	public Pipeline appendStage(PipelineStage stage) {
		if (isStageNeedFit(stage)) {
			lastEstimatorIndex = stages.size();
		} else if (!(stage instanceof Transformer)) {
			throw new RuntimeException(
				"All PipelineStages should be Estimator or Transformer, got:" +
					stage.getClass().getSimpleName());
		}
		stages.add(stage);
		return this;
	}

	/**
	 * Returns a list of all stages in this pipeline in order, the list is immutable.
	 *
	 * @return an immutable list of all stages in this pipeline in order.
	 */
	public List<PipelineStage> getStages() {
		return Collections.unmodifiableList(stages);
	}

	/**
	 * Check whether the pipeline acts as an {@link Estimator} or not. When the return value is
	 * true, that means this pipeline contains an {@link Estimator} and thus users must invoke
	 * {@link #fit(TableEnvironment, Table)} before they can use this pipeline as a {@link
	 * Transformer}. Otherwise, the pipeline can be used as a {@link Transformer} directly.
	 *
	 * @return {@code true} if this pipeline has an Estimator, {@code false} otherwise
	 */
	public boolean needFit() {
		return this.getIndexOfLastEstimator() >= 0;
	}

	public Params getParams() {
		return params;
	}

	//find the last Estimator or Pipeline that needs fit in stages, -1 stand for no Estimator in Pipeline
	private int getIndexOfLastEstimator() {
		return lastEstimatorIndex;
	}

	/**
	 * Train the pipeline to fit on the records in the given {@link Table}.
	 *
	 * <p>This method go through all the {@link PipelineStage}s in order and does the following
	 * on each stage until the last {@link Estimator}(inclusive).
	 *
	 * <ul>
	 * <li>
	 * If a stage is an {@link Estimator}, invoke {@link Estimator#fit(TableEnvironment, Table)}
	 * with the input table to generate a {@link Model}, transform the the input table with the
	 * generated {@link Model} to get a result table, then pass the result table to the next stage
	 * as input.
	 * </li>
	 * <li>
	 * If a stage is a {@link Transformer}, invoke {@link Transformer#transform(TableEnvironment,
	 * Table)} on the input table to get a result table, and pass the result table to the next stage
	 * as input.
	 * </li>
	 * </ul>
	 *
	 * <p>After all the {@link Estimator}s are trained to fit their input tables, a new
	 * pipeline will be created with the same stages in this pipeline, except that all the
	 * Estimators in the new pipeline are replaced with their corresponding Models generated in the
	 * above process.
	 *
	 * <p>If there is no {@link Estimator} in the pipeline, the method returns a copy of this
	 * pipeline.
	 *
	 * @param tEnv  the table environment to which the input table is bound.
	 * @param input the table with records to train the Pipeline.
	 * @return a pipeline with same stages as this Pipeline except all Estimators replaced with
	 * their corresponding Models.
	 */
	@Override
	public Pipeline fit(TableEnvironment tEnv, Table input) {
		List<PipelineStage> transformStages = new ArrayList<>(stages.size());
		int lastEstimatorIdx = getIndexOfLastEstimator();
		for (int i = 0; i < stages.size(); i++) {
			PipelineStage s = stages.get(i);
			if (i <= lastEstimatorIdx) {
				Transformer t;
				boolean needFit = isStageNeedFit(s);
				if (needFit) {
					t = ((Estimator) s).fit(tEnv, input);
				} else {
					// stage is Transformer, guaranteed in appendStage() method
					t = (Transformer) s;
				}
				transformStages.add(t);
				input = t.transform(tEnv, input);
			} else {
				transformStages.add(s);
			}
		}
		return new Pipeline(transformStages);
	}

	/**
	 * Generate a result table by applying all the stages in this pipeline to the input table in
	 * order.
	 *
	 * @param tEnv  the table environment to which the input table is bound.
	 * @param input the table to be transformed
	 * @return a result table with all the stages applied to the input tables in order.
	 */
	@Override
	public Table transform(TableEnvironment tEnv, Table input) {
		if (needFit()) {
			throw new RuntimeException("Pipeline contains Estimator, need to fit first.");
		}
		for (PipelineStage s : stages) {
			input = ((Transformer) s).transform(tEnv, input);
		}
		return input;
	}

	@Override
	public String toJson() {
		ObjectMapper mapper = new ObjectMapper();

		List<Map<String, String>> stageJsons = new ArrayList<>();
		for (PipelineStage s : getStages()) {
			Map<String, String> stageMap = new HashMap<>();
			stageMap.put("stageClassName", s.getClass().getTypeName());
			stageMap.put("stageJson", s.toJson());
			stageJsons.add(stageMap);
		}

		try {
			return mapper.writeValueAsString(stageJsons);
		} catch (JsonProcessingException e) {
			throw new RuntimeException("Failed to serialize pipeline", e);
		}
	}

	@Override
	@SuppressWarnings("unchecked")
	public void loadJson(String json) {
		ObjectMapper mapper = new ObjectMapper();
		List<Map<String, String>> stageJsons;
		try {
			stageJsons = mapper.readValue(json, List.class);
		} catch (IOException e) {
			throw new RuntimeException("Failed to deserialize pipeline json:" + json, e);
		}
		for (Map<String, String> stageMap : stageJsons) {
			appendStage(restoreInnerStage(stageMap));
		}
	}

	private PipelineStage<?> restoreInnerStage(Map<String, String> stageMap) {
		String className = stageMap.get("stageClassName");
		Class<?> clz;
		try {
			clz = Class.forName(className);
		} catch (ClassNotFoundException e) {
			throw new RuntimeException("PipelineStage class " + className + " not exists", e);
		}
		InstantiationUtil.checkForInstantiation(clz);

		PipelineStage<?> s;
		try {
			s = (PipelineStage<?>) clz.newInstance();
		} catch (Exception e) {
			throw new RuntimeException("Class is instantiable but failed to new an instance", e);
		}

		String stageJson = stageMap.get("stageJson");
		s.loadJson(stageJson);
		return s;
	}
}
