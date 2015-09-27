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

package org.apache.flink.ml.common

import java.io._
import javax.xml.transform.stream.StreamResult

import org.apache.flink.ml.MLUtils
import org.dmg.pmml.{Header, PMML}
import org.jpmml.model.JAXBUtil

/**
 * Allows a trained model to be exported in PMML format
 *
 * Predictive Model Markup Language is an XML based file format developed by the Data Mining
 * Group [[http://www.dmg.org]] to provide a way for applications to describe and exchange models
 * produced by Machine learning and data mining algorithms.
 */
trait PMMLExportable {

  /**
   * Export the model to a file
   *
   * @param path Path to the file
   */
  final def exportToPMML(path: String): Unit = {
    val pmml = toPMML()
    if (pmml.getHeader == null) pmml.setHeader(new Header())
    pmml.getHeader.setApplication(MLUtils.pmmlApp)
    if (pmml.getHeader.getDescription == null) pmml.getHeader.setDescription(getClass.getName)
    JAXBUtil.marshalPMML(pmml, new StreamResult(new File(path)))
  }

  /**
   * Exports the trained model to PMML format.
   *
   * @return Model in PMML format
   */
  private[ml] def toPMML(): PMML
}
