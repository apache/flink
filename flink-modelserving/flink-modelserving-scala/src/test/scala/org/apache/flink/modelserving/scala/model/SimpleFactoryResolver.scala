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

package org.apache.flink.modelserving.scala.model

import org.apache.flink.model.modeldescriptor.ModelDescriptor

/**
  * Implementation of Factory resolver for testing.
  */
class SimpleFactoryResolver extends ModelFactoryResolver[Double, Double]{

  private val factories = Map(
    ModelDescriptor.ModelType.TENSORFLOW.value -> SimpleTensorflowModel,
    ModelDescriptor.ModelType.TENSORFLOWSAVED.value -> SimpleTensorflowBundleModel
  )

  /**
    * Get factory based on type.
    *
    * @param type model type.
    * @return model factory.
    */
  override def getFactory(`type`: Int): Option[ModelFactory[Double, Double]] = factories.get(`type`)
}
