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

package org.apache.flink.api.java.table.windows

import org.apache.flink.api.table.SlideWithSize

/**
  * Helper class for creating a sliding window. In a sliding window elements are assigned to
  * windows of fixed length equal to window size, as in tumbling windows, but in this case, windows
  * can be overlapping. Thus, an element can be assigned to multiple windows.
  *
  * For example, you could have windows of size 15 minutes that slide by 3 minutes. With this
  * 15 minutes worth of elements are grouped every 3 minutes.
  */
object Slide {

  /**
    * Defines the size of a sliding window. In a sliding window elements are assigned to
    * windows of fixed length equal to window size, as in tumbling windows, but in this case,
    * windows can be overlapping. Thus, an element can be assigned to multiple windows.
    * The slide can be specified on the result of this method.
    *
    * For example, you could have windows of size 15 minutes that slide by 3 minutes. With this
    * 15 minutes worth of elements are grouped every 3 minutes.
    *
    * @param size size of the window either as number of rows or interval of milliseconds
    * @return a partially specified sliding window
    */
  def over(size: String): SlideWithSize = new SlideWithSize(size)
}
