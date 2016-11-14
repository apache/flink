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

package org.apache.flink.api.common.typeutils;

import org.apache.flink.annotation.PublicEvolving;

import java.util.LinkedList;
import java.util.List;

@SuppressWarnings("rawtypes")
@PublicEvolving
public abstract class CompositeTypeComparator<T> extends TypeComparator<T> {
	
	private static final long serialVersionUID = 1L;

	@Override
	public TypeComparator[] getFlatComparators() {
		List<TypeComparator> flatComparators = new LinkedList<TypeComparator>();
		this.getFlatComparator(flatComparators);
		return flatComparators.toArray(new TypeComparator[flatComparators.size()]);
	}
	
	public abstract void getFlatComparator(List<TypeComparator> flatComparators);
}
