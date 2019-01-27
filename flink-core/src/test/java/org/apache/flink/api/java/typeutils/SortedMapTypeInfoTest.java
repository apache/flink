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

package org.apache.flink.api.java.typeutils;

import org.apache.flink.api.common.functions.Comparator;
import org.apache.flink.api.common.functions.NaturalComparator;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.TypeInformationTestBase;
import org.apache.flink.core.fs.Path;

/**
 * Test for {@link SortedMapTypeInfo}.
 */
public class SortedMapTypeInfoTest extends TypeInformationTestBase<SortedMapTypeInfo<?, ?>> {

	private static class PathComparator implements Comparator<Path> {

		private static final long serialVersionUID = 1L;

		@Override
		public int compare(Path o1, Path o2) {
			return o1 == null ? -1 : o1.compareTo(o2);
		}

		@Override
		public boolean equals(Object o) {
			return (o == this) || (o != null && o.getClass() == getClass());
		}

		@Override
		public int hashCode() {
			return "PathComparator".hashCode();
		}
	}

	@Override
	protected SortedMapTypeInfo<?, ?>[] getTestData() {

		return new SortedMapTypeInfo<?, ?>[] {
			new SortedMapTypeInfo<>(BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO, new NaturalComparator<>()),
			new SortedMapTypeInfo<>(BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, new NaturalComparator<>()),
			new SortedMapTypeInfo<>(String.class, Boolean.class),
			new SortedMapTypeInfo<>(Path.class, Boolean.class, new PathComparator())
		};
	}
}
