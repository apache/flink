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

package org.apache.flink.test.io.csv.custom.type.complex;

import org.apache.flink.test.io.csv.custom.type.NestedCustomJsonType;

/**
 * A user-defined type (with a Java Generic parameter inside) that is represented in one or several csv fields.
 */
public class GenericsAwareCustomJsonType<T> {

	private String f1;
	private NestedCustomJsonType f2;
	private T f3;

	public String getF1() {
		return f1;
	}

	public void setF1(String f1) {
		this.f1 = f1;
	}

	public NestedCustomJsonType getF2() {
		return f2;
	}

	public void setF2(NestedCustomJsonType f2) {
		this.f2 = f2;
	}

	public T getF3() {
		return f3;
	}

	public void setF3(T f3) {
		this.f3 = f3;
	}

	@Override
	public String toString() {
		return "GenericsAwareCustomJsonType{" +
			"f1='" + f1 + '\'' +
			", f2=" + f2 +
			", f3=" + f3 +
			'}';
	}
}
