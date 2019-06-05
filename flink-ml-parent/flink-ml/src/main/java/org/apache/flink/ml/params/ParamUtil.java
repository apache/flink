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

package org.apache.flink.ml.params;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.ml.common.MLSession;

import org.apache.commons.lang3.StringUtils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Util for the parameters.
 */
public class ParamUtil {

	/**
	 * given the class of an operator, list all parameters.
	 *
	 * @param cls the class of operator
	 * @return
	 */
	public static List <ParamInfo> getParametersByOperator(Class cls) {
		Stream <ParamInfo> s = Stream.of(cls.getFields())
			.filter(x -> Modifier.isStatic(x.getModifiers()) && Modifier.isFinal(x.getModifiers()))
			.filter(x -> ParamInfo.class.isAssignableFrom(x.getType()))
			.map(x -> {
				try {
					return (ParamInfo) x.get(null);
				} catch (Exception ex) {
					return null;
				}
			})
			.filter(Objects::nonNull);

		try {
			final Object obj = cls.getConstructor(Params.class).newInstance(new Params());
			s = Stream.concat(s, Stream.of(cls.getMethods())
				.filter(x -> !Modifier.isStatic(x.getModifiers()) && x.getParameterCount() == 0)
				.filter(x -> ParamInfo.class.isAssignableFrom(x.getReturnType()))
				.map(x -> {
					try {
						return ((ParamInfo) x.invoke(obj));
					} catch (IllegalAccessException | InvocationTargetException e) {
						return null;
					}
				})
				.filter(Objects::nonNull))
				.distinct();
		} catch (Exception e) {
		}

		return s.sorted((a, b) -> a.isOptional() ? (b.isOptional() ? 0 : 1) : (b.isOptional() ? -1 : 0))
			.collect(Collectors.toList());
	}

	private static void printOneRow(String[] cells, int[] maxLength) {
		System.out.print("|");
		for (int i = 0; i < cells.length; ++i) {
			System.out.print(" ");
			System.out.print(cells[i]);
			System.out.print(StringUtils.repeat(" ", maxLength[i] - cells[i].length()));
			System.out.print(" |");
		}
		System.out.println();
	}

	/**
	 * given one operator, print the help information.
	 *
	 * @param cls the class of operator
	 */
	public static void help(Class cls) {
		List <String[]> g = getParametersByOperator(cls).stream()
			.map(x -> new String[] {
				x.getName(),
				x.getDescription(),
				x.isOptional() ? "optional" : "required",
				x.getDefaultValue() == null ? "null" : MLSession.jsonConverter.toJson(x.getDefaultValue())
			})
			.collect(Collectors.toList());

		final String[] tableHeader = new String[] {"name", "description", "optional", "defaultValue"};

		final int[] maxLengthOfCells = IntStream.range(0, tableHeader.length)
			.map(idx -> Math.max(tableHeader[idx].length(),
				g.stream().mapToInt(x -> x[idx].length()).max().orElse(0)))
			.toArray();

		final int maxLength = IntStream.of(maxLengthOfCells).sum() + maxLengthOfCells.length * 3 + 1;

		System.out.println(StringUtils.repeat("-", maxLength));
		printOneRow(tableHeader, maxLengthOfCells);
		System.out.println(StringUtils.repeat("-", maxLength));

		g.forEach(x -> printOneRow(x, maxLengthOfCells));
		System.out.println(StringUtils.repeat("-", maxLength));
	}
}
