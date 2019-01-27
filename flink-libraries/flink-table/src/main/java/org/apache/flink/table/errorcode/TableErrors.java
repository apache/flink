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

package org.apache.flink.table.errorcode;

import org.apache.commons.lang3.StringUtils;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Proxy;
import java.text.DateFormat;
import java.text.Format;
import java.text.MessageFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * wrapper class for table error code instance.
 */
public class TableErrors {

	public static final TableErrorCode INST = (TableErrorCode) createProxy(TableErrorCode.class);

	protected static Pattern pattern1 = Pattern.compile("[0-9]{8}");
	protected static Set<String> modNames = new HashSet<>();

	static {
		modNames.add("SQL");
		modNames.add("PAR");
		modNames.add("CON");
		modNames.add("STB");
		modNames.add("RUN");
	}

	/**
	 * create proxy instance for one module's error code interface.
	 *
	 * @param clazz interface that has error code definitions for one module.
	 * @return instance of the interface that can be used by developer for specifying error code
	 * (for now, it is for dumping error code and its cause&action message)
	 * when throwing exceptions
	 */
	public static Object createProxy(Class clazz) {

		return Proxy.newProxyInstance(
			clazz.getClassLoader(),
			clazz.isInterface() ? new Class[]{clazz} : clazz.getInterfaces(),
			(obj, method, args) -> {
				checkParam(method, args);
				return assemblyErrCodeString(method, args);
			});

	}

	/**
	 * Parameter check when invoking method.
	 *
	 * @param method
	 * @param args
	 */
	protected static void checkParam(Method method, Object[] args) {
		TableErrorCode.ErrCode errCode = method.getAnnotation(TableErrorCode.ErrCode.class);
		String errDetail = errCode.details();
		String errCause = errCode.cause();

		MessageFormat format1 = new MessageFormat(errDetail);
		MessageFormat format2 = new MessageFormat(errCause);

		if (args == null || args.length == 0) {
			if ((format1.getFormatsByArgumentIndex() != null && format1.getFormatsByArgumentIndex().length > 0)
				|| (format2.getFormatsByArgumentIndex() != null && format2.getFormatsByArgumentIndex().length > 0)) {
				throw new AssertionError("mismatched parameter length between "
					+ method.getName() + " and its annotation @ErrCode");
			}
		} else {
			if ((format1.getFormatsByArgumentIndex() != null && format1.getFormatsByArgumentIndex().length > args.length)
				|| format1.getFormatsByArgumentIndex() == null
				|| (format2.getFormatsByArgumentIndex() != null && format2.getFormatsByArgumentIndex().length > args.length)) {
				throw new AssertionError("mismatched parameter length between "
					+ method.getName() + " and its annotation @ErrCode");
			}
		}
	}

	/**
	 * assembly error code messages.
	 *
	 * @param method error code related function declared in error interface
	 * @param args   args passed to that related function
	 * @return error code messages containing code id, cause and action.
	 */
	protected static String assemblyErrCodeString(Method method, Object[] args) {
		TableErrorCode.ErrCode errCode = method.getAnnotation(TableErrorCode.ErrCode.class);
		String errId = errCode.codeId();
		String errCause = errCode.cause();
		String errDetail = errCode.details();
		String errAction = errCode.action();

		if (args != null && args.length != 0) {
			MessageFormat format1 = new MessageFormat(errDetail);
			errDetail = format1.format(args);

			MessageFormat format2 = new MessageFormat(errCause);
			errCause = format2.format(args);
		}

		errId = prettyPrint(errId);
		errCause = prettyPrint(errCause);
		errDetail = prettyPrint(errDetail);
		errAction = prettyPrint(errAction);

		String msg = "\n************\n"
			//"\n*******************************************************\n"
			+ "ERR_ID:\n"
			+ errId + "\n"
			+ "CAUSE:\n"
			+ errCause + "\n"
			+ "ACTION:\n"
			+ errAction + "\n"
			+ "DETAIL:\n"
			+ errDetail + "\n"
			//+ "*******************************************************";
			+ "************";
		return msg;
	}

	/**
	 * Print out error code in a pretty way.
	 *
	 * @param str
	 * @return
	 */
	public static String prettyPrint(String str) {
		if (str != null && str.length() != 0) {
			str = indent(5) + str.replaceAll("\n", "\n" + indent(5));
		}

		return str;
	}

	/**
	 * Get multiple indent.
	 *
	 * @param cnt
	 * @return
	 */
	public static String indent(int cnt) {
		if (cnt <= 0) {
			return null;
		}
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < cnt; i++) {
			sb.append(" ");
		}
		return sb.toString();
	}

	/**
	 * validate an error code definition interface to check its annotation usage
	 * and err code format.
	 *
	 * @param clazz err code definition interface
	 */
	public static void validate(Class<?> clazz) {
		validate(clazz, EnumSet.allOf(ValidationType.class));
	}

	/**
	 * validate an error code definition interface to check its annotation usage
	 * and err code format.
	 *
	 * @param clazz       err code definition interface
	 * @param validations types of validations to perform.
	 */
	public static void validate(Class<?> clazz, EnumSet<ValidationType> validations) {
		int cnt = 0;
		Set<String> errIds = new HashSet<>();

		for (Method method : clazz.getMethods()) {
			if (!Modifier.isStatic(method.getModifiers())) {
				cnt++;

				final TableErrorCode.ErrCode anno1 = method.getAnnotation(TableErrorCode.ErrCode.class);

				for (ValidationType validation : validations) {
					switch (validation) {
					case ANNOTATION_SPECIFIED:
						if (anno1 == null || StringUtils.isEmpty(anno1.codeId())
							|| StringUtils.isEmpty(anno1.cause())) {
							throw new AssertionError(String.format("error code method[%s] "
									+ "must specify @ErrCode annotation with cause, details and none-empty codeId!",
								method.getName()));
						}
						break;

					case ERROR_ID_CHECK:
						if (anno1 == null) {
							throw new AssertionError(String.format("error code method[%s]"
									+ "has no @ErrId annotation!",
								method.getName()));
						}
						String errId = anno1.codeId();
						if (!checkErrorCodeFmt(errId)) {
							throw new AssertionError(String.format("error code method[%s]"
									+ "has invalid error code: %s",
								method.getName(),
								errId));
						}
						if (errIds.contains(errId)) {
							throw new AssertionError(String.format("error code method[%s]"
									+ "has duplicated err id: %s",
								method.getName(),
								errId));
						}
						errIds.add(errId);
						break;

					case ARGUMENT_MATCH:
						if (anno1 == null) {
							throw new AssertionError(String.format("error code method[%s]"
									+ "has no @ErrCode annotation!",
								method.getName()));
						}

						String msg = anno1.details();
						String cause = anno1.cause();

						MessageFormat msgFmt = new MessageFormat(msg);
						MessageFormat causeFmt = new MessageFormat(cause);

						final Format[] msgFormats = msgFmt.getFormatsByArgumentIndex();
						final Format[] causeFormats = causeFmt.getFormatsByArgumentIndex();

						final Format[] formats = msgFormats.length != 0 ? msgFormats : causeFormats;

						final List<Class> types = new ArrayList<>();
						final Class<?>[] paramTypes = method.getParameterTypes();

						/* implement check on case when only one field get parameter(s)
						 * TODO implement check on case when both fields get parameter(s)
						 */
						if (!(msgFormats.length != 0 && causeFormats.length != 0)) {

							for (int i = 0; i < formats.length; i++) {
								Format fmt1 = formats[i];
								Class paramType = paramTypes[i];
								final Class<?> e;
								if (fmt1 instanceof NumberFormat) {
									e = paramType == short.class
										|| paramType == int.class
										|| paramType == long.class
										|| paramType == float.class
										|| paramType == double.class
										|| Number.class.isAssignableFrom(paramType)
										? paramType
										: Number.class;
								} else if (fmt1 instanceof DateFormat) {
									e = Date.class;
								} else {
									e = String.class;
								}

								types.add(e);
							}

							final List<Class<?>> paramTypeList = Arrays.asList(paramTypes);
							if (!types.equals(paramTypeList)) {
								throw new AssertionError(String.format("error code[%s]"
										+ " has type mismatch(s) between method param %s and"
										+ " format elements %s in annotation",
									method.getName(),
									types,
									paramTypeList
								));
							}
						}
						break;

					default:
						break;
					}
				}
			}
		}

		if (cnt == 0 && validations.contains(ValidationType.AT_LEAST_ONE)) {
			throw new AssertionError(clazz + " contains no error code");
		}
	}

	/**
	 * Check error code id format.
	 *
	 * @param errId
	 * @return
	 */
	protected static boolean checkErrorCodeFmt(String errId) {
		if (errId == null || errId.isEmpty()) {
			return false;
		}
		String[] parts = errId.split("-");
		if (parts.length != 2) {
			return false;
		}
		if (!modNames.contains(parts[0])) {
			return false;
		}
		if (!pattern1.matcher(parts[1]).matches()) {
			return false;
		}

		return true;
	}

	/**
	 * Types of validation that can be performed on a resource.
	 */
	public enum ValidationType {
		/**
		 * Checks that the ErrId, ErrCause and ErrAction annotations are on every resource.
		 */
		ANNOTATION_SPECIFIED,

		/**
		 * Checks that there is at least one resource.
		 */
		AT_LEAST_ONE,

		/**
		 * Checks that @ErrId anno has non-null value which has expected error id format,
		 * and there's no duplicated error id in resource instance.
		 */
		ERROR_ID_CHECK,

		/**
		 * Checks that the parameters of the method are consistent with the
		 * format elements in the error cause message.
		 */
		ARGUMENT_MATCH,
	}
}
