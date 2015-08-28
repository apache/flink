/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.examples.join.util;

public class WindowJoinData {

	public static final String GRADES_INPUT = "(john,5)\n" + "(tom,3)\n" + "(alice,1)\n" + "(grace,5)\n" +
			"(john,4)\n" + "(bob,1)\n" + "(alice,2)\n" + "(alice,3)\n" + "(bob,5)\n" + "(alice,3)\n" + "(tom,5)\n" +
			"(john,2)\n" + "(john,1)\n" + "(grace,2)\n" + "(jerry,2)\n" + "(tom,4)\n" + "(bob,4)\n" + "(bob,2)\n" +
			"(tom,2)\n" + "(alice,5)\n" + "(grace,5)\n" + "(grace,1)\n" + "(alice,1)\n" + "(grace,3)\n" + "(tom,1)\n" +
			"(jerry,5)\n" + "(john,3)\n" + "(john,4)\n" + "(john,1)\n" + "(jerry,3)\n" + "(grace,3)\n" + "(bob,3)\n" +
			"(john,3)\n" + "(jerry,4)\n" + "(tom,5)\n" + "(tom,4)\n" + "(john,2)\n" + "(jerry,1)\n" + "(bob,1)\n" +
			"(john,5)\n" + "(grace,4)\n" + "(tom,5)\n" + "(john,4)\n" + "(tom,1)\n" + "(grace,1)\n" + "(john,2)\n" +
			"(jerry,3)\n" + "(jerry,5)\n" + "(tom,2)\n" + "(tom,2)\n" + "(alice,4)\n" + "(tom,4)\n" + "(jerry,4)\n" +
			"(john,3)\n" + "(grace,4)\n" + "(tom,3)\n" + "(jerry,4)\n" + "(john,5)\n" + "(john,4)\n" + "(jerry,1)\n" +
			"(john,5)\n" + "(alice,2)\n" + "(tom,1)\n" + "(alice,5)\n" + "(grace,4)\n" + "(bob,4)\n" + "(jerry,1)\n" +
			"(john,5)\n" + "(tom,4)\n" + "(tom,5)\n" + "(jerry,5)\n" + "(tom,1)\n" + "(grace,3)\n" + "(bob,5)\n" +
			"(john,1)\n" + "(alice,1)\n" + "(grace,3)\n" + "(grace,1)\n" + "(jerry,1)\n" + "(jerry,4)\n" +
			"(bob,4)\n" + "(alice,3)\n" + "(tom,5)\n" + "(alice,4)\n" + "(alice,4)\n" + "(grace,4)\n" + "(john,5)\n" +
			"(john,5)\n" + "(grace,4)\n" + "(tom,4)\n" + "(john,4)\n" + "(john,5)\n" + "(alice,5)\n" + "(jerry,5)\n" +
			"(john,3)\n" + "(tom,5)\n" + "(jerry,4)\n" + "(grace,4)\n" + "(john,3)\n" + "(bob,2)";

	public static final String SALARIES_INPUT = "(john,6469)\n" + "(jerry,6760)\n" + "(jerry,8069)\n" +
			"(tom,3662)\n" + "(grace,8427)\n" + "(john,9425)\n" + "(bob,9018)\n" + "(john,352)\n" + "(tom,3770)\n" +
			"(grace,7622)\n" + "(jerry,7441)\n" + "(alice,1468)\n" + "(bob,5472)\n" + "(grace,898)\n" +
			"(tom,3849)\n" + "(grace,1865)\n" + "(alice,5582)\n" + "(john,9511)\n" + "(alice,1541)\n" +
			"(john,2477)\n" + "(grace,3561)\n" + "(john,1670)\n" + "(grace,7290)\n" + "(grace,6565)\n" +
			"(tom,6179)\n" + "(tom,1601)\n" + "(john,2940)\n" + "(bob,4685)\n" + "(bob,710)\n" + "(bob,5936)\n" +
			"(jerry,1412)\n" + "(grace,6515)\n" + "(grace,3321)\n" + "(tom,8088)\n" + "(john,2876)\n" +
			"(bob,9896)\n" + "(grace,7368)\n" + "(grace,9749)\n" + "(bob,2048)\n" + "(alice,4782)\n" +
			"(alice,3375)\n" + "(tom,5841)\n" + "(bob,958)\n" + "(bob,5258)\n" + "(tom,3935)\n" + "(jerry,4394)\n" +
			"(alice,102)\n" + "(alice,4931)\n" + "(alice,5240)\n" + "(jerry,7951)\n" + "(john,5675)\n" +
			"(bob,609)\n" + "(alice,5997)\n" + "(jerry,9651)\n" + "(alice,1328)\n" + "(bob,1022)\n" +
			"(grace,2578)\n" + "(jerry,9704)\n" + "(tom,4476)\n" + "(grace,3784)\n" + "(alice,6144)\n" +
			"(bob,6213)\n" + "(alice,7525)\n" + "(jerry,2908)\n" + "(grace,8464)\n" + "(jerry,9920)\n" +
			"(bob,3720)\n" + "(bob,7612)\n" + "(alice,7211)\n" + "(jerry,6484)\n" + "(alice,1711)\n" +
			"(jerry,5994)\n" + "(grace,928)\n" + "(jerry,2492)\n" + "(grace,9080)\n" + "(tom,4330)\n" +
			"(bob,8302)\n" + "(john,4981)\n" + "(tom,1781)\n" + "(grace,1379)\n" + "(jerry,3700)\n" +
			"(jerry,3584)\n" + "(jerry,2038)\n" + "(jerry,3902)\n" + "(tom,1336)\n" + "(jerry,7500)\n" +
			"(tom,3648)\n" + "(alice,2533)\n" + "(tom,8685)\n" + "(bob,3968)\n" + "(tom,3241)\n" + "(bob,7461)\n" +
			"(jerry,2138)\n" + "(alice,7503)\n" + "(alice,6424)\n" + "(tom,140)\n" + "(john,9802)\n" +
			"(grace,2977)\n" + "(grace,889)\n" + "(john,1338)";

	private WindowJoinData() {
	}
}
