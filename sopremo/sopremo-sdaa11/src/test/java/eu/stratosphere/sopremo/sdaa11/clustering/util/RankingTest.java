/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/
package eu.stratosphere.sopremo.sdaa11.clustering.util;

import org.junit.Assert;
import org.junit.Test;

import eu.stratosphere.sopremo.sdaa11.util.Ranking;

/**
 * @author skruse
 * 
 */
public class RankingTest {

	@Test
	public void testEmptyRanking() {
		final Ranking<?> ranking = new Ranking<Object>(5);
		Assert.assertEquals(0, ranking.toArray().length);
	}

	@Test
	public void testClear() {
		final Ranking<String> ranking = new Ranking<String>(5);
		ranking.insert("a", 1);
		ranking.insert("b", 2);
		ranking.insert("c", 0);
		ranking.clear();
		Assert.assertEquals(0, ranking.toArray().length);
	}

	@Test
	public void testUnderfulRanking() {
		final Ranking<String> ranking = new Ranking<String>(5);
		Assert.assertNull(ranking.insert("a", 1));
		Assert.assertNull(ranking.insert("b", 2));
		Assert.assertNull(ranking.insert("c", 0));
		final String[] expected = { "c", "a", "b" };
		Assert.assertArrayEquals(expected, ranking.toArray());
	}

	@Test
	public void testExactlyRanking() {
		final Ranking<String> ranking = new Ranking<String>(3);
		Assert.assertNull(ranking.insert("a", 1));
		Assert.assertNull(ranking.insert("b", 2));
		Assert.assertNull(ranking.insert("c", 0));
		final String[] expected = { "c", "a", "b" };
		Assert.assertArrayEquals(expected, ranking.toArray());
	}

	@Test
	public void testOverfulRanking() {
		final Ranking<String> ranking = new Ranking<String>(3);
		Assert.assertNull(ranking.insert("a", 1));
		Assert.assertNull(ranking.insert("e", -1));
		Assert.assertNull(ranking.insert("b", 2));
		Assert.assertEquals("b", ranking.insert("c", 0).getContent());
		Assert.assertEquals("g", ranking.insert("g", 5).getContent());
		Assert.assertEquals("a", ranking.insert("f", -2).getContent());
		final String[] expected = { "f", "e", "c" };
		Assert.assertArrayEquals(expected, ranking.toArray());
	}

}
