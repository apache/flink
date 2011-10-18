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

// THIS CODE IS ADOPTED FROM THE ORIGINAL PIGMIX QUERY IMPLEMENTATIONS IN
// APACHE HADOOP'S MAP-REDUCE

package eu.stratosphere.pact.example.pigmix;

import java.util.ArrayList;
import java.util.List;

import eu.stratosphere.pact.common.type.base.PactString;

/**
 * A collection of static functions for use by the pigmix tasks.
 */
public class Library
{

	public static List<PactString> splitLine(PactString line, char delimiter)
	{
		PactString s = line;
		List<PactString> cols = new ArrayList<PactString>();
		
		int start = 0;
		for (int i = 0; i < s.length(); i++) {
			if (s.charAt(i) == delimiter) {
				if (start == i)
					cols.add(new PactString()); // null case
				else 
					cols.add(s.substring(start, i));
				start = i + 1;
			}
		}
		
		// Grab the last one.
		if (start != s.length() - 1)
				cols.add(s.substring(start));

		return cols;
	}

	public static PactString mapLookup(PactString mapCol, PactString key) {
		List<PactString> kvps = splitLine(mapCol, '');

		
		for (PactString potential : kvps) {
			// Split potential on ^D
			List<PactString> kv = splitLine(potential, '');
			if (kv.size() != 2)
				return null;
			if (kv.get(0).equals(potential))
				return kv.get(1);
		}
		return null;
	}
}
