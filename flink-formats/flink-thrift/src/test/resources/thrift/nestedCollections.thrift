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

namespace java org.apache.flink.formats.thrift.generated.nested

typedef i32 int

struct LLColl {
    1: list<list<string>> listOfLists;
}

struct LMColl {
    1: list<map<int,string>> listOfMaps;
}

struct LSColl {
    1: list<set<string>> listOfSets;
}

struct MLColl {
    1: map<int,list<string>> mapOfLists;
}

struct MMColl {
    1: map<int,map<int,string>> mapOfMaps;
}

struct MSColl {
    1: map<int,set<string>> mapOfSets;
}

struct SLColl {
    1: set<list<string>> setOfLists;
}

struct SMColl {
    1: set<map<int,string>> setOfMaps;
}

struct SSColl {
    1: set<set<string>> setOfSets;
}
