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

namespace java org.apache.flink.formats.thrift.generated

enum TestEnum {
  ENUM_TYPE1 = 0,
  ENUM_TYPE2 = 1,
  ENUM_TYPE3 = 2
}

struct TestData {
    1: optional bool testBool
    2: optional byte testByte
    3: optional i16 testShort
    4: optional i32 testInt
    5: optional i64 testLong
    6: optional double testDouble
    7: optional string testString
    8: optional binary testBinary
    9: optional TestEnum testEnum
    10: optional list<string> testListString
    11: optional map<string, string> testMapString
    12: optional set<string> testSetString
    13: optional list<binary> testListBinary
    14: optional map<binary, binary> testMapBinary
    15: optional set<binary> testSetBinary
}
