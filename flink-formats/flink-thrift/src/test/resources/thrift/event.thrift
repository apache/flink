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

enum UserEventType {
  START = 0,
  BROWSE = 1,
  CLICK_THROUGH = 2,
  SHUTDOWN = 5,
  UNKOWN = 100
}

struct Diagnostics {
   1: optional string hostname,
   2: optional string ipaddress
}

struct UserAction {
  1: optional i64 timestamp,
  2: optional string url,
  3: optional string referralUrl,
}

struct Event {
  1: i64 timestamp,
  2: i64 userId,
  3: UserEventType eventType,
  4: map<string, string> auxData,
  5: map<string, UserAction> userActions;
  6: optional Diagnostics diagnostics
}

struct EventBinaryData {
  1: i64 timestamp,
  2: i64 userId,
  3: string stringData,
  4: binary binaryData
}
