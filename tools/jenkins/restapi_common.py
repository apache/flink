#!/usr/bin/env python

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
#
# This common file writes the functions of getting qps from flink restful api
#
# Note by AiHua Li:

import urllib2
import json


def execute_get(url):
    result_url = ""
    try:
        result_url = urllib2.urlopen(url, timeout=300).read()
    except urllib2.URLError, e:
        print e.reason
    return result_url


def get_source_node(plan):
    source_nodes = []
    nodes = plan["nodes"] if "nodes" in plan else []
    for node in nodes:
        inputs = node["inputs"] if "inputs" in node else []
        if len(inputs) == 0:
            source_nodes.append(node["id"])
    return source_nodes


def get_avg_qps_by_restful_interface(am_seserver_dddress, job_id):
    url = "http://%s/jobs/%s" % (am_seserver_dddress, job_id)
    result = execute_get(url)
    result = json.loads(result)
    vertices = result["vertices"] if "vertices" in result else ""
    plan = result["plan"] if "plan" in result else ""
    source_nodes = get_source_node(plan)
    for vertice in vertices:
        id = vertice["id"] if "id" in vertice else ""
        if id in source_nodes:
            url="http://%s/jobs/%s/vertices/%s/subtasks/metrics?agg=avg" % (am_seserver_dddress, job_id, id)
            keyresult=execute_get(url)
            keyresult = json.loads(keyresult)
            for key in keyresult:
                metrics_name=key["id"]
                if metrics_name.endswith("numBuffersOutPerSecond"):
                    url="http://%s/jobs/%s/vertices/%s/subtasks/metrics?get=%s" % (am_seserver_dddress, job_id, id,
                                                                                  metrics_name)
                    value_result = execute_get(url)
                    value_result = json.loads(value_result)
                    for value in value_result:
                        tps = value["avg"]
                        if tps > 0:
                            totaltps = totaltps + tps
    return totaltps
