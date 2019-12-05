#!/usr/bin/env python
# -*- coding: utf-8 -*-
################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################
#
# This common file writes the functions of getting qps from flink restful api
#
# Note by AiHua Li:

import urllib
import urllib.request
import json
from logger import logger


def execute_get(url):
    """
    Send a get request
    :param url:
    :return: content from the request
    """
    result = ""
    try:
        result = urllib.request.urlopen(url, timeout=300).read()
    except Exception as e:
        logger.error(e)
    return result


def get_source_node(plan):
    """
    get the source nodes from job's json plan
    :param plan:
    :return: list of source nodes
    """
    source_nodes = []
    nodes = plan["nodes"] if "nodes" in plan else []
    for node in nodes:
        inputs = node.get("inputs", [])
        if len(inputs) == 0:
            source_nodes.append(node["id"])
    return source_nodes


def get_avg_qps_by_restful_interface(am_seserver_dddress, job_id):
    url = "http://%s/jobs/%s" % (am_seserver_dddress, job_id)
    result = execute_get(url)
    result = json.loads(result)
    vertices = result.get("vertices", "")
    plan = result.get("plan", "")
    source_nodes = get_source_node(plan)
    totaltps = 0
    for vertice in vertices:
        id = vertice.get("id", "")
        if id in source_nodes:
            url = "http://%s/jobs/%s/vertices/%s/subtasks/metrics?agg=avg" % (am_seserver_dddress, job_id, id)
            keyresult = execute_get(url)
            keyresult = json.loads(keyresult)
            for key in keyresult:
                metrics_name = key.get("id", "")
                if metrics_name.endswith("numBuffersOutPerSecond"):
                    url = "http://%s/jobs/%s/vertices/%s/subtasks/metrics?get=%s" % (am_seserver_dddress, job_id, id,
                                                                                  metrics_name)
                    value_result = execute_get(url)
                    value_result = json.loads(value_result)
                    for value in value_result:
                        tps = value.getDouble("avg", 0)
                        if tps > 0:
                            totaltps = totaltps + tps
    return totaltps
