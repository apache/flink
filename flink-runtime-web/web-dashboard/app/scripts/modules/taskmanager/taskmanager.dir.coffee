#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

angular.module('flinkApp')

.directive 'livechart', () ->
  {
    link: (scope, element, attrs) ->
      getChartType = () ->
        if attrs.key == "cpuLoad"
          "spline"
        else
          "area"

      getYAxisTitle = () ->
        if attrs.key == "cpuLoad"
          "CPU Usage(%)"
        else
          "Memory(MB)"

      getKey1 = () ->
        "memory.total." + attrs.key
      getKey2 = () ->
        "memory.heap." + attrs.key
      getKey3 = () ->
        "memory.non-heap." + attrs.key
      getKey4 = () ->
        "cpuLoad"

      getChartOptions = () -> {
        title: {text: ' '},
        chart: {type: getChartType(), zoomType: 'x'},
        xAxis: {type: 'datetime'},
        yAxis: {
          title: {text: getYAxisTitle() }
          min: 0 if attrs.key == "cpuLoad",
          max: 100 if attrs.key == "cpuLoad"
        },
        series: [
          {name: "Memory: Total", id: getKey1(), data: [], color: "#7cb5ec"},
          {name: "Memory: Heap", id: getKey2(), data: [], color: "#434348"},
          {name: "Memory: Non-Heap", id: getKey3(), data: [], color: "#90ed7d"},
          {name: "CPU Usage", id: getKey4(), data: [], color: "#f7a35c", showInLegend: false}
        ],
        legend: {enabled: false},
        tooltip: {shared: true},
        exporting: {enabled: false},
        credits: {enabled: false}
      }

      if !element.highcharts()?
        element.highcharts(getChartOptions())

      scope.$watch(attrs.data, (value) ->
        updateCharts(value)
      )

      updateCharts = (value) ->
        do(value) ->
          heartbeat = value.timeSinceLastHeartbeat
          chart = element.highcharts()
          if attrs.key == "cpuLoad"
            chart.get(getKey4()).addPoint([
              heartbeat, +((value.metrics.gauges[getKey4()].value * 100).toFixed(2))
            ], true, false)
          else
            divider = 1048576
            chart.get(getKey1()).addPoint([
              heartbeat, +((value.metrics.gauges[getKey1()].value / divider).toFixed(2))
            ], true, false)
            chart.get(getKey2()).addPoint([
              heartbeat, +((value.metrics.gauges[getKey2()].value / divider).toFixed(2))
            ], true, false)
            chart.get(getKey3()).addPoint([
              heartbeat, +((value.metrics.gauges[getKey3()].value / divider).toFixed(2))
            ], true, false)
  }

