angular.module('flinkApp', ['ui.router', 'angularMoment']).run(["$rootScope", function($rootScope) {
  $rootScope.sidebarVisible = false;
  return $rootScope.showSidebar = function() {
    $rootScope.sidebarVisible = !$rootScope.sidebarVisible;
    return $rootScope.sidebarClass = 'force-show';
  };
}]).constant('flinkConfig', {
  webServer: 'http://localhost:8080',
  jobServer: 'http://localhost:8081',
  newServer: 'http://localhost:8082',
  refreshInterval: 10000
}).run(["JobsService", "flinkConfig", "$interval", function(JobsService, flinkConfig, $interval) {
  JobsService.listJobs();
  return $interval(function() {
    return JobsService.listJobs();
  }, flinkConfig.refreshInterval);
}]).config(["$stateProvider", "$urlRouterProvider", function($stateProvider, $urlRouterProvider) {
  $stateProvider.state("overview", {
    url: "/overview",
    views: {
      main: {
        templateUrl: "partials/overview.html",
        controller: 'OverviewController'
      }
    }
  }).state("running-jobs", {
    url: "/running-jobs",
    views: {
      main: {
        templateUrl: "partials/jobs/running-jobs.html",
        controller: 'RunningJobsController'
      }
    }
  }).state("completed-jobs", {
    url: "/completed-jobs",
    views: {
      main: {
        templateUrl: "partials/jobs/completed-jobs.html",
        controller: 'CompletedJobsController'
      }
    }
  }).state("single-job", {
    url: "/jobs/{jobid}",
    abstract: true,
    views: {
      main: {
        templateUrl: "partials/jobs/job.html",
        controller: 'SingleJobController'
      }
    }
  }).state("single-job.plan", {
    url: "",
    views: {
      details: {
        templateUrl: "partials/jobs/job.plan.html",
        controller: 'JobPlanController'
      }
    }
  }).state("single-job.plan.node", {
    url: "/{nodeid:int}",
    views: {
      node: {
        templateUrl: "partials/jobs/job.plan.node.html",
        controller: 'JobPlanNodeController'
      }
    }
  }).state("single-job.timeline", {
    url: "/timeline",
    views: {
      details: {
        templateUrl: "partials/jobs/job.timeline.html"
      }
    }
  }).state("single-job.timeline.vertex", {
    url: "/{vertexId}",
    views: {
      vertex: {
        templateUrl: "partials/jobs/job.timeline.vertex.html",
        controller: 'JobTimelineVertexController'
      }
    }
  }).state("single-job.statistics", {
    url: "/statistics",
    views: {
      details: {
        templateUrl: "partials/jobs/job.statistics.html"
      }
    }
  }).state("single-job.exceptions", {
    url: "/exceptions",
    views: {
      details: {
        templateUrl: "partials/jobs/job.exceptions.html"
      }
    }
  });
  return $urlRouterProvider.otherwise("/overview");
}]);

angular.module('flinkApp').directive('bsLabel', ["JobsService", function(JobsService) {
  return {
    transclude: true,
    replace: true,
    scope: {
      getLabelClass: "&",
      status: "@"
    },
    template: "<span title='{{status}}' ng-class='getLabelClass()'><ng-transclude></ng-transclude></span>",
    link: function(scope, element, attrs) {
      return scope.getLabelClass = function() {
        return 'label label-' + JobsService.translateLabelState(attrs.status);
      };
    }
  };
}]).directive('indicatorPrimary', ["JobsService", function(JobsService) {
  return {
    replace: true,
    scope: {
      getLabelClass: "&",
      status: '@'
    },
    template: "<i title='{{status}}' ng-class='getLabelClass()' />",
    link: function(scope, element, attrs) {
      return scope.getLabelClass = function() {
        return 'fa fa-circle indicator indicator-' + JobsService.translateLabelState(attrs.status);
      };
    }
  };
}]).directive('tableProperty', function() {
  return {
    replace: true,
    scope: {
      value: '='
    },
    template: "<td title=\"{{value || 'None'}}\">{{value || 'None'}}</td>"
  };
});

angular.module('flinkApp').filter("amDurationFormatExtended", ["angularMomentConfig", function(angularMomentConfig) {
  var amDurationFormatExtendedFilter;
  amDurationFormatExtendedFilter = function(value, format, durationFormat) {
    if (typeof value === "undefined" || value === null) {
      return "";
    }
    return moment.duration(value, format).format(durationFormat, {
      trim: false
    });
  };
  amDurationFormatExtendedFilter.$stateful = angularMomentConfig.statefulFilters;
  return amDurationFormatExtendedFilter;
}]);

angular.module('flinkApp').controller('RunningJobsController', ["$scope", "$state", "$stateParams", "JobsService", function($scope, $state, $stateParams, JobsService) {
  $scope.jobObserver = function() {
    return $scope.jobs = JobsService.getJobs('running');
  };
  JobsService.registerObserver($scope.jobObserver);
  $scope.$on('$destroy', function() {
    return JobsService.unRegisterObserver($scope.jobObserver);
  });
  return $scope.jobObserver();
}]).controller('CompletedJobsController', ["$scope", "$state", "$stateParams", "JobsService", function($scope, $state, $stateParams, JobsService) {
  $scope.jobObserver = function() {
    return $scope.jobs = JobsService.getJobs('finished');
  };
  JobsService.registerObserver($scope.jobObserver);
  $scope.$on('$destroy', function() {
    return JobsService.unRegisterObserver($scope.jobObserver);
  });
  return $scope.jobObserver();
}]).controller('SingleJobController', ["$scope", "$state", "$stateParams", "JobsService", "$rootScope", function($scope, $state, $stateParams, JobsService, $rootScope) {
  $scope.jobid = $stateParams.jobid;
  $rootScope.job = null;
  JobsService.loadJob($stateParams.jobid).then(function(data) {
    return $rootScope.job = data;
  });
  return $scope.$on('$destroy', function() {
    return $rootScope.job = null;
  });
}]).controller('JobPlanController', ["$scope", "$state", "$stateParams", "JobsService", function($scope, $state, $stateParams, JobsService) {
  return JobsService.loadPlan($stateParams.jobid).then(function(data) {
    return $scope.plan = data;
  });
}]).controller('JobPlanNodeController', ["$scope", "$state", "$stateParams", "JobsService", function($scope, $state, $stateParams, JobsService) {
  $scope.nodeid = $stateParams.nodeid;
  $scope.stateList = JobsService.stateList();
  return JobsService.getNode($scope.nodeid).then(function(data) {
    return $scope.node = data;
  });
}]).controller('JobTimelineVertexController', ["$scope", "$state", "$stateParams", "JobsService", function($scope, $state, $stateParams, JobsService) {
  return JobsService.getVertex($stateParams.jobid, $stateParams.vertexId).then(function(data) {
    return $scope.vertex = data;
  });
}]);

angular.module('flinkApp').directive('vertex', ["$state", function($state) {
  return {
    template: "<svg class='timeline secondary' width='0' height='0'></svg>",
    scope: {
      data: "="
    },
    link: function(scope, elem, attrs) {
      var analyzeTime, containerW, svgEl, zoom;
      zoom = d3.behavior.zoom();
      svgEl = elem.children()[0];
      containerW = elem.width();
      angular.element(svgEl).attr('width', containerW - 16);
      analyzeTime = function(data) {
        var bbox, chart, svg, svgG, testData;
        testData = [];
        angular.forEach(data.groupvertex.groupmembers, function(vertex, i) {
          var vTime;
          vTime = data.verticetimes[vertex.vertexid];
          return testData.push({
            label: vertex.vertexinstancename + " (" + i + ")",
            times: [
              {
                label: "Scheduled",
                color: "#666",
                starting_time: vTime["SCHEDULED"] * 100,
                ending_time: vTime["DEPLOYING"] * 100
              }, {
                label: "Deploying",
                color: "#aaa",
                starting_time: vTime["DEPLOYING"] * 100,
                ending_time: vTime["RUNNING"] * 100
              }, {
                label: "Running",
                color: "#ddd",
                starting_time: vTime["RUNNING"] * 100,
                ending_time: vTime["FINISHED"] * 100
              }
            ]
          });
        });
        chart = d3.timeline().stack().tickFormat({
          format: d3.time.format("%S"),
          tickInterval: 1,
          tickSize: 1
        }).labelFormat(function(label) {
          return label;
        }).margin({
          left: 100,
          right: 0,
          top: 0,
          bottom: 0
        });
        svg = d3.select(svgEl).datum(testData).call(chart).call(zoom);
        svgG = svg.select("g");
        zoom.on("zoom", function() {
          var ev;
          ev = d3.event;
          svgG.selectAll('rect').attr("transform", "translate(" + ev.translate[0] + ",0) scale(" + ev.scale + ",1)");
          return svgG.selectAll('text').attr("transform", "translate(" + ev.translate[0] + ",0) scale(" + ev.scale + ",1)");
        });
        bbox = svgG[0][0].getBBox();
        return svg.attr('height', bbox.height + 30);
      };
      analyzeTime(scope.data);
    }
  };
}]).directive('timeline', ["$state", function($state) {
  return {
    template: "<svg class='timeline' width='0' height='0'></svg>",
    scope: {
      job: "="
    },
    link: function(scope, elem, attrs) {
      var analyzeTime, containerW, svgEl, zoom;
      zoom = d3.behavior.zoom();
      svgEl = elem.children()[0];
      containerW = elem.width();
      angular.element(svgEl).attr('width', containerW - 16);
      analyzeTime = function(data) {
        var bbox, chart, svg, svgG, testData;
        testData = [];
        angular.forEach(data.oldV.groupvertices, function(vertex) {
          var vTime;
          vTime = data.oldV.groupverticetimes[vertex.groupvertexid];
          return testData.push({
            times: [
              {
                label: vertex.groupvertexname,
                color: "#3fb6d8",
                starting_time: vTime["STARTED"],
                ending_time: vTime["ENDED"],
                link: vertex.groupvertexid
              }
            ]
          });
        });
        chart = d3.timeline().stack().click(function(d, i, datum) {
          return $state.go("single-job.timeline.vertex", {
            jobid: data.jid,
            vertexId: d.link
          });
        }).tickFormat({
          format: d3.time.format("%S"),
          tickInterval: 1,
          tickSize: 1
        }).margin({
          left: 0,
          right: 0,
          top: 0,
          bottom: 0
        });
        svg = d3.select(svgEl).datum(testData).call(chart).call(zoom);
        svgG = svg.select("g");
        zoom.on("zoom", function() {
          var ev;
          ev = d3.event;
          svgG.selectAll('rect').attr("transform", "translate(" + ev.translate[0] + ",0) scale(" + ev.scale + ",1)");
          return svgG.selectAll('text').attr("transform", "translate(" + ev.translate[0] + ",0) scale(" + ev.scale + ",1)");
        });
        bbox = svgG[0][0].getBBox();
        return svg.attr('height', bbox.height + 30);
      };
      scope.$watch(attrs.job, function(data) {
        if (data) {
          return analyzeTime(data);
        }
      });
    }
  };
}]).directive('jobPlan', ["$timeout", function($timeout) {
  return {
    template: "<svg class='graph' width='500' height='400'><g /></svg> <svg class='tmp' width='1' height='1'><g /></svg> <div class='btn-group zoom-buttons'> <a class='btn btn-default zoom-in' ng-click='zoomIn()'><i class='fa fa-plus' /></a> <a class='btn btn-default zoom-out' ng-click='zoomOut()'><i class='fa fa-minus' /></a> </div>",
    scope: {
      plan: '='
    },
    link: function(scope, elem, attrs) {
      var containerW, createEdge, createLabelEdge, createLabelNode, createNode, d3mainSvg, d3mainSvgG, d3tmpSvg, drawGraph, extendLabelNodeForIteration, getNodeType, isSpecialIterationNode, jobid, loadJsonToDagre, mainG, mainSvgElement, mainTmpElement, mainZoom, searchForNode, shortenString, subgraphs;
      mainZoom = d3.behavior.zoom();
      subgraphs = [];
      jobid = attrs.jobid;
      mainSvgElement = elem.children()[0];
      mainG = elem.children().children()[0];
      mainTmpElement = elem.children()[1];
      d3mainSvg = d3.select(mainSvgElement);
      d3mainSvgG = d3.select(mainG);
      d3tmpSvg = d3.select(mainTmpElement);
      containerW = elem.width();
      angular.element(elem.children()[0]).width(containerW);
      scope.zoomIn = function() {
        var translate, v1, v2;
        if (mainZoom.scale() < 2.99) {
          translate = mainZoom.translate();
          v1 = translate[0] * (mainZoom.scale() + 0.1 / (mainZoom.scale()));
          v2 = translate[1] * (mainZoom.scale() + 0.1 / (mainZoom.scale()));
          mainZoom.scale(mainZoom.scale() + 0.1);
          mainZoom.translate([v1, v2]);
          return d3mainSvgG.attr("transform", "translate(" + v1 + "," + v2 + ") scale(" + mainZoom.scale() + ")");
        }
      };
      scope.zoomOut = function() {
        var translate, v1, v2;
        if (mainZoom.scale() > 0.31) {
          mainZoom.scale(mainZoom.scale() - 0.1);
          translate = mainZoom.translate();
          v1 = translate[0] * (mainZoom.scale() - 0.1 / (mainZoom.scale()));
          v2 = translate[1] * (mainZoom.scale() - 0.1 / (mainZoom.scale()));
          mainZoom.translate([v1, v2]);
          return d3mainSvgG.attr("transform", "translate(" + v1 + "," + v2 + ") scale(" + mainZoom.scale() + ")");
        }
      };
      createLabelEdge = function(el) {
        var labelValue;
        labelValue = "";
        if ((el.ship_strategy != null) || (el.local_strategy != null)) {
          labelValue += "<div class='edge-label'>";
          if (el.ship_strategy != null) {
            labelValue += el.ship_strategy;
          }
          if (el.temp_mode !== undefined) {
            labelValue += " (" + el.temp_mode + ")";
          }
          if (el.local_strategy !== undefined) {
            labelValue += ",<br>" + el.local_strategy;
          }
          labelValue += "</div>";
        }
        return labelValue;
      };
      isSpecialIterationNode = function(info) {
        return info === "partialSolution" || info === "nextPartialSolution" || info === "workset" || info === "nextWorkset" || info === "solutionSet" || info === "solutionDelta";
      };
      getNodeType = function(el, info) {
        if (info === "mirror") {
          return 'node-mirror';
        } else if (isSpecialIterationNode(info)) {
          return 'node-iteration';
        } else {
          if (el.pact === "Data Source") {
            return 'node-source';
          } else if (el.pact === "Data Sink") {
            return 'node-sink';
          } else {
            return 'node-normal';
          }
        }
      };
      createLabelNode = function(el, info, maxW, maxH) {
        var labelValue, stepName;
        labelValue = "<a href='#/jobs/" + jobid + "/" + el.id + "' class='node-label " + getNodeType(el, info) + "'>";
        if (info === "mirror") {
          labelValue += "<h3 class='node-name'>Mirror of " + el.pact + "</h3>";
        } else {
          labelValue += "<h3 class='node-name'>" + el.pact + "</h3>";
        }
        if (el.contents === "") {
          labelValue += "";
        } else {
          stepName = el.contents;
          stepName = shortenString(stepName);
          labelValue += "<h4 class='step-name'>" + stepName + "</h4>";
        }
        if (el.step_function != null) {
          labelValue += extendLabelNodeForIteration(el.id, maxW, maxH);
        } else {
          if (isSpecialIterationNode(info)) {
            labelValue += "<h5>" + info + " Node</h5>";
          }
          if (el.parallelism !== "") {
            labelValue += "<h5>Parallelism: " + el.parallelism + "</h5>";
          }
          if (el.driver_strategy !== undefined) {
            labelValue += "<h5>Driver Strategy: " + shortenString(el.driver_strategy) + "</h5";
          }
        }
        labelValue += "</a>";
        return labelValue;
      };
      extendLabelNodeForIteration = function(id, maxW, maxH) {
        var labelValue, svgID;
        svgID = "svg-" + id;
        labelValue = "<svg class='" + svgID + "' width=" + maxW + " height=" + maxH + "><g /></svg>";
        return labelValue;
      };
      shortenString = function(s) {
        var sbr;
        if (s.charAt(0) === "<") {
          s = s.replace("<", "&lt;");
          s = s.replace(">", "&gt;");
        }
        sbr = "";
        while (s.length > 30) {
          sbr = sbr + s.substring(0, 30) + "<br>";
          s = s.substring(30, s.length);
        }
        sbr = sbr + s;
        return sbr;
      };
      createNode = function(g, data, el, isParent, maxW, maxH) {
        if (isParent == null) {
          isParent = false;
        }
        if (el.id === data.partial_solution) {
          return g.setNode(el.id, {
            label: createLabelNode(el, "partialSolution", maxW, maxH),
            labelType: 'html',
            "class": getNodeType(el, "partialSolution")
          });
        } else if (el.id === data.next_partial_solution) {
          return g.setNode(el.id, {
            label: createLabelNode(el, "nextPartialSolution", maxW, maxH),
            labelType: 'html',
            "class": getNodeType(el, "nextPartialSolution")
          });
        } else if (el.id === data.workset) {
          return g.setNode(el.id, {
            label: createLabelNode(el, "workset", maxW, maxH),
            labelType: 'html',
            "class": getNodeType(el, "workset")
          });
        } else if (el.id === data.next_workset) {
          return g.setNode(el.id, {
            label: createLabelNode(el, "nextWorkset", maxW, maxH),
            labelType: 'html',
            "class": getNodeType(el, "nextWorkset")
          });
        } else if (el.id === data.solution_set) {
          return g.setNode(el.id, {
            label: createLabelNode(el, "solutionSet", maxW, maxH),
            labelType: 'html',
            "class": getNodeType(el, "solutionSet")
          });
        } else if (el.id === data.solution_delta) {
          return g.setNode(el.id, {
            label: createLabelNode(el, "solutionDelta", maxW, maxH),
            labelType: 'html',
            "class": getNodeType(el, "solutionDelta")
          });
        } else {
          return g.setNode(el.id, {
            label: createLabelNode(el, "", maxW, maxH),
            labelType: 'html',
            "class": getNodeType(el, "")
          });
        }
      };
      createEdge = function(g, data, el, existingNodes, pred) {
        var missingNode;
        if (existingNodes.indexOf(pred.id) !== -1) {
          return g.setEdge(pred.id, el.id, {
            label: createLabelEdge(pred),
            labelType: 'html',
            arrowhead: 'normal'
          });
        } else {
          missingNode = searchForNode(data, pred.id);
          if (!(!missingNode || missingNode.alreadyAdded === true)) {
            missingNode.alreadyAdded = true;
            g.setNode(missingNode.id, {
              label: createLabelNode(missingNode, "mirror"),
              labelType: 'html',
              "class": getNodeType(missingNode, 'mirror')
            });
            return g.setEdge(missingNode.id, el.id, {
              label: createLabelEdge(missingNode),
              labelType: 'html'
            });
          }
        }
      };
      loadJsonToDagre = function(g, data) {
        var el, existingNodes, isParent, k, l, len, len1, maxH, maxW, pred, r, ref, sg, toIterate;
        existingNodes = [];
        if (data.nodes != null) {
          toIterate = data.nodes;
        } else {
          toIterate = data.step_function;
          isParent = true;
        }
        for (k = 0, len = toIterate.length; k < len; k++) {
          el = toIterate[k];
          maxW = 0;
          maxH = 0;
          if (el.step_function) {
            sg = new dagreD3.graphlib.Graph({
              multigraph: true,
              compound: true
            }).setGraph({
              nodesep: 20,
              edgesep: 0,
              ranksep: 20,
              rankdir: "LR",
              marginx: 10,
              marginy: 10
            });
            subgraphs[el.id] = sg;
            loadJsonToDagre(sg, el);
            r = new dagreD3.render();
            d3tmpSvg.select('g').call(r, sg);
            maxW = sg.graph().width;
            maxH = sg.graph().height;
            angular.element(mainTmpElement).empty();
          }
          createNode(g, data, el, isParent, maxW, maxH);
          existingNodes.push(el.id);
          if (el.predecessors != null) {
            ref = el.predecessors;
            for (l = 0, len1 = ref.length; l < len1; l++) {
              pred = ref[l];
              createEdge(g, data, el, existingNodes, pred);
            }
          }
        }
        return g;
      };
      searchForNode = function(data, nodeID) {
        var el, i, j;
        for (i in data.nodes) {
          el = data.nodes[i];
          if (el.id === nodeID) {
            return el;
          }
          if (el.step_function != null) {
            for (j in el.step_function) {
              if (el.step_function[j].id === nodeID) {
                return el.step_function[j];
              }
            }
          }
        }
      };
      drawGraph = function(data) {
        var g, i, newScale, renderer, sg, xCenterOffset, yCenterOffset;
        g = new dagreD3.graphlib.Graph({
          multigraph: true,
          compound: true
        }).setGraph({
          nodesep: 70,
          edgesep: 0,
          ranksep: 50,
          rankdir: "LR",
          marginx: 40,
          marginy: 40
        });
        loadJsonToDagre(g, data);
        renderer = new dagreD3.render();
        d3mainSvgG.call(renderer, g);
        for (i in subgraphs) {
          sg = subgraphs[i];
          d3mainSvg.select('svg.svg-' + i + ' g').call(renderer, sg);
        }
        newScale = 0.5;
        xCenterOffset = Math.floor((angular.element(mainSvgElement).width() - g.graph().width * newScale) / 2);
        yCenterOffset = Math.floor((angular.element(mainSvgElement).height() - g.graph().height * newScale) / 2);
        mainZoom.scale(newScale).translate([xCenterOffset, yCenterOffset]);
        d3mainSvgG.attr("transform", "translate(" + xCenterOffset + ", " + yCenterOffset + ") scale(" + mainZoom.scale() + ")");
        mainZoom.on("zoom", function() {
          var ev;
          ev = d3.event;
          return d3mainSvgG.attr("transform", "translate(" + ev.translate + ") scale(" + ev.scale + ")");
        });
        return mainZoom(d3mainSvg);
      };
      scope.$watch(attrs.plan, function(newPlan) {
        if (newPlan) {
          return drawGraph(newPlan);
        }
      });
    }
  };
}]);

angular.module('flinkApp').service('JobsService', ["$http", "flinkConfig", "$log", "amMoment", "$q", "$timeout", function($http, flinkConfig, $log, amMoment, $q, $timeout) {
  var currentJob, currentPlan, deferreds, jobObservers, jobs, notifyObservers;
  currentJob = null;
  currentPlan = null;
  deferreds = {};
  jobs = {
    running: [],
    finished: [],
    cancelled: [],
    failed: []
  };
  jobObservers = [];
  notifyObservers = function() {
    return angular.forEach(jobObservers, function(callback) {
      return callback();
    });
  };
  this.registerObserver = function(callback) {
    return jobObservers.push(callback);
  };
  this.unRegisterObserver = function(callback) {
    var index;
    index = jobObservers.indexOf(callback);
    return jobObservers.splice(index, 1);
  };
  this.stateList = function() {
    return ['SCHEDULED', 'DEPLOYING', 'RUNNING', 'FINISHED', 'FAILED', 'CANCELING', 'CANCELED'];
  };
  this.translateLabelState = function(state) {
    switch (state.toLowerCase()) {
      case 'finished':
        return 'success';
      case 'failed':
        return 'danger';
      case 'scheduled':
        return 'default';
      case 'deploying':
        return 'info';
      case 'running':
        return 'primary';
      case 'canceling':
        return 'warning';
      case 'pending':
        return 'info';
      case 'total':
        return 'black';
      default:
        return 'default';
    }
  };
  this.listJobs = function() {
    var deferred;
    deferred = $q.defer();
    $http.get(flinkConfig.newServer + "/jobs").success(function(data, status, headers, config) {
      angular.forEach(data, function(list, listKey) {
        switch (listKey) {
          case 'jobs-running':
            jobs.running = list;
            break;
          case 'jobs-finished':
            jobs.finished = list;
            break;
          case 'jobs-cancelled':
            jobs.cancelled = list;
            break;
          case 'jobs-failed':
            jobs.failed = list;
        }
        return angular.forEach(list, function(jobid, index) {
          return $http.get(flinkConfig.newServer + "/jobs/" + jobid).success(function(details) {
            return list[index] = details;
          });
        });
      });
      deferred.resolve(jobs);
      return notifyObservers();
    });
    return deferred.promise;
  };
  this.getJobs = function(type) {
    return jobs[type];
  };
  this.getAllJobs = function() {
    return jobs;
  };
  this.loadJob = function(jobid) {
    currentJob = null;
    deferreds.job = $q.defer();
    $http.get(flinkConfig.newServer + "/jobs/" + jobid).success(function(data, status, headers, config) {
      data.time = Date.now();
      return $http.get(flinkConfig.newServer + "/jobs/" + jobid + "/vertices").success(function(vertices) {
        data = angular.extend(data, vertices);
        return $http.get(flinkConfig.jobServer + "/jobsInfo?get=job&job=" + jobid).success(function(oldVertices) {
          data.oldV = oldVertices[0];
          currentJob = data;
          return deferreds.job.resolve(data);
        });
      });
    });
    return deferreds.job.promise;
  };
  this.loadPlan = function(jobid) {
    currentPlan = null;
    deferreds.plan = $q.defer();
    $http.get(flinkConfig.newServer + "/jobs/" + jobid + "/plan").success(function(data) {
      currentPlan = data;
      return deferreds.plan.resolve(data);
    });
    return deferreds.plan.promise;
  };
  this.getNode = function(nodeid) {
    var deferred, seekNode;
    seekNode = function(nodeid, data) {
      var i, len, node, sub;
      nodeid = parseInt(nodeid);
      for (i = 0, len = data.length; i < len; i++) {
        node = data[i];
        if (node.id === nodeid) {
          return node;
        }
        if (node.step_function) {
          sub = seekNode(nodeid, node.step_function);
        }
        if (sub) {
          return sub;
        }
      }
      return null;
    };
    deferred = $q.defer();
    $q.all([deferreds.plan.promise, deferreds.job.promise]).then((function(_this) {
      return function(data) {
        var foundNode;
        foundNode = seekNode(nodeid, currentPlan.nodes);
        return _this.getVertex(currentJob.jid, currentJob.oldV.groupvertices[0].groupvertexid).then(function(vertex) {
          foundNode.vertex = vertex;
          return deferred.resolve(foundNode);
        });
      };
    })(this));
    return deferred.promise;
  };
  this.getVertex = function(jobId, vertexId) {
    var deferred;
    deferred = $q.defer();
    $http.get(flinkConfig.jobServer + "/jobsInfo?get=groupvertex&job=" + jobId + "&groupvertex=" + vertexId).success(function(data) {
      return deferred.resolve(data);
    });
    return deferred.promise;
  };
  return this;
}]);

angular.module('flinkApp').controller('OverviewController', ["$scope", "OverviewService", "JobsService", function($scope, OverviewService, JobsService) {
  $scope.jobObserver = function() {
    $scope.runningJobs = JobsService.getJobs('running');
    return $scope.finishedJobs = JobsService.getJobs('finished');
  };
  JobsService.registerObserver($scope.jobObserver);
  $scope.$on('$destroy', function() {
    return JobsService.unRegisterObserver($scope.jobObserver);
  });
  return $scope.jobObserver();
}]);

angular.module('flinkApp').service('OverviewService', ["$http", "flinkConfig", "$log", function($http, flinkConfig, $log) {
  var serverStatus;
  serverStatus = {};
  this.loadServerStatus = function() {
    $http.get(flinkConfig.jobServer + "/monitor/status").success(function(data, status, headers, config) {
      return $log(data);
    }).error(function(data, status, headers, config) {});
    return serverStatus;
  };
  return this;
}]);

//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJzb3VyY2VzIjpbImluZGV4LmNvZmZlZSIsImluZGV4LmpzIiwiY29tbW9uL2RpcmVjdGl2ZXMuY29mZmVlIiwiY29tbW9uL2RpcmVjdGl2ZXMuanMiLCJjb21tb24vZmlsdGVycy5jb2ZmZWUiLCJjb21tb24vZmlsdGVycy5qcyIsIm1vZHVsZXMvam9icy9qb2JzLmN0cmwuY29mZmVlIiwibW9kdWxlcy9qb2JzL2pvYnMuY3RybC5qcyIsIm1vZHVsZXMvam9icy9qb2JzLmRpci5jb2ZmZWUiLCJtb2R1bGVzL2pvYnMvam9icy5kaXIuanMiLCJtb2R1bGVzL2pvYnMvam9icy5zdmMuY29mZmVlIiwibW9kdWxlcy9qb2JzL2pvYnMuc3ZjLmpzIiwibW9kdWxlcy9vdmVydmlldy9vdmVydmlldy5jdHJsLmNvZmZlZSIsIm1vZHVsZXMvb3ZlcnZpZXcvb3ZlcnZpZXcuY3RybC5qcyIsIm1vZHVsZXMvb3ZlcnZpZXcvb3ZlcnZpZXcuc3ZjLmNvZmZlZSIsIm1vZHVsZXMvb3ZlcnZpZXcvb3ZlcnZpZXcuc3ZjLmpzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiJBQUFBLFFBQVEsT0FBTyxZQUFZLENBQUMsYUFBYSxrQkFJeEMsbUJBQUksU0FBQyxZQUFEO0VBQ0gsV0FBVyxpQkFBaUI7RUNINUIsT0RJQSxXQUFXLGNBQWMsV0FBQTtJQUN2QixXQUFXLGlCQUFpQixDQUFDLFdBQVc7SUNIeEMsT0RJQSxXQUFXLGVBQWU7O0lBSTdCLFNBQVMsZUFBZTtFQUN2QixXQUFXO0VBQ1gsV0FBVztFQUNYLFdBQVc7RUFJWCxpQkFBaUI7R0FLbEIsZ0RBQUksU0FBQyxhQUFhLGFBQWEsV0FBM0I7RUFDSCxZQUFZO0VDWlosT0RjQSxVQUFVLFdBQUE7SUNiUixPRGNBLFlBQVk7S0FDWixZQUFZO0lBS2YsZ0RBQU8sU0FBQyxnQkFBZ0Isb0JBQWpCO0VBQ04sZUFBZSxNQUFNLFlBQ25CO0lBQUEsS0FBSztJQUNMLE9BQ0U7TUFBQSxNQUNFO1FBQUEsYUFBYTtRQUNiLFlBQVk7OztLQUVqQixNQUFN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QU8sVUFBUjtJQUNYLElBQUE7SUFBQSxXQUFXLEdBQUc7SUFFZCxNQUFNLElBQUksWUFBWSxZQUFZLG1DQUFtQyxRQUFRLGtCQUFrQixVQUM5RixRQUFRLFNBQUMsTUFBRDtNQ0hQLE9ESUEsU0FBUyxRQUFROztJQ0ZuQixPRElBLFNBQVM7O0VDRlgsT0RJQTs7QUNGRjtBQ3hKQSxRQUFRLE9BQU8sWUFFZCxXQUFXLG1FQUFzQixTQUFDLFFBQVEsaUJBQWlCLGFBQTFCO0VBQ2hDLE9BQU8sY0FBYyxXQUFBO0lBQ25CLE9BQU8sY0FBYyxZQUFZLFFBQVE7SUNEekMsT0RFQSxPQUFPLGVBQWUsWUFBWSxRQUFROztFQUU1QyxZQUFZLGlCQUFpQixPQUFPO0VBQ3BDLE9BQU8sSUFBSSxZQUFZLFdBQUE7SUNEckIsT0RFQSxZQUFZLG1CQUFtQixPQUFPOztFQ0F4QyxPREVBLE9BQU87O0FDQVQ7QUNYQSxRQUFRLE9BQU8sWUFFZCxRQUFRLG9EQUFtQixTQUFDLE9BQU8sYUFBYSxNQUFyQjtFQUMxQixJQUFBO0VBQUEsZUFBZTtFQUVmLEtBQUMsbUJBQW1CLFdBQUE7SUFDbEIsTUFBTSxJQUFJLFlBQVksWUFBWSxtQkFDakMsUUFBUSxTQUFDLE1BQU0sUUFBUSxTQUFTLFFBQXhCO01DRlAsT0RHQSxLQUFLO09BRU4sTUFBTSxTQUFDLE1BQU0sUUFBUSxTQUFTLFFBQXhCO0lDSFAsT0RNQTs7RUNKRixPRE1BOztBQ0pGIiwiZmlsZSI6ImluZGV4LmpzIiwic291cmNlc0NvbnRlbnQiOlsiYW5ndWxhci5tb2R1bGUoJ2ZsaW5rQXBwJywgWyd1aS5yb3V0ZXInLCAnYW5ndWxhck1vbWVudCddKVxuXG4jIC0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tXG5cbi5ydW4gKCRyb290U2NvcGUpIC0+XG4gICRyb290U2NvcGUuc2lkZWJhclZpc2libGUgPSBmYWxzZVxuICAkcm9vdFNjb3BlLnNob3dTaWRlYmFyID0gLT5cbiAgICAkcm9vdFNjb3BlLnNpZGViYXJWaXNpYmxlID0gISRyb290U2NvcGUuc2lkZWJhclZpc2libGVcbiAgICAkcm9vdFNjb3BlLnNpZGViYXJDbGFzcyA9ICdmb3JjZS1zaG93J1xuXG4jIC0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tXG5cbi5jb25zdGFudCAnZmxpbmtDb25maWcnLCB7XG4gIHdlYlNlcnZlcjogJ2h0dHA6Ly9sb2NhbGhvc3Q6ODA4MCdcbiAgam9iU2VydmVyOiAnaHR0cDovL2xvY2FsaG9zdDo4MDgxJ1xuICBuZXdTZXJ2ZXI6ICdodHRwOi8vbG9jYWxob3N0OjgwODInXG4jICB3ZWJTZXJ2ZXI6ICdodHRwOi8vbG9jYWxob3N0OjMwMDAvd2ViLXNlcnZlcidcbiMgIGpvYlNlcnZlcjogJ2h0dHA6Ly9sb2NhbGhvc3Q6MzAwMC9qb2Itc2VydmVyJ1xuIyAgbmV3U2VydmVyOiAnaHR0cDovL2xvY2FsaG9zdDozMDAwL25ldy1zZXJ2ZXInXG4gIHJlZnJlc2hJbnRlcnZhbDogMTAwMDBcbn1cblxuIyAtLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLVxuXG4ucnVuIChKb2JzU2VydmljZSwgZmxpbmtDb25maWcsICRpbnRlcnZhbCkgLT5cbiAgSm9ic1NlcnZpY2UubGlzdEpvYnMoKVxuXG4gICRpbnRlcnZhbCAtPlxuICAgIEpvYnNTZXJ2aWNlLmxpc3RKb2JzKClcbiAgLCBmbGlua0NvbmZpZy5yZWZyZXNoSW50ZXJ2YWxcblxuXG4jIC0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tXG5cbi5jb25maWcgKCRzdGF0ZVByb3ZpZGVyLCAkdXJsUm91dGVyUHJvdmlkZXIpIC0+XG4gICRzdGF0ZVByb3ZpZGVyLnN0YXRlIFwib3ZlcnZpZXdcIixcbiAgICB1cmw6IFwiL292ZXJ2aWV3XCJcbiAgICB2aWV3czpcbiAgICAgIG1haW46XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL292ZXJ2aWV3Lmh0bWxcIlxuICAgICAgICBjb250cm9sbGVyOiAnT3ZlcnZpZXdDb250cm9sbGVyJ1xuXG4gIC5zdGF0ZSBcInJ1bm5pbmctam9ic1wiLFxuICAgIHVybDogXCIvcnVubmluZy1qb2JzXCJcbiAgICB2aWV3czpcbiAgICAgIG1haW46XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYnMvcnVubmluZy1qb2JzLmh0bWxcIlxuICAgICAgICBjb250cm9sbGVyOiAnUnVubmluZ0pvYnNDb250cm9sbGVyJ1xuICBcbiAgLnN0YXRlIFwiY29tcGxldGVkLWpvYnNcIixcbiAgICB1cmw6IFwiL2NvbXBsZXRlZC1qb2JzXCJcbiAgICB2aWV3czpcbiAgICAgIG1haW46XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYnMvY29tcGxldGVkLWpvYnMuaHRtbFwiXG4gICAgICAgIGNvbnRyb2xsZXI6ICdDb21wbGV0ZWRKb2JzQ29udHJvbGxlcidcblxuICAuc3RhdGUgXCJzaW5nbGUtam9iXCIsXG4gICAgdXJsOiBcIi9qb2JzL3tqb2JpZH1cIlxuICAgIGFic3RyYWN0OiB0cnVlXG4gICAgdmlld3M6XG4gICAgICBtYWluOlxuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JzL2pvYi5odG1sXCJcbiAgICAgICAgY29udHJvbGxlcjogJ1NpbmdsZUpvYkNvbnRyb2xsZXInXG5cbiAgLnN0YXRlIFwic2luZ2xlLWpvYi5wbGFuXCIsXG4gICAgdXJsOiBcIlwiXG4gICAgdmlld3M6XG4gICAgICBkZXRhaWxzOlxuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JzL2pvYi5wbGFuLmh0bWxcIlxuICAgICAgICBjb250cm9sbGVyOiAnSm9iUGxhbkNvbnRyb2xsZXInXG5cbiAgLnN0YXRlIFwic2luZ2xlLWpvYi5wbGFuLm5vZGVcIixcbiAgICB1cmw6IFwiL3tub2RlaWQ6aW50fVwiXG4gICAgdmlld3M6XG4gICAgICBub2RlOlxuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JzL2pvYi5wbGFuLm5vZGUuaHRtbFwiXG4gICAgICAgIGNvbnRyb2xsZXI6ICdKb2JQbGFuTm9kZUNvbnRyb2xsZXInXG5cbiAgLnN0YXRlIFwic2luZ2xlLWpvYi50aW1lbGluZVwiLFxuICAgIHVybDogXCIvdGltZWxpbmVcIlxuICAgIHZpZXdzOlxuICAgICAgZGV0YWlsczpcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvam9icy9qb2IudGltZWxpbmUuaHRtbFwiXG5cbiAgLnN0YXRlIFwic2luZ2xlLWpvYi50aW1lbGluZS52ZXJ0ZXhcIixcbiAgICB1cmw6IFwiL3t2ZXJ0ZXhJZH1cIlxuICAgIHZpZXdzOlxuICAgICAgdmVydGV4OlxuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JzL2pvYi50aW1lbGluZS52ZXJ0ZXguaHRtbFwiXG4gICAgICAgIGNvbnRyb2xsZXI6ICdKb2JUaW1lbGluZVZlcnRleENvbnRyb2xsZXInXG5cbiAgLnN0YXRlIFwic2luZ2xlLWpvYi5zdGF0aXN0aWNzXCIsXG4gICAgdXJsOiBcIi9zdGF0aXN0aWNzXCJcbiAgICB2aWV3czpcbiAgICAgIGRldGFpbHM6XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYnMvam9iLnN0YXRpc3RpY3MuaHRtbFwiXG5cbiAgLnN0YXRlIFwic2luZ2xlLWpvYi5leGNlcHRpb25zXCIsXG4gICAgdXJsOiBcIi9leGNlcHRpb25zXCJcbiAgICB2aWV3czpcbiAgICAgIGRldGFpbHM6XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYnMvam9iLmV4Y2VwdGlvbnMuaHRtbFwiXG5cbiAgJHVybFJvdXRlclByb3ZpZGVyLm90aGVyd2lzZSBcIi9vdmVydmlld1wiXG4iLCJhbmd1bGFyLm1vZHVsZSgnZmxpbmtBcHAnLCBbJ3VpLnJvdXRlcicsICdhbmd1bGFyTW9tZW50J10pLnJ1bihmdW5jdGlvbigkcm9vdFNjb3BlKSB7XG4gICRyb290U2NvcGUuc2lkZWJhclZpc2libGUgPSBmYWxzZTtcbiAgcmV0dXJuICRyb290U2NvcGUuc2hvd1NpZGViYXIgPSBmdW5jdGlvbigpIHtcbiAgICAkcm9vdFNjb3BlLnNpZGViYXJWaXNpYmxlID0gISRyb290U2NvcGUuc2lkZWJhclZpc2libGU7XG4gICAgcmV0dXJuICRyb290U2NvcGUuc2lkZWJhckNsYXNzID0gJ2ZvcmNlLXNob3cnO1xuICB9O1xufSkuY29uc3RhbnQoJ2ZsaW5rQ29uZmlnJywge1xuICB3ZWJTZXJ2ZXI6ICdodHRwOi8vbG9jYWxob3N0OjgwODAnLFxuICBqb2JTZXJ2ZXI6ICdodHRwOi8vbG9jYWxob3N0OjgwODEnLFxuICBuZXdTZXJ2ZXI6ICdodHRwOi8vbG9jYWxob3N0OjgwODInLFxuICByZWZyZXNoSW50ZXJ2YWw6IDEwMDAwXG59KS5ydW4oZnVuY3Rpb24oSm9ic1NlcnZpY2UsIGZsaW5rQ29uZmlnLCAkaW50ZXJ2YWwpIHtcbiAgSm9ic1NlcnZpY2UubGlzdEpvYnMoKTtcbiAgcmV0dXJuICRpbnRlcnZhbChmdW5jdGlvbigpIHtcbiAgICByZXR1cm4gSm9ic1NlcnZpY2UubGlzdEpvYnMoKTtcbiAgfSwgZmxpbmtDb25maWcucmVmcmVzaEludGVydmFsKTtcbn0pLmNvbmZpZyhmdW5jdGlvbigkc3RhdGVQcm92aWRlciwgJHVybFJvdXRlclByb3ZpZGVyKSB7XG4gICRzdGF0ZVByb3ZpZGVyLnN0YXRlKFwib3ZlcnZpZXdcIiwge1xuICAgIHVybDogXCIvb3ZlcnZpZXdcIixcbiAgICB2aWV3czoge1xuICAgICAgbWFpbjoge1xuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9vdmVydmlldy5odG1sXCIsXG4gICAgICAgIGNvbnRyb2xsZXI6ICdPdmVydmlld0NvbnRyb2xsZXInXG4gICAgICB9XG4gICAgfVxuICB9KS5zdGF0ZShcInJ1bm5pbmctam9ic1wiLCB7XG4gICAgdXJsOiBcIi9ydW5uaW5nLWpvYnNcIixcbiAgICB2aWV3czoge1xuICAgICAgbWFpbjoge1xuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JzL3J1bm5pbmctam9icy5odG1sXCIsXG4gICAgICAgIGNvbnRyb2xsZXI6ICdSdW5uaW5nSm9ic0NvbnRyb2xsZXInXG4gICAgICB9XG4gICAgfVxuICB9KS5zdGF0ZShcImNvbXBsZXRlZC1qb2JzXCIsIHtcbiAgICB1cmw6IFwiL2NvbXBsZXRlZC1qb2JzXCIsXG4gICAgdmlld3M6IHtcbiAgICAgIG1haW46IHtcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvam9icy9jb21wbGV0ZWQtam9icy5odG1sXCIsXG4gICAgICAgIGNvbnRyb2xsZXI6ICdDb21wbGV0ZWRKb2JzQ29udHJvbGxlcidcbiAgICAgIH1cbiAgICB9XG4gIH0pLnN0YXRlKFwic2luZ2xlLWpvYlwiLCB7XG4gICAgdXJsOiBcIi9qb2JzL3tqb2JpZH1cIixcbiAgICBhYnN0cmFjdDogdHJ1ZSxcbiAgICB2aWV3czoge1xuICAgICAgbWFpbjoge1xuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JzL2pvYi5odG1sXCIsXG4gICAgICAgIGNvbnRyb2xsZXI6ICdTaW5nbGVKb2JDb250cm9sbGVyJ1xuICAgICAgfVxuICAgIH1cbiAgfSkuc3RhdGUoXCJzaW5nbGUtam9iLnBsYW5cIiwge1xuICAgIHVybDogXCJcIixcbiAgICB2aWV3czoge1xuICAgICAgZGV0YWlsczoge1xuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JzL2pvYi5wbGFuLmh0bWxcIixcbiAgICAgICAgY29udHJvbGxlcjogJ0pvYlBsYW5Db250cm9sbGVyJ1xuICAgICAgfVxuICAgIH1cbiAgfSkuc3RhdGUoXCJzaW5nbGUtam9iLnBsYW4ubm9kZVwiLCB7XG4gICAgdXJsOiBcIi97bm9kZWlkOmludH1cIixcbiAgICB2aWV3czoge1xuICAgICAgbm9kZToge1xuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JzL2pvYi5wbGFuLm5vZGUuaHRtbFwiLFxuICAgICAgICBjb250cm9sbGVyOiAnSm9iUGxhbk5vZGVDb250cm9sbGVyJ1xuICAgICAgfVxuICAgIH1cbiAgfSkuc3RhdGUoXCJzaW5nbGUtam9iLnRpbWVsaW5lXCIsIHtcbiAgICB1cmw6IFwiL3RpbWVsaW5lXCIsXG4gICAgdmlld3M6IHtcbiAgICAgIGRldGFpbHM6IHtcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvam9icy9qb2IudGltZWxpbmUuaHRtbFwiXG4gICAgICB9XG4gICAgfVxuICB9KS5zdGF0ZShcInNpbmdsZS1qb2IudGltZWxpbmUudmVydGV4XCIsIHtcbiAgICB1cmw6IFwiL3t2ZXJ0ZXhJZH1cIixcbiAgICB2aWV3czoge1xuICAgICAgdmVydGV4OiB7XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYnMvam9iLnRpbWVsaW5lLnZlcnRleC5odG1sXCIsXG4gICAgICAgIGNvbnRyb2xsZXI6ICdKb2JUaW1lbGluZVZlcnRleENvbnRyb2xsZXInXG4gICAgICB9XG4gICAgfVxuICB9KS5zdGF0ZShcInNpbmdsZS1qb2Iuc3RhdGlzdGljc1wiLCB7XG4gICAgdXJsOiBcIi9zdGF0aXN0aWNzXCIsXG4gICAgdmlld3M6IHtcbiAgICAgIGRldGFpbHM6IHtcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvam9icy9qb2Iuc3RhdGlzdGljcy5odG1sXCJcbiAgICAgIH1cbiAgICB9XG4gIH0pLnN0YXRlKFwic2luZ2xlLWpvYi5leGNlcHRpb25zXCIsIHtcbiAgICB1cmw6IFwiL2V4Y2VwdGlvbnNcIixcbiAgICB2aWV3czoge1xuICAgICAgZGV0YWlsczoge1xuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JzL2pvYi5leGNlcHRpb25zLmh0bWxcIlxuICAgICAgfVxuICAgIH1cbiAgfSk7XG4gIHJldHVybiAkdXJsUm91dGVyUHJvdmlkZXIub3RoZXJ3aXNlKFwiL292ZXJ2aWV3XCIpO1xufSk7XG4iLCJhbmd1bGFyLm1vZHVsZSgnZmxpbmtBcHAnKVxuXG4jIC0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS1cblxuLmRpcmVjdGl2ZSAnYnNMYWJlbCcsIChKb2JzU2VydmljZSkgLT5cbiAgdHJhbnNjbHVkZTogdHJ1ZVxuICByZXBsYWNlOiB0cnVlXG4gIHNjb3BlOiBcbiAgICBnZXRMYWJlbENsYXNzOiBcIiZcIlxuICAgIHN0YXR1czogXCJAXCJcblxuICB0ZW1wbGF0ZTogXCI8c3BhbiB0aXRsZT0ne3tzdGF0dXN9fScgbmctY2xhc3M9J2dldExhYmVsQ2xhc3MoKSc+PG5nLXRyYW5zY2x1ZGU+PC9uZy10cmFuc2NsdWRlPjwvc3Bhbj5cIlxuICBcbiAgbGluazogKHNjb3BlLCBlbGVtZW50LCBhdHRycykgLT5cbiAgICBzY29wZS5nZXRMYWJlbENsYXNzID0gLT5cbiAgICAgICdsYWJlbCBsYWJlbC0nICsgSm9ic1NlcnZpY2UudHJhbnNsYXRlTGFiZWxTdGF0ZShhdHRycy5zdGF0dXMpXG5cbiMgLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLVxuXG4uZGlyZWN0aXZlICdpbmRpY2F0b3JQcmltYXJ5JywgKEpvYnNTZXJ2aWNlKSAtPlxuICByZXBsYWNlOiB0cnVlXG4gIHNjb3BlOiBcbiAgICBnZXRMYWJlbENsYXNzOiBcIiZcIlxuICAgIHN0YXR1czogJ0AnXG5cbiAgdGVtcGxhdGU6IFwiPGkgdGl0bGU9J3t7c3RhdHVzfX0nIG5nLWNsYXNzPSdnZXRMYWJlbENsYXNzKCknIC8+XCJcbiAgXG4gIGxpbms6IChzY29wZSwgZWxlbWVudCwgYXR0cnMpIC0+XG4gICAgc2NvcGUuZ2V0TGFiZWxDbGFzcyA9IC0+XG4gICAgICAnZmEgZmEtY2lyY2xlIGluZGljYXRvciBpbmRpY2F0b3ItJyArIEpvYnNTZXJ2aWNlLnRyYW5zbGF0ZUxhYmVsU3RhdGUoYXR0cnMuc3RhdHVzKVxuXG4jIC0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS1cblxuLmRpcmVjdGl2ZSAndGFibGVQcm9wZXJ0eScsIC0+XG4gIHJlcGxhY2U6IHRydWVcbiAgc2NvcGU6XG4gICAgdmFsdWU6ICc9J1xuXG4gIHRlbXBsYXRlOiBcIjx0ZCB0aXRsZT1cXFwie3t2YWx1ZSB8fCAnTm9uZSd9fVxcXCI+e3t2YWx1ZSB8fCAnTm9uZSd9fTwvdGQ+XCJcbiIsImFuZ3VsYXIubW9kdWxlKCdmbGlua0FwcCcpLmRpcmVjdGl2ZSgnYnNMYWJlbCcsIGZ1bmN0aW9uKEpvYnNTZXJ2aWNlKSB7XG4gIHJldHVybiB7XG4gICAgdHJhbnNjbHVkZTogdHJ1ZSxcbiAgICByZXBsYWNlOiB0cnVlLFxuICAgIHNjb3BlOiB7XG4gICAgICBnZXRMYWJlbENsYXNzOiBcIiZcIixcbiAgICAgIHN0YXR1czogXCJAXCJcbiAgICB9LFxuICAgIHRlbXBsYXRlOiBcIjxzcGFuIHRpdGxlPSd7e3N0YXR1c319JyBuZy1jbGFzcz0nZ2V0TGFiZWxDbGFzcygpJz48bmctdHJhbnNjbHVkZT48L25nLXRyYW5zY2x1ZGU+PC9zcGFuPlwiLFxuICAgIGxpbms6IGZ1bmN0aW9uKHNjb3BlLCBlbGVtZW50LCBhdHRycykge1xuICAgICAgcmV0dXJuIHNjb3BlLmdldExhYmVsQ2xhc3MgPSBmdW5jdGlvbigpIHtcbiAgICAgICAgcmV0dXJuICdsYWJlbCBsYWJlbC0nICsgSm9ic1NlcnZpY2UudHJhbnNsYXRlTGFiZWxTdGF0ZShhdHRycy5zdGF0dXMpO1xuICAgICAgfTtcbiAgICB9XG4gIH07XG59KS5kaXJlY3RpdmUoJ2luZGljYXRvclByaW1hcnknLCBmdW5jdGlvbihKb2JzU2VydmljZSkge1xuICByZXR1cm4ge1xuICAgIHJlcGxhY2U6IHRydWUsXG4gICAgc2NvcGU6IHtcbiAgICAgIGdldExhYmVsQ2xhc3M6IFwiJlwiLFxuICAgICAgc3RhdHVzOiAnQCdcbiAgICB9LFxuICAgIHRlbXBsYXRlOiBcIjxpIHRpdGxlPSd7e3N0YXR1c319JyBuZy1jbGFzcz0nZ2V0TGFiZWxDbGFzcygpJyAvPlwiLFxuICAgIGxpbms6IGZ1bmN0aW9uKHNjb3BlLCBlbGVtZW50LCBhdHRycykge1xuICAgICAgcmV0dXJuIHNjb3BlLmdldExhYmVsQ2xhc3MgPSBmdW5jdGlvbigpIHtcbiAgICAgICAgcmV0dXJuICdmYSBmYS1jaXJjbGUgaW5kaWNhdG9yIGluZGljYXRvci0nICsgSm9ic1NlcnZpY2UudHJhbnNsYXRlTGFiZWxTdGF0ZShhdHRycy5zdGF0dXMpO1xuICAgICAgfTtcbiAgICB9XG4gIH07XG59KS5kaXJlY3RpdmUoJ3RhYmxlUHJvcGVydHknLCBmdW5jdGlvbigpIHtcbiAgcmV0dXJuIHtcbiAgICByZXBsYWNlOiB0cnVlLFxuICAgIHNjb3BlOiB7XG4gICAgICB2YWx1ZTogJz0nXG4gICAgfSxcbiAgICB0ZW1wbGF0ZTogXCI8dGQgdGl0bGU9XFxcInt7dmFsdWUgfHwgJ05vbmUnfX1cXFwiPnt7dmFsdWUgfHwgJ05vbmUnfX08L3RkPlwiXG4gIH07XG59KTtcbiIsImFuZ3VsYXIubW9kdWxlKCdmbGlua0FwcCcpXG5cbi5maWx0ZXIgXCJhbUR1cmF0aW9uRm9ybWF0RXh0ZW5kZWRcIiwgKGFuZ3VsYXJNb21lbnRDb25maWcpIC0+XG4gIGFtRHVyYXRpb25Gb3JtYXRFeHRlbmRlZEZpbHRlciA9ICh2YWx1ZSwgZm9ybWF0LCBkdXJhdGlvbkZvcm1hdCkgLT5cbiAgICByZXR1cm4gXCJcIiAgaWYgdHlwZW9mIHZhbHVlIGlzIFwidW5kZWZpbmVkXCIgb3IgdmFsdWUgaXMgbnVsbFxuXG4gICAgbW9tZW50LmR1cmF0aW9uKHZhbHVlLCBmb3JtYXQpLmZvcm1hdChkdXJhdGlvbkZvcm1hdCwgeyB0cmltOiBmYWxzZSB9KVxuXG4gIGFtRHVyYXRpb25Gb3JtYXRFeHRlbmRlZEZpbHRlci4kc3RhdGVmdWwgPSBhbmd1bGFyTW9tZW50Q29uZmlnLnN0YXRlZnVsRmlsdGVyc1xuXG4gIGFtRHVyYXRpb25Gb3JtYXRFeHRlbmRlZEZpbHRlciIsImFuZ3VsYXIubW9kdWxlKCdmbGlua0FwcCcpLmZpbHRlcihcImFtRHVyYXRpb25Gb3JtYXRFeHRlbmRlZFwiLCBmdW5jdGlvbihhbmd1bGFyTW9tZW50Q29uZmlnKSB7XG4gIHZhciBhbUR1cmF0aW9uRm9ybWF0RXh0ZW5kZWRGaWx0ZXI7XG4gIGFtRHVyYXRpb25Gb3JtYXRFeHRlbmRlZEZpbHRlciA9IGZ1bmN0aW9uKHZhbHVlLCBmb3JtYXQsIGR1cmF0aW9uRm9ybWF0KSB7XG4gICAgaWYgKHR5cGVvZiB2YWx1ZSA9PT0gXCJ1bmRlZmluZWRcIiB8fCB2YWx1ZSA9PT0gbnVsbCkge1xuICAgICAgcmV0dXJuIFwiXCI7XG4gICAgfVxuICAgIHJldHVybiBtb21lbnQuZHVyYXRpb24odmFsdWUsIGZvcm1hdCkuZm9ybWF0KGR1cmF0aW9uRm9ybWF0LCB7XG4gICAgICB0cmltOiBmYWxzZVxuICAgIH0pO1xuICB9O1xuICBhbUR1cmF0aW9uRm9ybWF0RXh0ZW5kZWRGaWx0ZXIuJHN0YXRlZnVsID0gYW5ndWxhck1vbWVudENvbmZpZy5zdGF0ZWZ1bEZpbHRlcnM7XG4gIHJldHVybiBhbUR1cmF0aW9uRm9ybWF0RXh0ZW5kZWRGaWx0ZXI7XG59KTtcbiIsImFuZ3VsYXIubW9kdWxlKCdmbGlua0FwcCcpXG5cbi5jb250cm9sbGVyICdSdW5uaW5nSm9ic0NvbnRyb2xsZXInLCAoJHNjb3BlLCAkc3RhdGUsICRzdGF0ZVBhcmFtcywgSm9ic1NlcnZpY2UpIC0+XG4gICRzY29wZS5qb2JPYnNlcnZlciA9IC0+XG4gICAgJHNjb3BlLmpvYnMgPSBKb2JzU2VydmljZS5nZXRKb2JzKCdydW5uaW5nJylcblxuICBKb2JzU2VydmljZS5yZWdpc3Rlck9ic2VydmVyKCRzY29wZS5qb2JPYnNlcnZlcilcbiAgJHNjb3BlLiRvbiAnJGRlc3Ryb3knLCAtPlxuICAgIEpvYnNTZXJ2aWNlLnVuUmVnaXN0ZXJPYnNlcnZlcigkc2NvcGUuam9iT2JzZXJ2ZXIpXG5cbiAgJHNjb3BlLmpvYk9ic2VydmVyKClcblxuIyAtLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLVxuXG4uY29udHJvbGxlciAnQ29tcGxldGVkSm9ic0NvbnRyb2xsZXInLCAoJHNjb3BlLCAkc3RhdGUsICRzdGF0ZVBhcmFtcywgSm9ic1NlcnZpY2UpIC0+XG4gICRzY29wZS5qb2JPYnNlcnZlciA9IC0+XG4gICAgJHNjb3BlLmpvYnMgPSBKb2JzU2VydmljZS5nZXRKb2JzKCdmaW5pc2hlZCcpXG5cbiAgSm9ic1NlcnZpY2UucmVnaXN0ZXJPYnNlcnZlcigkc2NvcGUuam9iT2JzZXJ2ZXIpXG4gICRzY29wZS4kb24gJyRkZXN0cm95JywgLT5cbiAgICBKb2JzU2VydmljZS51blJlZ2lzdGVyT2JzZXJ2ZXIoJHNjb3BlLmpvYk9ic2VydmVyKVxuXG4gICRzY29wZS5qb2JPYnNlcnZlcigpXG5cbiMgLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS1cblxuLmNvbnRyb2xsZXIgJ1NpbmdsZUpvYkNvbnRyb2xsZXInLCAoJHNjb3BlLCAkc3RhdGUsICRzdGF0ZVBhcmFtcywgSm9ic1NlcnZpY2UsICRyb290U2NvcGUpIC0+XG4gICRzY29wZS5qb2JpZCA9ICRzdGF0ZVBhcmFtcy5qb2JpZFxuICAkcm9vdFNjb3BlLmpvYiA9IG51bGxcblxuICBKb2JzU2VydmljZS5sb2FkSm9iKCRzdGF0ZVBhcmFtcy5qb2JpZCkudGhlbiAoZGF0YSkgLT5cbiAgICAkcm9vdFNjb3BlLmpvYiA9IGRhdGFcblxuICAkc2NvcGUuJG9uICckZGVzdHJveScsIC0+XG4gICAgJHJvb3RTY29wZS5qb2IgPSBudWxsXG5cbiMgLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS1cblxuLmNvbnRyb2xsZXIgJ0pvYlBsYW5Db250cm9sbGVyJywgKCRzY29wZSwgJHN0YXRlLCAkc3RhdGVQYXJhbXMsIEpvYnNTZXJ2aWNlKSAtPlxuICBKb2JzU2VydmljZS5sb2FkUGxhbigkc3RhdGVQYXJhbXMuam9iaWQpLnRoZW4gKGRhdGEpIC0+XG4gICAgJHNjb3BlLnBsYW4gPSBkYXRhXG5cbiMgLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS1cblxuLmNvbnRyb2xsZXIgJ0pvYlBsYW5Ob2RlQ29udHJvbGxlcicsICgkc2NvcGUsICRzdGF0ZSwgJHN0YXRlUGFyYW1zLCBKb2JzU2VydmljZSkgLT5cbiAgJHNjb3BlLm5vZGVpZCA9ICRzdGF0ZVBhcmFtcy5ub2RlaWRcbiAgJHNjb3BlLnN0YXRlTGlzdCA9IEpvYnNTZXJ2aWNlLnN0YXRlTGlzdCgpXG5cbiAgSm9ic1NlcnZpY2UuZ2V0Tm9kZSgkc2NvcGUubm9kZWlkKS50aGVuIChkYXRhKSAtPlxuICAgICRzY29wZS5ub2RlID0gZGF0YVxuXG4jIC0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tXG5cbi5jb250cm9sbGVyICdKb2JUaW1lbGluZVZlcnRleENvbnRyb2xsZXInLCAoJHNjb3BlLCAkc3RhdGUsICRzdGF0ZVBhcmFtcywgSm9ic1NlcnZpY2UpIC0+XG4gIEpvYnNTZXJ2aWNlLmdldFZlcnRleCgkc3RhdGVQYXJhbXMuam9iaWQsICRzdGF0ZVBhcmFtcy52ZXJ0ZXhJZCkudGhlbiAoZGF0YSkgLT5cbiAgICAkc2NvcGUudmVydGV4ID0gZGF0YVxuIiwiYW5ndWxhci5tb2R1bGUoJ2ZsaW5rQXBwJykuY29udHJvbGxlcignUnVubmluZ0pvYnNDb250cm9sbGVyJywgZnVuY3Rpb24oJHNjb3BlLCAkc3RhdGUsICRzdGF0ZVBhcmFtcywgSm9ic1NlcnZpY2UpIHtcbiAgJHNjb3BlLmpvYk9ic2VydmVyID0gZnVuY3Rpb24oKSB7XG4gICAgcmV0dXJuICRzY29wZS5qb2JzID0gSm9ic1NlcnZpY2UuZ2V0Sm9icygncnVubmluZycpO1xuICB9O1xuICBKb2JzU2VydmljZS5yZWdpc3Rlck9ic2VydmVyKCRzY29wZS5qb2JPYnNlcnZlcik7XG4gICRzY29wZS4kb24oJyRkZXN0cm95JywgZnVuY3Rpb24oKSB7XG4gICAgcmV0dXJuIEpvYnNTZXJ2aWNlLnVuUmVnaXN0ZXJPYnNlcnZlcigkc2NvcGUuam9iT2JzZXJ2ZXIpO1xuICB9KTtcbiAgcmV0dXJuICRzY29wZS5qb2JPYnNlcnZlcigpO1xufSkuY29udHJvbGxlcignQ29tcGxldGVkSm9ic0NvbnRyb2xsZXInLCBmdW5jdGlvbigkc2NvcGUsICRzdGF0ZSwgJHN0YXRlUGFyYW1zLCBKb2JzU2VydmljZSkge1xuICAkc2NvcGUuam9iT2JzZXJ2ZXIgPSBmdW5jdGlvbigpIHtcbiAgICByZXR1cm4gJHNjb3BlLmpvYnMgPSBKb2JzU2VydmljZS5nZXRKb2JzKCdmaW5pc2hlZCcpO1xuICB9O1xuICBKb2JzU2VydmljZS5yZWdpc3Rlck9ic2VydmVyKCRzY29wZS5qb2JPYnNlcnZlcik7XG4gICRzY29wZS4kb24oJyRkZXN0cm95JywgZnVuY3Rpb24oKSB7XG4gICAgcmV0dXJuIEpvYnNTZXJ2aWNlLnVuUmVnaXN0ZXJPYnNlcnZlcigkc2NvcGUuam9iT2JzZXJ2ZXIpO1xuICB9KTtcbiAgcmV0dXJuICRzY29wZS5qb2JPYnNlcnZlcigpO1xufSkuY29udHJvbGxlcignU2luZ2xlSm9iQ29udHJvbGxlcicsIGZ1bmN0aW9uKCRzY29wZSwgJHN0YXRlLCAkc3RhdGVQYXJhbXMsIEpvYnNTZXJ2aWNlLCAkcm9vdFNjb3BlKSB7XG4gICRzY29wZS5qb2JpZCA9ICRzdGF0ZVBhcmFtcy5qb2JpZDtcbiAgJHJvb3RTY29wZS5qb2IgPSBudWxsO1xuICBKb2JzU2VydmljZS5sb2FkSm9iKCRzdGF0ZVBhcmFtcy5qb2JpZCkudGhlbihmdW5jdGlvbihkYXRhKSB7XG4gICAgcmV0dXJuICRyb290U2NvcGUuam9iID0gZGF0YTtcbiAgfSk7XG4gIHJldHVybiAkc2NvcGUuJG9uKCckZGVzdHJveScsIGZ1bmN0aW9uKCkge1xuICAgIHJldHVybiAkcm9vdFNjb3BlLmpvYiA9IG51bGw7XG4gIH0pO1xufSkuY29udHJvbGxlcignSm9iUGxhbkNvbnRyb2xsZXInLCBmdW5jdGlvbigkc2NvcGUsICRzdGF0ZSwgJHN0YXRlUGFyYW1zLCBKb2JzU2VydmljZSkge1xuICByZXR1cm4gSm9ic1NlcnZpY2UubG9hZFBsYW4oJHN0YXRlUGFyYW1zLmpvYmlkKS50aGVuKGZ1bmN0aW9uKGRhdGEpIHtcbiAgICByZXR1cm4gJHNjb3BlLnBsYW4gPSBkYXRhO1xuICB9KTtcbn0pLmNvbnRyb2xsZXIoJ0pvYlBsYW5Ob2RlQ29udHJvbGxlcicsIGZ1bmN0aW9uKCRzY29wZSwgJHN0YXRlLCAkc3RhdGVQYXJhbXMsIEpvYnNTZXJ2aWNlKSB7XG4gICRzY29wZS5ub2RlaWQgPSAkc3RhdGVQYXJhbXMubm9kZWlkO1xuICAkc2NvcGUuc3RhdGVMaXN0ID0gSm9ic1NlcnZpY2Uuc3RhdGVMaXN0KCk7XG4gIHJldHVybiBKb2JzU2VydmljZS5nZXROb2RlKCRzY29wZS5ub2RlaWQpLnRoZW4oZnVuY3Rpb24oZGF0YSkge1xuICAgIHJldHVybiAkc2NvcGUubm9kZSA9IGRhdGE7XG4gIH0pO1xufSkuY29udHJvbGxlcignSm9iVGltZWxpbmVWZXJ0ZXhDb250cm9sbGVyJywgZnVuY3Rpb24oJHNjb3BlLCAkc3RhdGUsICRzdGF0ZVBhcmFtcywgSm9ic1NlcnZpY2UpIHtcbiAgcmV0dXJuIEpvYnNTZXJ2aWNlLmdldFZlcnRleCgkc3RhdGVQYXJhbXMuam9iaWQsICRzdGF0ZVBhcmFtcy52ZXJ0ZXhJZCkudGhlbihmdW5jdGlvbihkYXRhKSB7XG4gICAgcmV0dXJuICRzY29wZS52ZXJ0ZXggPSBkYXRhO1xuICB9KTtcbn0pO1xuIiwiYW5ndWxhci5tb2R1bGUoJ2ZsaW5rQXBwJylcblxuIyAtLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tXG5cbi5kaXJlY3RpdmUgJ3ZlcnRleCcsICgkc3RhdGUpIC0+XG4gIHRlbXBsYXRlOiBcIjxzdmcgY2xhc3M9J3RpbWVsaW5lIHNlY29uZGFyeScgd2lkdGg9JzAnIGhlaWdodD0nMCc+PC9zdmc+XCJcblxuICBzY29wZTpcbiAgICBkYXRhOiBcIj1cIlxuXG4gIGxpbms6IChzY29wZSwgZWxlbSwgYXR0cnMpIC0+XG4gICAgem9vbSA9IGQzLmJlaGF2aW9yLnpvb20oKVxuICAgIHN2Z0VsID0gZWxlbS5jaGlsZHJlbigpWzBdXG5cbiAgICBjb250YWluZXJXID0gZWxlbS53aWR0aCgpXG4gICAgYW5ndWxhci5lbGVtZW50KHN2Z0VsKS5hdHRyKCd3aWR0aCcsIGNvbnRhaW5lclcgLSAxNilcblxuICAgIGFuYWx5emVUaW1lID0gKGRhdGEpIC0+XG4gICAgICB0ZXN0RGF0YSA9IFtdXG5cbiAgICAgIGFuZ3VsYXIuZm9yRWFjaCBkYXRhLmdyb3VwdmVydGV4Lmdyb3VwbWVtYmVycywgKHZlcnRleCwgaSkgLT5cbiAgICAgICAgdlRpbWUgPSBkYXRhLnZlcnRpY2V0aW1lc1t2ZXJ0ZXgudmVydGV4aWRdXG5cbiAgICAgICAgdGVzdERhdGEucHVzaCB7XG4gICAgICAgICAgbGFiZWw6IFwiI3t2ZXJ0ZXgudmVydGV4aW5zdGFuY2VuYW1lfSAoI3tpfSlcIlxuICAgICAgICAgIHRpbWVzOiBbXG4gICAgICAgICAgICB7XG4gICAgICAgICAgICAgIGxhYmVsOiBcIlNjaGVkdWxlZFwiXG4gICAgICAgICAgICAgIGNvbG9yOiBcIiM2NjZcIlxuICAgICAgICAgICAgICBzdGFydGluZ190aW1lOiB2VGltZVtcIlNDSEVEVUxFRFwiXSAqIDEwMFxuICAgICAgICAgICAgICBlbmRpbmdfdGltZTogdlRpbWVbXCJERVBMT1lJTkdcIl0gKiAxMDBcbiAgICAgICAgICAgIH1cbiAgICAgICAgICAgIHtcbiAgICAgICAgICAgICAgbGFiZWw6IFwiRGVwbG95aW5nXCJcbiAgICAgICAgICAgICAgY29sb3I6IFwiI2FhYVwiXG4gICAgICAgICAgICAgIHN0YXJ0aW5nX3RpbWU6IHZUaW1lW1wiREVQTE9ZSU5HXCJdICogMTAwXG4gICAgICAgICAgICAgIGVuZGluZ190aW1lOiB2VGltZVtcIlJVTk5JTkdcIl0gKiAxMDBcbiAgICAgICAgICAgIH1cbiAgICAgICAgICAgIHtcbiAgICAgICAgICAgICAgbGFiZWw6IFwiUnVubmluZ1wiXG4gICAgICAgICAgICAgIGNvbG9yOiBcIiNkZGRcIlxuICAgICAgICAgICAgICBzdGFydGluZ190aW1lOiB2VGltZVtcIlJVTk5JTkdcIl0gKiAxMDBcbiAgICAgICAgICAgICAgZW5kaW5nX3RpbWU6IHZUaW1lW1wiRklOSVNIRURcIl0gKiAxMDBcbiAgICAgICAgICAgIH1cbiAgICAgICAgICBdXG4gICAgICAgIH1cblxuICAgICAgY2hhcnQgPSBkMy50aW1lbGluZSgpLnN0YWNrKCkudGlja0Zvcm1hdCh7XG4gICAgICAgIGZvcm1hdDogZDMudGltZS5mb3JtYXQoXCIlU1wiKSxcbiAgICAgICAgIyB0aWNrVGltZTogZDMudGltZS5taWxsaXNlY29uZHMsXG4gICAgICAgIHRpY2tJbnRlcnZhbDogMSxcbiAgICAgICAgdGlja1NpemU6IDFcbiAgICAgIH0pLmxhYmVsRm9ybWF0KChsYWJlbCkgLT5cbiAgICAgICAgbGFiZWxcbiAgICAgICkubWFyZ2luKHsgbGVmdDogMTAwLCByaWdodDogMCwgdG9wOiAwLCBib3R0b206IDAgfSlcblxuICAgICAgc3ZnID0gZDMuc2VsZWN0KHN2Z0VsKVxuICAgICAgLmRhdHVtKHRlc3REYXRhKVxuICAgICAgLmNhbGwoY2hhcnQpXG4gICAgICAuY2FsbCh6b29tKVxuXG4gICAgICBzdmdHID0gc3ZnLnNlbGVjdChcImdcIilcblxuICAgICAgem9vbS5vbihcInpvb21cIiwgLT5cbiAgICAgICAgZXYgPSBkMy5ldmVudFxuXG4gICAgICAgIHN2Z0cuc2VsZWN0QWxsKCdyZWN0JykuYXR0cihcInRyYW5zZm9ybVwiLCBcInRyYW5zbGF0ZShcIiArIGV2LnRyYW5zbGF0ZVswXSArIFwiLDApIHNjYWxlKFwiICsgZXYuc2NhbGUgKyBcIiwxKVwiKVxuICAgICAgICBzdmdHLnNlbGVjdEFsbCgndGV4dCcpLmF0dHIoXCJ0cmFuc2Zvcm1cIiwgXCJ0cmFuc2xhdGUoXCIgKyBldi50cmFuc2xhdGVbMF0gKyBcIiwwKSBzY2FsZShcIiArIGV2LnNjYWxlICsgXCIsMSlcIilcbiAgICAgIClcblxuICAgICAgYmJveCA9IHN2Z0dbMF1bMF0uZ2V0QkJveCgpXG4gICAgICBzdmcuYXR0cignaGVpZ2h0JywgYmJveC5oZWlnaHQgKyAzMClcblxuICAgIGFuYWx5emVUaW1lKHNjb3BlLmRhdGEpXG5cbiAgICByZXR1cm5cblxuIyAtLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tXG5cbi5kaXJlY3RpdmUgJ3RpbWVsaW5lJywgKCRzdGF0ZSkgLT5cbiAgdGVtcGxhdGU6IFwiPHN2ZyBjbGFzcz0ndGltZWxpbmUnIHdpZHRoPScwJyBoZWlnaHQ9JzAnPjwvc3ZnPlwiXG5cbiAgc2NvcGU6XG4gICAgam9iOiBcIj1cIlxuXG4gIGxpbms6IChzY29wZSwgZWxlbSwgYXR0cnMpIC0+XG4gICAgem9vbSA9IGQzLmJlaGF2aW9yLnpvb20oKVxuICAgIHN2Z0VsID0gZWxlbS5jaGlsZHJlbigpWzBdXG5cbiAgICBjb250YWluZXJXID0gZWxlbS53aWR0aCgpXG4gICAgYW5ndWxhci5lbGVtZW50KHN2Z0VsKS5hdHRyKCd3aWR0aCcsIGNvbnRhaW5lclcgLSAxNilcblxuICAgIGFuYWx5emVUaW1lID0gKGRhdGEpIC0+XG4gICAgICB0ZXN0RGF0YSA9IFtdXG5cbiAgICAgIGFuZ3VsYXIuZm9yRWFjaCBkYXRhLm9sZFYuZ3JvdXB2ZXJ0aWNlcywgKHZlcnRleCkgLT5cbiAgICAgICAgdlRpbWUgPSBkYXRhLm9sZFYuZ3JvdXB2ZXJ0aWNldGltZXNbdmVydGV4Lmdyb3VwdmVydGV4aWRdXG5cbiAgICAgICAgIyBjb25zb2xlLmxvZyB2VGltZSwgdmVydGV4Lmdyb3VwdmVydGV4aWRcblxuICAgICAgICB0ZXN0RGF0YS5wdXNoIFxuICAgICAgICAgIHRpbWVzOiBbXG4gICAgICAgICAgICBsYWJlbDogdmVydGV4Lmdyb3VwdmVydGV4bmFtZVxuICAgICAgICAgICAgY29sb3I6IFwiIzNmYjZkOFwiXG4gICAgICAgICAgICBzdGFydGluZ190aW1lOiB2VGltZVtcIlNUQVJURURcIl1cbiAgICAgICAgICAgIGVuZGluZ190aW1lOiB2VGltZVtcIkVOREVEXCJdXG4gICAgICAgICAgICBsaW5rOiB2ZXJ0ZXguZ3JvdXB2ZXJ0ZXhpZFxuICAgICAgICAgIF1cblxuICAgICAgY2hhcnQgPSBkMy50aW1lbGluZSgpLnN0YWNrKCkuY2xpY2soKGQsIGksIGRhdHVtKSAtPlxuICAgICAgICAkc3RhdGUuZ28gXCJzaW5nbGUtam9iLnRpbWVsaW5lLnZlcnRleFwiLCB7IGpvYmlkOiBkYXRhLmppZCwgdmVydGV4SWQ6IGQubGluayB9XG5cbiAgICAgICkudGlja0Zvcm1hdCh7XG4gICAgICAgIGZvcm1hdDogZDMudGltZS5mb3JtYXQoXCIlU1wiKVxuICAgICAgICAjIHRpY2tUaW1lOiBkMy50aW1lLm1pbGxpc2Vjb25kc1xuICAgICAgICB0aWNrSW50ZXJ2YWw6IDFcbiAgICAgICAgdGlja1NpemU6IDFcbiAgICAgIH0pLm1hcmdpbih7IGxlZnQ6IDAsIHJpZ2h0OiAwLCB0b3A6IDAsIGJvdHRvbTogMCB9KVxuXG4gICAgICBzdmcgPSBkMy5zZWxlY3Qoc3ZnRWwpXG4gICAgICAuZGF0dW0odGVzdERhdGEpXG4gICAgICAuY2FsbChjaGFydClcbiAgICAgIC5jYWxsKHpvb20pXG5cbiAgICAgIHN2Z0cgPSBzdmcuc2VsZWN0KFwiZ1wiKVxuXG4gICAgICB6b29tLm9uKFwiem9vbVwiLCAtPlxuICAgICAgICBldiA9IGQzLmV2ZW50XG5cbiAgICAgICAgc3ZnRy5zZWxlY3RBbGwoJ3JlY3QnKS5hdHRyKFwidHJhbnNmb3JtXCIsIFwidHJhbnNsYXRlKFwiICsgZXYudHJhbnNsYXRlWzBdICsgXCIsMCkgc2NhbGUoXCIgKyBldi5zY2FsZSArIFwiLDEpXCIpXG4gICAgICAgIHN2Z0cuc2VsZWN0QWxsKCd0ZXh0JykuYXR0cihcInRyYW5zZm9ybVwiLCBcInRyYW5zbGF0ZShcIiArIGV2LnRyYW5zbGF0ZVswXSArIFwiLDApIHNjYWxlKFwiICsgZXYuc2NhbGUgKyBcIiwxKVwiKVxuICAgICAgKVxuXG4gICAgICBiYm94ID0gc3ZnR1swXVswXS5nZXRCQm94KClcbiAgICAgIHN2Zy5hdHRyKCdoZWlnaHQnLCBiYm94LmhlaWdodCArIDMwKVxuXG4gICAgc2NvcGUuJHdhdGNoIGF0dHJzLmpvYiwgKGRhdGEpIC0+XG4gICAgICBhbmFseXplVGltZShkYXRhKSBpZiBkYXRhXG5cbiAgICByZXR1cm5cblxuIyAtLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tXG5cbi5kaXJlY3RpdmUgJ2pvYlBsYW4nLCAoJHRpbWVvdXQpIC0+XG4gIHRlbXBsYXRlOiBcIlxuICAgIDxzdmcgY2xhc3M9J2dyYXBoJyB3aWR0aD0nNTAwJyBoZWlnaHQ9JzQwMCc+PGcgLz48L3N2Zz5cbiAgICA8c3ZnIGNsYXNzPSd0bXAnIHdpZHRoPScxJyBoZWlnaHQ9JzEnPjxnIC8+PC9zdmc+XG4gICAgPGRpdiBjbGFzcz0nYnRuLWdyb3VwIHpvb20tYnV0dG9ucyc+XG4gICAgICA8YSBjbGFzcz0nYnRuIGJ0bi1kZWZhdWx0IHpvb20taW4nIG5nLWNsaWNrPSd6b29tSW4oKSc+PGkgY2xhc3M9J2ZhIGZhLXBsdXMnIC8+PC9hPlxuICAgICAgPGEgY2xhc3M9J2J0biBidG4tZGVmYXVsdCB6b29tLW91dCcgbmctY2xpY2s9J3pvb21PdXQoKSc+PGkgY2xhc3M9J2ZhIGZhLW1pbnVzJyAvPjwvYT5cbiAgICA8L2Rpdj5cIlxuXG4gIHNjb3BlOlxuICAgIHBsYW46ICc9J1xuXG4gIGxpbms6IChzY29wZSwgZWxlbSwgYXR0cnMpIC0+XG4gICAgbWFpblpvb20gPSBkMy5iZWhhdmlvci56b29tKClcbiAgICBzdWJncmFwaHMgPSBbXVxuICAgIGpvYmlkID0gYXR0cnMuam9iaWRcblxuICAgIG1haW5TdmdFbGVtZW50ID0gZWxlbS5jaGlsZHJlbigpWzBdXG4gICAgbWFpbkcgPSBlbGVtLmNoaWxkcmVuKCkuY2hpbGRyZW4oKVswXVxuICAgIG1haW5UbXBFbGVtZW50ID0gZWxlbS5jaGlsZHJlbigpWzFdXG5cbiAgICBkM21haW5TdmcgPSBkMy5zZWxlY3QobWFpblN2Z0VsZW1lbnQpXG4gICAgZDNtYWluU3ZnRyA9IGQzLnNlbGVjdChtYWluRylcbiAgICBkM3RtcFN2ZyA9IGQzLnNlbGVjdChtYWluVG1wRWxlbWVudClcblxuICAgICMgYW5ndWxhci5lbGVtZW50KG1haW5HKS5lbXB0eSgpXG5cbiAgICBjb250YWluZXJXID0gZWxlbS53aWR0aCgpXG4gICAgYW5ndWxhci5lbGVtZW50KGVsZW0uY2hpbGRyZW4oKVswXSkud2lkdGgoY29udGFpbmVyVylcblxuICAgIHNjb3BlLnpvb21JbiA9IC0+XG4gICAgICBpZiBtYWluWm9vbS5zY2FsZSgpIDwgMi45OVxuICAgICAgICBcbiAgICAgICAgIyBDYWxjdWxhdGUgYW5kIHN0b3JlIG5ldyB2YWx1ZXMgaW4gem9vbSBvYmplY3RcbiAgICAgICAgdHJhbnNsYXRlID0gbWFpblpvb20udHJhbnNsYXRlKClcbiAgICAgICAgdjEgPSB0cmFuc2xhdGVbMF0gKiAobWFpblpvb20uc2NhbGUoKSArIDAuMSAvIChtYWluWm9vbS5zY2FsZSgpKSlcbiAgICAgICAgdjIgPSB0cmFuc2xhdGVbMV0gKiAobWFpblpvb20uc2NhbGUoKSArIDAuMSAvIChtYWluWm9vbS5zY2FsZSgpKSlcbiAgICAgICAgbWFpblpvb20uc2NhbGUgbWFpblpvb20uc2NhbGUoKSArIDAuMVxuICAgICAgICBtYWluWm9vbS50cmFuc2xhdGUgWyB2MSwgdjIgXVxuICAgICAgICBcbiAgICAgICAgIyBUcmFuc2Zvcm0gc3ZnXG4gICAgICAgIGQzbWFpblN2Z0cuYXR0ciBcInRyYW5zZm9ybVwiLCBcInRyYW5zbGF0ZShcIiArIHYxICsgXCIsXCIgKyB2MiArIFwiKSBzY2FsZShcIiArIG1haW5ab29tLnNjYWxlKCkgKyBcIilcIlxuXG4gICAgc2NvcGUuem9vbU91dCA9IC0+XG4gICAgICBpZiBtYWluWm9vbS5zY2FsZSgpID4gMC4zMVxuICAgICAgICBcbiAgICAgICAgIyBDYWxjdWxhdGUgYW5kIHN0b3JlIG5ldyB2YWx1ZXMgaW4gbWFpblpvb20gb2JqZWN0XG4gICAgICAgIG1haW5ab29tLnNjYWxlIG1haW5ab29tLnNjYWxlKCkgLSAwLjFcbiAgICAgICAgdHJhbnNsYXRlID0gbWFpblpvb20udHJhbnNsYXRlKClcbiAgICAgICAgdjEgPSB0cmFuc2xhdGVbMF0gKiAobWFpblpvb20uc2NhbGUoKSAtIDAuMSAvIChtYWluWm9vbS5zY2FsZSgpKSlcbiAgICAgICAgdjIgPSB0cmFuc2xhdGVbMV0gKiAobWFpblpvb20uc2NhbGUoKSAtIDAuMSAvIChtYWluWm9vbS5zY2FsZSgpKSlcbiAgICAgICAgbWFpblpvb20udHJhbnNsYXRlIFsgdjEsIHYyIF1cbiAgICAgICAgXG4gICAgICAgICMgVHJhbnNmb3JtIHN2Z1xuICAgICAgICBkM21haW5TdmdHLmF0dHIgXCJ0cmFuc2Zvcm1cIiwgXCJ0cmFuc2xhdGUoXCIgKyB2MSArIFwiLFwiICsgdjIgKyBcIikgc2NhbGUoXCIgKyBtYWluWm9vbS5zY2FsZSgpICsgXCIpXCJcblxuICAgICNjcmVhdGUgYSBsYWJlbCBvZiBhbiBlZGdlXG4gICAgY3JlYXRlTGFiZWxFZGdlID0gKGVsKSAtPlxuICAgICAgbGFiZWxWYWx1ZSA9IFwiXCJcbiAgICAgIGlmIGVsLnNoaXBfc3RyYXRlZ3k/IG9yIGVsLmxvY2FsX3N0cmF0ZWd5P1xuICAgICAgICBsYWJlbFZhbHVlICs9IFwiPGRpdiBjbGFzcz0nZWRnZS1sYWJlbCc+XCJcbiAgICAgICAgbGFiZWxWYWx1ZSArPSBlbC5zaGlwX3N0cmF0ZWd5ICBpZiBlbC5zaGlwX3N0cmF0ZWd5P1xuICAgICAgICBsYWJlbFZhbHVlICs9IFwiIChcIiArIGVsLnRlbXBfbW9kZSArIFwiKVwiICB1bmxlc3MgZWwudGVtcF9tb2RlIGlzIGB1bmRlZmluZWRgXG4gICAgICAgIGxhYmVsVmFsdWUgKz0gXCIsPGJyPlwiICsgZWwubG9jYWxfc3RyYXRlZ3kgIHVubGVzcyBlbC5sb2NhbF9zdHJhdGVneSBpcyBgdW5kZWZpbmVkYFxuICAgICAgICBsYWJlbFZhbHVlICs9IFwiPC9kaXY+XCJcbiAgICAgIGxhYmVsVmFsdWVcblxuXG4gICAgIyB0cnVlLCBpZiB0aGUgbm9kZSBpcyBhIHNwZWNpYWwgbm9kZSBmcm9tIGFuIGl0ZXJhdGlvblxuICAgIGlzU3BlY2lhbEl0ZXJhdGlvbk5vZGUgPSAoaW5mbykgLT5cbiAgICAgIChpbmZvIGlzIFwicGFydGlhbFNvbHV0aW9uXCIgb3IgaW5mbyBpcyBcIm5leHRQYXJ0aWFsU29sdXRpb25cIiBvciBpbmZvIGlzIFwid29ya3NldFwiIG9yIGluZm8gaXMgXCJuZXh0V29ya3NldFwiIG9yIGluZm8gaXMgXCJzb2x1dGlvblNldFwiIG9yIGluZm8gaXMgXCJzb2x1dGlvbkRlbHRhXCIpXG5cbiAgICBnZXROb2RlVHlwZSA9IChlbCwgaW5mbykgLT5cbiAgICAgIGlmIGluZm8gaXMgXCJtaXJyb3JcIlxuICAgICAgICAnbm9kZS1taXJyb3InXG5cbiAgICAgIGVsc2UgaWYgaXNTcGVjaWFsSXRlcmF0aW9uTm9kZShpbmZvKVxuICAgICAgICAnbm9kZS1pdGVyYXRpb24nXG5cbiAgICAgIGVsc2VcbiAgICAgICAgaWYgZWwucGFjdCBpcyBcIkRhdGEgU291cmNlXCJcbiAgICAgICAgICAnbm9kZS1zb3VyY2UnXG4gICAgICAgIGVsc2UgaWYgZWwucGFjdCBpcyBcIkRhdGEgU2lua1wiXG4gICAgICAgICAgJ25vZGUtc2luaydcbiAgICAgICAgZWxzZVxuICAgICAgICAgICdub2RlLW5vcm1hbCdcbiAgICAgIFxuICAgICMgY3JlYXRlcyB0aGUgbGFiZWwgb2YgYSBub2RlLCBpbiBpbmZvIGlzIHN0b3JlZCwgd2hldGhlciBpdCBpcyBhIHNwZWNpYWwgbm9kZSAobGlrZSBhIG1pcnJvciBpbiBhbiBpdGVyYXRpb24pXG4gICAgY3JlYXRlTGFiZWxOb2RlID0gKGVsLCBpbmZvLCBtYXhXLCBtYXhIKSAtPlxuICAgICAgbGFiZWxWYWx1ZSA9IFwiPGEgaHJlZj0nIy9qb2JzL1wiICsgam9iaWQgKyBcIi9cIiArIGVsLmlkICsgXCInIGNsYXNzPSdub2RlLWxhYmVsIFwiICsgZ2V0Tm9kZVR5cGUoZWwsIGluZm8pICsgXCInPlwiXG5cbiAgICAgICMgTm9kZW5hbWVcbiAgICAgIGlmIGluZm8gaXMgXCJtaXJyb3JcIlxuICAgICAgICBsYWJlbFZhbHVlICs9IFwiPGgzIGNsYXNzPSdub2RlLW5hbWUnPk1pcnJvciBvZiBcIiArIGVsLnBhY3QgKyBcIjwvaDM+XCJcbiAgICAgIGVsc2VcbiAgICAgICAgbGFiZWxWYWx1ZSArPSBcIjxoMyBjbGFzcz0nbm9kZS1uYW1lJz5cIiArIGVsLnBhY3QgKyBcIjwvaDM+XCJcbiAgICAgIGlmIGVsLmNvbnRlbnRzIGlzIFwiXCJcbiAgICAgICAgbGFiZWxWYWx1ZSArPSBcIlwiXG4gICAgICBlbHNlXG4gICAgICAgIHN0ZXBOYW1lID0gZWwuY29udGVudHNcbiAgICAgICAgXG4gICAgICAgICMgY2xlYW4gc3RlcE5hbWVcbiAgICAgICAgc3RlcE5hbWUgPSBzaG9ydGVuU3RyaW5nKHN0ZXBOYW1lKVxuICAgICAgICBsYWJlbFZhbHVlICs9IFwiPGg0IGNsYXNzPSdzdGVwLW5hbWUnPlwiICsgc3RlcE5hbWUgKyBcIjwvaDQ+XCJcbiAgICAgIFxuICAgICAgIyBJZiB0aGlzIG5vZGUgaXMgYW4gXCJpdGVyYXRpb25cIiB3ZSBuZWVkIGEgZGlmZmVyZW50IHBhbmVsLWJvZHlcbiAgICAgIGlmIGVsLnN0ZXBfZnVuY3Rpb24/XG4gICAgICAgIGxhYmVsVmFsdWUgKz0gZXh0ZW5kTGFiZWxOb2RlRm9ySXRlcmF0aW9uKGVsLmlkLCBtYXhXLCBtYXhIKVxuICAgICAgZWxzZVxuICAgICAgICBcbiAgICAgICAgIyBPdGhlcndpc2UgYWRkIGluZm9zICAgIFxuICAgICAgICBsYWJlbFZhbHVlICs9IFwiPGg1PlwiICsgaW5mbyArIFwiIE5vZGU8L2g1PlwiICBpZiBpc1NwZWNpYWxJdGVyYXRpb25Ob2RlKGluZm8pXG4gICAgICAgIGxhYmVsVmFsdWUgKz0gXCI8aDU+UGFyYWxsZWxpc206IFwiICsgZWwucGFyYWxsZWxpc20gKyBcIjwvaDU+XCIgIHVubGVzcyBlbC5wYXJhbGxlbGlzbSBpcyBcIlwiXG4gICAgICAgIGxhYmVsVmFsdWUgKz0gXCI8aDU+RHJpdmVyIFN0cmF0ZWd5OiBcIiArIHNob3J0ZW5TdHJpbmcoZWwuZHJpdmVyX3N0cmF0ZWd5KSArIFwiPC9oNVwiICB1bmxlc3MgZWwuZHJpdmVyX3N0cmF0ZWd5IGlzIGB1bmRlZmluZWRgXG4gICAgICBcbiAgICAgIGxhYmVsVmFsdWUgKz0gXCI8L2E+XCJcbiAgICAgIGxhYmVsVmFsdWVcblxuICAgICMgRXh0ZW5kcyB0aGUgbGFiZWwgb2YgYSBub2RlIHdpdGggYW4gYWRkaXRpb25hbCBzdmcgRWxlbWVudCB0byBwcmVzZW50IHRoZSBpdGVyYXRpb24uXG4gICAgZXh0ZW5kTGFiZWxOb2RlRm9ySXRlcmF0aW9uID0gKGlkLCBtYXhXLCBtYXhIKSAtPlxuICAgICAgc3ZnSUQgPSBcInN2Zy1cIiArIGlkXG5cbiAgICAgIGxhYmVsVmFsdWUgPSBcIjxzdmcgY2xhc3M9J1wiICsgc3ZnSUQgKyBcIicgd2lkdGg9XCIgKyBtYXhXICsgXCIgaGVpZ2h0PVwiICsgbWF4SCArIFwiPjxnIC8+PC9zdmc+XCJcbiAgICAgIGxhYmVsVmFsdWVcblxuICAgICMgU3BsaXQgYSBzdHJpbmcgaW50byBtdWx0aXBsZSBsaW5lcyBzbyB0aGF0IGVhY2ggbGluZSBoYXMgbGVzcyB0aGFuIDMwIGxldHRlcnMuXG4gICAgc2hvcnRlblN0cmluZyA9IChzKSAtPlxuICAgICAgIyBtYWtlIHN1cmUgdGhhdCBuYW1lIGRvZXMgbm90IGNvbnRhaW4gYSA8IChiZWNhdXNlIG9mIGh0bWwpXG4gICAgICBpZiBzLmNoYXJBdCgwKSBpcyBcIjxcIlxuICAgICAgICBzID0gcy5yZXBsYWNlKFwiPFwiLCBcIiZsdDtcIilcbiAgICAgICAgcyA9IHMucmVwbGFjZShcIj5cIiwgXCImZ3Q7XCIpXG4gICAgICBzYnIgPSBcIlwiXG4gICAgICB3aGlsZSBzLmxlbmd0aCA+IDMwXG4gICAgICAgIHNiciA9IHNiciArIHMuc3Vic3RyaW5nKDAsIDMwKSArIFwiPGJyPlwiXG4gICAgICAgIHMgPSBzLnN1YnN0cmluZygzMCwgcy5sZW5ndGgpXG4gICAgICBzYnIgPSBzYnIgKyBzXG4gICAgICBzYnJcblxuICAgIGNyZWF0ZU5vZGUgPSAoZywgZGF0YSwgZWwsIGlzUGFyZW50ID0gZmFsc2UsIG1heFcsIG1heEgpIC0+XG4gICAgICAjIGNyZWF0ZSBub2RlLCBzZW5kIGFkZGl0aW9uYWwgaW5mb3JtYXRpb25zIGFib3V0IHRoZSBub2RlIGlmIGl0IGlzIGEgc3BlY2lhbCBvbmVcbiAgICAgIGlmIGVsLmlkIGlzIGRhdGEucGFydGlhbF9zb2x1dGlvblxuICAgICAgICBnLnNldE5vZGUgZWwuaWQsXG4gICAgICAgICAgbGFiZWw6IGNyZWF0ZUxhYmVsTm9kZShlbCwgXCJwYXJ0aWFsU29sdXRpb25cIiwgbWF4VywgbWF4SClcbiAgICAgICAgICBsYWJlbFR5cGU6ICdodG1sJ1xuICAgICAgICAgIGNsYXNzOiBnZXROb2RlVHlwZShlbCwgXCJwYXJ0aWFsU29sdXRpb25cIilcblxuICAgICAgZWxzZSBpZiBlbC5pZCBpcyBkYXRhLm5leHRfcGFydGlhbF9zb2x1dGlvblxuICAgICAgICBnLnNldE5vZGUgZWwuaWQsXG4gICAgICAgICAgbGFiZWw6IGNyZWF0ZUxhYmVsTm9kZShlbCwgXCJuZXh0UGFydGlhbFNvbHV0aW9uXCIsIG1heFcsIG1heEgpXG4gICAgICAgICAgbGFiZWxUeXBlOiAnaHRtbCdcbiAgICAgICAgICBjbGFzczogZ2V0Tm9kZVR5cGUoZWwsIFwibmV4dFBhcnRpYWxTb2x1dGlvblwiKVxuXG4gICAgICBlbHNlIGlmIGVsLmlkIGlzIGRhdGEud29ya3NldFxuICAgICAgICBnLnNldE5vZGUgZWwuaWQsXG4gICAgICAgICAgbGFiZWw6IGNyZWF0ZUxhYmVsTm9kZShlbCwgXCJ3b3Jrc2V0XCIsIG1heFcsIG1heEgpXG4gICAgICAgICAgbGFiZWxUeXBlOiAnaHRtbCdcbiAgICAgICAgICBjbGFzczogZ2V0Tm9kZVR5cGUoZWwsIFwid29ya3NldFwiKVxuXG4gICAgICBlbHNlIGlmIGVsLmlkIGlzIGRhdGEubmV4dF93b3Jrc2V0XG4gICAgICAgIGcuc2V0Tm9kZSBlbC5pZCxcbiAgICAgICAgICBsYWJlbDogY3JlYXRlTGFiZWxOb2RlKGVsLCBcIm5leHRXb3Jrc2V0XCIsIG1heFcsIG1heEgpXG4gICAgICAgICAgbGFiZWxUeXBlOiAnaHRtbCdcbiAgICAgICAgICBjbGFzczogZ2V0Tm9kZVR5cGUoZWwsIFwibmV4dFdvcmtzZXRcIilcblxuICAgICAgZWxzZSBpZiBlbC5pZCBpcyBkYXRhLnNvbHV0aW9uX3NldFxuICAgICAgICBnLnNldE5vZGUgZWwuaWQsXG4gICAgICAgICAgbGFiZWw6IGNyZWF0ZUxhYmVsTm9kZShlbCwgXCJzb2x1dGlvblNldFwiLCBtYXhXLCBtYXhIKVxuICAgICAgICAgIGxhYmVsVHlwZTogJ2h0bWwnXG4gICAgICAgICAgY2xhc3M6IGdldE5vZGVUeXBlKGVsLCBcInNvbHV0aW9uU2V0XCIpXG5cbiAgICAgIGVsc2UgaWYgZWwuaWQgaXMgZGF0YS5zb2x1dGlvbl9kZWx0YVxuICAgICAgICBnLnNldE5vZGUgZWwuaWQsXG4gICAgICAgICAgbGFiZWw6IGNyZWF0ZUxhYmVsTm9kZShlbCwgXCJzb2x1dGlvbkRlbHRhXCIsIG1heFcsIG1heEgpXG4gICAgICAgICAgbGFiZWxUeXBlOiAnaHRtbCdcbiAgICAgICAgICBjbGFzczogZ2V0Tm9kZVR5cGUoZWwsIFwic29sdXRpb25EZWx0YVwiKVxuXG4gICAgICBlbHNlXG4gICAgICAgIGcuc2V0Tm9kZSBlbC5pZCxcbiAgICAgICAgICBsYWJlbDogY3JlYXRlTGFiZWxOb2RlKGVsLCBcIlwiLCBtYXhXLCBtYXhIKVxuICAgICAgICAgIGxhYmVsVHlwZTogJ2h0bWwnXG4gICAgICAgICAgY2xhc3M6IGdldE5vZGVUeXBlKGVsLCBcIlwiKVxuXG4gICAgY3JlYXRlRWRnZSA9IChnLCBkYXRhLCBlbCwgZXhpc3RpbmdOb2RlcywgcHJlZCkgLT5cbiAgICAgIHVubGVzcyBleGlzdGluZ05vZGVzLmluZGV4T2YocHJlZC5pZCkgaXMgLTFcbiAgICAgICAgZy5zZXRFZGdlIHByZWQuaWQsIGVsLmlkLFxuICAgICAgICAgIGxhYmVsOiBjcmVhdGVMYWJlbEVkZ2UocHJlZClcbiAgICAgICAgICBsYWJlbFR5cGU6ICdodG1sJ1xuICAgICAgICAgIGFycm93aGVhZDogJ25vcm1hbCdcblxuICAgICAgZWxzZVxuICAgICAgICBtaXNzaW5nTm9kZSA9IHNlYXJjaEZvck5vZGUoZGF0YSwgcHJlZC5pZClcbiAgICAgICAgdW5sZXNzICFtaXNzaW5nTm9kZSBvciBtaXNzaW5nTm9kZS5hbHJlYWR5QWRkZWQgaXMgdHJ1ZVxuICAgICAgICAgIG1pc3NpbmdOb2RlLmFscmVhZHlBZGRlZCA9IHRydWVcbiAgICAgICAgICBnLnNldE5vZGUgbWlzc2luZ05vZGUuaWQsXG4gICAgICAgICAgICBsYWJlbDogY3JlYXRlTGFiZWxOb2RlKG1pc3NpbmdOb2RlLCBcIm1pcnJvclwiKVxuICAgICAgICAgICAgbGFiZWxUeXBlOiAnaHRtbCdcbiAgICAgICAgICAgIGNsYXNzOiBnZXROb2RlVHlwZShtaXNzaW5nTm9kZSwgJ21pcnJvcicpXG5cbiAgICAgICAgICBnLnNldEVkZ2UgbWlzc2luZ05vZGUuaWQsIGVsLmlkLFxuICAgICAgICAgICAgbGFiZWw6IGNyZWF0ZUxhYmVsRWRnZShtaXNzaW5nTm9kZSlcbiAgICAgICAgICAgIGxhYmVsVHlwZTogJ2h0bWwnXG5cbiAgICBsb2FkSnNvblRvRGFncmUgPSAoZywgZGF0YSkgLT5cbiAgICAgIGV4aXN0aW5nTm9kZXMgPSBbXVxuXG4gICAgICBpZiBkYXRhLm5vZGVzP1xuICAgICAgICAjIFRoaXMgaXMgdGhlIG5vcm1hbCBqc29uIGRhdGFcbiAgICAgICAgdG9JdGVyYXRlID0gZGF0YS5ub2Rlc1xuXG4gICAgICBlbHNlXG4gICAgICAgICMgVGhpcyBpcyBhbiBpdGVyYXRpb24sIHdlIG5vdyBzdG9yZSBzcGVjaWFsIGl0ZXJhdGlvbiBub2RlcyBpZiBwb3NzaWJsZVxuICAgICAgICB0b0l0ZXJhdGUgPSBkYXRhLnN0ZXBfZnVuY3Rpb25cbiAgICAgICAgaXNQYXJlbnQgPSB0cnVlXG5cbiAgICAgIGZvciBlbCBpbiB0b0l0ZXJhdGVcbiAgICAgICAgbWF4VyA9IDBcbiAgICAgICAgbWF4SCA9IDBcblxuICAgICAgICBpZiBlbC5zdGVwX2Z1bmN0aW9uXG4gICAgICAgICAgc2cgPSBuZXcgZGFncmVEMy5ncmFwaGxpYi5HcmFwaCh7IG11bHRpZ3JhcGg6IHRydWUsIGNvbXBvdW5kOiB0cnVlIH0pLnNldEdyYXBoKHtcbiAgICAgICAgICAgIG5vZGVzZXA6IDIwXG4gICAgICAgICAgICBlZGdlc2VwOiAwXG4gICAgICAgICAgICByYW5rc2VwOiAyMFxuICAgICAgICAgICAgcmFua2RpcjogXCJMUlwiXG4gICAgICAgICAgICBtYXJnaW54OiAxMFxuICAgICAgICAgICAgbWFyZ2lueTogMTBcbiAgICAgICAgICAgIH0pXG5cbiAgICAgICAgICBzdWJncmFwaHNbZWwuaWRdID0gc2dcblxuICAgICAgICAgIGxvYWRKc29uVG9EYWdyZShzZywgZWwpXG5cbiAgICAgICAgICByID0gbmV3IGRhZ3JlRDMucmVuZGVyKClcbiAgICAgICAgICBkM3RtcFN2Zy5zZWxlY3QoJ2cnKS5jYWxsKHIsIHNnKVxuICAgICAgICAgIG1heFcgPSBzZy5ncmFwaCgpLndpZHRoXG4gICAgICAgICAgbWF4SCA9IHNnLmdyYXBoKCkuaGVpZ2h0XG5cbiAgICAgICAgICBhbmd1bGFyLmVsZW1lbnQobWFpblRtcEVsZW1lbnQpLmVtcHR5KClcblxuICAgICAgICBjcmVhdGVOb2RlKGcsIGRhdGEsIGVsLCBpc1BhcmVudCwgbWF4VywgbWF4SClcblxuICAgICAgICBleGlzdGluZ05vZGVzLnB1c2ggZWwuaWRcbiAgICAgICAgXG4gICAgICAgICMgY3JlYXRlIGVkZ2VzIGZyb20gcHJlZGVjZXNzb3JzIHRvIGN1cnJlbnQgbm9kZVxuICAgICAgICBpZiBlbC5wcmVkZWNlc3NvcnM/XG4gICAgICAgICAgZm9yIHByZWQgaW4gZWwucHJlZGVjZXNzb3JzXG4gICAgICAgICAgICBjcmVhdGVFZGdlKGcsIGRhdGEsIGVsLCBleGlzdGluZ05vZGVzLCBwcmVkKVxuXG4gICAgICBnXG5cbiAgICAjIHNlYXJjaGVzIGluIHRoZSBnbG9iYWwgSlNPTkRhdGEgZm9yIHRoZSBub2RlIHdpdGggdGhlIGdpdmVuIGlkXG4gICAgc2VhcmNoRm9yTm9kZSA9IChkYXRhLCBub2RlSUQpIC0+XG4gICAgICBmb3IgaSBvZiBkYXRhLm5vZGVzXG4gICAgICAgIGVsID0gZGF0YS5ub2Rlc1tpXVxuICAgICAgICByZXR1cm4gZWwgIGlmIGVsLmlkIGlzIG5vZGVJRFxuICAgICAgICBcbiAgICAgICAgIyBsb29rIGZvciBub2RlcyB0aGF0IGFyZSBpbiBpdGVyYXRpb25zXG4gICAgICAgIGlmIGVsLnN0ZXBfZnVuY3Rpb24/XG4gICAgICAgICAgZm9yIGogb2YgZWwuc3RlcF9mdW5jdGlvblxuICAgICAgICAgICAgcmV0dXJuIGVsLnN0ZXBfZnVuY3Rpb25bal0gIGlmIGVsLnN0ZXBfZnVuY3Rpb25bal0uaWQgaXMgbm9kZUlEXG5cbiAgICBkcmF3R3JhcGggPSAoZGF0YSkgLT5cbiAgICAgIGcgPSBuZXcgZGFncmVEMy5ncmFwaGxpYi5HcmFwaCh7IG11bHRpZ3JhcGg6IHRydWUsIGNvbXBvdW5kOiB0cnVlIH0pLnNldEdyYXBoKHtcbiAgICAgICAgbm9kZXNlcDogNzBcbiAgICAgICAgZWRnZXNlcDogMFxuICAgICAgICByYW5rc2VwOiA1MFxuICAgICAgICByYW5rZGlyOiBcIkxSXCJcbiAgICAgICAgbWFyZ2lueDogNDBcbiAgICAgICAgbWFyZ2lueTogNDBcbiAgICAgICAgfSlcblxuICAgICAgbG9hZEpzb25Ub0RhZ3JlKGcsIGRhdGEpXG5cbiAgICAgIHJlbmRlcmVyID0gbmV3IGRhZ3JlRDMucmVuZGVyKClcbiAgICAgIGQzbWFpblN2Z0cuY2FsbChyZW5kZXJlciwgZylcblxuICAgICAgZm9yIGksIHNnIG9mIHN1YmdyYXBoc1xuICAgICAgICBkM21haW5Tdmcuc2VsZWN0KCdzdmcuc3ZnLScgKyBpICsgJyBnJykuY2FsbChyZW5kZXJlciwgc2cpXG5cbiAgICAgIG5ld1NjYWxlID0gMC41XG5cbiAgICAgIHhDZW50ZXJPZmZzZXQgPSBNYXRoLmZsb29yKChhbmd1bGFyLmVsZW1lbnQobWFpblN2Z0VsZW1lbnQpLndpZHRoKCkgLSBnLmdyYXBoKCkud2lkdGggKiBuZXdTY2FsZSkgLyAyKVxuICAgICAgeUNlbnRlck9mZnNldCA9IE1hdGguZmxvb3IoKGFuZ3VsYXIuZWxlbWVudChtYWluU3ZnRWxlbWVudCkuaGVpZ2h0KCkgLSBnLmdyYXBoKCkuaGVpZ2h0ICogbmV3U2NhbGUpIC8gMilcblxuICAgICAgbWFpblpvb20uc2NhbGUobmV3U2NhbGUpLnRyYW5zbGF0ZShbeENlbnRlck9mZnNldCwgeUNlbnRlck9mZnNldF0pXG5cbiAgICAgIGQzbWFpblN2Z0cuYXR0cihcInRyYW5zZm9ybVwiLCBcInRyYW5zbGF0ZShcIiArIHhDZW50ZXJPZmZzZXQgKyBcIiwgXCIgKyB5Q2VudGVyT2Zmc2V0ICsgXCIpIHNjYWxlKFwiICsgbWFpblpvb20uc2NhbGUoKSArIFwiKVwiKVxuXG4gICAgICBtYWluWm9vbS5vbihcInpvb21cIiwgLT5cbiAgICAgICAgZXYgPSBkMy5ldmVudFxuICAgICAgICBkM21haW5TdmdHLmF0dHIgXCJ0cmFuc2Zvcm1cIiwgXCJ0cmFuc2xhdGUoXCIgKyBldi50cmFuc2xhdGUgKyBcIikgc2NhbGUoXCIgKyBldi5zY2FsZSArIFwiKVwiXG4gICAgICApXG4gICAgICBtYWluWm9vbShkM21haW5TdmcpXG5cbiAgICBzY29wZS4kd2F0Y2ggYXR0cnMucGxhbiwgKG5ld1BsYW4pIC0+XG4gICAgICBkcmF3R3JhcGgobmV3UGxhbikgaWYgbmV3UGxhblxuXG4gICAgcmV0dXJuXG4iLCJhbmd1bGFyLm1vZHVsZSgnZmxpbmtBcHAnKS5kaXJlY3RpdmUoJ3ZlcnRleCcsIGZ1bmN0aW9uKCRzdGF0ZSkge1xuICByZXR1cm4ge1xuICAgIHRlbXBsYXRlOiBcIjxzdmcgY2xhc3M9J3RpbWVsaW5lIHNlY29uZGFyeScgd2lkdGg9JzAnIGhlaWdodD0nMCc+PC9zdmc+XCIsXG4gICAgc2NvcGU6IHtcbiAgICAgIGRhdGE6IFwiPVwiXG4gICAgfSxcbiAgICBsaW5rOiBmdW5jdGlvbihzY29wZSwgZWxlbSwgYXR0cnMpIHtcbiAgICAgIHZhciBhbmFseXplVGltZSwgY29udGFpbmVyVywgc3ZnRWwsIHpvb207XG4gICAgICB6b29tID0gZDMuYmVoYXZpb3Iuem9vbSgpO1xuICAgICAgc3ZnRWwgPSBlbGVtLmNoaWxkcmVuKClbMF07XG4gICAgICBjb250YWluZXJXID0gZWxlbS53aWR0aCgpO1xuICAgICAgYW5ndWxhci5lbGVtZW50KHN2Z0VsKS5hdHRyKCd3aWR0aCcsIGNvbnRhaW5lclcgLSAxNik7XG4gICAgICBhbmFseXplVGltZSA9IGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgICAgdmFyIGJib3gsIGNoYXJ0LCBzdmcsIHN2Z0csIHRlc3REYXRhO1xuICAgICAgICB0ZXN0RGF0YSA9IFtdO1xuICAgICAgICBhbmd1bGFyLmZvckVhY2goZGF0YS5ncm91cHZlcnRleC5ncm91cG1lbWJlcnMsIGZ1bmN0aW9uKHZlcnRleCwgaSkge1xuICAgICAgICAgIHZhciB2VGltZTtcbiAgICAgICAgICB2VGltZSA9IGRhdGEudmVydGljZXRpbWVzW3ZlcnRleC52ZXJ0ZXhpZF07XG4gICAgICAgICAgcmV0dXJuIHRlc3REYXRhLnB1c2goe1xuICAgICAgICAgICAgbGFiZWw6IHZlcnRleC52ZXJ0ZXhpbnN0YW5jZW5hbWUgKyBcIiAoXCIgKyBpICsgXCIpXCIsXG4gICAgICAgICAgICB0aW1lczogW1xuICAgICAgICAgICAgICB7XG4gICAgICAgICAgICAgICAgbGFiZWw6IFwiU2NoZWR1bGVkXCIsXG4gICAgICAgICAgICAgICAgY29sb3I6IFwiIzY2NlwiLFxuICAgICAgICAgICAgICAgIHN0YXJ0aW5nX3RpbWU6IHZUaW1lW1wiU0NIRURVTEVEXCJdICogMTAwLFxuICAgICAgICAgICAgICAgIGVuZGluZ190aW1lOiB2VGltZVtcIkRFUExPWUlOR1wiXSAqIDEwMFxuICAgICAgICAgICAgICB9LCB7XG4gICAgICAgICAgICAgICAgbGFiZWw6IFwiRGVwbG95aW5nXCIsXG4gICAgICAgICAgICAgICAgY29sb3I6IFwiI2FhYVwiLFxuICAgICAgICAgICAgICAgIHN0YXJ0aW5nX3RpbWU6IHZUaW1lW1wiREVQTE9ZSU5HXCJdICogMTAwLFxuICAgICAgICAgICAgICAgIGVuZGluZ190aW1lOiB2VGltZVtcIlJVTk5JTkdcIl0gKiAxMDBcbiAgICAgICAgICAgICAgfSwge1xuICAgICAgICAgICAgICAgIGxhYmVsOiBcIlJ1bm5pbmdcIixcbiAgICAgICAgICAgICAgICBjb2xvcjogXCIjZGRkXCIsXG4gICAgICAgICAgICAgICAgc3RhcnRpbmdfdGltZTogdlRpbWVbXCJSVU5OSU5HXCJdICogMTAwLFxuICAgICAgICAgICAgICAgIGVuZGluZ190aW1lOiB2VGltZVtcIkZJTklTSEVEXCJdICogMTAwXG4gICAgICAgICAgICAgIH1cbiAgICAgICAgICAgIF1cbiAgICAgICAgICB9KTtcbiAgICAgICAgfSk7XG4gICAgICAgIGNoYXJ0ID0gZDMudGltZWxpbmUoKS5zdGFjaygpLnRpY2tGb3JtYXQoe1xuICAgICAgICAgIGZvcm1hdDogZDMudGltZS5mb3JtYXQoXCIlU1wiKSxcbiAgICAgICAgICB0aWNrSW50ZXJ2YWw6IDEsXG4gICAgICAgICAgdGlja1NpemU6IDFcbiAgICAgICAgfSkubGFiZWxGb3JtYXQoZnVuY3Rpb24obGFiZWwpIHtcbiAgICAgICAgICByZXR1cm4gbGFiZWw7XG4gICAgICAgIH0pLm1hcmdpbih7XG4gICAgICAgICAgbGVmdDogMTAwLFxuICAgICAgICAgIHJpZ2h0OiAwLFxuICAgICAgICAgIHRvcDogMCxcbiAgICAgICAgICBib3R0b206IDBcbiAgICAgICAgfSk7XG4gICAgICAgIHN2ZyA9IGQzLnNlbGVjdChzdmdFbCkuZGF0dW0odGVzdERhdGEpLmNhbGwoY2hhcnQpLmNhbGwoem9vbSk7XG4gICAgICAgIHN2Z0cgPSBzdmcuc2VsZWN0KFwiZ1wiKTtcbiAgICAgICAgem9vbS5vbihcInpvb21cIiwgZnVuY3Rpb24oKSB7XG4gICAgICAgICAgdmFyIGV2O1xuICAgICAgICAgIGV2ID0gZDMuZXZlbnQ7XG4gICAgICAgICAgc3ZnRy5zZWxlY3RBbGwoJ3JlY3QnKS5hdHRyKFwidHJhbnNmb3JtXCIsIFwidHJhbnNsYXRlKFwiICsgZXYudHJhbnNsYXRlWzBdICsgXCIsMCkgc2NhbGUoXCIgKyBldi5zY2FsZSArIFwiLDEpXCIpO1xuICAgICAgICAgIHJldHVybiBzdmdHLnNlbGVjdEFsbCgndGV4dCcpLmF0dHIoXCJ0cmFuc2Zvcm1cIiwgXCJ0cmFuc2xhdGUoXCIgKyBldi50cmFuc2xhdGVbMF0gKyBcIiwwKSBzY2FsZShcIiArIGV2LnNjYWxlICsgXCIsMSlcIik7XG4gICAgICAgIH0pO1xuICAgICAgICBiYm94ID0gc3ZnR1swXVswXS5nZXRCQm94KCk7XG4gICAgICAgIHJldHVybiBzdmcuYXR0cignaGVpZ2h0JywgYmJveC5oZWlnaHQgKyAzMCk7XG4gICAgICB9O1xuICAgICAgYW5hbHl6ZVRpbWUoc2NvcGUuZGF0YSk7XG4gICAgfVxuICB9O1xufSkuZGlyZWN0aXZlKCd0aW1lbGluZScsIGZ1bmN0aW9uKCRzdGF0ZSkge1xuICByZXR1cm4ge1xuICAgIHRlbXBsYXRlOiBcIjxzdmcgY2xhc3M9J3RpbWVsaW5lJyB3aWR0aD0nMCcgaGVpZ2h0PScwJz48L3N2Zz5cIixcbiAgICBzY29wZToge1xuICAgICAgam9iOiBcIj1cIlxuICAgIH0sXG4gICAgbGluazogZnVuY3Rpb24oc2NvcGUsIGVsZW0sIGF0dHJzKSB7XG4gICAgICB2YXIgYW5hbHl6ZVRpbWUsIGNvbnRhaW5lclcsIHN2Z0VsLCB6b29tO1xuICAgICAgem9vbSA9IGQzLmJlaGF2aW9yLnpvb20oKTtcbiAgICAgIHN2Z0VsID0gZWxlbS5jaGlsZHJlbigpWzBdO1xuICAgICAgY29udGFpbmVyVyA9IGVsZW0ud2lkdGgoKTtcbiAgICAgIGFuZ3VsYXIuZWxlbWVudChzdmdFbCkuYXR0cignd2lkdGgnLCBjb250YWluZXJXIC0gMTYpO1xuICAgICAgYW5hbHl6ZVRpbWUgPSBmdW5jdGlvbihkYXRhKSB7XG4gICAgICAgIHZhciBiYm94LCBjaGFydCwgc3ZnLCBzdmdHLCB0ZXN0RGF0YTtcbiAgICAgICAgdGVzdERhdGEgPSBbXTtcbiAgICAgICAgYW5ndWxhci5mb3JFYWNoKGRhdGEub2xkVi5ncm91cHZlcnRpY2VzLCBmdW5jdGlvbih2ZXJ0ZXgpIHtcbiAgICAgICAgICB2YXIgdlRpbWU7XG4gICAgICAgICAgdlRpbWUgPSBkYXRhLm9sZFYuZ3JvdXB2ZXJ0aWNldGltZXNbdmVydGV4Lmdyb3VwdmVydGV4aWRdO1xuICAgICAgICAgIHJldHVybiB0ZXN0RGF0YS5wdXNoKHtcbiAgICAgICAgICAgIHRpbWVzOiBbXG4gICAgICAgICAgICAgIHtcbiAgICAgICAgICAgICAgICBsYWJlbDogdmVydGV4Lmdyb3VwdmVydGV4bmFtZSxcbiAgICAgICAgICAgICAgICBjb2xvcjogXCIjM2ZiNmQ4XCIsXG4gICAgICAgICAgICAgICAgc3RhcnRpbmdfdGltZTogdlRpbWVbXCJTVEFSVEVEXCJdLFxuICAgICAgICAgICAgICAgIGVuZGluZ190aW1lOiB2VGltZVtcIkVOREVEXCJdLFxuICAgICAgICAgICAgICAgIGxpbms6IHZlcnRleC5ncm91cHZlcnRleGlkXG4gICAgICAgICAgICAgIH1cbiAgICAgICAgICAgIF1cbiAgICAgICAgICB9KTtcbiAgICAgICAgfSk7XG4gICAgICAgIGNoYXJ0ID0gZDMudGltZWxpbmUoKS5zdGFjaygpLmNsaWNrKGZ1bmN0aW9uKGQsIGksIGRhdHVtKSB7XG4gICAgICAgICAgcmV0dXJuICRzdGF0ZS5nbyhcInNpbmdsZS1qb2IudGltZWxpbmUudmVydGV4XCIsIHtcbiAgICAgICAgICAgIGpvYmlkOiBkYXRhLmppZCxcbiAgICAgICAgICAgIHZlcnRleElkOiBkLmxpbmtcbiAgICAgICAgICB9KTtcbiAgICAgICAgfSkudGlja0Zvcm1hdCh7XG4gICAgICAgICAgZm9ybWF0OiBkMy50aW1lLmZvcm1hdChcIiVTXCIpLFxuICAgICAgICAgIHRpY2tJbnRlcnZhbDogMSxcbiAgICAgICAgICB0aWNrU2l6ZTogMVxuICAgICAgICB9KS5tYXJnaW4oe1xuICAgICAgICAgIGxlZnQ6IDAsXG4gICAgICAgICAgcmlnaHQ6IDAsXG4gICAgICAgICAgdG9wOiAwLFxuICAgICAgICAgIGJvdHRvbTogMFxuICAgICAgICB9KTtcbiAgICAgICAgc3ZnID0gZDMuc2VsZWN0KHN2Z0VsKS5kYXR1bSh0ZXN0RGF0YSkuY2FsbChjaGFydCkuY2FsbCh6b29tKTtcbiAgICAgICAgc3ZnRyA9IHN2Zy5zZWxlY3QoXCJnXCIpO1xuICAgICAgICB6b29tLm9uKFwiem9vbVwiLCBmdW5jdGlvbigpIHtcbiAgICAgICAgICB2YXIgZXY7XG4gICAgICAgICAgZXYgPSBkMy5ldmVudDtcbiAgICAgICAgICBzdmdHLnNlbGVjdEFsbCgncmVjdCcpLmF0dHIoXCJ0cmFuc2Zvcm1cIiwgXCJ0cmFuc2xhdGUoXCIgKyBldi50cmFuc2xhdGVbMF0gKyBcIiwwKSBzY2FsZShcIiArIGV2LnNjYWxlICsgXCIsMSlcIik7XG4gICAgICAgICAgcmV0dXJuIHN2Z0cuc2VsZWN0QWxsKCd0ZXh0JykuYXR0cihcInRyYW5zZm9ybVwiLCBcInRyYW5zbGF0ZShcIiArIGV2LnRyYW5zbGF0ZVswXSArIFwiLDApIHNjYWxlKFwiICsgZXYuc2NhbGUgKyBcIiwxKVwiKTtcbiAgICAgICAgfSk7XG4gICAgICAgIGJib3ggPSBzdmdHWzBdWzBdLmdldEJCb3goKTtcbiAgICAgICAgcmV0dXJuIHN2Zy5hdHRyKCdoZWlnaHQnLCBiYm94LmhlaWdodCArIDMwKTtcbiAgICAgIH07XG4gICAgICBzY29wZS4kd2F0Y2goYXR0cnMuam9iLCBmdW5jdGlvbihkYXRhKSB7XG4gICAgICAgIGlmIChkYXRhKSB7XG4gICAgICAgICAgcmV0dXJuIGFuYWx5emVUaW1lKGRhdGEpO1xuICAgICAgICB9XG4gICAgICB9KTtcbiAgICB9XG4gIH07XG59KS5kaXJlY3RpdmUoJ2pvYlBsYW4nLCBmdW5jdGlvbigkdGltZW91dCkge1xuICByZXR1cm4ge1xuICAgIHRlbXBsYXRlOiBcIjxzdmcgY2xhc3M9J2dyYXBoJyB3aWR0aD0nNTAwJyBoZWlnaHQ9JzQwMCc+PGcgLz48L3N2Zz4gPHN2ZyBjbGFzcz0ndG1wJyB3aWR0aD0nMScgaGVpZ2h0PScxJz48ZyAvPjwvc3ZnPiA8ZGl2IGNsYXNzPSdidG4tZ3JvdXAgem9vbS1idXR0b25zJz4gPGEgY2xhc3M9J2J0biBidG4tZGVmYXVsdCB6b29tLWluJyBuZy1jbGljaz0nem9vbUluKCknPjxpIGNsYXNzPSdmYSBmYS1wbHVzJyAvPjwvYT4gPGEgY2xhc3M9J2J0biBidG4tZGVmYXVsdCB6b29tLW91dCcgbmctY2xpY2s9J3pvb21PdXQoKSc+PGkgY2xhc3M9J2ZhIGZhLW1pbnVzJyAvPjwvYT4gPC9kaXY+XCIsXG4gICAgc2NvcGU6IHtcbiAgICAgIHBsYW46ICc9J1xuICAgIH0sXG4gICAgbGluazogZnVuY3Rpb24oc2NvcGUsIGVsZW0sIGF0dHJzKSB7XG4gICAgICB2YXIgY29udGFpbmVyVywgY3JlYXRlRWRnZSwgY3JlYXRlTGFiZWxFZGdlLCBjcmVhdGVMYWJlbE5vZGUsIGNyZWF0ZU5vZGUsIGQzbWFpblN2ZywgZDNtYWluU3ZnRywgZDN0bXBTdmcsIGRyYXdHcmFwaCwgZXh0ZW5kTGFiZWxOb2RlRm9ySXRlcmF0aW9uLCBnZXROb2RlVHlwZSwgaXNTcGVjaWFsSXRlcmF0aW9uTm9kZSwgam9iaWQsIGxvYWRKc29uVG9EYWdyZSwgbWFpbkcsIG1haW5TdmdFbGVtZW50LCBtYWluVG1wRWxlbWVudCwgbWFpblpvb20sIHNlYXJjaEZvck5vZGUsIHNob3J0ZW5TdHJpbmcsIHN1YmdyYXBocztcbiAgICAgIG1haW5ab29tID0gZDMuYmVoYXZpb3Iuem9vbSgpO1xuICAgICAgc3ViZ3JhcGhzID0gW107XG4gICAgICBqb2JpZCA9IGF0dHJzLmpvYmlkO1xuICAgICAgbWFpblN2Z0VsZW1lbnQgPSBlbGVtLmNoaWxkcmVuKClbMF07XG4gICAgICBtYWluRyA9IGVsZW0uY2hpbGRyZW4oKS5jaGlsZHJlbigpWzBdO1xuICAgICAgbWFpblRtcEVsZW1lbnQgPSBlbGVtLmNoaWxkcmVuKClbMV07XG4gICAgICBkM21haW5TdmcgPSBkMy5zZWxlY3QobWFpblN2Z0VsZW1lbnQpO1xuICAgICAgZDNtYWluU3ZnRyA9IGQzLnNlbGVjdChtYWluRyk7XG4gICAgICBkM3RtcFN2ZyA9IGQzLnNlbGVjdChtYWluVG1wRWxlbWVudCk7XG4gICAgICBjb250YWluZXJXID0gZWxlbS53aWR0aCgpO1xuICAgICAgYW5ndWxhci5lbGVtZW50KGVsZW0uY2hpbGRyZW4oKVswXSkud2lkdGgoY29udGFpbmVyVyk7XG4gICAgICBzY29wZS56b29tSW4gPSBmdW5jdGlvbigpIHtcbiAgICAgICAgdmFyIHRyYW5zbGF0ZSwgdjEsIHYyO1xuICAgICAgICBpZiAobWFpblpvb20uc2NhbGUoKSA8IDIuOTkpIHtcbiAgICAgICAgICB0cmFuc2xhdGUgPSBtYWluWm9vbS50cmFuc2xhdGUoKTtcbiAgICAgICAgICB2MSA9IHRyYW5zbGF0ZVswXSAqIChtYWluWm9vbS5zY2FsZSgpICsgMC4xIC8gKG1haW5ab29tLnNjYWxlKCkpKTtcbiAgICAgICAgICB2MiA9IHRyYW5zbGF0ZVsxXSAqIChtYWluWm9vbS5zY2FsZSgpICsgMC4xIC8gKG1haW5ab29tLnNjYWxlKCkpKTtcbiAgICAgICAgICBtYWluWm9vbS5zY2FsZShtYWluWm9vbS5zY2FsZSgpICsgMC4xKTtcbiAgICAgICAgICBtYWluWm9vbS50cmFuc2xhdGUoW3YxLCB2Ml0pO1xuICAgICAgICAgIHJldHVybiBkM21haW5TdmdHLmF0dHIoXCJ0cmFuc2Zvcm1cIiwgXCJ0cmFuc2xhdGUoXCIgKyB2MSArIFwiLFwiICsgdjIgKyBcIikgc2NhbGUoXCIgKyBtYWluWm9vbS5zY2FsZSgpICsgXCIpXCIpO1xuICAgICAgICB9XG4gICAgICB9O1xuICAgICAgc2NvcGUuem9vbU91dCA9IGZ1bmN0aW9uKCkge1xuICAgICAgICB2YXIgdHJhbnNsYXRlLCB2MSwgdjI7XG4gICAgICAgIGlmIChtYWluWm9vbS5zY2FsZSgpID4gMC4zMSkge1xuICAgICAgICAgIG1haW5ab29tLnNjYWxlKG1haW5ab29tLnNjYWxlKCkgLSAwLjEpO1xuICAgICAgICAgIHRyYW5zbGF0ZSA9IG1haW5ab29tLnRyYW5zbGF0ZSgpO1xuICAgICAgICAgIHYxID0gdHJhbnNsYXRlWzBdICogKG1haW5ab29tLnNjYWxlKCkgLSAwLjEgLyAobWFpblpvb20uc2NhbGUoKSkpO1xuICAgICAgICAgIHYyID0gdHJhbnNsYXRlWzFdICogKG1haW5ab29tLnNjYWxlKCkgLSAwLjEgLyAobWFpblpvb20uc2NhbGUoKSkpO1xuICAgICAgICAgIG1haW5ab29tLnRyYW5zbGF0ZShbdjEsIHYyXSk7XG4gICAgICAgICAgcmV0dXJuIGQzbWFpblN2Z0cuYXR0cihcInRyYW5zZm9ybVwiLCBcInRyYW5zbGF0ZShcIiArIHYxICsgXCIsXCIgKyB2MiArIFwiKSBzY2FsZShcIiArIG1haW5ab29tLnNjYWxlKCkgKyBcIilcIik7XG4gICAgICAgIH1cbiAgICAgIH07XG4gICAgICBjcmVhdGVMYWJlbEVkZ2UgPSBmdW5jdGlvbihlbCkge1xuICAgICAgICB2YXIgbGFiZWxWYWx1ZTtcbiAgICAgICAgbGFiZWxWYWx1ZSA9IFwiXCI7XG4gICAgICAgIGlmICgoZWwuc2hpcF9zdHJhdGVneSAhPSBudWxsKSB8fCAoZWwubG9jYWxfc3RyYXRlZ3kgIT0gbnVsbCkpIHtcbiAgICAgICAgICBsYWJlbFZhbHVlICs9IFwiPGRpdiBjbGFzcz0nZWRnZS1sYWJlbCc+XCI7XG4gICAgICAgICAgaWYgKGVsLnNoaXBfc3RyYXRlZ3kgIT0gbnVsbCkge1xuICAgICAgICAgICAgbGFiZWxWYWx1ZSArPSBlbC5zaGlwX3N0cmF0ZWd5O1xuICAgICAgICAgIH1cbiAgICAgICAgICBpZiAoZWwudGVtcF9tb2RlICE9PSB1bmRlZmluZWQpIHtcbiAgICAgICAgICAgIGxhYmVsVmFsdWUgKz0gXCIgKFwiICsgZWwudGVtcF9tb2RlICsgXCIpXCI7XG4gICAgICAgICAgfVxuICAgICAgICAgIGlmIChlbC5sb2NhbF9zdHJhdGVneSAhPT0gdW5kZWZpbmVkKSB7XG4gICAgICAgICAgICBsYWJlbFZhbHVlICs9IFwiLDxicj5cIiArIGVsLmxvY2FsX3N0cmF0ZWd5O1xuICAgICAgICAgIH1cbiAgICAgICAgICBsYWJlbFZhbHVlICs9IFwiPC9kaXY+XCI7XG4gICAgICAgIH1cbiAgICAgICAgcmV0dXJuIGxhYmVsVmFsdWU7XG4gICAgICB9O1xuICAgICAgaXNTcGVjaWFsSXRlcmF0aW9uTm9kZSA9IGZ1bmN0aW9uKGluZm8pIHtcbiAgICAgICAgcmV0dXJuIGluZm8gPT09IFwicGFydGlhbFNvbHV0aW9uXCIgfHwgaW5mbyA9PT0gXCJuZXh0UGFydGlhbFNvbHV0aW9uXCIgfHwgaW5mbyA9PT0gXCJ3b3Jrc2V0XCIgfHwgaW5mbyA9PT0gXCJuZXh0V29ya3NldFwiIHx8IGluZm8gPT09IFwic29sdXRpb25TZXRcIiB8fCBpbmZvID09PSBcInNvbHV0aW9uRGVsdGFcIjtcbiAgICAgIH07XG4gICAgICBnZXROb2RlVHlwZSA9IGZ1bmN0aW9uKGVsLCBpbmZvKSB7XG4gICAgICAgIGlmIChpbmZvID09PSBcIm1pcnJvclwiKSB7XG4gICAgICAgICAgcmV0dXJuICdub2RlLW1pcnJvcic7XG4gICAgICAgIH0gZWxzZSBpZiAoaXNTcGVjaWFsSXRlcmF0aW9uTm9kZShpbmZvKSkge1xuICAgICAgICAgIHJldHVybiAnbm9kZS1pdGVyYXRpb24nO1xuICAgICAgICB9IGVsc2Uge1xuICAgICAgICAgIGlmIChlbC5wYWN0ID09PSBcIkRhdGEgU291cmNlXCIpIHtcbiAgICAgICAgICAgIHJldHVybiAnbm9kZS1zb3VyY2UnO1xuICAgICAgICAgIH0gZWxzZSBpZiAoZWwucGFjdCA9PT0gXCJEYXRhIFNpbmtcIikge1xuICAgICAgICAgICAgcmV0dXJuICdub2RlLXNpbmsnO1xuICAgICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgICByZXR1cm4gJ25vZGUtbm9ybWFsJztcbiAgICAgICAgICB9XG4gICAgICAgIH1cbiAgICAgIH07XG4gICAgICBjcmVhdGVMYWJlbE5vZGUgPSBmdW5jdGlvbihlbCwgaW5mbywgbWF4VywgbWF4SCkge1xuICAgICAgICB2YXIgbGFiZWxWYWx1ZSwgc3RlcE5hbWU7XG4gICAgICAgIGxhYmVsVmFsdWUgPSBcIjxhIGhyZWY9JyMvam9icy9cIiArIGpvYmlkICsgXCIvXCIgKyBlbC5pZCArIFwiJyBjbGFzcz0nbm9kZS1sYWJlbCBcIiArIGdldE5vZGVUeXBlKGVsLCBpbmZvKSArIFwiJz5cIjtcbiAgICAgICAgaWYgKGluZm8gPT09IFwibWlycm9yXCIpIHtcbiAgICAgICAgICBsYWJlbFZhbHVlICs9IFwiPGgzIGNsYXNzPSdub2RlLW5hbWUnPk1pcnJvciBvZiBcIiArIGVsLnBhY3QgKyBcIjwvaDM+XCI7XG4gICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgbGFiZWxWYWx1ZSArPSBcIjxoMyBjbGFzcz0nbm9kZS1uYW1lJz5cIiArIGVsLnBhY3QgKyBcIjwvaDM+XCI7XG4gICAgICAgIH1cbiAgICAgICAgaWYgKGVsLmNvbnRlbnRzID09PSBcIlwiKSB7XG4gICAgICAgICAgbGFiZWxWYWx1ZSArPSBcIlwiO1xuICAgICAgICB9IGVsc2Uge1xuICAgICAgICAgIHN0ZXBOYW1lID0gZWwuY29udGVudHM7XG4gICAgICAgICAgc3RlcE5hbWUgPSBzaG9ydGVuU3RyaW5nKHN0ZXBOYW1lKTtcbiAgICAgICAgICBsYWJlbFZhbHVlICs9IFwiPGg0IGNsYXNzPSdzdGVwLW5hbWUnPlwiICsgc3RlcE5hbWUgKyBcIjwvaDQ+XCI7XG4gICAgICAgIH1cbiAgICAgICAgaWYgKGVsLnN0ZXBfZnVuY3Rpb24gIT0gbnVsbCkge1xuICAgICAgICAgIGxhYmVsVmFsdWUgKz0gZXh0ZW5kTGFiZWxOb2RlRm9ySXRlcmF0aW9uKGVsLmlkLCBtYXhXLCBtYXhIKTtcbiAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICBpZiAoaXNTcGVjaWFsSXRlcmF0aW9uTm9kZShpbmZvKSkge1xuICAgICAgICAgICAgbGFiZWxWYWx1ZSArPSBcIjxoNT5cIiArIGluZm8gKyBcIiBOb2RlPC9oNT5cIjtcbiAgICAgICAgICB9XG4gICAgICAgICAgaWYgKGVsLnBhcmFsbGVsaXNtICE9PSBcIlwiKSB7XG4gICAgICAgICAgICBsYWJlbFZhbHVlICs9IFwiPGg1PlBhcmFsbGVsaXNtOiBcIiArIGVsLnBhcmFsbGVsaXNtICsgXCI8L2g1PlwiO1xuICAgICAgICAgIH1cbiAgICAgICAgICBpZiAoZWwuZHJpdmVyX3N0cmF0ZWd5ICE9PSB1bmRlZmluZWQpIHtcbiAgICAgICAgICAgIGxhYmVsVmFsdWUgKz0gXCI8aDU+RHJpdmVyIFN0cmF0ZWd5OiBcIiArIHNob3J0ZW5TdHJpbmcoZWwuZHJpdmVyX3N0cmF0ZWd5KSArIFwiPC9oNVwiO1xuICAgICAgICAgIH1cbiAgICAgICAgfVxuICAgICAgICBsYWJlbFZhbHVlICs9IFwiPC9hPlwiO1xuICAgICAgICByZXR1cm4gbGFiZWxWYWx1ZTtcbiAgICAgIH07XG4gICAgICBleHRlbmRMYWJlbE5vZGVGb3JJdGVyYXRpb24gPSBmdW5jdGlvbihpZCwgbWF4VywgbWF4SCkge1xuICAgICAgICB2YXIgbGFiZWxWYWx1ZSwgc3ZnSUQ7XG4gICAgICAgIHN2Z0lEID0gXCJzdmctXCIgKyBpZDtcbiAgICAgICAgbGFiZWxWYWx1ZSA9IFwiPHN2ZyBjbGFzcz0nXCIgKyBzdmdJRCArIFwiJyB3aWR0aD1cIiArIG1heFcgKyBcIiBoZWlnaHQ9XCIgKyBtYXhIICsgXCI+PGcgLz48L3N2Zz5cIjtcbiAgICAgICAgcmV0dXJuIGxhYmVsVmFsdWU7XG4gICAgICB9O1xuICAgICAgc2hvcnRlblN0cmluZyA9IGZ1bmN0aW9uKHMpIHtcbiAgICAgICAgdmFyIHNicjtcbiAgICAgICAgaWYgKHMuY2hhckF0KDApID09PSBcIjxcIikge1xuICAgICAgICAgIHMgPSBzLnJlcGxhY2UoXCI8XCIsIFwiJmx0O1wiKTtcbiAgICAgICAgICBzID0gcy5yZXBsYWNlKFwiPlwiLCBcIiZndDtcIik7XG4gICAgICAgIH1cbiAgICAgICAgc2JyID0gXCJcIjtcbiAgICAgICAgd2hpbGUgKHMubGVuZ3RoID4gMzApIHtcbiAgICAgICAgICBzYnIgPSBzYnIgKyBzLnN1YnN0cmluZygwLCAzMCkgKyBcIjxicj5cIjtcbiAgICAgICAgICBzID0gcy5zdWJzdHJpbmcoMzAsIHMubGVuZ3RoKTtcbiAgICAgICAgfVxuICAgICAgICBzYnIgPSBzYnIgKyBzO1xuICAgICAgICByZXR1cm4gc2JyO1xuICAgICAgfTtcbiAgICAgIGNyZWF0ZU5vZGUgPSBmdW5jdGlvbihnLCBkYXRhLCBlbCwgaXNQYXJlbnQsIG1heFcsIG1heEgpIHtcbiAgICAgICAgaWYgKGlzUGFyZW50ID09IG51bGwpIHtcbiAgICAgICAgICBpc1BhcmVudCA9IGZhbHNlO1xuICAgICAgICB9XG4gICAgICAgIGlmIChlbC5pZCA9PT0gZGF0YS5wYXJ0aWFsX3NvbHV0aW9uKSB7XG4gICAgICAgICAgcmV0dXJuIGcuc2V0Tm9kZShlbC5pZCwge1xuICAgICAgICAgICAgbGFiZWw6IGNyZWF0ZUxhYmVsTm9kZShlbCwgXCJwYXJ0aWFsU29sdXRpb25cIiwgbWF4VywgbWF4SCksXG4gICAgICAgICAgICBsYWJlbFR5cGU6ICdodG1sJyxcbiAgICAgICAgICAgIFwiY2xhc3NcIjogZ2V0Tm9kZVR5cGUoZWwsIFwicGFydGlhbFNvbHV0aW9uXCIpXG4gICAgICAgICAgfSk7XG4gICAgICAgIH0gZWxzZSBpZiAoZWwuaWQgPT09IGRhdGEubmV4dF9wYXJ0aWFsX3NvbHV0aW9uKSB7XG4gICAgICAgICAgcmV0dXJuIGcuc2V0Tm9kZShlbC5pZCwge1xuICAgICAgICAgICAgbGFiZWw6IGNyZWF0ZUxhYmVsTm9kZShlbCwgXCJuZXh0UGFydGlhbFNvbHV0aW9uXCIsIG1heFcsIG1heEgpLFxuICAgICAgICAgICAgbGFiZWxUeXBlOiAnaHRtbCcsXG4gICAgICAgICAgICBcImNsYXNzXCI6IGdldE5vZGVUeXBlKGVsLCBcIm5leHRQYXJ0aWFsU29sdXRpb25cIilcbiAgICAgICAgICB9KTtcbiAgICAgICAgfSBlbHNlIGlmIChlbC5pZCA9PT0gZGF0YS53b3Jrc2V0KSB7XG4gICAgICAgICAgcmV0dXJuIGcuc2V0Tm9kZShlbC5pZCwge1xuICAgICAgICAgICAgbGFiZWw6IGNyZWF0ZUxhYmVsTm9kZShlbCwgXCJ3b3Jrc2V0XCIsIG1heFcsIG1heEgpLFxuICAgICAgICAgICAgbGFiZWxUeXBlOiAnaHRtbCcsXG4gICAgICAgICAgICBcImNsYXNzXCI6IGdldE5vZGVUeXBlKGVsLCBcIndvcmtzZXRcIilcbiAgICAgICAgICB9KTtcbiAgICAgICAgfSBlbHNlIGlmIChlbC5pZCA9PT0gZGF0YS5uZXh0X3dvcmtzZXQpIHtcbiAgICAgICAgICByZXR1cm4gZy5zZXROb2RlKGVsLmlkLCB7XG4gICAgICAgICAgICBsYWJlbDogY3JlYXRlTGFiZWxOb2RlKGVsLCBcIm5leHRXb3Jrc2V0XCIsIG1heFcsIG1heEgpLFxuICAgICAgICAgICAgbGFiZWxUeXBlOiAnaHRtbCcsXG4gICAgICAgICAgICBcImNsYXNzXCI6IGdldE5vZGVUeXBlKGVsLCBcIm5leHRXb3Jrc2V0XCIpXG4gICAgICAgICAgfSk7XG4gICAgICAgIH0gZWxzZSBpZiAoZWwuaWQgPT09IGRhdGEuc29sdXRpb25fc2V0KSB7XG4gICAgICAgICAgcmV0dXJuIGcuc2V0Tm9kZShlbC5pZCwge1xuICAgICAgICAgICAgbGFiZWw6IGNyZWF0ZUxhYmVsTm9kZShlbCwgXCJzb2x1dGlvblNldFwiLCBtYXhXLCBtYXhIKSxcbiAgICAgICAgICAgIGxhYmVsVHlwZTogJ2h0bWwnLFxuICAgICAgICAgICAgXCJjbGFzc1wiOiBnZXROb2RlVHlwZShlbCwgXCJzb2x1dGlvblNldFwiKVxuICAgICAgICAgIH0pO1xuICAgICAgICB9IGVsc2UgaWYgKGVsLmlkID09PSBkYXRhLnNvbHV0aW9uX2RlbHRhKSB7XG4gICAgICAgICAgcmV0dXJuIGcuc2V0Tm9kZShlbC5pZCwge1xuICAgICAgICAgICAgbGFiZWw6IGNyZWF0ZUxhYmVsTm9kZShlbCwgXCJzb2x1dGlvbkRlbHRhXCIsIG1heFcsIG1heEgpLFxuICAgICAgICAgICAgbGFiZWxUeXBlOiAnaHRtbCcsXG4gICAgICAgICAgICBcImNsYXNzXCI6IGdldE5vZGVUeXBlKGVsLCBcInNvbHV0aW9uRGVsdGFcIilcbiAgICAgICAgICB9KTtcbiAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICByZXR1cm4gZy5zZXROb2RlKGVsLmlkLCB7XG4gICAgICAgICAgICBsYWJlbDogY3JlYXRlTGFiZWxOb2RlKGVsLCBcIlwiLCBtYXhXLCBtYXhIKSxcbiAgICAgICAgICAgIGxhYmVsVHlwZTogJ2h0bWwnLFxuICAgICAgICAgICAgXCJjbGFzc1wiOiBnZXROb2RlVHlwZShlbCwgXCJcIilcbiAgICAgICAgICB9KTtcbiAgICAgICAgfVxuICAgICAgfTtcbiAgICAgIGNyZWF0ZUVkZ2UgPSBmdW5jdGlvbihnLCBkYXRhLCBlbCwgZXhpc3RpbmdOb2RlcywgcHJlZCkge1xuICAgICAgICB2YXIgbWlzc2luZ05vZGU7XG4gICAgICAgIGlmIChleGlzdGluZ05vZGVzLmluZGV4T2YocHJlZC5pZCkgIT09IC0xKSB7XG4gICAgICAgICAgcmV0dXJuIGcuc2V0RWRnZShwcmVkLmlkLCBlbC5pZCwge1xuICAgICAgICAgICAgbGFiZWw6IGNyZWF0ZUxhYmVsRWRnZShwcmVkKSxcbiAgICAgICAgICAgIGxhYmVsVHlwZTogJ2h0bWwnLFxuICAgICAgICAgICAgYXJyb3doZWFkOiAnbm9ybWFsJ1xuICAgICAgICAgIH0pO1xuICAgICAgICB9IGVsc2Uge1xuICAgICAgICAgIG1pc3NpbmdOb2RlID0gc2VhcmNoRm9yTm9kZShkYXRhLCBwcmVkLmlkKTtcbiAgICAgICAgICBpZiAoISghbWlzc2luZ05vZGUgfHwgbWlzc2luZ05vZGUuYWxyZWFkeUFkZGVkID09PSB0cnVlKSkge1xuICAgICAgICAgICAgbWlzc2luZ05vZGUuYWxyZWFkeUFkZGVkID0gdHJ1ZTtcbiAgICAgICAgICAgIGcuc2V0Tm9kZShtaXNzaW5nTm9kZS5pZCwge1xuICAgICAgICAgICAgICBsYWJlbDogY3JlYXRlTGFiZWxOb2RlKG1pc3NpbmdOb2RlLCBcIm1pcnJvclwiKSxcbiAgICAgICAgICAgICAgbGFiZWxUeXBlOiAnaHRtbCcsXG4gICAgICAgICAgICAgIFwiY2xhc3NcIjogZ2V0Tm9kZVR5cGUobWlzc2luZ05vZGUsICdtaXJyb3InKVxuICAgICAgICAgICAgfSk7XG4gICAgICAgICAgICByZXR1cm4gZy5zZXRFZGdlKG1pc3NpbmdOb2RlLmlkLCBlbC5pZCwge1xuICAgICAgICAgICAgICBsYWJlbDogY3JlYXRlTGFiZWxFZGdlKG1pc3NpbmdOb2RlKSxcbiAgICAgICAgICAgICAgbGFiZWxUeXBlOiAnaHRtbCdcbiAgICAgICAgICAgIH0pO1xuICAgICAgICAgIH1cbiAgICAgICAgfVxuICAgICAgfTtcbiAgICAgIGxvYWRKc29uVG9EYWdyZSA9IGZ1bmN0aW9uKGcsIGRhdGEpIHtcbiAgICAgICAgdmFyIGVsLCBleGlzdGluZ05vZGVzLCBpc1BhcmVudCwgaywgbCwgbGVuLCBsZW4xLCBtYXhILCBtYXhXLCBwcmVkLCByLCByZWYsIHNnLCB0b0l0ZXJhdGU7XG4gICAgICAgIGV4aXN0aW5nTm9kZXMgPSBbXTtcbiAgICAgICAgaWYgKGRhdGEubm9kZXMgIT0gbnVsbCkge1xuICAgICAgICAgIHRvSXRlcmF0ZSA9IGRhdGEubm9kZXM7XG4gICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgdG9JdGVyYXRlID0gZGF0YS5zdGVwX2Z1bmN0aW9uO1xuICAgICAgICAgIGlzUGFyZW50ID0gdHJ1ZTtcbiAgICAgICAgfVxuICAgICAgICBmb3IgKGsgPSAwLCBsZW4gPSB0b0l0ZXJhdGUubGVuZ3RoOyBrIDwgbGVuOyBrKyspIHtcbiAgICAgICAgICBlbCA9IHRvSXRlcmF0ZVtrXTtcbiAgICAgICAgICBtYXhXID0gMDtcbiAgICAgICAgICBtYXhIID0gMDtcbiAgICAgICAgICBpZiAoZWwuc3RlcF9mdW5jdGlvbikge1xuICAgICAgICAgICAgc2cgPSBuZXcgZGFncmVEMy5ncmFwaGxpYi5HcmFwaCh7XG4gICAgICAgICAgICAgIG11bHRpZ3JhcGg6IHRydWUsXG4gICAgICAgICAgICAgIGNvbXBvdW5kOiB0cnVlXG4gICAgICAgICAgICB9KS5zZXRHcmFwaCh7XG4gICAgICAgICAgICAgIG5vZGVzZXA6IDIwLFxuICAgICAgICAgICAgICBlZGdlc2VwOiAwLFxuICAgICAgICAgICAgICByYW5rc2VwOiAyMCxcbiAgICAgICAgICAgICAgcmFua2RpcjogXCJMUlwiLFxuICAgICAgICAgICAgICBtYXJnaW54OiAxMCxcbiAgICAgICAgICAgICAgbWFyZ2lueTogMTBcbiAgICAgICAgICAgIH0pO1xuICAgICAgICAgICAgc3ViZ3JhcGhzW2VsLmlkXSA9IHNnO1xuICAgICAgICAgICAgbG9hZEpzb25Ub0RhZ3JlKHNnLCBlbCk7XG4gICAgICAgICAgICByID0gbmV3IGRhZ3JlRDMucmVuZGVyKCk7XG4gICAgICAgICAgICBkM3RtcFN2Zy5zZWxlY3QoJ2cnKS5jYWxsKHIsIHNnKTtcbiAgICAgICAgICAgIG1heFcgPSBzZy5ncmFwaCgpLndpZHRoO1xuICAgICAgICAgICAgbWF4SCA9IHNnLmdyYXBoKCkuaGVpZ2h0O1xuICAgICAgICAgICAgYW5ndWxhci5lbGVtZW50KG1haW5UbXBFbGVtZW50KS5lbXB0eSgpO1xuICAgICAgICAgIH1cbiAgICAgICAgICBjcmVhdGVOb2RlKGcsIGRhdGEsIGVsLCBpc1BhcmVudCwgbWF4VywgbWF4SCk7XG4gICAgICAgICAgZXhpc3RpbmdOb2Rlcy5wdXNoKGVsLmlkKTtcbiAgICAgICAgICBpZiAoZWwucHJlZGVjZXNzb3JzICE9IG51bGwpIHtcbiAgICAgICAgICAgIHJlZiA9IGVsLnByZWRlY2Vzc29ycztcbiAgICAgICAgICAgIGZvciAobCA9IDAsIGxlbjEgPSByZWYubGVuZ3RoOyBsIDwgbGVuMTsgbCsrKSB7XG4gICAgICAgICAgICAgIHByZWQgPSByZWZbbF07XG4gICAgICAgICAgICAgIGNyZWF0ZUVkZ2UoZywgZGF0YSwgZWwsIGV4aXN0aW5nTm9kZXMsIHByZWQpO1xuICAgICAgICAgICAgfVxuICAgICAgICAgIH1cbiAgICAgICAgfVxuICAgICAgICByZXR1cm4gZztcbiAgICAgIH07XG4gICAgICBzZWFyY2hGb3JOb2RlID0gZnVuY3Rpb24oZGF0YSwgbm9kZUlEKSB7XG4gICAgICAgIHZhciBlbCwgaSwgajtcbiAgICAgICAgZm9yIChpIGluIGRhdGEubm9kZXMpIHtcbiAgICAgICAgICBlbCA9IGRhdGEubm9kZXNbaV07XG4gICAgICAgICAgaWYgKGVsLmlkID09PSBub2RlSUQpIHtcbiAgICAgICAgICAgIHJldHVybiBlbDtcbiAgICAgICAgICB9XG4gICAgICAgICAgaWYgKGVsLnN0ZXBfZnVuY3Rpb24gIT0gbnVsbCkge1xuICAgICAgICAgICAgZm9yIChqIGluIGVsLnN0ZXBfZnVuY3Rpb24pIHtcbiAgICAgICAgICAgICAgaWYgKGVsLnN0ZXBfZnVuY3Rpb25bal0uaWQgPT09IG5vZGVJRCkge1xuICAgICAgICAgICAgICAgIHJldHVybiBlbC5zdGVwX2Z1bmN0aW9uW2pdO1xuICAgICAgICAgICAgICB9XG4gICAgICAgICAgICB9XG4gICAgICAgICAgfVxuICAgICAgICB9XG4gICAgICB9O1xuICAgICAgZHJhd0dyYXBoID0gZnVuY3Rpb24oZGF0YSkge1xuICAgICAgICB2YXIgZywgaSwgbmV3U2NhbGUsIHJlbmRlcmVyLCBzZywgeENlbnRlck9mZnNldCwgeUNlbnRlck9mZnNldDtcbiAgICAgICAgZyA9IG5ldyBkYWdyZUQzLmdyYXBobGliLkdyYXBoKHtcbiAgICAgICAgICBtdWx0aWdyYXBoOiB0cnVlLFxuICAgICAgICAgIGNvbXBvdW5kOiB0cnVlXG4gICAgICAgIH0pLnNldEdyYXBoKHtcbiAgICAgICAgICBub2Rlc2VwOiA3MCxcbiAgICAgICAgICBlZGdlc2VwOiAwLFxuICAgICAgICAgIHJhbmtzZXA6IDUwLFxuICAgICAgICAgIHJhbmtkaXI6IFwiTFJcIixcbiAgICAgICAgICBtYXJnaW54OiA0MCxcbiAgICAgICAgICBtYXJnaW55OiA0MFxuICAgICAgICB9KTtcbiAgICAgICAgbG9hZEpzb25Ub0RhZ3JlKGcsIGRhdGEpO1xuICAgICAgICByZW5kZXJlciA9IG5ldyBkYWdyZUQzLnJlbmRlcigpO1xuICAgICAgICBkM21haW5TdmdHLmNhbGwocmVuZGVyZXIsIGcpO1xuICAgICAgICBmb3IgKGkgaW4gc3ViZ3JhcGhzKSB7XG4gICAgICAgICAgc2cgPSBzdWJncmFwaHNbaV07XG4gICAgICAgICAgZDNtYWluU3ZnLnNlbGVjdCgnc3ZnLnN2Zy0nICsgaSArICcgZycpLmNhbGwocmVuZGVyZXIsIHNnKTtcbiAgICAgICAgfVxuICAgICAgICBuZXdTY2FsZSA9IDAuNTtcbiAgICAgICAgeENlbnRlck9mZnNldCA9IE1hdGguZmxvb3IoKGFuZ3VsYXIuZWxlbWVudChtYWluU3ZnRWxlbWVudCkud2lkdGgoKSAtIGcuZ3JhcGgoKS53aWR0aCAqIG5ld1NjYWxlKSAvIDIpO1xuICAgICAgICB5Q2VudGVyT2Zmc2V0ID0gTWF0aC5mbG9vcigoYW5ndWxhci5lbGVtZW50KG1haW5TdmdFbGVtZW50KS5oZWlnaHQoKSAtIGcuZ3JhcGgoKS5oZWlnaHQgKiBuZXdTY2FsZSkgLyAyKTtcbiAgICAgICAgbWFpblpvb20uc2NhbGUobmV3U2NhbGUpLnRyYW5zbGF0ZShbeENlbnRlck9mZnNldCwgeUNlbnRlck9mZnNldF0pO1xuICAgICAgICBkM21haW5TdmdHLmF0dHIoXCJ0cmFuc2Zvcm1cIiwgXCJ0cmFuc2xhdGUoXCIgKyB4Q2VudGVyT2Zmc2V0ICsgXCIsIFwiICsgeUNlbnRlck9mZnNldCArIFwiKSBzY2FsZShcIiArIG1haW5ab29tLnNjYWxlKCkgKyBcIilcIik7XG4gICAgICAgIG1haW5ab29tLm9uKFwiem9vbVwiLCBmdW5jdGlvbigpIHtcbiAgICAgICAgICB2YXIgZXY7XG4gICAgICAgICAgZXYgPSBkMy5ldmVudDtcbiAgICAgICAgICByZXR1cm4gZDNtYWluU3ZnRy5hdHRyKFwidHJhbnNmb3JtXCIsIFwidHJhbnNsYXRlKFwiICsgZXYudHJhbnNsYXRlICsgXCIpIHNjYWxlKFwiICsgZXYuc2NhbGUgKyBcIilcIik7XG4gICAgICAgIH0pO1xuICAgICAgICByZXR1cm4gbWFpblpvb20oZDNtYWluU3ZnKTtcbiAgICAgIH07XG4gICAgICBzY29wZS4kd2F0Y2goYXR0cnMucGxhbiwgZnVuY3Rpb24obmV3UGxhbikge1xuICAgICAgICBpZiAobmV3UGxhbikge1xuICAgICAgICAgIHJldHVybiBkcmF3R3JhcGgobmV3UGxhbik7XG4gICAgICAgIH1cbiAgICAgIH0pO1xuICAgIH1cbiAgfTtcbn0pO1xuIiwiYW5ndWxhci5tb2R1bGUoJ2ZsaW5rQXBwJylcblxuLnNlcnZpY2UgJ0pvYnNTZXJ2aWNlJywgKCRodHRwLCBmbGlua0NvbmZpZywgJGxvZywgYW1Nb21lbnQsICRxLCAkdGltZW91dCkgLT5cbiAgY3VycmVudEpvYiA9IG51bGxcbiAgY3VycmVudFBsYW4gPSBudWxsXG4gIGRlZmVycmVkcyA9IHt9XG4gIGpvYnMgPSB7XG4gICAgcnVubmluZzogW11cbiAgICBmaW5pc2hlZDogW11cbiAgICBjYW5jZWxsZWQ6IFtdXG4gICAgZmFpbGVkOiBbXVxuICB9XG5cbiAgam9iT2JzZXJ2ZXJzID0gW11cblxuICBub3RpZnlPYnNlcnZlcnMgPSAtPlxuICAgIGFuZ3VsYXIuZm9yRWFjaCBqb2JPYnNlcnZlcnMsIChjYWxsYmFjaykgLT5cbiAgICAgIGNhbGxiYWNrKClcblxuICBAcmVnaXN0ZXJPYnNlcnZlciA9IChjYWxsYmFjaykgLT5cbiAgICBqb2JPYnNlcnZlcnMucHVzaChjYWxsYmFjaylcblxuICBAdW5SZWdpc3Rlck9ic2VydmVyID0gKGNhbGxiYWNrKSAtPlxuICAgIGluZGV4ID0gam9iT2JzZXJ2ZXJzLmluZGV4T2YoY2FsbGJhY2spXG4gICAgam9iT2JzZXJ2ZXJzLnNwbGljZShpbmRleCwgMSlcblxuICBAc3RhdGVMaXN0ID0gLT5cbiAgICBbIFxuICAgICAgIyAnQ1JFQVRFRCdcbiAgICAgICdTQ0hFRFVMRUQnXG4gICAgICAnREVQTE9ZSU5HJ1xuICAgICAgJ1JVTk5JTkcnXG4gICAgICAnRklOSVNIRUQnXG4gICAgICAnRkFJTEVEJ1xuICAgICAgJ0NBTkNFTElORydcbiAgICAgICdDQU5DRUxFRCdcbiAgICBdXG5cbiAgQHRyYW5zbGF0ZUxhYmVsU3RhdGUgPSAoc3RhdGUpIC0+XG4gICAgc3dpdGNoIHN0YXRlLnRvTG93ZXJDYXNlKClcbiAgICAgIHdoZW4gJ2ZpbmlzaGVkJyB0aGVuICdzdWNjZXNzJ1xuICAgICAgd2hlbiAnZmFpbGVkJyB0aGVuICdkYW5nZXInXG4gICAgICB3aGVuICdzY2hlZHVsZWQnIHRoZW4gJ2RlZmF1bHQnXG4gICAgICB3aGVuICdkZXBsb3lpbmcnIHRoZW4gJ2luZm8nXG4gICAgICB3aGVuICdydW5uaW5nJyB0aGVuICdwcmltYXJ5J1xuICAgICAgd2hlbiAnY2FuY2VsaW5nJyB0aGVuICd3YXJuaW5nJ1xuICAgICAgd2hlbiAncGVuZGluZycgdGhlbiAnaW5mbydcbiAgICAgIHdoZW4gJ3RvdGFsJyB0aGVuICdibGFjaydcbiAgICAgIGVsc2UgJ2RlZmF1bHQnXG5cbiAgQGxpc3RKb2JzID0gLT5cbiAgICBkZWZlcnJlZCA9ICRxLmRlZmVyKClcblxuICAgICRodHRwLmdldCBmbGlua0NvbmZpZy5uZXdTZXJ2ZXIgKyBcIi9qb2JzXCJcbiAgICAuc3VjY2VzcyAoZGF0YSwgc3RhdHVzLCBoZWFkZXJzLCBjb25maWcpIC0+XG5cbiAgICAgIGFuZ3VsYXIuZm9yRWFjaCBkYXRhLCAobGlzdCwgbGlzdEtleSkgLT5cblxuICAgICAgICBzd2l0Y2ggbGlzdEtleVxuICAgICAgICAgIHdoZW4gJ2pvYnMtcnVubmluZycgdGhlbiBqb2JzLnJ1bm5pbmcgPSBsaXN0XG4gICAgICAgICAgd2hlbiAnam9icy1maW5pc2hlZCcgdGhlbiBqb2JzLmZpbmlzaGVkID0gbGlzdFxuICAgICAgICAgIHdoZW4gJ2pvYnMtY2FuY2VsbGVkJyB0aGVuIGpvYnMuY2FuY2VsbGVkID0gbGlzdFxuICAgICAgICAgIHdoZW4gJ2pvYnMtZmFpbGVkJyB0aGVuIGpvYnMuZmFpbGVkID0gbGlzdFxuXG4gICAgICAgIGFuZ3VsYXIuZm9yRWFjaCBsaXN0LCAoam9iaWQsIGluZGV4KSAtPlxuICAgICAgICAgICRodHRwLmdldCBmbGlua0NvbmZpZy5uZXdTZXJ2ZXIgKyBcIi9qb2JzL1wiICsgam9iaWRcbiAgICAgICAgICAuc3VjY2VzcyAoZGV0YWlscykgLT5cbiAgICAgICAgICAgIGxpc3RbaW5kZXhdID0gZGV0YWlsc1xuXG4gICAgICBkZWZlcnJlZC5yZXNvbHZlKGpvYnMpXG4gICAgICBub3RpZnlPYnNlcnZlcnMoKVxuXG4gICAgZGVmZXJyZWQucHJvbWlzZVxuXG4gIEBnZXRKb2JzID0gKHR5cGUpIC0+XG4gICAgam9ic1t0eXBlXVxuXG4gIEBnZXRBbGxKb2JzID0gLT5cbiAgICBqb2JzXG5cbiAgQGxvYWRKb2IgPSAoam9iaWQpIC0+XG4gICAgY3VycmVudEpvYiA9IG51bGxcbiAgICBkZWZlcnJlZHMuam9iID0gJHEuZGVmZXIoKVxuXG4gICAgJGh0dHAuZ2V0IGZsaW5rQ29uZmlnLm5ld1NlcnZlciArIFwiL2pvYnMvXCIgKyBqb2JpZFxuICAgIC5zdWNjZXNzIChkYXRhLCBzdGF0dXMsIGhlYWRlcnMsIGNvbmZpZykgLT5cbiAgICAgIGRhdGEudGltZSA9IERhdGUubm93KClcblxuICAgICAgJGh0dHAuZ2V0IGZsaW5rQ29uZmlnLm5ld1NlcnZlciArIFwiL2pvYnMvXCIgKyBqb2JpZCArIFwiL3ZlcnRpY2VzXCJcbiAgICAgIC5zdWNjZXNzICh2ZXJ0aWNlcykgLT5cbiAgICAgICAgZGF0YSA9IGFuZ3VsYXIuZXh0ZW5kKGRhdGEsIHZlcnRpY2VzKVxuXG4gICAgICAgICRodHRwLmdldCBmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcIi9qb2JzSW5mbz9nZXQ9am9iJmpvYj1cIiArIGpvYmlkXG4gICAgICAgIC5zdWNjZXNzIChvbGRWZXJ0aWNlcykgLT5cbiAgICAgICAgICBkYXRhLm9sZFYgPSBvbGRWZXJ0aWNlc1swXVxuXG4gICAgICAgICAgY3VycmVudEpvYiA9IGRhdGFcbiAgICAgICAgICBkZWZlcnJlZHMuam9iLnJlc29sdmUoZGF0YSlcblxuICAgIGRlZmVycmVkcy5qb2IucHJvbWlzZVxuXG4gIEBsb2FkUGxhbiA9IChqb2JpZCkgLT5cbiAgICBjdXJyZW50UGxhbiA9IG51bGxcbiAgICBkZWZlcnJlZHMucGxhbiA9ICRxLmRlZmVyKClcblxuICAgICRodHRwLmdldCBmbGlua0NvbmZpZy5uZXdTZXJ2ZXIgKyBcIi9qb2JzL1wiICsgam9iaWQgKyBcIi9wbGFuXCJcbiAgICAuc3VjY2VzcyAoZGF0YSkgLT5cbiAgICAgIGN1cnJlbnRQbGFuID0gZGF0YVxuXG4gICAgICBkZWZlcnJlZHMucGxhbi5yZXNvbHZlKGRhdGEpXG5cbiAgICBkZWZlcnJlZHMucGxhbi5wcm9taXNlXG5cbiAgQGdldE5vZGUgPSAobm9kZWlkKSAtPlxuICAgIHNlZWtOb2RlID0gKG5vZGVpZCwgZGF0YSkgLT5cbiAgICAgIG5vZGVpZCA9IHBhcnNlSW50KG5vZGVpZClcblxuICAgICAgZm9yIG5vZGUgaW4gZGF0YVxuICAgICAgICByZXR1cm4gbm9kZSBpZiBub2RlLmlkIGlzIG5vZGVpZFxuICAgICAgICBzdWIgPSBzZWVrTm9kZShub2RlaWQsIG5vZGUuc3RlcF9mdW5jdGlvbikgaWYgbm9kZS5zdGVwX2Z1bmN0aW9uXG4gICAgICAgIHJldHVybiBzdWIgaWYgc3ViXG5cbiAgICAgIG51bGxcblxuICAgIGRlZmVycmVkID0gJHEuZGVmZXIoKVxuXG4gICAgIyBpZiBjdXJyZW50UGxhblxuICAgICMgICBkZWZlcnJlZC5yZXNvbHZlKHNlZWtOb2RlKG5vZGVpZCwgY3VycmVudFBsYW4ubm9kZXMpKVxuICAgICMgZWxzZVxuICAgICMgICAjIGRlZmVycmVkcy5wbGFuLnByb21pc2UudGhlbiAoZGF0YSkgLT5cbiAgICAjICAgJHEuYWxsKFtkZWZlcnJlZHMucGxhbi5wcm9taXNlLCBkZWZlcnJlZHMuam9iLnByb21pc2VdKS50aGVuIChkYXRhKSAtPlxuICAgICMgICAgIGNvbnNvbGUubG9nICdyZXNvbHZpbmcgZ2V0Tm9kZSdcbiAgICAjICAgICBkZWZlcnJlZC5yZXNvbHZlKHNlZWtOb2RlKG5vZGVpZCwgY3VycmVudFBsYW4ubm9kZXMpKVxuXG4gICAgJHEuYWxsKFtkZWZlcnJlZHMucGxhbi5wcm9taXNlLCBkZWZlcnJlZHMuam9iLnByb21pc2VdKS50aGVuIChkYXRhKSA9PlxuICAgICAgZm91bmROb2RlID0gc2Vla05vZGUobm9kZWlkLCBjdXJyZW50UGxhbi5ub2RlcylcblxuICAgICAgIyBUT0RPIGxpbmsgdG8gcmVhbCB2ZXJ0ZXguIGZvciBub3cgdGhlcmUgaXMgbm8gd2F5IHRvIGdldCB0aGUgcmlnaHQgb25lLCBzbyB3ZSBhcmUgc2hvd2luZyB0aGUgZmlyc3Qgb25lIC0ganVzdCBmb3IgdGVzdGluZ1xuICAgICAgQGdldFZlcnRleChjdXJyZW50Sm9iLmppZCwgY3VycmVudEpvYi5vbGRWLmdyb3VwdmVydGljZXNbMF0uZ3JvdXB2ZXJ0ZXhpZCkudGhlbiAodmVydGV4KSAtPlxuICAgICAgICBmb3VuZE5vZGUudmVydGV4ID0gdmVydGV4XG4gICAgICAgIGRlZmVycmVkLnJlc29sdmUoZm91bmROb2RlKVxuXG4gICAgZGVmZXJyZWQucHJvbWlzZVxuXG5cbiAgQGdldFZlcnRleCA9IChqb2JJZCwgdmVydGV4SWQpIC0+XG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpXG5cbiAgICAkaHR0cC5nZXQgZmxpbmtDb25maWcuam9iU2VydmVyICsgXCIvam9ic0luZm8/Z2V0PWdyb3VwdmVydGV4JmpvYj1cIiArIGpvYklkICsgXCImZ3JvdXB2ZXJ0ZXg9XCIgKyB2ZXJ0ZXhJZFxuICAgIC5zdWNjZXNzIChkYXRhKSAtPlxuICAgICAgZGVmZXJyZWQucmVzb2x2ZShkYXRhKVxuXG4gICAgZGVmZXJyZWQucHJvbWlzZVxuXG4gIEBcbiIsImFuZ3VsYXIubW9kdWxlKCdmbGlua0FwcCcpLnNlcnZpY2UoJ0pvYnNTZXJ2aWNlJywgZnVuY3Rpb24oJGh0dHAsIGZsaW5rQ29uZmlnLCAkbG9nLCBhbU1vbWVudCwgJHEsICR0aW1lb3V0KSB7XG4gIHZhciBjdXJyZW50Sm9iLCBjdXJyZW50UGxhbiwgZGVmZXJyZWRzLCBqb2JPYnNlcnZlcnMsIGpvYnMsIG5vdGlmeU9ic2VydmVycztcbiAgY3VycmVudEpvYiA9IG51bGw7XG4gIGN1cnJlbnRQbGFuID0gbnVsbDtcbiAgZGVmZXJyZWRzID0ge307XG4gIGpvYnMgPSB7XG4gICAgcnVubmluZzogW10sXG4gICAgZmluaXNoZWQ6IFtdLFxuICAgIGNhbmNlbGxlZDogW10sXG4gICAgZmFpbGVkOiBbXVxuICB9O1xuICBqb2JPYnNlcnZlcnMgPSBbXTtcbiAgbm90aWZ5T2JzZXJ2ZXJzID0gZnVuY3Rpb24oKSB7XG4gICAgcmV0dXJuIGFuZ3VsYXIuZm9yRWFjaChqb2JPYnNlcnZlcnMsIGZ1bmN0aW9uKGNhbGxiYWNrKSB7XG4gICAgICByZXR1cm4gY2FsbGJhY2soKTtcbiAgICB9KTtcbiAgfTtcbiAgdGhpcy5yZWdpc3Rlck9ic2VydmVyID0gZnVuY3Rpb24oY2FsbGJhY2spIHtcbiAgICByZXR1cm4gam9iT2JzZXJ2ZXJzLnB1c2goY2FsbGJhY2spO1xuICB9O1xuICB0aGlzLnVuUmVnaXN0ZXJPYnNlcnZlciA9IGZ1bmN0aW9uKGNhbGxiYWNrKSB7XG4gICAgdmFyIGluZGV4O1xuICAgIGluZGV4ID0gam9iT2JzZXJ2ZXJzLmluZGV4T2YoY2FsbGJhY2spO1xuICAgIHJldHVybiBqb2JPYnNlcnZlcnMuc3BsaWNlKGluZGV4LCAxKTtcbiAgfTtcbiAgdGhpcy5zdGF0ZUxpc3QgPSBmdW5jdGlvbigpIHtcbiAgICByZXR1cm4gWydTQ0hFRFVMRUQnLCAnREVQTE9ZSU5HJywgJ1JVTk5JTkcnLCAnRklOSVNIRUQnLCAnRkFJTEVEJywgJ0NBTkNFTElORycsICdDQU5DRUxFRCddO1xuICB9O1xuICB0aGlzLnRyYW5zbGF0ZUxhYmVsU3RhdGUgPSBmdW5jdGlvbihzdGF0ZSkge1xuICAgIHN3aXRjaCAoc3RhdGUudG9Mb3dlckNhc2UoKSkge1xuICAgICAgY2FzZSAnZmluaXNoZWQnOlxuICAgICAgICByZXR1cm4gJ3N1Y2Nlc3MnO1xuICAgICAgY2FzZSAnZmFpbGVkJzpcbiAgICAgICAgcmV0dXJuICdkYW5nZXInO1xuICAgICAgY2FzZSAnc2NoZWR1bGVkJzpcbiAgICAgICAgcmV0dXJuICdkZWZhdWx0JztcbiAgICAgIGNhc2UgJ2RlcGxveWluZyc6XG4gICAgICAgIHJldHVybiAnaW5mbyc7XG4gICAgICBjYXNlICdydW5uaW5nJzpcbiAgICAgICAgcmV0dXJuICdwcmltYXJ5JztcbiAgICAgIGNhc2UgJ2NhbmNlbGluZyc6XG4gICAgICAgIHJldHVybiAnd2FybmluZyc7XG4gICAgICBjYXNlICdwZW5kaW5nJzpcbiAgICAgICAgcmV0dXJuICdpbmZvJztcbiAgICAgIGNhc2UgJ3RvdGFsJzpcbiAgICAgICAgcmV0dXJuICdibGFjayc7XG4gICAgICBkZWZhdWx0OlxuICAgICAgICByZXR1cm4gJ2RlZmF1bHQnO1xuICAgIH1cbiAgfTtcbiAgdGhpcy5saXN0Sm9icyA9IGZ1bmN0aW9uKCkge1xuICAgIHZhciBkZWZlcnJlZDtcbiAgICBkZWZlcnJlZCA9ICRxLmRlZmVyKCk7XG4gICAgJGh0dHAuZ2V0KGZsaW5rQ29uZmlnLm5ld1NlcnZlciArIFwiL2pvYnNcIikuc3VjY2VzcyhmdW5jdGlvbihkYXRhLCBzdGF0dXMsIGhlYWRlcnMsIGNvbmZpZykge1xuICAgICAgYW5ndWxhci5mb3JFYWNoKGRhdGEsIGZ1bmN0aW9uKGxpc3QsIGxpc3RLZXkpIHtcbiAgICAgICAgc3dpdGNoIChsaXN0S2V5KSB7XG4gICAgICAgICAgY2FzZSAnam9icy1ydW5uaW5nJzpcbiAgICAgICAgICAgIGpvYnMucnVubmluZyA9IGxpc3Q7XG4gICAgICAgICAgICBicmVhaztcbiAgICAgICAgICBjYXNlICdqb2JzLWZpbmlzaGVkJzpcbiAgICAgICAgICAgIGpvYnMuZmluaXNoZWQgPSBsaXN0O1xuICAgICAgICAgICAgYnJlYWs7XG4gICAgICAgICAgY2FzZSAnam9icy1jYW5jZWxsZWQnOlxuICAgICAgICAgICAgam9icy5jYW5jZWxsZWQgPSBsaXN0O1xuICAgICAgICAgICAgYnJlYWs7XG4gICAgICAgICAgY2FzZSAnam9icy1mYWlsZWQnOlxuICAgICAgICAgICAgam9icy5mYWlsZWQgPSBsaXN0O1xuICAgICAgICB9XG4gICAgICAgIHJldHVybiBhbmd1bGFyLmZvckVhY2gobGlzdCwgZnVuY3Rpb24oam9iaWQsIGluZGV4KSB7XG4gICAgICAgICAgcmV0dXJuICRodHRwLmdldChmbGlua0NvbmZpZy5uZXdTZXJ2ZXIgKyBcIi9qb2JzL1wiICsgam9iaWQpLnN1Y2Nlc3MoZnVuY3Rpb24oZGV0YWlscykge1xuICAgICAgICAgICAgcmV0dXJuIGxpc3RbaW5kZXhdID0gZGV0YWlscztcbiAgICAgICAgICB9KTtcbiAgICAgICAgfSk7XG4gICAgICB9KTtcbiAgICAgIGRlZmVycmVkLnJlc29sdmUoam9icyk7XG4gICAgICByZXR1cm4gbm90aWZ5T2JzZXJ2ZXJzKCk7XG4gICAgfSk7XG4gICAgcmV0dXJuIGRlZmVycmVkLnByb21pc2U7XG4gIH07XG4gIHRoaXMuZ2V0Sm9icyA9IGZ1bmN0aW9uKHR5cGUpIHtcbiAgICByZXR1cm4gam9ic1t0eXBlXTtcbiAgfTtcbiAgdGhpcy5nZXRBbGxKb2JzID0gZnVuY3Rpb24oKSB7XG4gICAgcmV0dXJuIGpvYnM7XG4gIH07XG4gIHRoaXMubG9hZEpvYiA9IGZ1bmN0aW9uKGpvYmlkKSB7XG4gICAgY3VycmVudEpvYiA9IG51bGw7XG4gICAgZGVmZXJyZWRzLmpvYiA9ICRxLmRlZmVyKCk7XG4gICAgJGh0dHAuZ2V0KGZsaW5rQ29uZmlnLm5ld1NlcnZlciArIFwiL2pvYnMvXCIgKyBqb2JpZCkuc3VjY2VzcyhmdW5jdGlvbihkYXRhLCBzdGF0dXMsIGhlYWRlcnMsIGNvbmZpZykge1xuICAgICAgZGF0YS50aW1lID0gRGF0ZS5ub3coKTtcbiAgICAgIHJldHVybiAkaHR0cC5nZXQoZmxpbmtDb25maWcubmV3U2VydmVyICsgXCIvam9icy9cIiArIGpvYmlkICsgXCIvdmVydGljZXNcIikuc3VjY2VzcyhmdW5jdGlvbih2ZXJ0aWNlcykge1xuICAgICAgICBkYXRhID0gYW5ndWxhci5leHRlbmQoZGF0YSwgdmVydGljZXMpO1xuICAgICAgICByZXR1cm4gJGh0dHAuZ2V0KGZsaW5rQ29uZmlnLmpvYlNlcnZlciArIFwiL2pvYnNJbmZvP2dldD1qb2Imam9iPVwiICsgam9iaWQpLnN1Y2Nlc3MoZnVuY3Rpb24ob2xkVmVydGljZXMpIHtcbiAgICAgICAgICBkYXRhLm9sZFYgPSBvbGRWZXJ0aWNlc1swXTtcbiAgICAgICAgICBjdXJyZW50Sm9iID0gZGF0YTtcbiAgICAgICAgICByZXR1cm4gZGVmZXJyZWRzLmpvYi5yZXNvbHZlKGRhdGEpO1xuICAgICAgICB9KTtcbiAgICAgIH0pO1xuICAgIH0pO1xuICAgIHJldHVybiBkZWZlcnJlZHMuam9iLnByb21pc2U7XG4gIH07XG4gIHRoaXMubG9hZFBsYW4gPSBmdW5jdGlvbihqb2JpZCkge1xuICAgIGN1cnJlbnRQbGFuID0gbnVsbDtcbiAgICBkZWZlcnJlZHMucGxhbiA9ICRxLmRlZmVyKCk7XG4gICAgJGh0dHAuZ2V0KGZsaW5rQ29uZmlnLm5ld1NlcnZlciArIFwiL2pvYnMvXCIgKyBqb2JpZCArIFwiL3BsYW5cIikuc3VjY2VzcyhmdW5jdGlvbihkYXRhKSB7XG4gICAgICBjdXJyZW50UGxhbiA9IGRhdGE7XG4gICAgICByZXR1cm4gZGVmZXJyZWRzLnBsYW4ucmVzb2x2ZShkYXRhKTtcbiAgICB9KTtcbiAgICByZXR1cm4gZGVmZXJyZWRzLnBsYW4ucHJvbWlzZTtcbiAgfTtcbiAgdGhpcy5nZXROb2RlID0gZnVuY3Rpb24obm9kZWlkKSB7XG4gICAgdmFyIGRlZmVycmVkLCBzZWVrTm9kZTtcbiAgICBzZWVrTm9kZSA9IGZ1bmN0aW9uKG5vZGVpZCwgZGF0YSkge1xuICAgICAgdmFyIGksIGxlbiwgbm9kZSwgc3ViO1xuICAgICAgbm9kZWlkID0gcGFyc2VJbnQobm9kZWlkKTtcbiAgICAgIGZvciAoaSA9IDAsIGxlbiA9IGRhdGEubGVuZ3RoOyBpIDwgbGVuOyBpKyspIHtcbiAgICAgICAgbm9kZSA9IGRhdGFbaV07XG4gICAgICAgIGlmIChub2RlLmlkID09PSBub2RlaWQpIHtcbiAgICAgICAgICByZXR1cm4gbm9kZTtcbiAgICAgICAgfVxuICAgICAgICBpZiAobm9kZS5zdGVwX2Z1bmN0aW9uKSB7XG4gICAgICAgICAgc3ViID0gc2Vla05vZGUobm9kZWlkLCBub2RlLnN0ZXBfZnVuY3Rpb24pO1xuICAgICAgICB9XG4gICAgICAgIGlmIChzdWIpIHtcbiAgICAgICAgICByZXR1cm4gc3ViO1xuICAgICAgICB9XG4gICAgICB9XG4gICAgICByZXR1cm4gbnVsbDtcbiAgICB9O1xuICAgIGRlZmVycmVkID0gJHEuZGVmZXIoKTtcbiAgICAkcS5hbGwoW2RlZmVycmVkcy5wbGFuLnByb21pc2UsIGRlZmVycmVkcy5qb2IucHJvbWlzZV0pLnRoZW4oKGZ1bmN0aW9uKF90aGlzKSB7XG4gICAgICByZXR1cm4gZnVuY3Rpb24oZGF0YSkge1xuICAgICAgICB2YXIgZm91bmROb2RlO1xuICAgICAgICBmb3VuZE5vZGUgPSBzZWVrTm9kZShub2RlaWQsIGN1cnJlbnRQbGFuLm5vZGVzKTtcbiAgICAgICAgcmV0dXJuIF90aGlzLmdldFZlcnRleChjdXJyZW50Sm9iLmppZCwgY3VycmVudEpvYi5vbGRWLmdyb3VwdmVydGljZXNbMF0uZ3JvdXB2ZXJ0ZXhpZCkudGhlbihmdW5jdGlvbih2ZXJ0ZXgpIHtcbiAgICAgICAgICBmb3VuZE5vZGUudmVydGV4ID0gdmVydGV4O1xuICAgICAgICAgIHJldHVybiBkZWZlcnJlZC5yZXNvbHZlKGZvdW5kTm9kZSk7XG4gICAgICAgIH0pO1xuICAgICAgfTtcbiAgICB9KSh0aGlzKSk7XG4gICAgcmV0dXJuIGRlZmVycmVkLnByb21pc2U7XG4gIH07XG4gIHRoaXMuZ2V0VmVydGV4ID0gZnVuY3Rpb24oam9iSWQsIHZlcnRleElkKSB7XG4gICAgdmFyIGRlZmVycmVkO1xuICAgIGRlZmVycmVkID0gJHEuZGVmZXIoKTtcbiAgICAkaHR0cC5nZXQoZmxpbmtDb25maWcuam9iU2VydmVyICsgXCIvam9ic0luZm8/Z2V0PWdyb3VwdmVydGV4JmpvYj1cIiArIGpvYklkICsgXCImZ3JvdXB2ZXJ0ZXg9XCIgKyB2ZXJ0ZXhJZCkuc3VjY2VzcyhmdW5jdGlvbihkYXRhKSB7XG4gICAgICByZXR1cm4gZGVmZXJyZWQucmVzb2x2ZShkYXRhKTtcbiAgICB9KTtcbiAgICByZXR1cm4gZGVmZXJyZWQucHJvbWlzZTtcbiAgfTtcbiAgcmV0dXJuIHRoaXM7XG59KTtcbiIsImFuZ3VsYXIubW9kdWxlKCdmbGlua0FwcCcpXG5cbi5jb250cm9sbGVyICdPdmVydmlld0NvbnRyb2xsZXInLCAoJHNjb3BlLCBPdmVydmlld1NlcnZpY2UsIEpvYnNTZXJ2aWNlKSAtPlxuICAkc2NvcGUuam9iT2JzZXJ2ZXIgPSAtPlxuICAgICRzY29wZS5ydW5uaW5nSm9icyA9IEpvYnNTZXJ2aWNlLmdldEpvYnMoJ3J1bm5pbmcnKVxuICAgICRzY29wZS5maW5pc2hlZEpvYnMgPSBKb2JzU2VydmljZS5nZXRKb2JzKCdmaW5pc2hlZCcpXG5cbiAgSm9ic1NlcnZpY2UucmVnaXN0ZXJPYnNlcnZlcigkc2NvcGUuam9iT2JzZXJ2ZXIpXG4gICRzY29wZS4kb24gJyRkZXN0cm95JywgLT5cbiAgICBKb2JzU2VydmljZS51blJlZ2lzdGVyT2JzZXJ2ZXIoJHNjb3BlLmpvYk9ic2VydmVyKVxuXG4gICRzY29wZS5qb2JPYnNlcnZlcigpXG4iLCJhbmd1bGFyLm1vZHVsZSgnZmxpbmtBcHAnKS5jb250cm9sbGVyKCdPdmVydmlld0NvbnRyb2xsZXInLCBmdW5jdGlvbigkc2NvcGUsIE92ZXJ2aWV3U2VydmljZSwgSm9ic1NlcnZpY2UpIHtcbiAgJHNjb3BlLmpvYk9ic2VydmVyID0gZnVuY3Rpb24oKSB7XG4gICAgJHNjb3BlLnJ1bm5pbmdKb2JzID0gSm9ic1NlcnZpY2UuZ2V0Sm9icygncnVubmluZycpO1xuICAgIHJldHVybiAkc2NvcGUuZmluaXNoZWRKb2JzID0gSm9ic1NlcnZpY2UuZ2V0Sm9icygnZmluaXNoZWQnKTtcbiAgfTtcbiAgSm9ic1NlcnZpY2UucmVnaXN0ZXJPYnNlcnZlcigkc2NvcGUuam9iT2JzZXJ2ZXIpO1xuICAkc2NvcGUuJG9uKCckZGVzdHJveScsIGZ1bmN0aW9uKCkge1xuICAgIHJldHVybiBKb2JzU2VydmljZS51blJlZ2lzdGVyT2JzZXJ2ZXIoJHNjb3BlLmpvYk9ic2VydmVyKTtcbiAgfSk7XG4gIHJldHVybiAkc2NvcGUuam9iT2JzZXJ2ZXIoKTtcbn0pO1xuIiwiYW5ndWxhci5tb2R1bGUoJ2ZsaW5rQXBwJylcblxuLnNlcnZpY2UgJ092ZXJ2aWV3U2VydmljZScsICgkaHR0cCwgZmxpbmtDb25maWcsICRsb2cpIC0+XG4gIHNlcnZlclN0YXR1cyA9IHt9XG5cbiAgQGxvYWRTZXJ2ZXJTdGF0dXMgPSAtPlxuICAgICRodHRwLmdldChmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcIi9tb25pdG9yL3N0YXR1c1wiKVxuICAgIC5zdWNjZXNzIChkYXRhLCBzdGF0dXMsIGhlYWRlcnMsIGNvbmZpZykgLT5cbiAgICAgICRsb2cgZGF0YVxuXG4gICAgLmVycm9yIChkYXRhLCBzdGF0dXMsIGhlYWRlcnMsIGNvbmZpZykgLT5cbiAgICAgIHJldHVyblxuXG4gICAgc2VydmVyU3RhdHVzXG5cbiAgQFxuIiwiYW5ndWxhci5tb2R1bGUoJ2ZsaW5rQXBwJykuc2VydmljZSgnT3ZlcnZpZXdTZXJ2aWNlJywgZnVuY3Rpb24oJGh0dHAsIGZsaW5rQ29uZmlnLCAkbG9nKSB7XG4gIHZhciBzZXJ2ZXJTdGF0dXM7XG4gIHNlcnZlclN0YXR1cyA9IHt9O1xuICB0aGlzLmxvYWRTZXJ2ZXJTdGF0dXMgPSBmdW5jdGlvbigpIHtcbiAgICAkaHR0cC5nZXQoZmxpbmtDb25maWcuam9iU2VydmVyICsgXCIvbW9uaXRvci9zdGF0dXNcIikuc3VjY2VzcyhmdW5jdGlvbihkYXRhLCBzdGF0dXMsIGhlYWRlcnMsIGNvbmZpZykge1xuICAgICAgcmV0dXJuICRsb2coZGF0YSk7XG4gICAgfSkuZXJyb3IoZnVuY3Rpb24oZGF0YSwgc3RhdHVzLCBoZWFkZXJzLCBjb25maWcpIHt9KTtcbiAgICByZXR1cm4gc2VydmVyU3RhdHVzO1xuICB9O1xuICByZXR1cm4gdGhpcztcbn0pO1xuIl0sInNvdXJjZVJvb3QiOiIvc291cmNlLyJ9