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

//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJzb3VyY2VzIjpbImluZGV4LmNvZmZlZSIsImluZGV4LmpzIiwiY29tbW9uL2RpcmVjdGl2ZXMuY29mZmVlIiwiY29tbW9uL2RpcmVjdGl2ZXMuanMiLCJjb21tb24vZmlsdGVycy5jb2ZmZWUiLCJjb21tb24vZmlsdGVycy5qcyIsIm1vZHVsZXMvam9icy9qb2JzLmN0cmwuY29mZmVlIiwibW9kdWxlcy9qb2JzL2pvYnMuY3RybC5qcyIsIm1vZHVsZXMvam9icy9qb2JzLmRpci5jb2ZmZWUiLCJtb2R1bGVzL2pvYnMvam9icy5kaXIuanMiLCJtb2R1bGVzL2pvYnMvam9icy5zdmMuY29mZmVlIiwibW9kdWxlcy9qb2JzL2pvYnMuc3ZjLmpzIiwibW9kdWxlcy9vdmVydmlldy9vdmVydmlldy5jdHJsLmNvZmZlZSIsIm1vZHVsZXMvb3ZlcnZpZXcvb3ZlcnZpZXcuY3RybC5qcyIsIm1vZHVsZXMvb3ZlcnZpZXcvb3ZlcnZpZXcuc3ZjLmNvZmZlZSIsIm1vZHVsZXMvb3ZlcnZpZXcvb3ZlcnZpZXcuc3ZjLmpzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiJBQWtCQSxRQUFRLE9BQU8sWUFBWSxDQUFDLGFBQWEsa0JBSXhDLG1CQUFJLFNBQUMsWUFBRDtFQUNILFdBQVcsaUJBQWlCO0VDckI1QixPRHNCQSxXQUFXLGNBQWMsV0FBQTtJQUN2QixXQUFXLGlCQUFpQixDQUFDLFdBQVc7SUNyQnhDLE9Ec0JBLFdBQVcsZUFBZTs7SUFJN0IsU0FBUyxlQUFlO0VBQ3ZCLFdBQVc7RUFDWCxXQUFXO0VBQ1gsV0FBVztFQUlYLGlCQUFpQjtHQUtsQixnREFBSSxTQUFDLGFBQWEsYUFBYSxXQUEzQjtFQUNILFlBQVk7RUM5QlosT0RnQ0EsVUFBVSxXQUFBO0lDL0JSLE9EZ0NBLFlBQVk7S0FDWixZQUFZO0lBS2YsZ0RBQU8sU0FBQyxnQkFBZ0Isb0JBQWpCO0VBQ04sZUFBZSxNQUFNLFlBQ25CO0lBQUEsS0FBSztJQUNMLE9BQ0U7TUFBQSxNQUNFO1FBQUEsYUFBYTtRQUNiLFlBQVk7OztL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VSxXQUFXLEtBQUssV0FBVyxLQUFLLGNBQWMsR0FBRyxlQUFlLEtBQUssU0FBQyxRQUFEO1VBQzlFLFVBQVUsU0FBUztVQ3JCakIsT0RzQkYsU0FBUyxRQUFROzs7T0FOd0M7SUNaN0QsT0RvQkEsU0FBUzs7RUFHWCxLQUFDLFlBQVksU0FBQyxPQUFPLFVBQVI7SUFDWCxJQUFBO0lBQUEsV0FBVyxHQUFHO0lBRWQsTUFBTSxJQUFJLFlBQVksWUFBWSxtQ0FBbUMsUUFBUSxrQkFBa0IsVUFDOUYsUUFBUSxTQUFDLE1BQUQ7TUNyQlAsT0RzQkEsU0FBUyxRQUFROztJQ3BCbkIsT0RzQkEsU0FBUzs7RUNwQlgsT0RzQkE7O0FDcEJGO0FDdElBLFFBQVEsT0FBTyxZQUVkLFdBQVcsbUVBQXNCLFNBQUMsUUFBUSxpQkFBaUIsYUFBMUI7RUFDaEMsT0FBTyxjQUFjLFdBQUE7SUFDbkIsT0FBTyxjQUFjLFlBQVksUUFBUTtJQ25CekMsT0RvQkEsT0FBTyxlQUFlLFlBQVksUUFBUTs7RUFFNUMsWUFBWSxpQkFBaUIsT0FBTztFQUNwQyxPQUFPLElBQUksWUFBWSxXQUFBO0lDbkJyQixPRG9CQSxZQUFZLG1CQUFtQixPQUFPOztFQ2xCeEMsT0RvQkEsT0FBTzs7QUNsQlQ7QUNPQSxRQUFRLE9BQU8sWUFFZCxRQUFRLG9EQUFtQixTQUFDLE9BQU8sYUFBYSxNQUFyQjtFQUMxQixJQUFBO0VBQUEsZUFBZTtFQUVmLEtBQUMsbUJBQW1CLFdBQUE7SUFDbEIsTUFBTSxJQUFJLFlBQVksWUFBWSxtQkFDakMsUUFBUSxTQUFDLE1BQU0sUUFBUSxTQUFTLFFBQXhCO01DcEJQLE9EcUJBLEtBQUs7T0FFTixNQUFNLFNBQUMsTUFBTSxRQUFRLFNBQVMsUUFBeEI7SUNyQlAsT0R3QkE7O0VDdEJGLE9Ed0JBOztBQ3RCRiIsImZpbGUiOiJpbmRleC5qcyIsInNvdXJjZXNDb250ZW50IjpbIiNcbiMgTGljZW5zZWQgdG8gdGhlIEFwYWNoZSBTb2Z0d2FyZSBGb3VuZGF0aW9uIChBU0YpIHVuZGVyIG9uZVxuIyBvciBtb3JlIGNvbnRyaWJ1dG9yIGxpY2Vuc2UgYWdyZWVtZW50cy4gIFNlZSB0aGUgTk9USUNFIGZpbGVcbiMgZGlzdHJpYnV0ZWQgd2l0aCB0aGlzIHdvcmsgZm9yIGFkZGl0aW9uYWwgaW5mb3JtYXRpb25cbiMgcmVnYXJkaW5nIGNvcHlyaWdodCBvd25lcnNoaXAuICBUaGUgQVNGIGxpY2Vuc2VzIHRoaXMgZmlsZVxuIyB0byB5b3UgdW5kZXIgdGhlIEFwYWNoZSBMaWNlbnNlLCBWZXJzaW9uIDIuMCAodGhlXG4jIFwiTGljZW5zZVwiKTsgeW91IG1heSBub3QgdXNlIHRoaXMgZmlsZSBleGNlcHQgaW4gY29tcGxpYW5jZVxuIyB3aXRoIHRoZSBMaWNlbnNlLiAgWW91IG1heSBvYnRhaW4gYSBjb3B5IG9mIHRoZSBMaWNlbnNlIGF0XG4jXG4jICAgICBodHRwOi8vd3d3LmFwYWNoZS5vcmcvbGljZW5zZXMvTElDRU5TRS0yLjBcbiNcbiMgVW5sZXNzIHJlcXVpcmVkIGJ5IGFwcGxpY2FibGUgbGF3IG9yIGFncmVlZCB0byBpbiB3cml0aW5nLCBzb2Z0d2FyZVxuIyBkaXN0cmlidXRlZCB1bmRlciB0aGUgTGljZW5zZSBpcyBkaXN0cmlidXRlZCBvbiBhbiBcIkFTIElTXCIgQkFTSVMsXG4jIFdJVEhPVVQgV0FSUkFOVElFUyBPUiBDT05ESVRJT05TIE9GIEFOWSBLSU5ELCBlaXRoZXIgZXhwcmVzcyBvciBpbXBsaWVkLlxuIyBTZWUgdGhlIExpY2Vuc2UgZm9yIHRoZSBzcGVjaWZpYyBsYW5ndWFnZSBnb3Zlcm5pbmcgcGVybWlzc2lvbnMgYW5kXG4jIGxpbWl0YXRpb25zIHVuZGVyIHRoZSBMaWNlbnNlLlxuI1xuXG5hbmd1bGFyLm1vZHVsZSgnZmxpbmtBcHAnLCBbJ3VpLnJvdXRlcicsICdhbmd1bGFyTW9tZW50J10pXG5cbiMgLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS1cblxuLnJ1biAoJHJvb3RTY29wZSkgLT5cbiAgJHJvb3RTY29wZS5zaWRlYmFyVmlzaWJsZSA9IGZhbHNlXG4gICRyb290U2NvcGUuc2hvd1NpZGViYXIgPSAtPlxuICAgICRyb290U2NvcGUuc2lkZWJhclZpc2libGUgPSAhJHJvb3RTY29wZS5zaWRlYmFyVmlzaWJsZVxuICAgICRyb290U2NvcGUuc2lkZWJhckNsYXNzID0gJ2ZvcmNlLXNob3cnXG5cbiMgLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS1cblxuLmNvbnN0YW50ICdmbGlua0NvbmZpZycsIHtcbiAgd2ViU2VydmVyOiAnaHR0cDovL2xvY2FsaG9zdDo4MDgwJ1xuICBqb2JTZXJ2ZXI6ICdodHRwOi8vbG9jYWxob3N0OjgwODEnXG4gIG5ld1NlcnZlcjogJ2h0dHA6Ly9sb2NhbGhvc3Q6ODA4MidcbiMgIHdlYlNlcnZlcjogJ2h0dHA6Ly9sb2NhbGhvc3Q6MzAwMC93ZWItc2VydmVyJ1xuIyAgam9iU2VydmVyOiAnaHR0cDovL2xvY2FsaG9zdDozMDAwL2pvYi1zZXJ2ZXInXG4jICBuZXdTZXJ2ZXI6ICdodHRwOi8vbG9jYWxob3N0OjMwMDAvbmV3LXNlcnZlcidcbiAgcmVmcmVzaEludGVydmFsOiAxMDAwMFxufVxuXG4jIC0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tXG5cbi5ydW4gKEpvYnNTZXJ2aWNlLCBmbGlua0NvbmZpZywgJGludGVydmFsKSAtPlxuICBKb2JzU2VydmljZS5saXN0Sm9icygpXG5cbiAgJGludGVydmFsIC0+XG4gICAgSm9ic1NlcnZpY2UubGlzdEpvYnMoKVxuICAsIGZsaW5rQ29uZmlnLnJlZnJlc2hJbnRlcnZhbFxuXG5cbiMgLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS1cblxuLmNvbmZpZyAoJHN0YXRlUHJvdmlkZXIsICR1cmxSb3V0ZXJQcm92aWRlcikgLT5cbiAgJHN0YXRlUHJvdmlkZXIuc3RhdGUgXCJvdmVydmlld1wiLFxuICAgIHVybDogXCIvb3ZlcnZpZXdcIlxuICAgIHZpZXdzOlxuICAgICAgbWFpbjpcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvb3ZlcnZpZXcuaHRtbFwiXG4gICAgICAgIGNvbnRyb2xsZXI6ICdPdmVydmlld0NvbnRyb2xsZXInXG5cbiAgLnN0YXRlIFwicnVubmluZy1qb2JzXCIsXG4gICAgdXJsOiBcIi9ydW5uaW5nLWpvYnNcIlxuICAgIHZpZXdzOlxuICAgICAgbWFpbjpcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvam9icy9ydW5uaW5nLWpvYnMuaHRtbFwiXG4gICAgICAgIGNvbnRyb2xsZXI6ICdSdW5uaW5nSm9ic0NvbnRyb2xsZXInXG4gIFxuICAuc3RhdGUgXCJjb21wbGV0ZWQtam9ic1wiLFxuICAgIHVybDogXCIvY29tcGxldGVkLWpvYnNcIlxuICAgIHZpZXdzOlxuICAgICAgbWFpbjpcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvam9icy9jb21wbGV0ZWQtam9icy5odG1sXCJcbiAgICAgICAgY29udHJvbGxlcjogJ0NvbXBsZXRlZEpvYnNDb250cm9sbGVyJ1xuXG4gIC5zdGF0ZSBcInNpbmdsZS1qb2JcIixcbiAgICB1cmw6IFwiL2pvYnMve2pvYmlkfVwiXG4gICAgYWJzdHJhY3Q6IHRydWVcbiAgICB2aWV3czpcbiAgICAgIG1haW46XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYnMvam9iLmh0bWxcIlxuICAgICAgICBjb250cm9sbGVyOiAnU2luZ2xlSm9iQ29udHJvbGxlcidcblxuICAuc3RhdGUgXCJzaW5nbGUtam9iLnBsYW5cIixcbiAgICB1cmw6IFwiXCJcbiAgICB2aWV3czpcbiAgICAgIGRldGFpbHM6XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYnMvam9iLnBsYW4uaHRtbFwiXG4gICAgICAgIGNvbnRyb2xsZXI6ICdKb2JQbGFuQ29udHJvbGxlcidcblxuICAuc3RhdGUgXCJzaW5nbGUtam9iLnBsYW4ubm9kZVwiLFxuICAgIHVybDogXCIve25vZGVpZDppbnR9XCJcbiAgICB2aWV3czpcbiAgICAgIG5vZGU6XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYnMvam9iLnBsYW4ubm9kZS5odG1sXCJcbiAgICAgICAgY29udHJvbGxlcjogJ0pvYlBsYW5Ob2RlQ29udHJvbGxlcidcblxuICAuc3RhdGUgXCJzaW5nbGUtam9iLnRpbWVsaW5lXCIsXG4gICAgdXJsOiBcIi90aW1lbGluZVwiXG4gICAgdmlld3M6XG4gICAgICBkZXRhaWxzOlxuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JzL2pvYi50aW1lbGluZS5odG1sXCJcblxuICAuc3RhdGUgXCJzaW5nbGUtam9iLnRpbWVsaW5lLnZlcnRleFwiLFxuICAgIHVybDogXCIve3ZlcnRleElkfVwiXG4gICAgdmlld3M6XG4gICAgICB2ZXJ0ZXg6XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYnMvam9iLnRpbWVsaW5lLnZlcnRleC5odG1sXCJcbiAgICAgICAgY29udHJvbGxlcjogJ0pvYlRpbWVsaW5lVmVydGV4Q29udHJvbGxlcidcblxuICAuc3RhdGUgXCJzaW5nbGUtam9iLnN0YXRpc3RpY3NcIixcbiAgICB1cmw6IFwiL3N0YXRpc3RpY3NcIlxuICAgIHZpZXdzOlxuICAgICAgZGV0YWlsczpcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvam9icy9qb2Iuc3RhdGlzdGljcy5odG1sXCJcblxuICAuc3RhdGUgXCJzaW5nbGUtam9iLmV4Y2VwdGlvbnNcIixcbiAgICB1cmw6IFwiL2V4Y2VwdGlvbnNcIlxuICAgIHZpZXdzOlxuICAgICAgZGV0YWlsczpcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvam9icy9qb2IuZXhjZXB0aW9ucy5odG1sXCJcblxuICAkdXJsUm91dGVyUHJvdmlkZXIub3RoZXJ3aXNlIFwiL292ZXJ2aWV3XCJcbiIsImFuZ3VsYXIubW9kdWxlKCdmbGlua0FwcCcsIFsndWkucm91dGVyJywgJ2FuZ3VsYXJNb21lbnQnXSkucnVuKGZ1bmN0aW9uKCRyb290U2NvcGUpIHtcbiAgJHJvb3RTY29wZS5zaWRlYmFyVmlzaWJsZSA9IGZhbHNlO1xuICByZXR1cm4gJHJvb3RTY29wZS5zaG93U2lkZWJhciA9IGZ1bmN0aW9uKCkge1xuICAgICRyb290U2NvcGUuc2lkZWJhclZpc2libGUgPSAhJHJvb3RTY29wZS5zaWRlYmFyVmlzaWJsZTtcbiAgICByZXR1cm4gJHJvb3RTY29wZS5zaWRlYmFyQ2xhc3MgPSAnZm9yY2Utc2hvdyc7XG4gIH07XG59KS5jb25zdGFudCgnZmxpbmtDb25maWcnLCB7XG4gIHdlYlNlcnZlcjogJ2h0dHA6Ly9sb2NhbGhvc3Q6ODA4MCcsXG4gIGpvYlNlcnZlcjogJ2h0dHA6Ly9sb2NhbGhvc3Q6ODA4MScsXG4gIG5ld1NlcnZlcjogJ2h0dHA6Ly9sb2NhbGhvc3Q6ODA4MicsXG4gIHJlZnJlc2hJbnRlcnZhbDogMTAwMDBcbn0pLnJ1bihmdW5jdGlvbihKb2JzU2VydmljZSwgZmxpbmtDb25maWcsICRpbnRlcnZhbCkge1xuICBKb2JzU2VydmljZS5saXN0Sm9icygpO1xuICByZXR1cm4gJGludGVydmFsKGZ1bmN0aW9uKCkge1xuICAgIHJldHVybiBKb2JzU2VydmljZS5saXN0Sm9icygpO1xuICB9LCBmbGlua0NvbmZpZy5yZWZyZXNoSW50ZXJ2YWwpO1xufSkuY29uZmlnKGZ1bmN0aW9uKCRzdGF0ZVByb3ZpZGVyLCAkdXJsUm91dGVyUHJvdmlkZXIpIHtcbiAgJHN0YXRlUHJvdmlkZXIuc3RhdGUoXCJvdmVydmlld1wiLCB7XG4gICAgdXJsOiBcIi9vdmVydmlld1wiLFxuICAgIHZpZXdzOiB7XG4gICAgICBtYWluOiB7XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL292ZXJ2aWV3Lmh0bWxcIixcbiAgICAgICAgY29udHJvbGxlcjogJ092ZXJ2aWV3Q29udHJvbGxlcidcbiAgICAgIH1cbiAgICB9XG4gIH0pLnN0YXRlKFwicnVubmluZy1qb2JzXCIsIHtcbiAgICB1cmw6IFwiL3J1bm5pbmctam9ic1wiLFxuICAgIHZpZXdzOiB7XG4gICAgICBtYWluOiB7XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYnMvcnVubmluZy1qb2JzLmh0bWxcIixcbiAgICAgICAgY29udHJvbGxlcjogJ1J1bm5pbmdKb2JzQ29udHJvbGxlcidcbiAgICAgIH1cbiAgICB9XG4gIH0pLnN0YXRlKFwiY29tcGxldGVkLWpvYnNcIiwge1xuICAgIHVybDogXCIvY29tcGxldGVkLWpvYnNcIixcbiAgICB2aWV3czoge1xuICAgICAgbWFpbjoge1xuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JzL2NvbXBsZXRlZC1qb2JzLmh0bWxcIixcbiAgICAgICAgY29udHJvbGxlcjogJ0NvbXBsZXRlZEpvYnNDb250cm9sbGVyJ1xuICAgICAgfVxuICAgIH1cbiAgfSkuc3RhdGUoXCJzaW5nbGUtam9iXCIsIHtcbiAgICB1cmw6IFwiL2pvYnMve2pvYmlkfVwiLFxuICAgIGFic3RyYWN0OiB0cnVlLFxuICAgIHZpZXdzOiB7XG4gICAgICBtYWluOiB7XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYnMvam9iLmh0bWxcIixcbiAgICAgICAgY29udHJvbGxlcjogJ1NpbmdsZUpvYkNvbnRyb2xsZXInXG4gICAgICB9XG4gICAgfVxuICB9KS5zdGF0ZShcInNpbmdsZS1qb2IucGxhblwiLCB7XG4gICAgdXJsOiBcIlwiLFxuICAgIHZpZXdzOiB7XG4gICAgICBkZXRhaWxzOiB7XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYnMvam9iLnBsYW4uaHRtbFwiLFxuICAgICAgICBjb250cm9sbGVyOiAnSm9iUGxhbkNvbnRyb2xsZXInXG4gICAgICB9XG4gICAgfVxuICB9KS5zdGF0ZShcInNpbmdsZS1qb2IucGxhbi5ub2RlXCIsIHtcbiAgICB1cmw6IFwiL3tub2RlaWQ6aW50fVwiLFxuICAgIHZpZXdzOiB7XG4gICAgICBub2RlOiB7XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYnMvam9iLnBsYW4ubm9kZS5odG1sXCIsXG4gICAgICAgIGNvbnRyb2xsZXI6ICdKb2JQbGFuTm9kZUNvbnRyb2xsZXInXG4gICAgICB9XG4gICAgfVxuICB9KS5zdGF0ZShcInNpbmdsZS1qb2IudGltZWxpbmVcIiwge1xuICAgIHVybDogXCIvdGltZWxpbmVcIixcbiAgICB2aWV3czoge1xuICAgICAgZGV0YWlsczoge1xuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JzL2pvYi50aW1lbGluZS5odG1sXCJcbiAgICAgIH1cbiAgICB9XG4gIH0pLnN0YXRlKFwic2luZ2xlLWpvYi50aW1lbGluZS52ZXJ0ZXhcIiwge1xuICAgIHVybDogXCIve3ZlcnRleElkfVwiLFxuICAgIHZpZXdzOiB7XG4gICAgICB2ZXJ0ZXg6IHtcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvam9icy9qb2IudGltZWxpbmUudmVydGV4Lmh0bWxcIixcbiAgICAgICAgY29udHJvbGxlcjogJ0pvYlRpbWVsaW5lVmVydGV4Q29udHJvbGxlcidcbiAgICAgIH1cbiAgICB9XG4gIH0pLnN0YXRlKFwic2luZ2xlLWpvYi5zdGF0aXN0aWNzXCIsIHtcbiAgICB1cmw6IFwiL3N0YXRpc3RpY3NcIixcbiAgICB2aWV3czoge1xuICAgICAgZGV0YWlsczoge1xuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JzL2pvYi5zdGF0aXN0aWNzLmh0bWxcIlxuICAgICAgfVxuICAgIH1cbiAgfSkuc3RhdGUoXCJzaW5nbGUtam9iLmV4Y2VwdGlvbnNcIiwge1xuICAgIHVybDogXCIvZXhjZXB0aW9uc1wiLFxuICAgIHZpZXdzOiB7XG4gICAgICBkZXRhaWxzOiB7XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYnMvam9iLmV4Y2VwdGlvbnMuaHRtbFwiXG4gICAgICB9XG4gICAgfVxuICB9KTtcbiAgcmV0dXJuICR1cmxSb3V0ZXJQcm92aWRlci5vdGhlcndpc2UoXCIvb3ZlcnZpZXdcIik7XG59KTtcbiIsIiNcbiMgTGljZW5zZWQgdG8gdGhlIEFwYWNoZSBTb2Z0d2FyZSBGb3VuZGF0aW9uIChBU0YpIHVuZGVyIG9uZVxuIyBvciBtb3JlIGNvbnRyaWJ1dG9yIGxpY2Vuc2UgYWdyZWVtZW50cy4gIFNlZSB0aGUgTk9USUNFIGZpbGVcbiMgZGlzdHJpYnV0ZWQgd2l0aCB0aGlzIHdvcmsgZm9yIGFkZGl0aW9uYWwgaW5mb3JtYXRpb25cbiMgcmVnYXJkaW5nIGNvcHlyaWdodCBvd25lcnNoaXAuICBUaGUgQVNGIGxpY2Vuc2VzIHRoaXMgZmlsZVxuIyB0byB5b3UgdW5kZXIgdGhlIEFwYWNoZSBMaWNlbnNlLCBWZXJzaW9uIDIuMCAodGhlXG4jIFwiTGljZW5zZVwiKTsgeW91IG1heSBub3QgdXNlIHRoaXMgZmlsZSBleGNlcHQgaW4gY29tcGxpYW5jZVxuIyB3aXRoIHRoZSBMaWNlbnNlLiAgWW91IG1heSBvYnRhaW4gYSBjb3B5IG9mIHRoZSBMaWNlbnNlIGF0XG4jXG4jICAgICBodHRwOi8vd3d3LmFwYWNoZS5vcmcvbGljZW5zZXMvTElDRU5TRS0yLjBcbiNcbiMgVW5sZXNzIHJlcXVpcmVkIGJ5IGFwcGxpY2FibGUgbGF3IG9yIGFncmVlZCB0byBpbiB3cml0aW5nLCBzb2Z0d2FyZVxuIyBkaXN0cmlidXRlZCB1bmRlciB0aGUgTGljZW5zZSBpcyBkaXN0cmlidXRlZCBvbiBhbiBcIkFTIElTXCIgQkFTSVMsXG4jIFdJVEhPVVQgV0FSUkFOVElFUyBPUiBDT05ESVRJT05TIE9GIEFOWSBLSU5ELCBlaXRoZXIgZXhwcmVzcyBvciBpbXBsaWVkLlxuIyBTZWUgdGhlIExpY2Vuc2UgZm9yIHRoZSBzcGVjaWZpYyBsYW5ndWFnZSBnb3Zlcm5pbmcgcGVybWlzc2lvbnMgYW5kXG4jIGxpbWl0YXRpb25zIHVuZGVyIHRoZSBMaWNlbnNlLlxuI1xuXG5hbmd1bGFyLm1vZHVsZSgnZmxpbmtBcHAnKVxuXG4jIC0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS1cblxuLmRpcmVjdGl2ZSAnYnNMYWJlbCcsIChKb2JzU2VydmljZSkgLT5cbiAgdHJhbnNjbHVkZTogdHJ1ZVxuICByZXBsYWNlOiB0cnVlXG4gIHNjb3BlOiBcbiAgICBnZXRMYWJlbENsYXNzOiBcIiZcIlxuICAgIHN0YXR1czogXCJAXCJcblxuICB0ZW1wbGF0ZTogXCI8c3BhbiB0aXRsZT0ne3tzdGF0dXN9fScgbmctY2xhc3M9J2dldExhYmVsQ2xhc3MoKSc+PG5nLXRyYW5zY2x1ZGU+PC9uZy10cmFuc2NsdWRlPjwvc3Bhbj5cIlxuICBcbiAgbGluazogKHNjb3BlLCBlbGVtZW50LCBhdHRycykgLT5cbiAgICBzY29wZS5nZXRMYWJlbENsYXNzID0gLT5cbiAgICAgICdsYWJlbCBsYWJlbC0nICsgSm9ic1NlcnZpY2UudHJhbnNsYXRlTGFiZWxTdGF0ZShhdHRycy5zdGF0dXMpXG5cbiMgLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLVxuXG4uZGlyZWN0aXZlICdpbmRpY2F0b3JQcmltYXJ5JywgKEpvYnNTZXJ2aWNlKSAtPlxuICByZXBsYWNlOiB0cnVlXG4gIHNjb3BlOiBcbiAgICBnZXRMYWJlbENsYXNzOiBcIiZcIlxuICAgIHN0YXR1czogJ0AnXG5cbiAgdGVtcGxhdGU6IFwiPGkgdGl0bGU9J3t7c3RhdHVzfX0nIG5nLWNsYXNzPSdnZXRMYWJlbENsYXNzKCknIC8+XCJcbiAgXG4gIGxpbms6IChzY29wZSwgZWxlbWVudCwgYXR0cnMpIC0+XG4gICAgc2NvcGUuZ2V0TGFiZWxDbGFzcyA9IC0+XG4gICAgICAnZmEgZmEtY2lyY2xlIGluZGljYXRvciBpbmRpY2F0b3ItJyArIEpvYnNTZXJ2aWNlLnRyYW5zbGF0ZUxhYmVsU3RhdGUoYXR0cnMuc3RhdHVzKVxuXG4jIC0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS1cblxuLmRpcmVjdGl2ZSAndGFibGVQcm9wZXJ0eScsIC0+XG4gIHJlcGxhY2U6IHRydWVcbiAgc2NvcGU6XG4gICAgdmFsdWU6ICc9J1xuXG4gIHRlbXBsYXRlOiBcIjx0ZCB0aXRsZT1cXFwie3t2YWx1ZSB8fCAnTm9uZSd9fVxcXCI+e3t2YWx1ZSB8fCAnTm9uZSd9fTwvdGQ+XCJcbiIsImFuZ3VsYXIubW9kdWxlKCdmbGlua0FwcCcpLmRpcmVjdGl2ZSgnYnNMYWJlbCcsIGZ1bmN0aW9uKEpvYnNTZXJ2aWNlKSB7XG4gIHJldHVybiB7XG4gICAgdHJhbnNjbHVkZTogdHJ1ZSxcbiAgICByZXBsYWNlOiB0cnVlLFxuICAgIHNjb3BlOiB7XG4gICAgICBnZXRMYWJlbENsYXNzOiBcIiZcIixcbiAgICAgIHN0YXR1czogXCJAXCJcbiAgICB9LFxuICAgIHRlbXBsYXRlOiBcIjxzcGFuIHRpdGxlPSd7e3N0YXR1c319JyBuZy1jbGFzcz0nZ2V0TGFiZWxDbGFzcygpJz48bmctdHJhbnNjbHVkZT48L25nLXRyYW5zY2x1ZGU+PC9zcGFuPlwiLFxuICAgIGxpbms6IGZ1bmN0aW9uKHNjb3BlLCBlbGVtZW50LCBhdHRycykge1xuICAgICAgcmV0dXJuIHNjb3BlLmdldExhYmVsQ2xhc3MgPSBmdW5jdGlvbigpIHtcbiAgICAgICAgcmV0dXJuICdsYWJlbCBsYWJlbC0nICsgSm9ic1NlcnZpY2UudHJhbnNsYXRlTGFiZWxTdGF0ZShhdHRycy5zdGF0dXMpO1xuICAgICAgfTtcbiAgICB9XG4gIH07XG59KS5kaXJlY3RpdmUoJ2luZGljYXRvclByaW1hcnknLCBmdW5jdGlvbihKb2JzU2VydmljZSkge1xuICByZXR1cm4ge1xuICAgIHJlcGxhY2U6IHRydWUsXG4gICAgc2NvcGU6IHtcbiAgICAgIGdldExhYmVsQ2xhc3M6IFwiJlwiLFxuICAgICAgc3RhdHVzOiAnQCdcbiAgICB9LFxuICAgIHRlbXBsYXRlOiBcIjxpIHRpdGxlPSd7e3N0YXR1c319JyBuZy1jbGFzcz0nZ2V0TGFiZWxDbGFzcygpJyAvPlwiLFxuICAgIGxpbms6IGZ1bmN0aW9uKHNjb3BlLCBlbGVtZW50LCBhdHRycykge1xuICAgICAgcmV0dXJuIHNjb3BlLmdldExhYmVsQ2xhc3MgPSBmdW5jdGlvbigpIHtcbiAgICAgICAgcmV0dXJuICdmYSBmYS1jaXJjbGUgaW5kaWNhdG9yIGluZGljYXRvci0nICsgSm9ic1NlcnZpY2UudHJhbnNsYXRlTGFiZWxTdGF0ZShhdHRycy5zdGF0dXMpO1xuICAgICAgfTtcbiAgICB9XG4gIH07XG59KS5kaXJlY3RpdmUoJ3RhYmxlUHJvcGVydHknLCBmdW5jdGlvbigpIHtcbiAgcmV0dXJuIHtcbiAgICByZXBsYWNlOiB0cnVlLFxuICAgIHNjb3BlOiB7XG4gICAgICB2YWx1ZTogJz0nXG4gICAgfSxcbiAgICB0ZW1wbGF0ZTogXCI8dGQgdGl0bGU9XFxcInt7dmFsdWUgfHwgJ05vbmUnfX1cXFwiPnt7dmFsdWUgfHwgJ05vbmUnfX08L3RkPlwiXG4gIH07XG59KTtcbiIsIiNcbiMgTGljZW5zZWQgdG8gdGhlIEFwYWNoZSBTb2Z0d2FyZSBGb3VuZGF0aW9uIChBU0YpIHVuZGVyIG9uZVxuIyBvciBtb3JlIGNvbnRyaWJ1dG9yIGxpY2Vuc2UgYWdyZWVtZW50cy4gIFNlZSB0aGUgTk9USUNFIGZpbGVcbiMgZGlzdHJpYnV0ZWQgd2l0aCB0aGlzIHdvcmsgZm9yIGFkZGl0aW9uYWwgaW5mb3JtYXRpb25cbiMgcmVnYXJkaW5nIGNvcHlyaWdodCBvd25lcnNoaXAuICBUaGUgQVNGIGxpY2Vuc2VzIHRoaXMgZmlsZVxuIyB0byB5b3UgdW5kZXIgdGhlIEFwYWNoZSBMaWNlbnNlLCBWZXJzaW9uIDIuMCAodGhlXG4jIFwiTGljZW5zZVwiKTsgeW91IG1heSBub3QgdXNlIHRoaXMgZmlsZSBleGNlcHQgaW4gY29tcGxpYW5jZVxuIyB3aXRoIHRoZSBMaWNlbnNlLiAgWW91IG1heSBvYnRhaW4gYSBjb3B5IG9mIHRoZSBMaWNlbnNlIGF0XG4jXG4jICAgICBodHRwOi8vd3d3LmFwYWNoZS5vcmcvbGljZW5zZXMvTElDRU5TRS0yLjBcbiNcbiMgVW5sZXNzIHJlcXVpcmVkIGJ5IGFwcGxpY2FibGUgbGF3IG9yIGFncmVlZCB0byBpbiB3cml0aW5nLCBzb2Z0d2FyZVxuIyBkaXN0cmlidXRlZCB1bmRlciB0aGUgTGljZW5zZSBpcyBkaXN0cmlidXRlZCBvbiBhbiBcIkFTIElTXCIgQkFTSVMsXG4jIFdJVEhPVVQgV0FSUkFOVElFUyBPUiBDT05ESVRJT05TIE9GIEFOWSBLSU5ELCBlaXRoZXIgZXhwcmVzcyBvciBpbXBsaWVkLlxuIyBTZWUgdGhlIExpY2Vuc2UgZm9yIHRoZSBzcGVjaWZpYyBsYW5ndWFnZSBnb3Zlcm5pbmcgcGVybWlzc2lvbnMgYW5kXG4jIGxpbWl0YXRpb25zIHVuZGVyIHRoZSBMaWNlbnNlLlxuI1xuXG5hbmd1bGFyLm1vZHVsZSgnZmxpbmtBcHAnKVxuXG4uZmlsdGVyIFwiYW1EdXJhdGlvbkZvcm1hdEV4dGVuZGVkXCIsIChhbmd1bGFyTW9tZW50Q29uZmlnKSAtPlxuICBhbUR1cmF0aW9uRm9ybWF0RXh0ZW5kZWRGaWx0ZXIgPSAodmFsdWUsIGZvcm1hdCwgZHVyYXRpb25Gb3JtYXQpIC0+XG4gICAgcmV0dXJuIFwiXCIgIGlmIHR5cGVvZiB2YWx1ZSBpcyBcInVuZGVmaW5lZFwiIG9yIHZhbHVlIGlzIG51bGxcblxuICAgIG1vbWVudC5kdXJhdGlvbih2YWx1ZSwgZm9ybWF0KS5mb3JtYXQoZHVyYXRpb25Gb3JtYXQsIHsgdHJpbTogZmFsc2UgfSlcblxuICBhbUR1cmF0aW9uRm9ybWF0RXh0ZW5kZWRGaWx0ZXIuJHN0YXRlZnVsID0gYW5ndWxhck1vbWVudENvbmZpZy5zdGF0ZWZ1bEZpbHRlcnNcblxuICBhbUR1cmF0aW9uRm9ybWF0RXh0ZW5kZWRGaWx0ZXJcbiIsImFuZ3VsYXIubW9kdWxlKCdmbGlua0FwcCcpLmZpbHRlcihcImFtRHVyYXRpb25Gb3JtYXRFeHRlbmRlZFwiLCBmdW5jdGlvbihhbmd1bGFyTW9tZW50Q29uZmlnKSB7XG4gIHZhciBhbUR1cmF0aW9uRm9ybWF0RXh0ZW5kZWRGaWx0ZXI7XG4gIGFtRHVyYXRpb25Gb3JtYXRFeHRlbmRlZEZpbHRlciA9IGZ1bmN0aW9uKHZhbHVlLCBmb3JtYXQsIGR1cmF0aW9uRm9ybWF0KSB7XG4gICAgaWYgKHR5cGVvZiB2YWx1ZSA9PT0gXCJ1bmRlZmluZWRcIiB8fCB2YWx1ZSA9PT0gbnVsbCkge1xuICAgICAgcmV0dXJuIFwiXCI7XG4gICAgfVxuICAgIHJldHVybiBtb21lbnQuZHVyYXRpb24odmFsdWUsIGZvcm1hdCkuZm9ybWF0KGR1cmF0aW9uRm9ybWF0LCB7XG4gICAgICB0cmltOiBmYWxzZVxuICAgIH0pO1xuICB9O1xuICBhbUR1cmF0aW9uRm9ybWF0RXh0ZW5kZWRGaWx0ZXIuJHN0YXRlZnVsID0gYW5ndWxhck1vbWVudENvbmZpZy5zdGF0ZWZ1bEZpbHRlcnM7XG4gIHJldHVybiBhbUR1cmF0aW9uRm9ybWF0RXh0ZW5kZWRGaWx0ZXI7XG59KTtcbiIsIiNcbiMgTGljZW5zZWQgdG8gdGhlIEFwYWNoZSBTb2Z0d2FyZSBGb3VuZGF0aW9uIChBU0YpIHVuZGVyIG9uZVxuIyBvciBtb3JlIGNvbnRyaWJ1dG9yIGxpY2Vuc2UgYWdyZWVtZW50cy4gIFNlZSB0aGUgTk9USUNFIGZpbGVcbiMgZGlzdHJpYnV0ZWQgd2l0aCB0aGlzIHdvcmsgZm9yIGFkZGl0aW9uYWwgaW5mb3JtYXRpb25cbiMgcmVnYXJkaW5nIGNvcHlyaWdodCBvd25lcnNoaXAuICBUaGUgQVNGIGxpY2Vuc2VzIHRoaXMgZmlsZVxuIyB0byB5b3UgdW5kZXIgdGhlIEFwYWNoZSBMaWNlbnNlLCBWZXJzaW9uIDIuMCAodGhlXG4jIFwiTGljZW5zZVwiKTsgeW91IG1heSBub3QgdXNlIHRoaXMgZmlsZSBleGNlcHQgaW4gY29tcGxpYW5jZVxuIyB3aXRoIHRoZSBMaWNlbnNlLiAgWW91IG1heSBvYnRhaW4gYSBjb3B5IG9mIHRoZSBMaWNlbnNlIGF0XG4jXG4jICAgICBodHRwOi8vd3d3LmFwYWNoZS5vcmcvbGljZW5zZXMvTElDRU5TRS0yLjBcbiNcbiMgVW5sZXNzIHJlcXVpcmVkIGJ5IGFwcGxpY2FibGUgbGF3IG9yIGFncmVlZCB0byBpbiB3cml0aW5nLCBzb2Z0d2FyZVxuIyBkaXN0cmlidXRlZCB1bmRlciB0aGUgTGljZW5zZSBpcyBkaXN0cmlidXRlZCBvbiBhbiBcIkFTIElTXCIgQkFTSVMsXG4jIFdJVEhPVVQgV0FSUkFOVElFUyBPUiBDT05ESVRJT05TIE9GIEFOWSBLSU5ELCBlaXRoZXIgZXhwcmVzcyBvciBpbXBsaWVkLlxuIyBTZWUgdGhlIExpY2Vuc2UgZm9yIHRoZSBzcGVjaWZpYyBsYW5ndWFnZSBnb3Zlcm5pbmcgcGVybWlzc2lvbnMgYW5kXG4jIGxpbWl0YXRpb25zIHVuZGVyIHRoZSBMaWNlbnNlLlxuI1xuXG5hbmd1bGFyLm1vZHVsZSgnZmxpbmtBcHAnKVxuXG4uY29udHJvbGxlciAnUnVubmluZ0pvYnNDb250cm9sbGVyJywgKCRzY29wZSwgJHN0YXRlLCAkc3RhdGVQYXJhbXMsIEpvYnNTZXJ2aWNlKSAtPlxuICAkc2NvcGUuam9iT2JzZXJ2ZXIgPSAtPlxuICAgICRzY29wZS5qb2JzID0gSm9ic1NlcnZpY2UuZ2V0Sm9icygncnVubmluZycpXG5cbiAgSm9ic1NlcnZpY2UucmVnaXN0ZXJPYnNlcnZlcigkc2NvcGUuam9iT2JzZXJ2ZXIpXG4gICRzY29wZS4kb24gJyRkZXN0cm95JywgLT5cbiAgICBKb2JzU2VydmljZS51blJlZ2lzdGVyT2JzZXJ2ZXIoJHNjb3BlLmpvYk9ic2VydmVyKVxuXG4gICRzY29wZS5qb2JPYnNlcnZlcigpXG5cbiMgLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS1cblxuLmNvbnRyb2xsZXIgJ0NvbXBsZXRlZEpvYnNDb250cm9sbGVyJywgKCRzY29wZSwgJHN0YXRlLCAkc3RhdGVQYXJhbXMsIEpvYnNTZXJ2aWNlKSAtPlxuICAkc2NvcGUuam9iT2JzZXJ2ZXIgPSAtPlxuICAgICRzY29wZS5qb2JzID0gSm9ic1NlcnZpY2UuZ2V0Sm9icygnZmluaXNoZWQnKVxuXG4gIEpvYnNTZXJ2aWNlLnJlZ2lzdGVyT2JzZXJ2ZXIoJHNjb3BlLmpvYk9ic2VydmVyKVxuICAkc2NvcGUuJG9uICckZGVzdHJveScsIC0+XG4gICAgSm9ic1NlcnZpY2UudW5SZWdpc3Rlck9ic2VydmVyKCRzY29wZS5qb2JPYnNlcnZlcilcblxuICAkc2NvcGUuam9iT2JzZXJ2ZXIoKVxuXG4jIC0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tXG5cbi5jb250cm9sbGVyICdTaW5nbGVKb2JDb250cm9sbGVyJywgKCRzY29wZSwgJHN0YXRlLCAkc3RhdGVQYXJhbXMsIEpvYnNTZXJ2aWNlLCAkcm9vdFNjb3BlKSAtPlxuICAkc2NvcGUuam9iaWQgPSAkc3RhdGVQYXJhbXMuam9iaWRcbiAgJHJvb3RTY29wZS5qb2IgPSBudWxsXG5cbiAgSm9ic1NlcnZpY2UubG9hZEpvYigkc3RhdGVQYXJhbXMuam9iaWQpLnRoZW4gKGRhdGEpIC0+XG4gICAgJHJvb3RTY29wZS5qb2IgPSBkYXRhXG5cbiAgJHNjb3BlLiRvbiAnJGRlc3Ryb3knLCAtPlxuICAgICRyb290U2NvcGUuam9iID0gbnVsbFxuXG4jIC0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tXG5cbi5jb250cm9sbGVyICdKb2JQbGFuQ29udHJvbGxlcicsICgkc2NvcGUsICRzdGF0ZSwgJHN0YXRlUGFyYW1zLCBKb2JzU2VydmljZSkgLT5cbiAgSm9ic1NlcnZpY2UubG9hZFBsYW4oJHN0YXRlUGFyYW1zLmpvYmlkKS50aGVuIChkYXRhKSAtPlxuICAgICRzY29wZS5wbGFuID0gZGF0YVxuXG4jIC0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tXG5cbi5jb250cm9sbGVyICdKb2JQbGFuTm9kZUNvbnRyb2xsZXInLCAoJHNjb3BlLCAkc3RhdGUsICRzdGF0ZVBhcmFtcywgSm9ic1NlcnZpY2UpIC0+XG4gICRzY29wZS5ub2RlaWQgPSAkc3RhdGVQYXJhbXMubm9kZWlkXG4gICRzY29wZS5zdGF0ZUxpc3QgPSBKb2JzU2VydmljZS5zdGF0ZUxpc3QoKVxuXG4gIEpvYnNTZXJ2aWNlLmdldE5vZGUoJHNjb3BlLm5vZGVpZCkudGhlbiAoZGF0YSkgLT5cbiAgICAkc2NvcGUubm9kZSA9IGRhdGFcblxuIyAtLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLVxuXG4uY29udHJvbGxlciAnSm9iVGltZWxpbmVWZXJ0ZXhDb250cm9sbGVyJywgKCRzY29wZSwgJHN0YXRlLCAkc3RhdGVQYXJhbXMsIEpvYnNTZXJ2aWNlKSAtPlxuICBKb2JzU2VydmljZS5nZXRWZXJ0ZXgoJHN0YXRlUGFyYW1zLmpvYmlkLCAkc3RhdGVQYXJhbXMudmVydGV4SWQpLnRoZW4gKGRhdGEpIC0+XG4gICAgJHNjb3BlLnZlcnRleCA9IGRhdGFcbiIsImFuZ3VsYXIubW9kdWxlKCdmbGlua0FwcCcpLmNvbnRyb2xsZXIoJ1J1bm5pbmdKb2JzQ29udHJvbGxlcicsIGZ1bmN0aW9uKCRzY29wZSwgJHN0YXRlLCAkc3RhdGVQYXJhbXMsIEpvYnNTZXJ2aWNlKSB7XG4gICRzY29wZS5qb2JPYnNlcnZlciA9IGZ1bmN0aW9uKCkge1xuICAgIHJldHVybiAkc2NvcGUuam9icyA9IEpvYnNTZXJ2aWNlLmdldEpvYnMoJ3J1bm5pbmcnKTtcbiAgfTtcbiAgSm9ic1NlcnZpY2UucmVnaXN0ZXJPYnNlcnZlcigkc2NvcGUuam9iT2JzZXJ2ZXIpO1xuICAkc2NvcGUuJG9uKCckZGVzdHJveScsIGZ1bmN0aW9uKCkge1xuICAgIHJldHVybiBKb2JzU2VydmljZS51blJlZ2lzdGVyT2JzZXJ2ZXIoJHNjb3BlLmpvYk9ic2VydmVyKTtcbiAgfSk7XG4gIHJldHVybiAkc2NvcGUuam9iT2JzZXJ2ZXIoKTtcbn0pLmNvbnRyb2xsZXIoJ0NvbXBsZXRlZEpvYnNDb250cm9sbGVyJywgZnVuY3Rpb24oJHNjb3BlLCAkc3RhdGUsICRzdGF0ZVBhcmFtcywgSm9ic1NlcnZpY2UpIHtcbiAgJHNjb3BlLmpvYk9ic2VydmVyID0gZnVuY3Rpb24oKSB7XG4gICAgcmV0dXJuICRzY29wZS5qb2JzID0gSm9ic1NlcnZpY2UuZ2V0Sm9icygnZmluaXNoZWQnKTtcbiAgfTtcbiAgSm9ic1NlcnZpY2UucmVnaXN0ZXJPYnNlcnZlcigkc2NvcGUuam9iT2JzZXJ2ZXIpO1xuICAkc2NvcGUuJG9uKCckZGVzdHJveScsIGZ1bmN0aW9uKCkge1xuICAgIHJldHVybiBKb2JzU2VydmljZS51blJlZ2lzdGVyT2JzZXJ2ZXIoJHNjb3BlLmpvYk9ic2VydmVyKTtcbiAgfSk7XG4gIHJldHVybiAkc2NvcGUuam9iT2JzZXJ2ZXIoKTtcbn0pLmNvbnRyb2xsZXIoJ1NpbmdsZUpvYkNvbnRyb2xsZXInLCBmdW5jdGlvbigkc2NvcGUsICRzdGF0ZSwgJHN0YXRlUGFyYW1zLCBKb2JzU2VydmljZSwgJHJvb3RTY29wZSkge1xuICAkc2NvcGUuam9iaWQgPSAkc3RhdGVQYXJhbXMuam9iaWQ7XG4gICRyb290U2NvcGUuam9iID0gbnVsbDtcbiAgSm9ic1NlcnZpY2UubG9hZEpvYigkc3RhdGVQYXJhbXMuam9iaWQpLnRoZW4oZnVuY3Rpb24oZGF0YSkge1xuICAgIHJldHVybiAkcm9vdFNjb3BlLmpvYiA9IGRhdGE7XG4gIH0pO1xuICByZXR1cm4gJHNjb3BlLiRvbignJGRlc3Ryb3knLCBmdW5jdGlvbigpIHtcbiAgICByZXR1cm4gJHJvb3RTY29wZS5qb2IgPSBudWxsO1xuICB9KTtcbn0pLmNvbnRyb2xsZXIoJ0pvYlBsYW5Db250cm9sbGVyJywgZnVuY3Rpb24oJHNjb3BlLCAkc3RhdGUsICRzdGF0ZVBhcmFtcywgSm9ic1NlcnZpY2UpIHtcbiAgcmV0dXJuIEpvYnNTZXJ2aWNlLmxvYWRQbGFuKCRzdGF0ZVBhcmFtcy5qb2JpZCkudGhlbihmdW5jdGlvbihkYXRhKSB7XG4gICAgcmV0dXJuICRzY29wZS5wbGFuID0gZGF0YTtcbiAgfSk7XG59KS5jb250cm9sbGVyKCdKb2JQbGFuTm9kZUNvbnRyb2xsZXInLCBmdW5jdGlvbigkc2NvcGUsICRzdGF0ZSwgJHN0YXRlUGFyYW1zLCBKb2JzU2VydmljZSkge1xuICAkc2NvcGUubm9kZWlkID0gJHN0YXRlUGFyYW1zLm5vZGVpZDtcbiAgJHNjb3BlLnN0YXRlTGlzdCA9IEpvYnNTZXJ2aWNlLnN0YXRlTGlzdCgpO1xuICByZXR1cm4gSm9ic1NlcnZpY2UuZ2V0Tm9kZSgkc2NvcGUubm9kZWlkKS50aGVuKGZ1bmN0aW9uKGRhdGEpIHtcbiAgICByZXR1cm4gJHNjb3BlLm5vZGUgPSBkYXRhO1xuICB9KTtcbn0pLmNvbnRyb2xsZXIoJ0pvYlRpbWVsaW5lVmVydGV4Q29udHJvbGxlcicsIGZ1bmN0aW9uKCRzY29wZSwgJHN0YXRlLCAkc3RhdGVQYXJhbXMsIEpvYnNTZXJ2aWNlKSB7XG4gIHJldHVybiBKb2JzU2VydmljZS5nZXRWZXJ0ZXgoJHN0YXRlUGFyYW1zLmpvYmlkLCAkc3RhdGVQYXJhbXMudmVydGV4SWQpLnRoZW4oZnVuY3Rpb24oZGF0YSkge1xuICAgIHJldHVybiAkc2NvcGUudmVydGV4ID0gZGF0YTtcbiAgfSk7XG59KTtcbiIsIiNcbiMgTGljZW5zZWQgdG8gdGhlIEFwYWNoZSBTb2Z0d2FyZSBGb3VuZGF0aW9uIChBU0YpIHVuZGVyIG9uZVxuIyBvciBtb3JlIGNvbnRyaWJ1dG9yIGxpY2Vuc2UgYWdyZWVtZW50cy4gIFNlZSB0aGUgTk9USUNFIGZpbGVcbiMgZGlzdHJpYnV0ZWQgd2l0aCB0aGlzIHdvcmsgZm9yIGFkZGl0aW9uYWwgaW5mb3JtYXRpb25cbiMgcmVnYXJkaW5nIGNvcHlyaWdodCBvd25lcnNoaXAuICBUaGUgQVNGIGxpY2Vuc2VzIHRoaXMgZmlsZVxuIyB0byB5b3UgdW5kZXIgdGhlIEFwYWNoZSBMaWNlbnNlLCBWZXJzaW9uIDIuMCAodGhlXG4jIFwiTGljZW5zZVwiKTsgeW91IG1heSBub3QgdXNlIHRoaXMgZmlsZSBleGNlcHQgaW4gY29tcGxpYW5jZVxuIyB3aXRoIHRoZSBMaWNlbnNlLiAgWW91IG1heSBvYnRhaW4gYSBjb3B5IG9mIHRoZSBMaWNlbnNlIGF0XG4jXG4jICAgICBodHRwOi8vd3d3LmFwYWNoZS5vcmcvbGljZW5zZXMvTElDRU5TRS0yLjBcbiNcbiMgVW5sZXNzIHJlcXVpcmVkIGJ5IGFwcGxpY2FibGUgbGF3IG9yIGFncmVlZCB0byBpbiB3cml0aW5nLCBzb2Z0d2FyZVxuIyBkaXN0cmlidXRlZCB1bmRlciB0aGUgTGljZW5zZSBpcyBkaXN0cmlidXRlZCBvbiBhbiBcIkFTIElTXCIgQkFTSVMsXG4jIFdJVEhPVVQgV0FSUkFOVElFUyBPUiBDT05ESVRJT05TIE9GIEFOWSBLSU5ELCBlaXRoZXIgZXhwcmVzcyBvciBpbXBsaWVkLlxuIyBTZWUgdGhlIExpY2Vuc2UgZm9yIHRoZSBzcGVjaWZpYyBsYW5ndWFnZSBnb3Zlcm5pbmcgcGVybWlzc2lvbnMgYW5kXG4jIGxpbWl0YXRpb25zIHVuZGVyIHRoZSBMaWNlbnNlLlxuI1xuXG5hbmd1bGFyLm1vZHVsZSgnZmxpbmtBcHAnKVxuXG4jIC0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS1cblxuLmRpcmVjdGl2ZSAndmVydGV4JywgKCRzdGF0ZSkgLT5cbiAgdGVtcGxhdGU6IFwiPHN2ZyBjbGFzcz0ndGltZWxpbmUgc2Vjb25kYXJ5JyB3aWR0aD0nMCcgaGVpZ2h0PScwJz48L3N2Zz5cIlxuXG4gIHNjb3BlOlxuICAgIGRhdGE6IFwiPVwiXG5cbiAgbGluazogKHNjb3BlLCBlbGVtLCBhdHRycykgLT5cbiAgICB6b29tID0gZDMuYmVoYXZpb3Iuem9vbSgpXG4gICAgc3ZnRWwgPSBlbGVtLmNoaWxkcmVuKClbMF1cblxuICAgIGNvbnRhaW5lclcgPSBlbGVtLndpZHRoKClcbiAgICBhbmd1bGFyLmVsZW1lbnQoc3ZnRWwpLmF0dHIoJ3dpZHRoJywgY29udGFpbmVyVyAtIDE2KVxuXG4gICAgYW5hbHl6ZVRpbWUgPSAoZGF0YSkgLT5cbiAgICAgIHRlc3REYXRhID0gW11cblxuICAgICAgYW5ndWxhci5mb3JFYWNoIGRhdGEuZ3JvdXB2ZXJ0ZXguZ3JvdXBtZW1iZXJzLCAodmVydGV4LCBpKSAtPlxuICAgICAgICB2VGltZSA9IGRhdGEudmVydGljZXRpbWVzW3ZlcnRleC52ZXJ0ZXhpZF1cblxuICAgICAgICB0ZXN0RGF0YS5wdXNoIHtcbiAgICAgICAgICBsYWJlbDogXCIje3ZlcnRleC52ZXJ0ZXhpbnN0YW5jZW5hbWV9ICgje2l9KVwiXG4gICAgICAgICAgdGltZXM6IFtcbiAgICAgICAgICAgIHtcbiAgICAgICAgICAgICAgbGFiZWw6IFwiU2NoZWR1bGVkXCJcbiAgICAgICAgICAgICAgY29sb3I6IFwiIzY2NlwiXG4gICAgICAgICAgICAgIHN0YXJ0aW5nX3RpbWU6IHZUaW1lW1wiU0NIRURVTEVEXCJdICogMTAwXG4gICAgICAgICAgICAgIGVuZGluZ190aW1lOiB2VGltZVtcIkRFUExPWUlOR1wiXSAqIDEwMFxuICAgICAgICAgICAgfVxuICAgICAgICAgICAge1xuICAgICAgICAgICAgICBsYWJlbDogXCJEZXBsb3lpbmdcIlxuICAgICAgICAgICAgICBjb2xvcjogXCIjYWFhXCJcbiAgICAgICAgICAgICAgc3RhcnRpbmdfdGltZTogdlRpbWVbXCJERVBMT1lJTkdcIl0gKiAxMDBcbiAgICAgICAgICAgICAgZW5kaW5nX3RpbWU6IHZUaW1lW1wiUlVOTklOR1wiXSAqIDEwMFxuICAgICAgICAgICAgfVxuICAgICAgICAgICAge1xuICAgICAgICAgICAgICBsYWJlbDogXCJSdW5uaW5nXCJcbiAgICAgICAgICAgICAgY29sb3I6IFwiI2RkZFwiXG4gICAgICAgICAgICAgIHN0YXJ0aW5nX3RpbWU6IHZUaW1lW1wiUlVOTklOR1wiXSAqIDEwMFxuICAgICAgICAgICAgICBlbmRpbmdfdGltZTogdlRpbWVbXCJGSU5JU0hFRFwiXSAqIDEwMFxuICAgICAgICAgICAgfVxuICAgICAgICAgIF1cbiAgICAgICAgfVxuXG4gICAgICBjaGFydCA9IGQzLnRpbWVsaW5lKCkuc3RhY2soKS50aWNrRm9ybWF0KHtcbiAgICAgICAgZm9ybWF0OiBkMy50aW1lLmZvcm1hdChcIiVTXCIpLFxuICAgICAgICAjIHRpY2tUaW1lOiBkMy50aW1lLm1pbGxpc2Vjb25kcyxcbiAgICAgICAgdGlja0ludGVydmFsOiAxLFxuICAgICAgICB0aWNrU2l6ZTogMVxuICAgICAgfSkubGFiZWxGb3JtYXQoKGxhYmVsKSAtPlxuICAgICAgICBsYWJlbFxuICAgICAgKS5tYXJnaW4oeyBsZWZ0OiAxMDAsIHJpZ2h0OiAwLCB0b3A6IDAsIGJvdHRvbTogMCB9KVxuXG4gICAgICBzdmcgPSBkMy5zZWxlY3Qoc3ZnRWwpXG4gICAgICAuZGF0dW0odGVzdERhdGEpXG4gICAgICAuY2FsbChjaGFydClcbiAgICAgIC5jYWxsKHpvb20pXG5cbiAgICAgIHN2Z0cgPSBzdmcuc2VsZWN0KFwiZ1wiKVxuXG4gICAgICB6b29tLm9uKFwiem9vbVwiLCAtPlxuICAgICAgICBldiA9IGQzLmV2ZW50XG5cbiAgICAgICAgc3ZnRy5zZWxlY3RBbGwoJ3JlY3QnKS5hdHRyKFwidHJhbnNmb3JtXCIsIFwidHJhbnNsYXRlKFwiICsgZXYudHJhbnNsYXRlWzBdICsgXCIsMCkgc2NhbGUoXCIgKyBldi5zY2FsZSArIFwiLDEpXCIpXG4gICAgICAgIHN2Z0cuc2VsZWN0QWxsKCd0ZXh0JykuYXR0cihcInRyYW5zZm9ybVwiLCBcInRyYW5zbGF0ZShcIiArIGV2LnRyYW5zbGF0ZVswXSArIFwiLDApIHNjYWxlKFwiICsgZXYuc2NhbGUgKyBcIiwxKVwiKVxuICAgICAgKVxuXG4gICAgICBiYm94ID0gc3ZnR1swXVswXS5nZXRCQm94KClcbiAgICAgIHN2Zy5hdHRyKCdoZWlnaHQnLCBiYm94LmhlaWdodCArIDMwKVxuXG4gICAgYW5hbHl6ZVRpbWUoc2NvcGUuZGF0YSlcblxuICAgIHJldHVyblxuXG4jIC0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS1cblxuLmRpcmVjdGl2ZSAndGltZWxpbmUnLCAoJHN0YXRlKSAtPlxuICB0ZW1wbGF0ZTogXCI8c3ZnIGNsYXNzPSd0aW1lbGluZScgd2lkdGg9JzAnIGhlaWdodD0nMCc+PC9zdmc+XCJcblxuICBzY29wZTpcbiAgICBqb2I6IFwiPVwiXG5cbiAgbGluazogKHNjb3BlLCBlbGVtLCBhdHRycykgLT5cbiAgICB6b29tID0gZDMuYmVoYXZpb3Iuem9vbSgpXG4gICAgc3ZnRWwgPSBlbGVtLmNoaWxkcmVuKClbMF1cblxuICAgIGNvbnRhaW5lclcgPSBlbGVtLndpZHRoKClcbiAgICBhbmd1bGFyLmVsZW1lbnQoc3ZnRWwpLmF0dHIoJ3dpZHRoJywgY29udGFpbmVyVyAtIDE2KVxuXG4gICAgYW5hbHl6ZVRpbWUgPSAoZGF0YSkgLT5cbiAgICAgIHRlc3REYXRhID0gW11cblxuICAgICAgYW5ndWxhci5mb3JFYWNoIGRhdGEub2xkVi5ncm91cHZlcnRpY2VzLCAodmVydGV4KSAtPlxuICAgICAgICB2VGltZSA9IGRhdGEub2xkVi5ncm91cHZlcnRpY2V0aW1lc1t2ZXJ0ZXguZ3JvdXB2ZXJ0ZXhpZF1cblxuICAgICAgICAjIGNvbnNvbGUubG9nIHZUaW1lLCB2ZXJ0ZXguZ3JvdXB2ZXJ0ZXhpZFxuXG4gICAgICAgIHRlc3REYXRhLnB1c2ggXG4gICAgICAgICAgdGltZXM6IFtcbiAgICAgICAgICAgIGxhYmVsOiB2ZXJ0ZXguZ3JvdXB2ZXJ0ZXhuYW1lXG4gICAgICAgICAgICBjb2xvcjogXCIjM2ZiNmQ4XCJcbiAgICAgICAgICAgIHN0YXJ0aW5nX3RpbWU6IHZUaW1lW1wiU1RBUlRFRFwiXVxuICAgICAgICAgICAgZW5kaW5nX3RpbWU6IHZUaW1lW1wiRU5ERURcIl1cbiAgICAgICAgICAgIGxpbms6IHZlcnRleC5ncm91cHZlcnRleGlkXG4gICAgICAgICAgXVxuXG4gICAgICBjaGFydCA9IGQzLnRpbWVsaW5lKCkuc3RhY2soKS5jbGljaygoZCwgaSwgZGF0dW0pIC0+XG4gICAgICAgICRzdGF0ZS5nbyBcInNpbmdsZS1qb2IudGltZWxpbmUudmVydGV4XCIsIHsgam9iaWQ6IGRhdGEuamlkLCB2ZXJ0ZXhJZDogZC5saW5rIH1cblxuICAgICAgKS50aWNrRm9ybWF0KHtcbiAgICAgICAgZm9ybWF0OiBkMy50aW1lLmZvcm1hdChcIiVTXCIpXG4gICAgICAgICMgdGlja1RpbWU6IGQzLnRpbWUubWlsbGlzZWNvbmRzXG4gICAgICAgIHRpY2tJbnRlcnZhbDogMVxuICAgICAgICB0aWNrU2l6ZTogMVxuICAgICAgfSkubWFyZ2luKHsgbGVmdDogMCwgcmlnaHQ6IDAsIHRvcDogMCwgYm90dG9tOiAwIH0pXG5cbiAgICAgIHN2ZyA9IGQzLnNlbGVjdChzdmdFbClcbiAgICAgIC5kYXR1bSh0ZXN0RGF0YSlcbiAgICAgIC5jYWxsKGNoYXJ0KVxuICAgICAgLmNhbGwoem9vbSlcblxuICAgICAgc3ZnRyA9IHN2Zy5zZWxlY3QoXCJnXCIpXG5cbiAgICAgIHpvb20ub24oXCJ6b29tXCIsIC0+XG4gICAgICAgIGV2ID0gZDMuZXZlbnRcblxuICAgICAgICBzdmdHLnNlbGVjdEFsbCgncmVjdCcpLmF0dHIoXCJ0cmFuc2Zvcm1cIiwgXCJ0cmFuc2xhdGUoXCIgKyBldi50cmFuc2xhdGVbMF0gKyBcIiwwKSBzY2FsZShcIiArIGV2LnNjYWxlICsgXCIsMSlcIilcbiAgICAgICAgc3ZnRy5zZWxlY3RBbGwoJ3RleHQnKS5hdHRyKFwidHJhbnNmb3JtXCIsIFwidHJhbnNsYXRlKFwiICsgZXYudHJhbnNsYXRlWzBdICsgXCIsMCkgc2NhbGUoXCIgKyBldi5zY2FsZSArIFwiLDEpXCIpXG4gICAgICApXG5cbiAgICAgIGJib3ggPSBzdmdHWzBdWzBdLmdldEJCb3goKVxuICAgICAgc3ZnLmF0dHIoJ2hlaWdodCcsIGJib3guaGVpZ2h0ICsgMzApXG5cbiAgICBzY29wZS4kd2F0Y2ggYXR0cnMuam9iLCAoZGF0YSkgLT5cbiAgICAgIGFuYWx5emVUaW1lKGRhdGEpIGlmIGRhdGFcblxuICAgIHJldHVyblxuXG4jIC0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS1cblxuLmRpcmVjdGl2ZSAnam9iUGxhbicsICgkdGltZW91dCkgLT5cbiAgdGVtcGxhdGU6IFwiXG4gICAgPHN2ZyBjbGFzcz0nZ3JhcGgnIHdpZHRoPSc1MDAnIGhlaWdodD0nNDAwJz48ZyAvPjwvc3ZnPlxuICAgIDxzdmcgY2xhc3M9J3RtcCcgd2lkdGg9JzEnIGhlaWdodD0nMSc+PGcgLz48L3N2Zz5cbiAgICA8ZGl2IGNsYXNzPSdidG4tZ3JvdXAgem9vbS1idXR0b25zJz5cbiAgICAgIDxhIGNsYXNzPSdidG4gYnRuLWRlZmF1bHQgem9vbS1pbicgbmctY2xpY2s9J3pvb21JbigpJz48aSBjbGFzcz0nZmEgZmEtcGx1cycgLz48L2E+XG4gICAgICA8YSBjbGFzcz0nYnRuIGJ0bi1kZWZhdWx0IHpvb20tb3V0JyBuZy1jbGljaz0nem9vbU91dCgpJz48aSBjbGFzcz0nZmEgZmEtbWludXMnIC8+PC9hPlxuICAgIDwvZGl2PlwiXG5cbiAgc2NvcGU6XG4gICAgcGxhbjogJz0nXG5cbiAgbGluazogKHNjb3BlLCBlbGVtLCBhdHRycykgLT5cbiAgICBtYWluWm9vbSA9IGQzLmJlaGF2aW9yLnpvb20oKVxuICAgIHN1YmdyYXBocyA9IFtdXG4gICAgam9iaWQgPSBhdHRycy5qb2JpZFxuXG4gICAgbWFpblN2Z0VsZW1lbnQgPSBlbGVtLmNoaWxkcmVuKClbMF1cbiAgICBtYWluRyA9IGVsZW0uY2hpbGRyZW4oKS5jaGlsZHJlbigpWzBdXG4gICAgbWFpblRtcEVsZW1lbnQgPSBlbGVtLmNoaWxkcmVuKClbMV1cblxuICAgIGQzbWFpblN2ZyA9IGQzLnNlbGVjdChtYWluU3ZnRWxlbWVudClcbiAgICBkM21haW5TdmdHID0gZDMuc2VsZWN0KG1haW5HKVxuICAgIGQzdG1wU3ZnID0gZDMuc2VsZWN0KG1haW5UbXBFbGVtZW50KVxuXG4gICAgIyBhbmd1bGFyLmVsZW1lbnQobWFpbkcpLmVtcHR5KClcblxuICAgIGNvbnRhaW5lclcgPSBlbGVtLndpZHRoKClcbiAgICBhbmd1bGFyLmVsZW1lbnQoZWxlbS5jaGlsZHJlbigpWzBdKS53aWR0aChjb250YWluZXJXKVxuXG4gICAgc2NvcGUuem9vbUluID0gLT5cbiAgICAgIGlmIG1haW5ab29tLnNjYWxlKCkgPCAyLjk5XG4gICAgICAgIFxuICAgICAgICAjIENhbGN1bGF0ZSBhbmQgc3RvcmUgbmV3IHZhbHVlcyBpbiB6b29tIG9iamVjdFxuICAgICAgICB0cmFuc2xhdGUgPSBtYWluWm9vbS50cmFuc2xhdGUoKVxuICAgICAgICB2MSA9IHRyYW5zbGF0ZVswXSAqIChtYWluWm9vbS5zY2FsZSgpICsgMC4xIC8gKG1haW5ab29tLnNjYWxlKCkpKVxuICAgICAgICB2MiA9IHRyYW5zbGF0ZVsxXSAqIChtYWluWm9vbS5zY2FsZSgpICsgMC4xIC8gKG1haW5ab29tLnNjYWxlKCkpKVxuICAgICAgICBtYWluWm9vbS5zY2FsZSBtYWluWm9vbS5zY2FsZSgpICsgMC4xXG4gICAgICAgIG1haW5ab29tLnRyYW5zbGF0ZSBbIHYxLCB2MiBdXG4gICAgICAgIFxuICAgICAgICAjIFRyYW5zZm9ybSBzdmdcbiAgICAgICAgZDNtYWluU3ZnRy5hdHRyIFwidHJhbnNmb3JtXCIsIFwidHJhbnNsYXRlKFwiICsgdjEgKyBcIixcIiArIHYyICsgXCIpIHNjYWxlKFwiICsgbWFpblpvb20uc2NhbGUoKSArIFwiKVwiXG5cbiAgICBzY29wZS56b29tT3V0ID0gLT5cbiAgICAgIGlmIG1haW5ab29tLnNjYWxlKCkgPiAwLjMxXG4gICAgICAgIFxuICAgICAgICAjIENhbGN1bGF0ZSBhbmQgc3RvcmUgbmV3IHZhbHVlcyBpbiBtYWluWm9vbSBvYmplY3RcbiAgICAgICAgbWFpblpvb20uc2NhbGUgbWFpblpvb20uc2NhbGUoKSAtIDAuMVxuICAgICAgICB0cmFuc2xhdGUgPSBtYWluWm9vbS50cmFuc2xhdGUoKVxuICAgICAgICB2MSA9IHRyYW5zbGF0ZVswXSAqIChtYWluWm9vbS5zY2FsZSgpIC0gMC4xIC8gKG1haW5ab29tLnNjYWxlKCkpKVxuICAgICAgICB2MiA9IHRyYW5zbGF0ZVsxXSAqIChtYWluWm9vbS5zY2FsZSgpIC0gMC4xIC8gKG1haW5ab29tLnNjYWxlKCkpKVxuICAgICAgICBtYWluWm9vbS50cmFuc2xhdGUgWyB2MSwgdjIgXVxuICAgICAgICBcbiAgICAgICAgIyBUcmFuc2Zvcm0gc3ZnXG4gICAgICAgIGQzbWFpblN2Z0cuYXR0ciBcInRyYW5zZm9ybVwiLCBcInRyYW5zbGF0ZShcIiArIHYxICsgXCIsXCIgKyB2MiArIFwiKSBzY2FsZShcIiArIG1haW5ab29tLnNjYWxlKCkgKyBcIilcIlxuXG4gICAgI2NyZWF0ZSBhIGxhYmVsIG9mIGFuIGVkZ2VcbiAgICBjcmVhdGVMYWJlbEVkZ2UgPSAoZWwpIC0+XG4gICAgICBsYWJlbFZhbHVlID0gXCJcIlxuICAgICAgaWYgZWwuc2hpcF9zdHJhdGVneT8gb3IgZWwubG9jYWxfc3RyYXRlZ3k/XG4gICAgICAgIGxhYmVsVmFsdWUgKz0gXCI8ZGl2IGNsYXNzPSdlZGdlLWxhYmVsJz5cIlxuICAgICAgICBsYWJlbFZhbHVlICs9IGVsLnNoaXBfc3RyYXRlZ3kgIGlmIGVsLnNoaXBfc3RyYXRlZ3k/XG4gICAgICAgIGxhYmVsVmFsdWUgKz0gXCIgKFwiICsgZWwudGVtcF9tb2RlICsgXCIpXCIgIHVubGVzcyBlbC50ZW1wX21vZGUgaXMgYHVuZGVmaW5lZGBcbiAgICAgICAgbGFiZWxWYWx1ZSArPSBcIiw8YnI+XCIgKyBlbC5sb2NhbF9zdHJhdGVneSAgdW5sZXNzIGVsLmxvY2FsX3N0cmF0ZWd5IGlzIGB1bmRlZmluZWRgXG4gICAgICAgIGxhYmVsVmFsdWUgKz0gXCI8L2Rpdj5cIlxuICAgICAgbGFiZWxWYWx1ZVxuXG5cbiAgICAjIHRydWUsIGlmIHRoZSBub2RlIGlzIGEgc3BlY2lhbCBub2RlIGZyb20gYW4gaXRlcmF0aW9uXG4gICAgaXNTcGVjaWFsSXRlcmF0aW9uTm9kZSA9IChpbmZvKSAtPlxuICAgICAgKGluZm8gaXMgXCJwYXJ0aWFsU29sdXRpb25cIiBvciBpbmZvIGlzIFwibmV4dFBhcnRpYWxTb2x1dGlvblwiIG9yIGluZm8gaXMgXCJ3b3Jrc2V0XCIgb3IgaW5mbyBpcyBcIm5leHRXb3Jrc2V0XCIgb3IgaW5mbyBpcyBcInNvbHV0aW9uU2V0XCIgb3IgaW5mbyBpcyBcInNvbHV0aW9uRGVsdGFcIilcblxuICAgIGdldE5vZGVUeXBlID0gKGVsLCBpbmZvKSAtPlxuICAgICAgaWYgaW5mbyBpcyBcIm1pcnJvclwiXG4gICAgICAgICdub2RlLW1pcnJvcidcblxuICAgICAgZWxzZSBpZiBpc1NwZWNpYWxJdGVyYXRpb25Ob2RlKGluZm8pXG4gICAgICAgICdub2RlLWl0ZXJhdGlvbidcblxuICAgICAgZWxzZVxuICAgICAgICBpZiBlbC5wYWN0IGlzIFwiRGF0YSBTb3VyY2VcIlxuICAgICAgICAgICdub2RlLXNvdXJjZSdcbiAgICAgICAgZWxzZSBpZiBlbC5wYWN0IGlzIFwiRGF0YSBTaW5rXCJcbiAgICAgICAgICAnbm9kZS1zaW5rJ1xuICAgICAgICBlbHNlXG4gICAgICAgICAgJ25vZGUtbm9ybWFsJ1xuICAgICAgXG4gICAgIyBjcmVhdGVzIHRoZSBsYWJlbCBvZiBhIG5vZGUsIGluIGluZm8gaXMgc3RvcmVkLCB3aGV0aGVyIGl0IGlzIGEgc3BlY2lhbCBub2RlIChsaWtlIGEgbWlycm9yIGluIGFuIGl0ZXJhdGlvbilcbiAgICBjcmVhdGVMYWJlbE5vZGUgPSAoZWwsIGluZm8sIG1heFcsIG1heEgpIC0+XG4gICAgICBsYWJlbFZhbHVlID0gXCI8YSBocmVmPScjL2pvYnMvXCIgKyBqb2JpZCArIFwiL1wiICsgZWwuaWQgKyBcIicgY2xhc3M9J25vZGUtbGFiZWwgXCIgKyBnZXROb2RlVHlwZShlbCwgaW5mbykgKyBcIic+XCJcblxuICAgICAgIyBOb2RlbmFtZVxuICAgICAgaWYgaW5mbyBpcyBcIm1pcnJvclwiXG4gICAgICAgIGxhYmVsVmFsdWUgKz0gXCI8aDMgY2xhc3M9J25vZGUtbmFtZSc+TWlycm9yIG9mIFwiICsgZWwucGFjdCArIFwiPC9oMz5cIlxuICAgICAgZWxzZVxuICAgICAgICBsYWJlbFZhbHVlICs9IFwiPGgzIGNsYXNzPSdub2RlLW5hbWUnPlwiICsgZWwucGFjdCArIFwiPC9oMz5cIlxuICAgICAgaWYgZWwuY29udGVudHMgaXMgXCJcIlxuICAgICAgICBsYWJlbFZhbHVlICs9IFwiXCJcbiAgICAgIGVsc2VcbiAgICAgICAgc3RlcE5hbWUgPSBlbC5jb250ZW50c1xuICAgICAgICBcbiAgICAgICAgIyBjbGVhbiBzdGVwTmFtZVxuICAgICAgICBzdGVwTmFtZSA9IHNob3J0ZW5TdHJpbmcoc3RlcE5hbWUpXG4gICAgICAgIGxhYmVsVmFsdWUgKz0gXCI8aDQgY2xhc3M9J3N0ZXAtbmFtZSc+XCIgKyBzdGVwTmFtZSArIFwiPC9oND5cIlxuICAgICAgXG4gICAgICAjIElmIHRoaXMgbm9kZSBpcyBhbiBcIml0ZXJhdGlvblwiIHdlIG5lZWQgYSBkaWZmZXJlbnQgcGFuZWwtYm9keVxuICAgICAgaWYgZWwuc3RlcF9mdW5jdGlvbj9cbiAgICAgICAgbGFiZWxWYWx1ZSArPSBleHRlbmRMYWJlbE5vZGVGb3JJdGVyYXRpb24oZWwuaWQsIG1heFcsIG1heEgpXG4gICAgICBlbHNlXG4gICAgICAgIFxuICAgICAgICAjIE90aGVyd2lzZSBhZGQgaW5mb3MgICAgXG4gICAgICAgIGxhYmVsVmFsdWUgKz0gXCI8aDU+XCIgKyBpbmZvICsgXCIgTm9kZTwvaDU+XCIgIGlmIGlzU3BlY2lhbEl0ZXJhdGlvbk5vZGUoaW5mbylcbiAgICAgICAgbGFiZWxWYWx1ZSArPSBcIjxoNT5QYXJhbGxlbGlzbTogXCIgKyBlbC5wYXJhbGxlbGlzbSArIFwiPC9oNT5cIiAgdW5sZXNzIGVsLnBhcmFsbGVsaXNtIGlzIFwiXCJcbiAgICAgICAgbGFiZWxWYWx1ZSArPSBcIjxoNT5Ecml2ZXIgU3RyYXRlZ3k6IFwiICsgc2hvcnRlblN0cmluZyhlbC5kcml2ZXJfc3RyYXRlZ3kpICsgXCI8L2g1XCIgIHVubGVzcyBlbC5kcml2ZXJfc3RyYXRlZ3kgaXMgYHVuZGVmaW5lZGBcbiAgICAgIFxuICAgICAgbGFiZWxWYWx1ZSArPSBcIjwvYT5cIlxuICAgICAgbGFiZWxWYWx1ZVxuXG4gICAgIyBFeHRlbmRzIHRoZSBsYWJlbCBvZiBhIG5vZGUgd2l0aCBhbiBhZGRpdGlvbmFsIHN2ZyBFbGVtZW50IHRvIHByZXNlbnQgdGhlIGl0ZXJhdGlvbi5cbiAgICBleHRlbmRMYWJlbE5vZGVGb3JJdGVyYXRpb24gPSAoaWQsIG1heFcsIG1heEgpIC0+XG4gICAgICBzdmdJRCA9IFwic3ZnLVwiICsgaWRcblxuICAgICAgbGFiZWxWYWx1ZSA9IFwiPHN2ZyBjbGFzcz0nXCIgKyBzdmdJRCArIFwiJyB3aWR0aD1cIiArIG1heFcgKyBcIiBoZWlnaHQ9XCIgKyBtYXhIICsgXCI+PGcgLz48L3N2Zz5cIlxuICAgICAgbGFiZWxWYWx1ZVxuXG4gICAgIyBTcGxpdCBhIHN0cmluZyBpbnRvIG11bHRpcGxlIGxpbmVzIHNvIHRoYXQgZWFjaCBsaW5lIGhhcyBsZXNzIHRoYW4gMzAgbGV0dGVycy5cbiAgICBzaG9ydGVuU3RyaW5nID0gKHMpIC0+XG4gICAgICAjIG1ha2Ugc3VyZSB0aGF0IG5hbWUgZG9lcyBub3QgY29udGFpbiBhIDwgKGJlY2F1c2Ugb2YgaHRtbClcbiAgICAgIGlmIHMuY2hhckF0KDApIGlzIFwiPFwiXG4gICAgICAgIHMgPSBzLnJlcGxhY2UoXCI8XCIsIFwiJmx0O1wiKVxuICAgICAgICBzID0gcy5yZXBsYWNlKFwiPlwiLCBcIiZndDtcIilcbiAgICAgIHNiciA9IFwiXCJcbiAgICAgIHdoaWxlIHMubGVuZ3RoID4gMzBcbiAgICAgICAgc2JyID0gc2JyICsgcy5zdWJzdHJpbmcoMCwgMzApICsgXCI8YnI+XCJcbiAgICAgICAgcyA9IHMuc3Vic3RyaW5nKDMwLCBzLmxlbmd0aClcbiAgICAgIHNiciA9IHNiciArIHNcbiAgICAgIHNiclxuXG4gICAgY3JlYXRlTm9kZSA9IChnLCBkYXRhLCBlbCwgaXNQYXJlbnQgPSBmYWxzZSwgbWF4VywgbWF4SCkgLT5cbiAgICAgICMgY3JlYXRlIG5vZGUsIHNlbmQgYWRkaXRpb25hbCBpbmZvcm1hdGlvbnMgYWJvdXQgdGhlIG5vZGUgaWYgaXQgaXMgYSBzcGVjaWFsIG9uZVxuICAgICAgaWYgZWwuaWQgaXMgZGF0YS5wYXJ0aWFsX3NvbHV0aW9uXG4gICAgICAgIGcuc2V0Tm9kZSBlbC5pZCxcbiAgICAgICAgICBsYWJlbDogY3JlYXRlTGFiZWxOb2RlKGVsLCBcInBhcnRpYWxTb2x1dGlvblwiLCBtYXhXLCBtYXhIKVxuICAgICAgICAgIGxhYmVsVHlwZTogJ2h0bWwnXG4gICAgICAgICAgY2xhc3M6IGdldE5vZGVUeXBlKGVsLCBcInBhcnRpYWxTb2x1dGlvblwiKVxuXG4gICAgICBlbHNlIGlmIGVsLmlkIGlzIGRhdGEubmV4dF9wYXJ0aWFsX3NvbHV0aW9uXG4gICAgICAgIGcuc2V0Tm9kZSBlbC5pZCxcbiAgICAgICAgICBsYWJlbDogY3JlYXRlTGFiZWxOb2RlKGVsLCBcIm5leHRQYXJ0aWFsU29sdXRpb25cIiwgbWF4VywgbWF4SClcbiAgICAgICAgICBsYWJlbFR5cGU6ICdodG1sJ1xuICAgICAgICAgIGNsYXNzOiBnZXROb2RlVHlwZShlbCwgXCJuZXh0UGFydGlhbFNvbHV0aW9uXCIpXG5cbiAgICAgIGVsc2UgaWYgZWwuaWQgaXMgZGF0YS53b3Jrc2V0XG4gICAgICAgIGcuc2V0Tm9kZSBlbC5pZCxcbiAgICAgICAgICBsYWJlbDogY3JlYXRlTGFiZWxOb2RlKGVsLCBcIndvcmtzZXRcIiwgbWF4VywgbWF4SClcbiAgICAgICAgICBsYWJlbFR5cGU6ICdodG1sJ1xuICAgICAgICAgIGNsYXNzOiBnZXROb2RlVHlwZShlbCwgXCJ3b3Jrc2V0XCIpXG5cbiAgICAgIGVsc2UgaWYgZWwuaWQgaXMgZGF0YS5uZXh0X3dvcmtzZXRcbiAgICAgICAgZy5zZXROb2RlIGVsLmlkLFxuICAgICAgICAgIGxhYmVsOiBjcmVhdGVMYWJlbE5vZGUoZWwsIFwibmV4dFdvcmtzZXRcIiwgbWF4VywgbWF4SClcbiAgICAgICAgICBsYWJlbFR5cGU6ICdodG1sJ1xuICAgICAgICAgIGNsYXNzOiBnZXROb2RlVHlwZShlbCwgXCJuZXh0V29ya3NldFwiKVxuXG4gICAgICBlbHNlIGlmIGVsLmlkIGlzIGRhdGEuc29sdXRpb25fc2V0XG4gICAgICAgIGcuc2V0Tm9kZSBlbC5pZCxcbiAgICAgICAgICBsYWJlbDogY3JlYXRlTGFiZWxOb2RlKGVsLCBcInNvbHV0aW9uU2V0XCIsIG1heFcsIG1heEgpXG4gICAgICAgICAgbGFiZWxUeXBlOiAnaHRtbCdcbiAgICAgICAgICBjbGFzczogZ2V0Tm9kZVR5cGUoZWwsIFwic29sdXRpb25TZXRcIilcblxuICAgICAgZWxzZSBpZiBlbC5pZCBpcyBkYXRhLnNvbHV0aW9uX2RlbHRhXG4gICAgICAgIGcuc2V0Tm9kZSBlbC5pZCxcbiAgICAgICAgICBsYWJlbDogY3JlYXRlTGFiZWxOb2RlKGVsLCBcInNvbHV0aW9uRGVsdGFcIiwgbWF4VywgbWF4SClcbiAgICAgICAgICBsYWJlbFR5cGU6ICdodG1sJ1xuICAgICAgICAgIGNsYXNzOiBnZXROb2RlVHlwZShlbCwgXCJzb2x1dGlvbkRlbHRhXCIpXG5cbiAgICAgIGVsc2VcbiAgICAgICAgZy5zZXROb2RlIGVsLmlkLFxuICAgICAgICAgIGxhYmVsOiBjcmVhdGVMYWJlbE5vZGUoZWwsIFwiXCIsIG1heFcsIG1heEgpXG4gICAgICAgICAgbGFiZWxUeXBlOiAnaHRtbCdcbiAgICAgICAgICBjbGFzczogZ2V0Tm9kZVR5cGUoZWwsIFwiXCIpXG5cbiAgICBjcmVhdGVFZGdlID0gKGcsIGRhdGEsIGVsLCBleGlzdGluZ05vZGVzLCBwcmVkKSAtPlxuICAgICAgdW5sZXNzIGV4aXN0aW5nTm9kZXMuaW5kZXhPZihwcmVkLmlkKSBpcyAtMVxuICAgICAgICBnLnNldEVkZ2UgcHJlZC5pZCwgZWwuaWQsXG4gICAgICAgICAgbGFiZWw6IGNyZWF0ZUxhYmVsRWRnZShwcmVkKVxuICAgICAgICAgIGxhYmVsVHlwZTogJ2h0bWwnXG4gICAgICAgICAgYXJyb3doZWFkOiAnbm9ybWFsJ1xuXG4gICAgICBlbHNlXG4gICAgICAgIG1pc3NpbmdOb2RlID0gc2VhcmNoRm9yTm9kZShkYXRhLCBwcmVkLmlkKVxuICAgICAgICB1bmxlc3MgIW1pc3NpbmdOb2RlIG9yIG1pc3NpbmdOb2RlLmFscmVhZHlBZGRlZCBpcyB0cnVlXG4gICAgICAgICAgbWlzc2luZ05vZGUuYWxyZWFkeUFkZGVkID0gdHJ1ZVxuICAgICAgICAgIGcuc2V0Tm9kZSBtaXNzaW5nTm9kZS5pZCxcbiAgICAgICAgICAgIGxhYmVsOiBjcmVhdGVMYWJlbE5vZGUobWlzc2luZ05vZGUsIFwibWlycm9yXCIpXG4gICAgICAgICAgICBsYWJlbFR5cGU6ICdodG1sJ1xuICAgICAgICAgICAgY2xhc3M6IGdldE5vZGVUeXBlKG1pc3NpbmdOb2RlLCAnbWlycm9yJylcblxuICAgICAgICAgIGcuc2V0RWRnZSBtaXNzaW5nTm9kZS5pZCwgZWwuaWQsXG4gICAgICAgICAgICBsYWJlbDogY3JlYXRlTGFiZWxFZGdlKG1pc3NpbmdOb2RlKVxuICAgICAgICAgICAgbGFiZWxUeXBlOiAnaHRtbCdcblxuICAgIGxvYWRKc29uVG9EYWdyZSA9IChnLCBkYXRhKSAtPlxuICAgICAgZXhpc3RpbmdOb2RlcyA9IFtdXG5cbiAgICAgIGlmIGRhdGEubm9kZXM/XG4gICAgICAgICMgVGhpcyBpcyB0aGUgbm9ybWFsIGpzb24gZGF0YVxuICAgICAgICB0b0l0ZXJhdGUgPSBkYXRhLm5vZGVzXG5cbiAgICAgIGVsc2VcbiAgICAgICAgIyBUaGlzIGlzIGFuIGl0ZXJhdGlvbiwgd2Ugbm93IHN0b3JlIHNwZWNpYWwgaXRlcmF0aW9uIG5vZGVzIGlmIHBvc3NpYmxlXG4gICAgICAgIHRvSXRlcmF0ZSA9IGRhdGEuc3RlcF9mdW5jdGlvblxuICAgICAgICBpc1BhcmVudCA9IHRydWVcblxuICAgICAgZm9yIGVsIGluIHRvSXRlcmF0ZVxuICAgICAgICBtYXhXID0gMFxuICAgICAgICBtYXhIID0gMFxuXG4gICAgICAgIGlmIGVsLnN0ZXBfZnVuY3Rpb25cbiAgICAgICAgICBzZyA9IG5ldyBkYWdyZUQzLmdyYXBobGliLkdyYXBoKHsgbXVsdGlncmFwaDogdHJ1ZSwgY29tcG91bmQ6IHRydWUgfSkuc2V0R3JhcGgoe1xuICAgICAgICAgICAgbm9kZXNlcDogMjBcbiAgICAgICAgICAgIGVkZ2VzZXA6IDBcbiAgICAgICAgICAgIHJhbmtzZXA6IDIwXG4gICAgICAgICAgICByYW5rZGlyOiBcIkxSXCJcbiAgICAgICAgICAgIG1hcmdpbng6IDEwXG4gICAgICAgICAgICBtYXJnaW55OiAxMFxuICAgICAgICAgICAgfSlcblxuICAgICAgICAgIHN1YmdyYXBoc1tlbC5pZF0gPSBzZ1xuXG4gICAgICAgICAgbG9hZEpzb25Ub0RhZ3JlKHNnLCBlbClcblxuICAgICAgICAgIHIgPSBuZXcgZGFncmVEMy5yZW5kZXIoKVxuICAgICAgICAgIGQzdG1wU3ZnLnNlbGVjdCgnZycpLmNhbGwociwgc2cpXG4gICAgICAgICAgbWF4VyA9IHNnLmdyYXBoKCkud2lkdGhcbiAgICAgICAgICBtYXhIID0gc2cuZ3JhcGgoKS5oZWlnaHRcblxuICAgICAgICAgIGFuZ3VsYXIuZWxlbWVudChtYWluVG1wRWxlbWVudCkuZW1wdHkoKVxuXG4gICAgICAgIGNyZWF0ZU5vZGUoZywgZGF0YSwgZWwsIGlzUGFyZW50LCBtYXhXLCBtYXhIKVxuXG4gICAgICAgIGV4aXN0aW5nTm9kZXMucHVzaCBlbC5pZFxuICAgICAgICBcbiAgICAgICAgIyBjcmVhdGUgZWRnZXMgZnJvbSBwcmVkZWNlc3NvcnMgdG8gY3VycmVudCBub2RlXG4gICAgICAgIGlmIGVsLnByZWRlY2Vzc29ycz9cbiAgICAgICAgICBmb3IgcHJlZCBpbiBlbC5wcmVkZWNlc3NvcnNcbiAgICAgICAgICAgIGNyZWF0ZUVkZ2UoZywgZGF0YSwgZWwsIGV4aXN0aW5nTm9kZXMsIHByZWQpXG5cbiAgICAgIGdcblxuICAgICMgc2VhcmNoZXMgaW4gdGhlIGdsb2JhbCBKU09ORGF0YSBmb3IgdGhlIG5vZGUgd2l0aCB0aGUgZ2l2ZW4gaWRcbiAgICBzZWFyY2hGb3JOb2RlID0gKGRhdGEsIG5vZGVJRCkgLT5cbiAgICAgIGZvciBpIG9mIGRhdGEubm9kZXNcbiAgICAgICAgZWwgPSBkYXRhLm5vZGVzW2ldXG4gICAgICAgIHJldHVybiBlbCAgaWYgZWwuaWQgaXMgbm9kZUlEXG4gICAgICAgIFxuICAgICAgICAjIGxvb2sgZm9yIG5vZGVzIHRoYXQgYXJlIGluIGl0ZXJhdGlvbnNcbiAgICAgICAgaWYgZWwuc3RlcF9mdW5jdGlvbj9cbiAgICAgICAgICBmb3IgaiBvZiBlbC5zdGVwX2Z1bmN0aW9uXG4gICAgICAgICAgICByZXR1cm4gZWwuc3RlcF9mdW5jdGlvbltqXSAgaWYgZWwuc3RlcF9mdW5jdGlvbltqXS5pZCBpcyBub2RlSURcblxuICAgIGRyYXdHcmFwaCA9IChkYXRhKSAtPlxuICAgICAgZyA9IG5ldyBkYWdyZUQzLmdyYXBobGliLkdyYXBoKHsgbXVsdGlncmFwaDogdHJ1ZSwgY29tcG91bmQ6IHRydWUgfSkuc2V0R3JhcGgoe1xuICAgICAgICBub2Rlc2VwOiA3MFxuICAgICAgICBlZGdlc2VwOiAwXG4gICAgICAgIHJhbmtzZXA6IDUwXG4gICAgICAgIHJhbmtkaXI6IFwiTFJcIlxuICAgICAgICBtYXJnaW54OiA0MFxuICAgICAgICBtYXJnaW55OiA0MFxuICAgICAgICB9KVxuXG4gICAgICBsb2FkSnNvblRvRGFncmUoZywgZGF0YSlcblxuICAgICAgcmVuZGVyZXIgPSBuZXcgZGFncmVEMy5yZW5kZXIoKVxuICAgICAgZDNtYWluU3ZnRy5jYWxsKHJlbmRlcmVyLCBnKVxuXG4gICAgICBmb3IgaSwgc2cgb2Ygc3ViZ3JhcGhzXG4gICAgICAgIGQzbWFpblN2Zy5zZWxlY3QoJ3N2Zy5zdmctJyArIGkgKyAnIGcnKS5jYWxsKHJlbmRlcmVyLCBzZylcblxuICAgICAgbmV3U2NhbGUgPSAwLjVcblxuICAgICAgeENlbnRlck9mZnNldCA9IE1hdGguZmxvb3IoKGFuZ3VsYXIuZWxlbWVudChtYWluU3ZnRWxlbWVudCkud2lkdGgoKSAtIGcuZ3JhcGgoKS53aWR0aCAqIG5ld1NjYWxlKSAvIDIpXG4gICAgICB5Q2VudGVyT2Zmc2V0ID0gTWF0aC5mbG9vcigoYW5ndWxhci5lbGVtZW50KG1haW5TdmdFbGVtZW50KS5oZWlnaHQoKSAtIGcuZ3JhcGgoKS5oZWlnaHQgKiBuZXdTY2FsZSkgLyAyKVxuXG4gICAgICBtYWluWm9vbS5zY2FsZShuZXdTY2FsZSkudHJhbnNsYXRlKFt4Q2VudGVyT2Zmc2V0LCB5Q2VudGVyT2Zmc2V0XSlcblxuICAgICAgZDNtYWluU3ZnRy5hdHRyKFwidHJhbnNmb3JtXCIsIFwidHJhbnNsYXRlKFwiICsgeENlbnRlck9mZnNldCArIFwiLCBcIiArIHlDZW50ZXJPZmZzZXQgKyBcIikgc2NhbGUoXCIgKyBtYWluWm9vbS5zY2FsZSgpICsgXCIpXCIpXG5cbiAgICAgIG1haW5ab29tLm9uKFwiem9vbVwiLCAtPlxuICAgICAgICBldiA9IGQzLmV2ZW50XG4gICAgICAgIGQzbWFpblN2Z0cuYXR0ciBcInRyYW5zZm9ybVwiLCBcInRyYW5zbGF0ZShcIiArIGV2LnRyYW5zbGF0ZSArIFwiKSBzY2FsZShcIiArIGV2LnNjYWxlICsgXCIpXCJcbiAgICAgIClcbiAgICAgIG1haW5ab29tKGQzbWFpblN2ZylcblxuICAgIHNjb3BlLiR3YXRjaCBhdHRycy5wbGFuLCAobmV3UGxhbikgLT5cbiAgICAgIGRyYXdHcmFwaChuZXdQbGFuKSBpZiBuZXdQbGFuXG5cbiAgICByZXR1cm5cbiIsImFuZ3VsYXIubW9kdWxlKCdmbGlua0FwcCcpLmRpcmVjdGl2ZSgndmVydGV4JywgZnVuY3Rpb24oJHN0YXRlKSB7XG4gIHJldHVybiB7XG4gICAgdGVtcGxhdGU6IFwiPHN2ZyBjbGFzcz0ndGltZWxpbmUgc2Vjb25kYXJ5JyB3aWR0aD0nMCcgaGVpZ2h0PScwJz48L3N2Zz5cIixcbiAgICBzY29wZToge1xuICAgICAgZGF0YTogXCI9XCJcbiAgICB9LFxuICAgIGxpbms6IGZ1bmN0aW9uKHNjb3BlLCBlbGVtLCBhdHRycykge1xuICAgICAgdmFyIGFuYWx5emVUaW1lLCBjb250YWluZXJXLCBzdmdFbCwgem9vbTtcbiAgICAgIHpvb20gPSBkMy5iZWhhdmlvci56b29tKCk7XG4gICAgICBzdmdFbCA9IGVsZW0uY2hpbGRyZW4oKVswXTtcbiAgICAgIGNvbnRhaW5lclcgPSBlbGVtLndpZHRoKCk7XG4gICAgICBhbmd1bGFyLmVsZW1lbnQoc3ZnRWwpLmF0dHIoJ3dpZHRoJywgY29udGFpbmVyVyAtIDE2KTtcbiAgICAgIGFuYWx5emVUaW1lID0gZnVuY3Rpb24oZGF0YSkge1xuICAgICAgICB2YXIgYmJveCwgY2hhcnQsIHN2Zywgc3ZnRywgdGVzdERhdGE7XG4gICAgICAgIHRlc3REYXRhID0gW107XG4gICAgICAgIGFuZ3VsYXIuZm9yRWFjaChkYXRhLmdyb3VwdmVydGV4Lmdyb3VwbWVtYmVycywgZnVuY3Rpb24odmVydGV4LCBpKSB7XG4gICAgICAgICAgdmFyIHZUaW1lO1xuICAgICAgICAgIHZUaW1lID0gZGF0YS52ZXJ0aWNldGltZXNbdmVydGV4LnZlcnRleGlkXTtcbiAgICAgICAgICByZXR1cm4gdGVzdERhdGEucHVzaCh7XG4gICAgICAgICAgICBsYWJlbDogdmVydGV4LnZlcnRleGluc3RhbmNlbmFtZSArIFwiIChcIiArIGkgKyBcIilcIixcbiAgICAgICAgICAgIHRpbWVzOiBbXG4gICAgICAgICAgICAgIHtcbiAgICAgICAgICAgICAgICBsYWJlbDogXCJTY2hlZHVsZWRcIixcbiAgICAgICAgICAgICAgICBjb2xvcjogXCIjNjY2XCIsXG4gICAgICAgICAgICAgICAgc3RhcnRpbmdfdGltZTogdlRpbWVbXCJTQ0hFRFVMRURcIl0gKiAxMDAsXG4gICAgICAgICAgICAgICAgZW5kaW5nX3RpbWU6IHZUaW1lW1wiREVQTE9ZSU5HXCJdICogMTAwXG4gICAgICAgICAgICAgIH0sIHtcbiAgICAgICAgICAgICAgICBsYWJlbDogXCJEZXBsb3lpbmdcIixcbiAgICAgICAgICAgICAgICBjb2xvcjogXCIjYWFhXCIsXG4gICAgICAgICAgICAgICAgc3RhcnRpbmdfdGltZTogdlRpbWVbXCJERVBMT1lJTkdcIl0gKiAxMDAsXG4gICAgICAgICAgICAgICAgZW5kaW5nX3RpbWU6IHZUaW1lW1wiUlVOTklOR1wiXSAqIDEwMFxuICAgICAgICAgICAgICB9LCB7XG4gICAgICAgICAgICAgICAgbGFiZWw6IFwiUnVubmluZ1wiLFxuICAgICAgICAgICAgICAgIGNvbG9yOiBcIiNkZGRcIixcbiAgICAgICAgICAgICAgICBzdGFydGluZ190aW1lOiB2VGltZVtcIlJVTk5JTkdcIl0gKiAxMDAsXG4gICAgICAgICAgICAgICAgZW5kaW5nX3RpbWU6IHZUaW1lW1wiRklOSVNIRURcIl0gKiAxMDBcbiAgICAgICAgICAgICAgfVxuICAgICAgICAgICAgXVxuICAgICAgICAgIH0pO1xuICAgICAgICB9KTtcbiAgICAgICAgY2hhcnQgPSBkMy50aW1lbGluZSgpLnN0YWNrKCkudGlja0Zvcm1hdCh7XG4gICAgICAgICAgZm9ybWF0OiBkMy50aW1lLmZvcm1hdChcIiVTXCIpLFxuICAgICAgICAgIHRpY2tJbnRlcnZhbDogMSxcbiAgICAgICAgICB0aWNrU2l6ZTogMVxuICAgICAgICB9KS5sYWJlbEZvcm1hdChmdW5jdGlvbihsYWJlbCkge1xuICAgICAgICAgIHJldHVybiBsYWJlbDtcbiAgICAgICAgfSkubWFyZ2luKHtcbiAgICAgICAgICBsZWZ0OiAxMDAsXG4gICAgICAgICAgcmlnaHQ6IDAsXG4gICAgICAgICAgdG9wOiAwLFxuICAgICAgICAgIGJvdHRvbTogMFxuICAgICAgICB9KTtcbiAgICAgICAgc3ZnID0gZDMuc2VsZWN0KHN2Z0VsKS5kYXR1bSh0ZXN0RGF0YSkuY2FsbChjaGFydCkuY2FsbCh6b29tKTtcbiAgICAgICAgc3ZnRyA9IHN2Zy5zZWxlY3QoXCJnXCIpO1xuICAgICAgICB6b29tLm9uKFwiem9vbVwiLCBmdW5jdGlvbigpIHtcbiAgICAgICAgICB2YXIgZXY7XG4gICAgICAgICAgZXYgPSBkMy5ldmVudDtcbiAgICAgICAgICBzdmdHLnNlbGVjdEFsbCgncmVjdCcpLmF0dHIoXCJ0cmFuc2Zvcm1cIiwgXCJ0cmFuc2xhdGUoXCIgKyBldi50cmFuc2xhdGVbMF0gKyBcIiwwKSBzY2FsZShcIiArIGV2LnNjYWxlICsgXCIsMSlcIik7XG4gICAgICAgICAgcmV0dXJuIHN2Z0cuc2VsZWN0QWxsKCd0ZXh0JykuYXR0cihcInRyYW5zZm9ybVwiLCBcInRyYW5zbGF0ZShcIiArIGV2LnRyYW5zbGF0ZVswXSArIFwiLDApIHNjYWxlKFwiICsgZXYuc2NhbGUgKyBcIiwxKVwiKTtcbiAgICAgICAgfSk7XG4gICAgICAgIGJib3ggPSBzdmdHWzBdWzBdLmdldEJCb3goKTtcbiAgICAgICAgcmV0dXJuIHN2Zy5hdHRyKCdoZWlnaHQnLCBiYm94LmhlaWdodCArIDMwKTtcbiAgICAgIH07XG4gICAgICBhbmFseXplVGltZShzY29wZS5kYXRhKTtcbiAgICB9XG4gIH07XG59KS5kaXJlY3RpdmUoJ3RpbWVsaW5lJywgZnVuY3Rpb24oJHN0YXRlKSB7XG4gIHJldHVybiB7XG4gICAgdGVtcGxhdGU6IFwiPHN2ZyBjbGFzcz0ndGltZWxpbmUnIHdpZHRoPScwJyBoZWlnaHQ9JzAnPjwvc3ZnPlwiLFxuICAgIHNjb3BlOiB7XG4gICAgICBqb2I6IFwiPVwiXG4gICAgfSxcbiAgICBsaW5rOiBmdW5jdGlvbihzY29wZSwgZWxlbSwgYXR0cnMpIHtcbiAgICAgIHZhciBhbmFseXplVGltZSwgY29udGFpbmVyVywgc3ZnRWwsIHpvb207XG4gICAgICB6b29tID0gZDMuYmVoYXZpb3Iuem9vbSgpO1xuICAgICAgc3ZnRWwgPSBlbGVtLmNoaWxkcmVuKClbMF07XG4gICAgICBjb250YWluZXJXID0gZWxlbS53aWR0aCgpO1xuICAgICAgYW5ndWxhci5lbGVtZW50KHN2Z0VsKS5hdHRyKCd3aWR0aCcsIGNvbnRhaW5lclcgLSAxNik7XG4gICAgICBhbmFseXplVGltZSA9IGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgICAgdmFyIGJib3gsIGNoYXJ0LCBzdmcsIHN2Z0csIHRlc3REYXRhO1xuICAgICAgICB0ZXN0RGF0YSA9IFtdO1xuICAgICAgICBhbmd1bGFyLmZvckVhY2goZGF0YS5vbGRWLmdyb3VwdmVydGljZXMsIGZ1bmN0aW9uKHZlcnRleCkge1xuICAgICAgICAgIHZhciB2VGltZTtcbiAgICAgICAgICB2VGltZSA9IGRhdGEub2xkVi5ncm91cHZlcnRpY2V0aW1lc1t2ZXJ0ZXguZ3JvdXB2ZXJ0ZXhpZF07XG4gICAgICAgICAgcmV0dXJuIHRlc3REYXRhLnB1c2goe1xuICAgICAgICAgICAgdGltZXM6IFtcbiAgICAgICAgICAgICAge1xuICAgICAgICAgICAgICAgIGxhYmVsOiB2ZXJ0ZXguZ3JvdXB2ZXJ0ZXhuYW1lLFxuICAgICAgICAgICAgICAgIGNvbG9yOiBcIiMzZmI2ZDhcIixcbiAgICAgICAgICAgICAgICBzdGFydGluZ190aW1lOiB2VGltZVtcIlNUQVJURURcIl0sXG4gICAgICAgICAgICAgICAgZW5kaW5nX3RpbWU6IHZUaW1lW1wiRU5ERURcIl0sXG4gICAgICAgICAgICAgICAgbGluazogdmVydGV4Lmdyb3VwdmVydGV4aWRcbiAgICAgICAgICAgICAgfVxuICAgICAgICAgICAgXVxuICAgICAgICAgIH0pO1xuICAgICAgICB9KTtcbiAgICAgICAgY2hhcnQgPSBkMy50aW1lbGluZSgpLnN0YWNrKCkuY2xpY2soZnVuY3Rpb24oZCwgaSwgZGF0dW0pIHtcbiAgICAgICAgICByZXR1cm4gJHN0YXRlLmdvKFwic2luZ2xlLWpvYi50aW1lbGluZS52ZXJ0ZXhcIiwge1xuICAgICAgICAgICAgam9iaWQ6IGRhdGEuamlkLFxuICAgICAgICAgICAgdmVydGV4SWQ6IGQubGlua1xuICAgICAgICAgIH0pO1xuICAgICAgICB9KS50aWNrRm9ybWF0KHtcbiAgICAgICAgICBmb3JtYXQ6IGQzLnRpbWUuZm9ybWF0KFwiJVNcIiksXG4gICAgICAgICAgdGlja0ludGVydmFsOiAxLFxuICAgICAgICAgIHRpY2tTaXplOiAxXG4gICAgICAgIH0pLm1hcmdpbih7XG4gICAgICAgICAgbGVmdDogMCxcbiAgICAgICAgICByaWdodDogMCxcbiAgICAgICAgICB0b3A6IDAsXG4gICAgICAgICAgYm90dG9tOiAwXG4gICAgICAgIH0pO1xuICAgICAgICBzdmcgPSBkMy5zZWxlY3Qoc3ZnRWwpLmRhdHVtKHRlc3REYXRhKS5jYWxsKGNoYXJ0KS5jYWxsKHpvb20pO1xuICAgICAgICBzdmdHID0gc3ZnLnNlbGVjdChcImdcIik7XG4gICAgICAgIHpvb20ub24oXCJ6b29tXCIsIGZ1bmN0aW9uKCkge1xuICAgICAgICAgIHZhciBldjtcbiAgICAgICAgICBldiA9IGQzLmV2ZW50O1xuICAgICAgICAgIHN2Z0cuc2VsZWN0QWxsKCdyZWN0JykuYXR0cihcInRyYW5zZm9ybVwiLCBcInRyYW5zbGF0ZShcIiArIGV2LnRyYW5zbGF0ZVswXSArIFwiLDApIHNjYWxlKFwiICsgZXYuc2NhbGUgKyBcIiwxKVwiKTtcbiAgICAgICAgICByZXR1cm4gc3ZnRy5zZWxlY3RBbGwoJ3RleHQnKS5hdHRyKFwidHJhbnNmb3JtXCIsIFwidHJhbnNsYXRlKFwiICsgZXYudHJhbnNsYXRlWzBdICsgXCIsMCkgc2NhbGUoXCIgKyBldi5zY2FsZSArIFwiLDEpXCIpO1xuICAgICAgICB9KTtcbiAgICAgICAgYmJveCA9IHN2Z0dbMF1bMF0uZ2V0QkJveCgpO1xuICAgICAgICByZXR1cm4gc3ZnLmF0dHIoJ2hlaWdodCcsIGJib3guaGVpZ2h0ICsgMzApO1xuICAgICAgfTtcbiAgICAgIHNjb3BlLiR3YXRjaChhdHRycy5qb2IsIGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgICAgaWYgKGRhdGEpIHtcbiAgICAgICAgICByZXR1cm4gYW5hbHl6ZVRpbWUoZGF0YSk7XG4gICAgICAgIH1cbiAgICAgIH0pO1xuICAgIH1cbiAgfTtcbn0pLmRpcmVjdGl2ZSgnam9iUGxhbicsIGZ1bmN0aW9uKCR0aW1lb3V0KSB7XG4gIHJldHVybiB7XG4gICAgdGVtcGxhdGU6IFwiPHN2ZyBjbGFzcz0nZ3JhcGgnIHdpZHRoPSc1MDAnIGhlaWdodD0nNDAwJz48ZyAvPjwvc3ZnPiA8c3ZnIGNsYXNzPSd0bXAnIHdpZHRoPScxJyBoZWlnaHQ9JzEnPjxnIC8+PC9zdmc+IDxkaXYgY2xhc3M9J2J0bi1ncm91cCB6b29tLWJ1dHRvbnMnPiA8YSBjbGFzcz0nYnRuIGJ0bi1kZWZhdWx0IHpvb20taW4nIG5nLWNsaWNrPSd6b29tSW4oKSc+PGkgY2xhc3M9J2ZhIGZhLXBsdXMnIC8+PC9hPiA8YSBjbGFzcz0nYnRuIGJ0bi1kZWZhdWx0IHpvb20tb3V0JyBuZy1jbGljaz0nem9vbU91dCgpJz48aSBjbGFzcz0nZmEgZmEtbWludXMnIC8+PC9hPiA8L2Rpdj5cIixcbiAgICBzY29wZToge1xuICAgICAgcGxhbjogJz0nXG4gICAgfSxcbiAgICBsaW5rOiBmdW5jdGlvbihzY29wZSwgZWxlbSwgYXR0cnMpIHtcbiAgICAgIHZhciBjb250YWluZXJXLCBjcmVhdGVFZGdlLCBjcmVhdGVMYWJlbEVkZ2UsIGNyZWF0ZUxhYmVsTm9kZSwgY3JlYXRlTm9kZSwgZDNtYWluU3ZnLCBkM21haW5TdmdHLCBkM3RtcFN2ZywgZHJhd0dyYXBoLCBleHRlbmRMYWJlbE5vZGVGb3JJdGVyYXRpb24sIGdldE5vZGVUeXBlLCBpc1NwZWNpYWxJdGVyYXRpb25Ob2RlLCBqb2JpZCwgbG9hZEpzb25Ub0RhZ3JlLCBtYWluRywgbWFpblN2Z0VsZW1lbnQsIG1haW5UbXBFbGVtZW50LCBtYWluWm9vbSwgc2VhcmNoRm9yTm9kZSwgc2hvcnRlblN0cmluZywgc3ViZ3JhcGhzO1xuICAgICAgbWFpblpvb20gPSBkMy5iZWhhdmlvci56b29tKCk7XG4gICAgICBzdWJncmFwaHMgPSBbXTtcbiAgICAgIGpvYmlkID0gYXR0cnMuam9iaWQ7XG4gICAgICBtYWluU3ZnRWxlbWVudCA9IGVsZW0uY2hpbGRyZW4oKVswXTtcbiAgICAgIG1haW5HID0gZWxlbS5jaGlsZHJlbigpLmNoaWxkcmVuKClbMF07XG4gICAgICBtYWluVG1wRWxlbWVudCA9IGVsZW0uY2hpbGRyZW4oKVsxXTtcbiAgICAgIGQzbWFpblN2ZyA9IGQzLnNlbGVjdChtYWluU3ZnRWxlbWVudCk7XG4gICAgICBkM21haW5TdmdHID0gZDMuc2VsZWN0KG1haW5HKTtcbiAgICAgIGQzdG1wU3ZnID0gZDMuc2VsZWN0KG1haW5UbXBFbGVtZW50KTtcbiAgICAgIGNvbnRhaW5lclcgPSBlbGVtLndpZHRoKCk7XG4gICAgICBhbmd1bGFyLmVsZW1lbnQoZWxlbS5jaGlsZHJlbigpWzBdKS53aWR0aChjb250YWluZXJXKTtcbiAgICAgIHNjb3BlLnpvb21JbiA9IGZ1bmN0aW9uKCkge1xuICAgICAgICB2YXIgdHJhbnNsYXRlLCB2MSwgdjI7XG4gICAgICAgIGlmIChtYWluWm9vbS5zY2FsZSgpIDwgMi45OSkge1xuICAgICAgICAgIHRyYW5zbGF0ZSA9IG1haW5ab29tLnRyYW5zbGF0ZSgpO1xuICAgICAgICAgIHYxID0gdHJhbnNsYXRlWzBdICogKG1haW5ab29tLnNjYWxlKCkgKyAwLjEgLyAobWFpblpvb20uc2NhbGUoKSkpO1xuICAgICAgICAgIHYyID0gdHJhbnNsYXRlWzFdICogKG1haW5ab29tLnNjYWxlKCkgKyAwLjEgLyAobWFpblpvb20uc2NhbGUoKSkpO1xuICAgICAgICAgIG1haW5ab29tLnNjYWxlKG1haW5ab29tLnNjYWxlKCkgKyAwLjEpO1xuICAgICAgICAgIG1haW5ab29tLnRyYW5zbGF0ZShbdjEsIHYyXSk7XG4gICAgICAgICAgcmV0dXJuIGQzbWFpblN2Z0cuYXR0cihcInRyYW5zZm9ybVwiLCBcInRyYW5zbGF0ZShcIiArIHYxICsgXCIsXCIgKyB2MiArIFwiKSBzY2FsZShcIiArIG1haW5ab29tLnNjYWxlKCkgKyBcIilcIik7XG4gICAgICAgIH1cbiAgICAgIH07XG4gICAgICBzY29wZS56b29tT3V0ID0gZnVuY3Rpb24oKSB7XG4gICAgICAgIHZhciB0cmFuc2xhdGUsIHYxLCB2MjtcbiAgICAgICAgaWYgKG1haW5ab29tLnNjYWxlKCkgPiAwLjMxKSB7XG4gICAgICAgICAgbWFpblpvb20uc2NhbGUobWFpblpvb20uc2NhbGUoKSAtIDAuMSk7XG4gICAgICAgICAgdHJhbnNsYXRlID0gbWFpblpvb20udHJhbnNsYXRlKCk7XG4gICAgICAgICAgdjEgPSB0cmFuc2xhdGVbMF0gKiAobWFpblpvb20uc2NhbGUoKSAtIDAuMSAvIChtYWluWm9vbS5zY2FsZSgpKSk7XG4gICAgICAgICAgdjIgPSB0cmFuc2xhdGVbMV0gKiAobWFpblpvb20uc2NhbGUoKSAtIDAuMSAvIChtYWluWm9vbS5zY2FsZSgpKSk7XG4gICAgICAgICAgbWFpblpvb20udHJhbnNsYXRlKFt2MSwgdjJdKTtcbiAgICAgICAgICByZXR1cm4gZDNtYWluU3ZnRy5hdHRyKFwidHJhbnNmb3JtXCIsIFwidHJhbnNsYXRlKFwiICsgdjEgKyBcIixcIiArIHYyICsgXCIpIHNjYWxlKFwiICsgbWFpblpvb20uc2NhbGUoKSArIFwiKVwiKTtcbiAgICAgICAgfVxuICAgICAgfTtcbiAgICAgIGNyZWF0ZUxhYmVsRWRnZSA9IGZ1bmN0aW9uKGVsKSB7XG4gICAgICAgIHZhciBsYWJlbFZhbHVlO1xuICAgICAgICBsYWJlbFZhbHVlID0gXCJcIjtcbiAgICAgICAgaWYgKChlbC5zaGlwX3N0cmF0ZWd5ICE9IG51bGwpIHx8IChlbC5sb2NhbF9zdHJhdGVneSAhPSBudWxsKSkge1xuICAgICAgICAgIGxhYmVsVmFsdWUgKz0gXCI8ZGl2IGNsYXNzPSdlZGdlLWxhYmVsJz5cIjtcbiAgICAgICAgICBpZiAoZWwuc2hpcF9zdHJhdGVneSAhPSBudWxsKSB7XG4gICAgICAgICAgICBsYWJlbFZhbHVlICs9IGVsLnNoaXBfc3RyYXRlZ3k7XG4gICAgICAgICAgfVxuICAgICAgICAgIGlmIChlbC50ZW1wX21vZGUgIT09IHVuZGVmaW5lZCkge1xuICAgICAgICAgICAgbGFiZWxWYWx1ZSArPSBcIiAoXCIgKyBlbC50ZW1wX21vZGUgKyBcIilcIjtcbiAgICAgICAgICB9XG4gICAgICAgICAgaWYgKGVsLmxvY2FsX3N0cmF0ZWd5ICE9PSB1bmRlZmluZWQpIHtcbiAgICAgICAgICAgIGxhYmVsVmFsdWUgKz0gXCIsPGJyPlwiICsgZWwubG9jYWxfc3RyYXRlZ3k7XG4gICAgICAgICAgfVxuICAgICAgICAgIGxhYmVsVmFsdWUgKz0gXCI8L2Rpdj5cIjtcbiAgICAgICAgfVxuICAgICAgICByZXR1cm4gbGFiZWxWYWx1ZTtcbiAgICAgIH07XG4gICAgICBpc1NwZWNpYWxJdGVyYXRpb25Ob2RlID0gZnVuY3Rpb24oaW5mbykge1xuICAgICAgICByZXR1cm4gaW5mbyA9PT0gXCJwYXJ0aWFsU29sdXRpb25cIiB8fCBpbmZvID09PSBcIm5leHRQYXJ0aWFsU29sdXRpb25cIiB8fCBpbmZvID09PSBcIndvcmtzZXRcIiB8fCBpbmZvID09PSBcIm5leHRXb3Jrc2V0XCIgfHwgaW5mbyA9PT0gXCJzb2x1dGlvblNldFwiIHx8IGluZm8gPT09IFwic29sdXRpb25EZWx0YVwiO1xuICAgICAgfTtcbiAgICAgIGdldE5vZGVUeXBlID0gZnVuY3Rpb24oZWwsIGluZm8pIHtcbiAgICAgICAgaWYgKGluZm8gPT09IFwibWlycm9yXCIpIHtcbiAgICAgICAgICByZXR1cm4gJ25vZGUtbWlycm9yJztcbiAgICAgICAgfSBlbHNlIGlmIChpc1NwZWNpYWxJdGVyYXRpb25Ob2RlKGluZm8pKSB7XG4gICAgICAgICAgcmV0dXJuICdub2RlLWl0ZXJhdGlvbic7XG4gICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgaWYgKGVsLnBhY3QgPT09IFwiRGF0YSBTb3VyY2VcIikge1xuICAgICAgICAgICAgcmV0dXJuICdub2RlLXNvdXJjZSc7XG4gICAgICAgICAgfSBlbHNlIGlmIChlbC5wYWN0ID09PSBcIkRhdGEgU2lua1wiKSB7XG4gICAgICAgICAgICByZXR1cm4gJ25vZGUtc2luayc7XG4gICAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICAgIHJldHVybiAnbm9kZS1ub3JtYWwnO1xuICAgICAgICAgIH1cbiAgICAgICAgfVxuICAgICAgfTtcbiAgICAgIGNyZWF0ZUxhYmVsTm9kZSA9IGZ1bmN0aW9uKGVsLCBpbmZvLCBtYXhXLCBtYXhIKSB7XG4gICAgICAgIHZhciBsYWJlbFZhbHVlLCBzdGVwTmFtZTtcbiAgICAgICAgbGFiZWxWYWx1ZSA9IFwiPGEgaHJlZj0nIy9qb2JzL1wiICsgam9iaWQgKyBcIi9cIiArIGVsLmlkICsgXCInIGNsYXNzPSdub2RlLWxhYmVsIFwiICsgZ2V0Tm9kZVR5cGUoZWwsIGluZm8pICsgXCInPlwiO1xuICAgICAgICBpZiAoaW5mbyA9PT0gXCJtaXJyb3JcIikge1xuICAgICAgICAgIGxhYmVsVmFsdWUgKz0gXCI8aDMgY2xhc3M9J25vZGUtbmFtZSc+TWlycm9yIG9mIFwiICsgZWwucGFjdCArIFwiPC9oMz5cIjtcbiAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICBsYWJlbFZhbHVlICs9IFwiPGgzIGNsYXNzPSdub2RlLW5hbWUnPlwiICsgZWwucGFjdCArIFwiPC9oMz5cIjtcbiAgICAgICAgfVxuICAgICAgICBpZiAoZWwuY29udGVudHMgPT09IFwiXCIpIHtcbiAgICAgICAgICBsYWJlbFZhbHVlICs9IFwiXCI7XG4gICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgc3RlcE5hbWUgPSBlbC5jb250ZW50cztcbiAgICAgICAgICBzdGVwTmFtZSA9IHNob3J0ZW5TdHJpbmcoc3RlcE5hbWUpO1xuICAgICAgICAgIGxhYmVsVmFsdWUgKz0gXCI8aDQgY2xhc3M9J3N0ZXAtbmFtZSc+XCIgKyBzdGVwTmFtZSArIFwiPC9oND5cIjtcbiAgICAgICAgfVxuICAgICAgICBpZiAoZWwuc3RlcF9mdW5jdGlvbiAhPSBudWxsKSB7XG4gICAgICAgICAgbGFiZWxWYWx1ZSArPSBleHRlbmRMYWJlbE5vZGVGb3JJdGVyYXRpb24oZWwuaWQsIG1heFcsIG1heEgpO1xuICAgICAgICB9IGVsc2Uge1xuICAgICAgICAgIGlmIChpc1NwZWNpYWxJdGVyYXRpb25Ob2RlKGluZm8pKSB7XG4gICAgICAgICAgICBsYWJlbFZhbHVlICs9IFwiPGg1PlwiICsgaW5mbyArIFwiIE5vZGU8L2g1PlwiO1xuICAgICAgICAgIH1cbiAgICAgICAgICBpZiAoZWwucGFyYWxsZWxpc20gIT09IFwiXCIpIHtcbiAgICAgICAgICAgIGxhYmVsVmFsdWUgKz0gXCI8aDU+UGFyYWxsZWxpc206IFwiICsgZWwucGFyYWxsZWxpc20gKyBcIjwvaDU+XCI7XG4gICAgICAgICAgfVxuICAgICAgICAgIGlmIChlbC5kcml2ZXJfc3RyYXRlZ3kgIT09IHVuZGVmaW5lZCkge1xuICAgICAgICAgICAgbGFiZWxWYWx1ZSArPSBcIjxoNT5Ecml2ZXIgU3RyYXRlZ3k6IFwiICsgc2hvcnRlblN0cmluZyhlbC5kcml2ZXJfc3RyYXRlZ3kpICsgXCI8L2g1XCI7XG4gICAgICAgICAgfVxuICAgICAgICB9XG4gICAgICAgIGxhYmVsVmFsdWUgKz0gXCI8L2E+XCI7XG4gICAgICAgIHJldHVybiBsYWJlbFZhbHVlO1xuICAgICAgfTtcbiAgICAgIGV4dGVuZExhYmVsTm9kZUZvckl0ZXJhdGlvbiA9IGZ1bmN0aW9uKGlkLCBtYXhXLCBtYXhIKSB7XG4gICAgICAgIHZhciBsYWJlbFZhbHVlLCBzdmdJRDtcbiAgICAgICAgc3ZnSUQgPSBcInN2Zy1cIiArIGlkO1xuICAgICAgICBsYWJlbFZhbHVlID0gXCI8c3ZnIGNsYXNzPSdcIiArIHN2Z0lEICsgXCInIHdpZHRoPVwiICsgbWF4VyArIFwiIGhlaWdodD1cIiArIG1heEggKyBcIj48ZyAvPjwvc3ZnPlwiO1xuICAgICAgICByZXR1cm4gbGFiZWxWYWx1ZTtcbiAgICAgIH07XG4gICAgICBzaG9ydGVuU3RyaW5nID0gZnVuY3Rpb24ocykge1xuICAgICAgICB2YXIgc2JyO1xuICAgICAgICBpZiAocy5jaGFyQXQoMCkgPT09IFwiPFwiKSB7XG4gICAgICAgICAgcyA9IHMucmVwbGFjZShcIjxcIiwgXCImbHQ7XCIpO1xuICAgICAgICAgIHMgPSBzLnJlcGxhY2UoXCI+XCIsIFwiJmd0O1wiKTtcbiAgICAgICAgfVxuICAgICAgICBzYnIgPSBcIlwiO1xuICAgICAgICB3aGlsZSAocy5sZW5ndGggPiAzMCkge1xuICAgICAgICAgIHNiciA9IHNiciArIHMuc3Vic3RyaW5nKDAsIDMwKSArIFwiPGJyPlwiO1xuICAgICAgICAgIHMgPSBzLnN1YnN0cmluZygzMCwgcy5sZW5ndGgpO1xuICAgICAgICB9XG4gICAgICAgIHNiciA9IHNiciArIHM7XG4gICAgICAgIHJldHVybiBzYnI7XG4gICAgICB9O1xuICAgICAgY3JlYXRlTm9kZSA9IGZ1bmN0aW9uKGcsIGRhdGEsIGVsLCBpc1BhcmVudCwgbWF4VywgbWF4SCkge1xuICAgICAgICBpZiAoaXNQYXJlbnQgPT0gbnVsbCkge1xuICAgICAgICAgIGlzUGFyZW50ID0gZmFsc2U7XG4gICAgICAgIH1cbiAgICAgICAgaWYgKGVsLmlkID09PSBkYXRhLnBhcnRpYWxfc29sdXRpb24pIHtcbiAgICAgICAgICByZXR1cm4gZy5zZXROb2RlKGVsLmlkLCB7XG4gICAgICAgICAgICBsYWJlbDogY3JlYXRlTGFiZWxOb2RlKGVsLCBcInBhcnRpYWxTb2x1dGlvblwiLCBtYXhXLCBtYXhIKSxcbiAgICAgICAgICAgIGxhYmVsVHlwZTogJ2h0bWwnLFxuICAgICAgICAgICAgXCJjbGFzc1wiOiBnZXROb2RlVHlwZShlbCwgXCJwYXJ0aWFsU29sdXRpb25cIilcbiAgICAgICAgICB9KTtcbiAgICAgICAgfSBlbHNlIGlmIChlbC5pZCA9PT0gZGF0YS5uZXh0X3BhcnRpYWxfc29sdXRpb24pIHtcbiAgICAgICAgICByZXR1cm4gZy5zZXROb2RlKGVsLmlkLCB7XG4gICAgICAgICAgICBsYWJlbDogY3JlYXRlTGFiZWxOb2RlKGVsLCBcIm5leHRQYXJ0aWFsU29sdXRpb25cIiwgbWF4VywgbWF4SCksXG4gICAgICAgICAgICBsYWJlbFR5cGU6ICdodG1sJyxcbiAgICAgICAgICAgIFwiY2xhc3NcIjogZ2V0Tm9kZVR5cGUoZWwsIFwibmV4dFBhcnRpYWxTb2x1dGlvblwiKVxuICAgICAgICAgIH0pO1xuICAgICAgICB9IGVsc2UgaWYgKGVsLmlkID09PSBkYXRhLndvcmtzZXQpIHtcbiAgICAgICAgICByZXR1cm4gZy5zZXROb2RlKGVsLmlkLCB7XG4gICAgICAgICAgICBsYWJlbDogY3JlYXRlTGFiZWxOb2RlKGVsLCBcIndvcmtzZXRcIiwgbWF4VywgbWF4SCksXG4gICAgICAgICAgICBsYWJlbFR5cGU6ICdodG1sJyxcbiAgICAgICAgICAgIFwiY2xhc3NcIjogZ2V0Tm9kZVR5cGUoZWwsIFwid29ya3NldFwiKVxuICAgICAgICAgIH0pO1xuICAgICAgICB9IGVsc2UgaWYgKGVsLmlkID09PSBkYXRhLm5leHRfd29ya3NldCkge1xuICAgICAgICAgIHJldHVybiBnLnNldE5vZGUoZWwuaWQsIHtcbiAgICAgICAgICAgIGxhYmVsOiBjcmVhdGVMYWJlbE5vZGUoZWwsIFwibmV4dFdvcmtzZXRcIiwgbWF4VywgbWF4SCksXG4gICAgICAgICAgICBsYWJlbFR5cGU6ICdodG1sJyxcbiAgICAgICAgICAgIFwiY2xhc3NcIjogZ2V0Tm9kZVR5cGUoZWwsIFwibmV4dFdvcmtzZXRcIilcbiAgICAgICAgICB9KTtcbiAgICAgICAgfSBlbHNlIGlmIChlbC5pZCA9PT0gZGF0YS5zb2x1dGlvbl9zZXQpIHtcbiAgICAgICAgICByZXR1cm4gZy5zZXROb2RlKGVsLmlkLCB7XG4gICAgICAgICAgICBsYWJlbDogY3JlYXRlTGFiZWxOb2RlKGVsLCBcInNvbHV0aW9uU2V0XCIsIG1heFcsIG1heEgpLFxuICAgICAgICAgICAgbGFiZWxUeXBlOiAnaHRtbCcsXG4gICAgICAgICAgICBcImNsYXNzXCI6IGdldE5vZGVUeXBlKGVsLCBcInNvbHV0aW9uU2V0XCIpXG4gICAgICAgICAgfSk7XG4gICAgICAgIH0gZWxzZSBpZiAoZWwuaWQgPT09IGRhdGEuc29sdXRpb25fZGVsdGEpIHtcbiAgICAgICAgICByZXR1cm4gZy5zZXROb2RlKGVsLmlkLCB7XG4gICAgICAgICAgICBsYWJlbDogY3JlYXRlTGFiZWxOb2RlKGVsLCBcInNvbHV0aW9uRGVsdGFcIiwgbWF4VywgbWF4SCksXG4gICAgICAgICAgICBsYWJlbFR5cGU6ICdodG1sJyxcbiAgICAgICAgICAgIFwiY2xhc3NcIjogZ2V0Tm9kZVR5cGUoZWwsIFwic29sdXRpb25EZWx0YVwiKVxuICAgICAgICAgIH0pO1xuICAgICAgICB9IGVsc2Uge1xuICAgICAgICAgIHJldHVybiBnLnNldE5vZGUoZWwuaWQsIHtcbiAgICAgICAgICAgIGxhYmVsOiBjcmVhdGVMYWJlbE5vZGUoZWwsIFwiXCIsIG1heFcsIG1heEgpLFxuICAgICAgICAgICAgbGFiZWxUeXBlOiAnaHRtbCcsXG4gICAgICAgICAgICBcImNsYXNzXCI6IGdldE5vZGVUeXBlKGVsLCBcIlwiKVxuICAgICAgICAgIH0pO1xuICAgICAgICB9XG4gICAgICB9O1xuICAgICAgY3JlYXRlRWRnZSA9IGZ1bmN0aW9uKGcsIGRhdGEsIGVsLCBleGlzdGluZ05vZGVzLCBwcmVkKSB7XG4gICAgICAgIHZhciBtaXNzaW5nTm9kZTtcbiAgICAgICAgaWYgKGV4aXN0aW5nTm9kZXMuaW5kZXhPZihwcmVkLmlkKSAhPT0gLTEpIHtcbiAgICAgICAgICByZXR1cm4gZy5zZXRFZGdlKHByZWQuaWQsIGVsLmlkLCB7XG4gICAgICAgICAgICBsYWJlbDogY3JlYXRlTGFiZWxFZGdlKHByZWQpLFxuICAgICAgICAgICAgbGFiZWxUeXBlOiAnaHRtbCcsXG4gICAgICAgICAgICBhcnJvd2hlYWQ6ICdub3JtYWwnXG4gICAgICAgICAgfSk7XG4gICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgbWlzc2luZ05vZGUgPSBzZWFyY2hGb3JOb2RlKGRhdGEsIHByZWQuaWQpO1xuICAgICAgICAgIGlmICghKCFtaXNzaW5nTm9kZSB8fCBtaXNzaW5nTm9kZS5hbHJlYWR5QWRkZWQgPT09IHRydWUpKSB7XG4gICAgICAgICAgICBtaXNzaW5nTm9kZS5hbHJlYWR5QWRkZWQgPSB0cnVlO1xuICAgICAgICAgICAgZy5zZXROb2RlKG1pc3NpbmdOb2RlLmlkLCB7XG4gICAgICAgICAgICAgIGxhYmVsOiBjcmVhdGVMYWJlbE5vZGUobWlzc2luZ05vZGUsIFwibWlycm9yXCIpLFxuICAgICAgICAgICAgICBsYWJlbFR5cGU6ICdodG1sJyxcbiAgICAgICAgICAgICAgXCJjbGFzc1wiOiBnZXROb2RlVHlwZShtaXNzaW5nTm9kZSwgJ21pcnJvcicpXG4gICAgICAgICAgICB9KTtcbiAgICAgICAgICAgIHJldHVybiBnLnNldEVkZ2UobWlzc2luZ05vZGUuaWQsIGVsLmlkLCB7XG4gICAgICAgICAgICAgIGxhYmVsOiBjcmVhdGVMYWJlbEVkZ2UobWlzc2luZ05vZGUpLFxuICAgICAgICAgICAgICBsYWJlbFR5cGU6ICdodG1sJ1xuICAgICAgICAgICAgfSk7XG4gICAgICAgICAgfVxuICAgICAgICB9XG4gICAgICB9O1xuICAgICAgbG9hZEpzb25Ub0RhZ3JlID0gZnVuY3Rpb24oZywgZGF0YSkge1xuICAgICAgICB2YXIgZWwsIGV4aXN0aW5nTm9kZXMsIGlzUGFyZW50LCBrLCBsLCBsZW4sIGxlbjEsIG1heEgsIG1heFcsIHByZWQsIHIsIHJlZiwgc2csIHRvSXRlcmF0ZTtcbiAgICAgICAgZXhpc3RpbmdOb2RlcyA9IFtdO1xuICAgICAgICBpZiAoZGF0YS5ub2RlcyAhPSBudWxsKSB7XG4gICAgICAgICAgdG9JdGVyYXRlID0gZGF0YS5ub2RlcztcbiAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICB0b0l0ZXJhdGUgPSBkYXRhLnN0ZXBfZnVuY3Rpb247XG4gICAgICAgICAgaXNQYXJlbnQgPSB0cnVlO1xuICAgICAgICB9XG4gICAgICAgIGZvciAoayA9IDAsIGxlbiA9IHRvSXRlcmF0ZS5sZW5ndGg7IGsgPCBsZW47IGsrKykge1xuICAgICAgICAgIGVsID0gdG9JdGVyYXRlW2tdO1xuICAgICAgICAgIG1heFcgPSAwO1xuICAgICAgICAgIG1heEggPSAwO1xuICAgICAgICAgIGlmIChlbC5zdGVwX2Z1bmN0aW9uKSB7XG4gICAgICAgICAgICBzZyA9IG5ldyBkYWdyZUQzLmdyYXBobGliLkdyYXBoKHtcbiAgICAgICAgICAgICAgbXVsdGlncmFwaDogdHJ1ZSxcbiAgICAgICAgICAgICAgY29tcG91bmQ6IHRydWVcbiAgICAgICAgICAgIH0pLnNldEdyYXBoKHtcbiAgICAgICAgICAgICAgbm9kZXNlcDogMjAsXG4gICAgICAgICAgICAgIGVkZ2VzZXA6IDAsXG4gICAgICAgICAgICAgIHJhbmtzZXA6IDIwLFxuICAgICAgICAgICAgICByYW5rZGlyOiBcIkxSXCIsXG4gICAgICAgICAgICAgIG1hcmdpbng6IDEwLFxuICAgICAgICAgICAgICBtYXJnaW55OiAxMFxuICAgICAgICAgICAgfSk7XG4gICAgICAgICAgICBzdWJncmFwaHNbZWwuaWRdID0gc2c7XG4gICAgICAgICAgICBsb2FkSnNvblRvRGFncmUoc2csIGVsKTtcbiAgICAgICAgICAgIHIgPSBuZXcgZGFncmVEMy5yZW5kZXIoKTtcbiAgICAgICAgICAgIGQzdG1wU3ZnLnNlbGVjdCgnZycpLmNhbGwociwgc2cpO1xuICAgICAgICAgICAgbWF4VyA9IHNnLmdyYXBoKCkud2lkdGg7XG4gICAgICAgICAgICBtYXhIID0gc2cuZ3JhcGgoKS5oZWlnaHQ7XG4gICAgICAgICAgICBhbmd1bGFyLmVsZW1lbnQobWFpblRtcEVsZW1lbnQpLmVtcHR5KCk7XG4gICAgICAgICAgfVxuICAgICAgICAgIGNyZWF0ZU5vZGUoZywgZGF0YSwgZWwsIGlzUGFyZW50LCBtYXhXLCBtYXhIKTtcbiAgICAgICAgICBleGlzdGluZ05vZGVzLnB1c2goZWwuaWQpO1xuICAgICAgICAgIGlmIChlbC5wcmVkZWNlc3NvcnMgIT0gbnVsbCkge1xuICAgICAgICAgICAgcmVmID0gZWwucHJlZGVjZXNzb3JzO1xuICAgICAgICAgICAgZm9yIChsID0gMCwgbGVuMSA9IHJlZi5sZW5ndGg7IGwgPCBsZW4xOyBsKyspIHtcbiAgICAgICAgICAgICAgcHJlZCA9IHJlZltsXTtcbiAgICAgICAgICAgICAgY3JlYXRlRWRnZShnLCBkYXRhLCBlbCwgZXhpc3RpbmdOb2RlcywgcHJlZCk7XG4gICAgICAgICAgICB9XG4gICAgICAgICAgfVxuICAgICAgICB9XG4gICAgICAgIHJldHVybiBnO1xuICAgICAgfTtcbiAgICAgIHNlYXJjaEZvck5vZGUgPSBmdW5jdGlvbihkYXRhLCBub2RlSUQpIHtcbiAgICAgICAgdmFyIGVsLCBpLCBqO1xuICAgICAgICBmb3IgKGkgaW4gZGF0YS5ub2Rlcykge1xuICAgICAgICAgIGVsID0gZGF0YS5ub2Rlc1tpXTtcbiAgICAgICAgICBpZiAoZWwuaWQgPT09IG5vZGVJRCkge1xuICAgICAgICAgICAgcmV0dXJuIGVsO1xuICAgICAgICAgIH1cbiAgICAgICAgICBpZiAoZWwuc3RlcF9mdW5jdGlvbiAhPSBudWxsKSB7XG4gICAgICAgICAgICBmb3IgKGogaW4gZWwuc3RlcF9mdW5jdGlvbikge1xuICAgICAgICAgICAgICBpZiAoZWwuc3RlcF9mdW5jdGlvbltqXS5pZCA9PT0gbm9kZUlEKSB7XG4gICAgICAgICAgICAgICAgcmV0dXJuIGVsLnN0ZXBfZnVuY3Rpb25bal07XG4gICAgICAgICAgICAgIH1cbiAgICAgICAgICAgIH1cbiAgICAgICAgICB9XG4gICAgICAgIH1cbiAgICAgIH07XG4gICAgICBkcmF3R3JhcGggPSBmdW5jdGlvbihkYXRhKSB7XG4gICAgICAgIHZhciBnLCBpLCBuZXdTY2FsZSwgcmVuZGVyZXIsIHNnLCB4Q2VudGVyT2Zmc2V0LCB5Q2VudGVyT2Zmc2V0O1xuICAgICAgICBnID0gbmV3IGRhZ3JlRDMuZ3JhcGhsaWIuR3JhcGgoe1xuICAgICAgICAgIG11bHRpZ3JhcGg6IHRydWUsXG4gICAgICAgICAgY29tcG91bmQ6IHRydWVcbiAgICAgICAgfSkuc2V0R3JhcGgoe1xuICAgICAgICAgIG5vZGVzZXA6IDcwLFxuICAgICAgICAgIGVkZ2VzZXA6IDAsXG4gICAgICAgICAgcmFua3NlcDogNTAsXG4gICAgICAgICAgcmFua2RpcjogXCJMUlwiLFxuICAgICAgICAgIG1hcmdpbng6IDQwLFxuICAgICAgICAgIG1hcmdpbnk6IDQwXG4gICAgICAgIH0pO1xuICAgICAgICBsb2FkSnNvblRvRGFncmUoZywgZGF0YSk7XG4gICAgICAgIHJlbmRlcmVyID0gbmV3IGRhZ3JlRDMucmVuZGVyKCk7XG4gICAgICAgIGQzbWFpblN2Z0cuY2FsbChyZW5kZXJlciwgZyk7XG4gICAgICAgIGZvciAoaSBpbiBzdWJncmFwaHMpIHtcbiAgICAgICAgICBzZyA9IHN1YmdyYXBoc1tpXTtcbiAgICAgICAgICBkM21haW5Tdmcuc2VsZWN0KCdzdmcuc3ZnLScgKyBpICsgJyBnJykuY2FsbChyZW5kZXJlciwgc2cpO1xuICAgICAgICB9XG4gICAgICAgIG5ld1NjYWxlID0gMC41O1xuICAgICAgICB4Q2VudGVyT2Zmc2V0ID0gTWF0aC5mbG9vcigoYW5ndWxhci5lbGVtZW50KG1haW5TdmdFbGVtZW50KS53aWR0aCgpIC0gZy5ncmFwaCgpLndpZHRoICogbmV3U2NhbGUpIC8gMik7XG4gICAgICAgIHlDZW50ZXJPZmZzZXQgPSBNYXRoLmZsb29yKChhbmd1bGFyLmVsZW1lbnQobWFpblN2Z0VsZW1lbnQpLmhlaWdodCgpIC0gZy5ncmFwaCgpLmhlaWdodCAqIG5ld1NjYWxlKSAvIDIpO1xuICAgICAgICBtYWluWm9vbS5zY2FsZShuZXdTY2FsZSkudHJhbnNsYXRlKFt4Q2VudGVyT2Zmc2V0LCB5Q2VudGVyT2Zmc2V0XSk7XG4gICAgICAgIGQzbWFpblN2Z0cuYXR0cihcInRyYW5zZm9ybVwiLCBcInRyYW5zbGF0ZShcIiArIHhDZW50ZXJPZmZzZXQgKyBcIiwgXCIgKyB5Q2VudGVyT2Zmc2V0ICsgXCIpIHNjYWxlKFwiICsgbWFpblpvb20uc2NhbGUoKSArIFwiKVwiKTtcbiAgICAgICAgbWFpblpvb20ub24oXCJ6b29tXCIsIGZ1bmN0aW9uKCkge1xuICAgICAgICAgIHZhciBldjtcbiAgICAgICAgICBldiA9IGQzLmV2ZW50O1xuICAgICAgICAgIHJldHVybiBkM21haW5TdmdHLmF0dHIoXCJ0cmFuc2Zvcm1cIiwgXCJ0cmFuc2xhdGUoXCIgKyBldi50cmFuc2xhdGUgKyBcIikgc2NhbGUoXCIgKyBldi5zY2FsZSArIFwiKVwiKTtcbiAgICAgICAgfSk7XG4gICAgICAgIHJldHVybiBtYWluWm9vbShkM21haW5TdmcpO1xuICAgICAgfTtcbiAgICAgIHNjb3BlLiR3YXRjaChhdHRycy5wbGFuLCBmdW5jdGlvbihuZXdQbGFuKSB7XG4gICAgICAgIGlmIChuZXdQbGFuKSB7XG4gICAgICAgICAgcmV0dXJuIGRyYXdHcmFwaChuZXdQbGFuKTtcbiAgICAgICAgfVxuICAgICAgfSk7XG4gICAgfVxuICB9O1xufSk7XG4iLCIjXG4jIExpY2Vuc2VkIHRvIHRoZSBBcGFjaGUgU29mdHdhcmUgRm91bmRhdGlvbiAoQVNGKSB1bmRlciBvbmVcbiMgb3IgbW9yZSBjb250cmlidXRvciBsaWNlbnNlIGFncmVlbWVudHMuICBTZWUgdGhlIE5PVElDRSBmaWxlXG4jIGRpc3RyaWJ1dGVkIHdpdGggdGhpcyB3b3JrIGZvciBhZGRpdGlvbmFsIGluZm9ybWF0aW9uXG4jIHJlZ2FyZGluZyBjb3B5cmlnaHQgb3duZXJzaGlwLiAgVGhlIEFTRiBsaWNlbnNlcyB0aGlzIGZpbGVcbiMgdG8geW91IHVuZGVyIHRoZSBBcGFjaGUgTGljZW5zZSwgVmVyc2lvbiAyLjAgKHRoZVxuIyBcIkxpY2Vuc2VcIik7IHlvdSBtYXkgbm90IHVzZSB0aGlzIGZpbGUgZXhjZXB0IGluIGNvbXBsaWFuY2VcbiMgd2l0aCB0aGUgTGljZW5zZS4gIFlvdSBtYXkgb2J0YWluIGEgY29weSBvZiB0aGUgTGljZW5zZSBhdFxuI1xuIyAgICAgaHR0cDovL3d3dy5hcGFjaGUub3JnL2xpY2Vuc2VzL0xJQ0VOU0UtMi4wXG4jXG4jIFVubGVzcyByZXF1aXJlZCBieSBhcHBsaWNhYmxlIGxhdyBvciBhZ3JlZWQgdG8gaW4gd3JpdGluZywgc29mdHdhcmVcbiMgZGlzdHJpYnV0ZWQgdW5kZXIgdGhlIExpY2Vuc2UgaXMgZGlzdHJpYnV0ZWQgb24gYW4gXCJBUyBJU1wiIEJBU0lTLFxuIyBXSVRIT1VUIFdBUlJBTlRJRVMgT1IgQ09ORElUSU9OUyBPRiBBTlkgS0lORCwgZWl0aGVyIGV4cHJlc3Mgb3IgaW1wbGllZC5cbiMgU2VlIHRoZSBMaWNlbnNlIGZvciB0aGUgc3BlY2lmaWMgbGFuZ3VhZ2UgZ292ZXJuaW5nIHBlcm1pc3Npb25zIGFuZFxuIyBsaW1pdGF0aW9ucyB1bmRlciB0aGUgTGljZW5zZS5cbiNcblxuYW5ndWxhci5tb2R1bGUoJ2ZsaW5rQXBwJylcblxuLnNlcnZpY2UgJ0pvYnNTZXJ2aWNlJywgKCRodHRwLCBmbGlua0NvbmZpZywgJGxvZywgYW1Nb21lbnQsICRxLCAkdGltZW91dCkgLT5cbiAgY3VycmVudEpvYiA9IG51bGxcbiAgY3VycmVudFBsYW4gPSBudWxsXG4gIGRlZmVycmVkcyA9IHt9XG4gIGpvYnMgPSB7XG4gICAgcnVubmluZzogW11cbiAgICBmaW5pc2hlZDogW11cbiAgICBjYW5jZWxsZWQ6IFtdXG4gICAgZmFpbGVkOiBbXVxuICB9XG5cbiAgam9iT2JzZXJ2ZXJzID0gW11cblxuICBub3RpZnlPYnNlcnZlcnMgPSAtPlxuICAgIGFuZ3VsYXIuZm9yRWFjaCBqb2JPYnNlcnZlcnMsIChjYWxsYmFjaykgLT5cbiAgICAgIGNhbGxiYWNrKClcblxuICBAcmVnaXN0ZXJPYnNlcnZlciA9IChjYWxsYmFjaykgLT5cbiAgICBqb2JPYnNlcnZlcnMucHVzaChjYWxsYmFjaylcblxuICBAdW5SZWdpc3Rlck9ic2VydmVyID0gKGNhbGxiYWNrKSAtPlxuICAgIGluZGV4ID0gam9iT2JzZXJ2ZXJzLmluZGV4T2YoY2FsbGJhY2spXG4gICAgam9iT2JzZXJ2ZXJzLnNwbGljZShpbmRleCwgMSlcblxuICBAc3RhdGVMaXN0ID0gLT5cbiAgICBbIFxuICAgICAgIyAnQ1JFQVRFRCdcbiAgICAgICdTQ0hFRFVMRUQnXG4gICAgICAnREVQTE9ZSU5HJ1xuICAgICAgJ1JVTk5JTkcnXG4gICAgICAnRklOSVNIRUQnXG4gICAgICAnRkFJTEVEJ1xuICAgICAgJ0NBTkNFTElORydcbiAgICAgICdDQU5DRUxFRCdcbiAgICBdXG5cbiAgQHRyYW5zbGF0ZUxhYmVsU3RhdGUgPSAoc3RhdGUpIC0+XG4gICAgc3dpdGNoIHN0YXRlLnRvTG93ZXJDYXNlKClcbiAgICAgIHdoZW4gJ2ZpbmlzaGVkJyB0aGVuICdzdWNjZXNzJ1xuICAgICAgd2hlbiAnZmFpbGVkJyB0aGVuICdkYW5nZXInXG4gICAgICB3aGVuICdzY2hlZHVsZWQnIHRoZW4gJ2RlZmF1bHQnXG4gICAgICB3aGVuICdkZXBsb3lpbmcnIHRoZW4gJ2luZm8nXG4gICAgICB3aGVuICdydW5uaW5nJyB0aGVuICdwcmltYXJ5J1xuICAgICAgd2hlbiAnY2FuY2VsaW5nJyB0aGVuICd3YXJuaW5nJ1xuICAgICAgd2hlbiAncGVuZGluZycgdGhlbiAnaW5mbydcbiAgICAgIHdoZW4gJ3RvdGFsJyB0aGVuICdibGFjaydcbiAgICAgIGVsc2UgJ2RlZmF1bHQnXG5cbiAgQGxpc3RKb2JzID0gLT5cbiAgICBkZWZlcnJlZCA9ICRxLmRlZmVyKClcblxuICAgICRodHRwLmdldCBmbGlua0NvbmZpZy5uZXdTZXJ2ZXIgKyBcIi9qb2JzXCJcbiAgICAuc3VjY2VzcyAoZGF0YSwgc3RhdHVzLCBoZWFkZXJzLCBjb25maWcpIC0+XG5cbiAgICAgIGFuZ3VsYXIuZm9yRWFjaCBkYXRhLCAobGlzdCwgbGlzdEtleSkgLT5cblxuICAgICAgICBzd2l0Y2ggbGlzdEtleVxuICAgICAgICAgIHdoZW4gJ2pvYnMtcnVubmluZycgdGhlbiBqb2JzLnJ1bm5pbmcgPSBsaXN0XG4gICAgICAgICAgd2hlbiAnam9icy1maW5pc2hlZCcgdGhlbiBqb2JzLmZpbmlzaGVkID0gbGlzdFxuICAgICAgICAgIHdoZW4gJ2pvYnMtY2FuY2VsbGVkJyB0aGVuIGpvYnMuY2FuY2VsbGVkID0gbGlzdFxuICAgICAgICAgIHdoZW4gJ2pvYnMtZmFpbGVkJyB0aGVuIGpvYnMuZmFpbGVkID0gbGlzdFxuXG4gICAgICAgIGFuZ3VsYXIuZm9yRWFjaCBsaXN0LCAoam9iaWQsIGluZGV4KSAtPlxuICAgICAgICAgICRodHRwLmdldCBmbGlua0NvbmZpZy5uZXdTZXJ2ZXIgKyBcIi9qb2JzL1wiICsgam9iaWRcbiAgICAgICAgICAuc3VjY2VzcyAoZGV0YWlscykgLT5cbiAgICAgICAgICAgIGxpc3RbaW5kZXhdID0gZGV0YWlsc1xuXG4gICAgICBkZWZlcnJlZC5yZXNvbHZlKGpvYnMpXG4gICAgICBub3RpZnlPYnNlcnZlcnMoKVxuXG4gICAgZGVmZXJyZWQucHJvbWlzZVxuXG4gIEBnZXRKb2JzID0gKHR5cGUpIC0+XG4gICAgam9ic1t0eXBlXVxuXG4gIEBnZXRBbGxKb2JzID0gLT5cbiAgICBqb2JzXG5cbiAgQGxvYWRKb2IgPSAoam9iaWQpIC0+XG4gICAgY3VycmVudEpvYiA9IG51bGxcbiAgICBkZWZlcnJlZHMuam9iID0gJHEuZGVmZXIoKVxuXG4gICAgJGh0dHAuZ2V0IGZsaW5rQ29uZmlnLm5ld1NlcnZlciArIFwiL2pvYnMvXCIgKyBqb2JpZFxuICAgIC5zdWNjZXNzIChkYXRhLCBzdGF0dXMsIGhlYWRlcnMsIGNvbmZpZykgLT5cbiAgICAgIGRhdGEudGltZSA9IERhdGUubm93KClcblxuICAgICAgJGh0dHAuZ2V0IGZsaW5rQ29uZmlnLm5ld1NlcnZlciArIFwiL2pvYnMvXCIgKyBqb2JpZCArIFwiL3ZlcnRpY2VzXCJcbiAgICAgIC5zdWNjZXNzICh2ZXJ0aWNlcykgLT5cbiAgICAgICAgZGF0YSA9IGFuZ3VsYXIuZXh0ZW5kKGRhdGEsIHZlcnRpY2VzKVxuXG4gICAgICAgICRodHRwLmdldCBmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcIi9qb2JzSW5mbz9nZXQ9am9iJmpvYj1cIiArIGpvYmlkXG4gICAgICAgIC5zdWNjZXNzIChvbGRWZXJ0aWNlcykgLT5cbiAgICAgICAgICBkYXRhLm9sZFYgPSBvbGRWZXJ0aWNlc1swXVxuXG4gICAgICAgICAgY3VycmVudEpvYiA9IGRhdGFcbiAgICAgICAgICBkZWZlcnJlZHMuam9iLnJlc29sdmUoZGF0YSlcblxuICAgIGRlZmVycmVkcy5qb2IucHJvbWlzZVxuXG4gIEBsb2FkUGxhbiA9IChqb2JpZCkgLT5cbiAgICBjdXJyZW50UGxhbiA9IG51bGxcbiAgICBkZWZlcnJlZHMucGxhbiA9ICRxLmRlZmVyKClcblxuICAgICRodHRwLmdldCBmbGlua0NvbmZpZy5uZXdTZXJ2ZXIgKyBcIi9qb2JzL1wiICsgam9iaWQgKyBcIi9wbGFuXCJcbiAgICAuc3VjY2VzcyAoZGF0YSkgLT5cbiAgICAgIGN1cnJlbnRQbGFuID0gZGF0YVxuXG4gICAgICBkZWZlcnJlZHMucGxhbi5yZXNvbHZlKGRhdGEpXG5cbiAgICBkZWZlcnJlZHMucGxhbi5wcm9taXNlXG5cbiAgQGdldE5vZGUgPSAobm9kZWlkKSAtPlxuICAgIHNlZWtOb2RlID0gKG5vZGVpZCwgZGF0YSkgLT5cbiAgICAgIG5vZGVpZCA9IHBhcnNlSW50KG5vZGVpZClcblxuICAgICAgZm9yIG5vZGUgaW4gZGF0YVxuICAgICAgICByZXR1cm4gbm9kZSBpZiBub2RlLmlkIGlzIG5vZGVpZFxuICAgICAgICBzdWIgPSBzZWVrTm9kZShub2RlaWQsIG5vZGUuc3RlcF9mdW5jdGlvbikgaWYgbm9kZS5zdGVwX2Z1bmN0aW9uXG4gICAgICAgIHJldHVybiBzdWIgaWYgc3ViXG5cbiAgICAgIG51bGxcblxuICAgIGRlZmVycmVkID0gJHEuZGVmZXIoKVxuXG4gICAgIyBpZiBjdXJyZW50UGxhblxuICAgICMgICBkZWZlcnJlZC5yZXNvbHZlKHNlZWtOb2RlKG5vZGVpZCwgY3VycmVudFBsYW4ubm9kZXMpKVxuICAgICMgZWxzZVxuICAgICMgICAjIGRlZmVycmVkcy5wbGFuLnByb21pc2UudGhlbiAoZGF0YSkgLT5cbiAgICAjICAgJHEuYWxsKFtkZWZlcnJlZHMucGxhbi5wcm9taXNlLCBkZWZlcnJlZHMuam9iLnByb21pc2VdKS50aGVuIChkYXRhKSAtPlxuICAgICMgICAgIGNvbnNvbGUubG9nICdyZXNvbHZpbmcgZ2V0Tm9kZSdcbiAgICAjICAgICBkZWZlcnJlZC5yZXNvbHZlKHNlZWtOb2RlKG5vZGVpZCwgY3VycmVudFBsYW4ubm9kZXMpKVxuXG4gICAgJHEuYWxsKFtkZWZlcnJlZHMucGxhbi5wcm9taXNlLCBkZWZlcnJlZHMuam9iLnByb21pc2VdKS50aGVuIChkYXRhKSA9PlxuICAgICAgZm91bmROb2RlID0gc2Vla05vZGUobm9kZWlkLCBjdXJyZW50UGxhbi5ub2RlcylcblxuICAgICAgIyBUT0RPIGxpbmsgdG8gcmVhbCB2ZXJ0ZXguIGZvciBub3cgdGhlcmUgaXMgbm8gd2F5IHRvIGdldCB0aGUgcmlnaHQgb25lLCBzbyB3ZSBhcmUgc2hvd2luZyB0aGUgZmlyc3Qgb25lIC0ganVzdCBmb3IgdGVzdGluZ1xuICAgICAgQGdldFZlcnRleChjdXJyZW50Sm9iLmppZCwgY3VycmVudEpvYi5vbGRWLmdyb3VwdmVydGljZXNbMF0uZ3JvdXB2ZXJ0ZXhpZCkudGhlbiAodmVydGV4KSAtPlxuICAgICAgICBmb3VuZE5vZGUudmVydGV4ID0gdmVydGV4XG4gICAgICAgIGRlZmVycmVkLnJlc29sdmUoZm91bmROb2RlKVxuXG4gICAgZGVmZXJyZWQucHJvbWlzZVxuXG5cbiAgQGdldFZlcnRleCA9IChqb2JJZCwgdmVydGV4SWQpIC0+XG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpXG5cbiAgICAkaHR0cC5nZXQgZmxpbmtDb25maWcuam9iU2VydmVyICsgXCIvam9ic0luZm8/Z2V0PWdyb3VwdmVydGV4JmpvYj1cIiArIGpvYklkICsgXCImZ3JvdXB2ZXJ0ZXg9XCIgKyB2ZXJ0ZXhJZFxuICAgIC5zdWNjZXNzIChkYXRhKSAtPlxuICAgICAgZGVmZXJyZWQucmVzb2x2ZShkYXRhKVxuXG4gICAgZGVmZXJyZWQucHJvbWlzZVxuXG4gIEBcbiIsImFuZ3VsYXIubW9kdWxlKCdmbGlua0FwcCcpLnNlcnZpY2UoJ0pvYnNTZXJ2aWNlJywgZnVuY3Rpb24oJGh0dHAsIGZsaW5rQ29uZmlnLCAkbG9nLCBhbU1vbWVudCwgJHEsICR0aW1lb3V0KSB7XG4gIHZhciBjdXJyZW50Sm9iLCBjdXJyZW50UGxhbiwgZGVmZXJyZWRzLCBqb2JPYnNlcnZlcnMsIGpvYnMsIG5vdGlmeU9ic2VydmVycztcbiAgY3VycmVudEpvYiA9IG51bGw7XG4gIGN1cnJlbnRQbGFuID0gbnVsbDtcbiAgZGVmZXJyZWRzID0ge307XG4gIGpvYnMgPSB7XG4gICAgcnVubmluZzogW10sXG4gICAgZmluaXNoZWQ6IFtdLFxuICAgIGNhbmNlbGxlZDogW10sXG4gICAgZmFpbGVkOiBbXVxuICB9O1xuICBqb2JPYnNlcnZlcnMgPSBbXTtcbiAgbm90aWZ5T2JzZXJ2ZXJzID0gZnVuY3Rpb24oKSB7XG4gICAgcmV0dXJuIGFuZ3VsYXIuZm9yRWFjaChqb2JPYnNlcnZlcnMsIGZ1bmN0aW9uKGNhbGxiYWNrKSB7XG4gICAgICByZXR1cm4gY2FsbGJhY2soKTtcbiAgICB9KTtcbiAgfTtcbiAgdGhpcy5yZWdpc3Rlck9ic2VydmVyID0gZnVuY3Rpb24oY2FsbGJhY2spIHtcbiAgICByZXR1cm4gam9iT2JzZXJ2ZXJzLnB1c2goY2FsbGJhY2spO1xuICB9O1xuICB0aGlzLnVuUmVnaXN0ZXJPYnNlcnZlciA9IGZ1bmN0aW9uKGNhbGxiYWNrKSB7XG4gICAgdmFyIGluZGV4O1xuICAgIGluZGV4ID0gam9iT2JzZXJ2ZXJzLmluZGV4T2YoY2FsbGJhY2spO1xuICAgIHJldHVybiBqb2JPYnNlcnZlcnMuc3BsaWNlKGluZGV4LCAxKTtcbiAgfTtcbiAgdGhpcy5zdGF0ZUxpc3QgPSBmdW5jdGlvbigpIHtcbiAgICByZXR1cm4gWydTQ0hFRFVMRUQnLCAnREVQTE9ZSU5HJywgJ1JVTk5JTkcnLCAnRklOSVNIRUQnLCAnRkFJTEVEJywgJ0NBTkNFTElORycsICdDQU5DRUxFRCddO1xuICB9O1xuICB0aGlzLnRyYW5zbGF0ZUxhYmVsU3RhdGUgPSBmdW5jdGlvbihzdGF0ZSkge1xuICAgIHN3aXRjaCAoc3RhdGUudG9Mb3dlckNhc2UoKSkge1xuICAgICAgY2FzZSAnZmluaXNoZWQnOlxuICAgICAgICByZXR1cm4gJ3N1Y2Nlc3MnO1xuICAgICAgY2FzZSAnZmFpbGVkJzpcbiAgICAgICAgcmV0dXJuICdkYW5nZXInO1xuICAgICAgY2FzZSAnc2NoZWR1bGVkJzpcbiAgICAgICAgcmV0dXJuICdkZWZhdWx0JztcbiAgICAgIGNhc2UgJ2RlcGxveWluZyc6XG4gICAgICAgIHJldHVybiAnaW5mbyc7XG4gICAgICBjYXNlICdydW5uaW5nJzpcbiAgICAgICAgcmV0dXJuICdwcmltYXJ5JztcbiAgICAgIGNhc2UgJ2NhbmNlbGluZyc6XG4gICAgICAgIHJldHVybiAnd2FybmluZyc7XG4gICAgICBjYXNlICdwZW5kaW5nJzpcbiAgICAgICAgcmV0dXJuICdpbmZvJztcbiAgICAgIGNhc2UgJ3RvdGFsJzpcbiAgICAgICAgcmV0dXJuICdibGFjayc7XG4gICAgICBkZWZhdWx0OlxuICAgICAgICByZXR1cm4gJ2RlZmF1bHQnO1xuICAgIH1cbiAgfTtcbiAgdGhpcy5saXN0Sm9icyA9IGZ1bmN0aW9uKCkge1xuICAgIHZhciBkZWZlcnJlZDtcbiAgICBkZWZlcnJlZCA9ICRxLmRlZmVyKCk7XG4gICAgJGh0dHAuZ2V0KGZsaW5rQ29uZmlnLm5ld1NlcnZlciArIFwiL2pvYnNcIikuc3VjY2VzcyhmdW5jdGlvbihkYXRhLCBzdGF0dXMsIGhlYWRlcnMsIGNvbmZpZykge1xuICAgICAgYW5ndWxhci5mb3JFYWNoKGRhdGEsIGZ1bmN0aW9uKGxpc3QsIGxpc3RLZXkpIHtcbiAgICAgICAgc3dpdGNoIChsaXN0S2V5KSB7XG4gICAgICAgICAgY2FzZSAnam9icy1ydW5uaW5nJzpcbiAgICAgICAgICAgIGpvYnMucnVubmluZyA9IGxpc3Q7XG4gICAgICAgICAgICBicmVhaztcbiAgICAgICAgICBjYXNlICdqb2JzLWZpbmlzaGVkJzpcbiAgICAgICAgICAgIGpvYnMuZmluaXNoZWQgPSBsaXN0O1xuICAgICAgICAgICAgYnJlYWs7XG4gICAgICAgICAgY2FzZSAnam9icy1jYW5jZWxsZWQnOlxuICAgICAgICAgICAgam9icy5jYW5jZWxsZWQgPSBsaXN0O1xuICAgICAgICAgICAgYnJlYWs7XG4gICAgICAgICAgY2FzZSAnam9icy1mYWlsZWQnOlxuICAgICAgICAgICAgam9icy5mYWlsZWQgPSBsaXN0O1xuICAgICAgICB9XG4gICAgICAgIHJldHVybiBhbmd1bGFyLmZvckVhY2gobGlzdCwgZnVuY3Rpb24oam9iaWQsIGluZGV4KSB7XG4gICAgICAgICAgcmV0dXJuICRodHRwLmdldChmbGlua0NvbmZpZy5uZXdTZXJ2ZXIgKyBcIi9qb2JzL1wiICsgam9iaWQpLnN1Y2Nlc3MoZnVuY3Rpb24oZGV0YWlscykge1xuICAgICAgICAgICAgcmV0dXJuIGxpc3RbaW5kZXhdID0gZGV0YWlscztcbiAgICAgICAgICB9KTtcbiAgICAgICAgfSk7XG4gICAgICB9KTtcbiAgICAgIGRlZmVycmVkLnJlc29sdmUoam9icyk7XG4gICAgICByZXR1cm4gbm90aWZ5T2JzZXJ2ZXJzKCk7XG4gICAgfSk7XG4gICAgcmV0dXJuIGRlZmVycmVkLnByb21pc2U7XG4gIH07XG4gIHRoaXMuZ2V0Sm9icyA9IGZ1bmN0aW9uKHR5cGUpIHtcbiAgICByZXR1cm4gam9ic1t0eXBlXTtcbiAgfTtcbiAgdGhpcy5nZXRBbGxKb2JzID0gZnVuY3Rpb24oKSB7XG4gICAgcmV0dXJuIGpvYnM7XG4gIH07XG4gIHRoaXMubG9hZEpvYiA9IGZ1bmN0aW9uKGpvYmlkKSB7XG4gICAgY3VycmVudEpvYiA9IG51bGw7XG4gICAgZGVmZXJyZWRzLmpvYiA9ICRxLmRlZmVyKCk7XG4gICAgJGh0dHAuZ2V0KGZsaW5rQ29uZmlnLm5ld1NlcnZlciArIFwiL2pvYnMvXCIgKyBqb2JpZCkuc3VjY2VzcyhmdW5jdGlvbihkYXRhLCBzdGF0dXMsIGhlYWRlcnMsIGNvbmZpZykge1xuICAgICAgZGF0YS50aW1lID0gRGF0ZS5ub3coKTtcbiAgICAgIHJldHVybiAkaHR0cC5nZXQoZmxpbmtDb25maWcubmV3U2VydmVyICsgXCIvam9icy9cIiArIGpvYmlkICsgXCIvdmVydGljZXNcIikuc3VjY2VzcyhmdW5jdGlvbih2ZXJ0aWNlcykge1xuICAgICAgICBkYXRhID0gYW5ndWxhci5leHRlbmQoZGF0YSwgdmVydGljZXMpO1xuICAgICAgICByZXR1cm4gJGh0dHAuZ2V0KGZsaW5rQ29uZmlnLmpvYlNlcnZlciArIFwiL2pvYnNJbmZvP2dldD1qb2Imam9iPVwiICsgam9iaWQpLnN1Y2Nlc3MoZnVuY3Rpb24ob2xkVmVydGljZXMpIHtcbiAgICAgICAgICBkYXRhLm9sZFYgPSBvbGRWZXJ0aWNlc1swXTtcbiAgICAgICAgICBjdXJyZW50Sm9iID0gZGF0YTtcbiAgICAgICAgICByZXR1cm4gZGVmZXJyZWRzLmpvYi5yZXNvbHZlKGRhdGEpO1xuICAgICAgICB9KTtcbiAgICAgIH0pO1xuICAgIH0pO1xuICAgIHJldHVybiBkZWZlcnJlZHMuam9iLnByb21pc2U7XG4gIH07XG4gIHRoaXMubG9hZFBsYW4gPSBmdW5jdGlvbihqb2JpZCkge1xuICAgIGN1cnJlbnRQbGFuID0gbnVsbDtcbiAgICBkZWZlcnJlZHMucGxhbiA9ICRxLmRlZmVyKCk7XG4gICAgJGh0dHAuZ2V0KGZsaW5rQ29uZmlnLm5ld1NlcnZlciArIFwiL2pvYnMvXCIgKyBqb2JpZCArIFwiL3BsYW5cIikuc3VjY2VzcyhmdW5jdGlvbihkYXRhKSB7XG4gICAgICBjdXJyZW50UGxhbiA9IGRhdGE7XG4gICAgICByZXR1cm4gZGVmZXJyZWRzLnBsYW4ucmVzb2x2ZShkYXRhKTtcbiAgICB9KTtcbiAgICByZXR1cm4gZGVmZXJyZWRzLnBsYW4ucHJvbWlzZTtcbiAgfTtcbiAgdGhpcy5nZXROb2RlID0gZnVuY3Rpb24obm9kZWlkKSB7XG4gICAgdmFyIGRlZmVycmVkLCBzZWVrTm9kZTtcbiAgICBzZWVrTm9kZSA9IGZ1bmN0aW9uKG5vZGVpZCwgZGF0YSkge1xuICAgICAgdmFyIGksIGxlbiwgbm9kZSwgc3ViO1xuICAgICAgbm9kZWlkID0gcGFyc2VJbnQobm9kZWlkKTtcbiAgICAgIGZvciAoaSA9IDAsIGxlbiA9IGRhdGEubGVuZ3RoOyBpIDwgbGVuOyBpKyspIHtcbiAgICAgICAgbm9kZSA9IGRhdGFbaV07XG4gICAgICAgIGlmIChub2RlLmlkID09PSBub2RlaWQpIHtcbiAgICAgICAgICByZXR1cm4gbm9kZTtcbiAgICAgICAgfVxuICAgICAgICBpZiAobm9kZS5zdGVwX2Z1bmN0aW9uKSB7XG4gICAgICAgICAgc3ViID0gc2Vla05vZGUobm9kZWlkLCBub2RlLnN0ZXBfZnVuY3Rpb24pO1xuICAgICAgICB9XG4gICAgICAgIGlmIChzdWIpIHtcbiAgICAgICAgICByZXR1cm4gc3ViO1xuICAgICAgICB9XG4gICAgICB9XG4gICAgICByZXR1cm4gbnVsbDtcbiAgICB9O1xuICAgIGRlZmVycmVkID0gJHEuZGVmZXIoKTtcbiAgICAkcS5hbGwoW2RlZmVycmVkcy5wbGFuLnByb21pc2UsIGRlZmVycmVkcy5qb2IucHJvbWlzZV0pLnRoZW4oKGZ1bmN0aW9uKF90aGlzKSB7XG4gICAgICByZXR1cm4gZnVuY3Rpb24oZGF0YSkge1xuICAgICAgICB2YXIgZm91bmROb2RlO1xuICAgICAgICBmb3VuZE5vZGUgPSBzZWVrTm9kZShub2RlaWQsIGN1cnJlbnRQbGFuLm5vZGVzKTtcbiAgICAgICAgcmV0dXJuIF90aGlzLmdldFZlcnRleChjdXJyZW50Sm9iLmppZCwgY3VycmVudEpvYi5vbGRWLmdyb3VwdmVydGljZXNbMF0uZ3JvdXB2ZXJ0ZXhpZCkudGhlbihmdW5jdGlvbih2ZXJ0ZXgpIHtcbiAgICAgICAgICBmb3VuZE5vZGUudmVydGV4ID0gdmVydGV4O1xuICAgICAgICAgIHJldHVybiBkZWZlcnJlZC5yZXNvbHZlKGZvdW5kTm9kZSk7XG4gICAgICAgIH0pO1xuICAgICAgfTtcbiAgICB9KSh0aGlzKSk7XG4gICAgcmV0dXJuIGRlZmVycmVkLnByb21pc2U7XG4gIH07XG4gIHRoaXMuZ2V0VmVydGV4ID0gZnVuY3Rpb24oam9iSWQsIHZlcnRleElkKSB7XG4gICAgdmFyIGRlZmVycmVkO1xuICAgIGRlZmVycmVkID0gJHEuZGVmZXIoKTtcbiAgICAkaHR0cC5nZXQoZmxpbmtDb25maWcuam9iU2VydmVyICsgXCIvam9ic0luZm8/Z2V0PWdyb3VwdmVydGV4JmpvYj1cIiArIGpvYklkICsgXCImZ3JvdXB2ZXJ0ZXg9XCIgKyB2ZXJ0ZXhJZCkuc3VjY2VzcyhmdW5jdGlvbihkYXRhKSB7XG4gICAgICByZXR1cm4gZGVmZXJyZWQucmVzb2x2ZShkYXRhKTtcbiAgICB9KTtcbiAgICByZXR1cm4gZGVmZXJyZWQucHJvbWlzZTtcbiAgfTtcbiAgcmV0dXJuIHRoaXM7XG59KTtcbiIsIiNcbiMgTGljZW5zZWQgdG8gdGhlIEFwYWNoZSBTb2Z0d2FyZSBGb3VuZGF0aW9uIChBU0YpIHVuZGVyIG9uZVxuIyBvciBtb3JlIGNvbnRyaWJ1dG9yIGxpY2Vuc2UgYWdyZWVtZW50cy4gIFNlZSB0aGUgTk9USUNFIGZpbGVcbiMgZGlzdHJpYnV0ZWQgd2l0aCB0aGlzIHdvcmsgZm9yIGFkZGl0aW9uYWwgaW5mb3JtYXRpb25cbiMgcmVnYXJkaW5nIGNvcHlyaWdodCBvd25lcnNoaXAuICBUaGUgQVNGIGxpY2Vuc2VzIHRoaXMgZmlsZVxuIyB0byB5b3UgdW5kZXIgdGhlIEFwYWNoZSBMaWNlbnNlLCBWZXJzaW9uIDIuMCAodGhlXG4jIFwiTGljZW5zZVwiKTsgeW91IG1heSBub3QgdXNlIHRoaXMgZmlsZSBleGNlcHQgaW4gY29tcGxpYW5jZVxuIyB3aXRoIHRoZSBMaWNlbnNlLiAgWW91IG1heSBvYnRhaW4gYSBjb3B5IG9mIHRoZSBMaWNlbnNlIGF0XG4jXG4jICAgICBodHRwOi8vd3d3LmFwYWNoZS5vcmcvbGljZW5zZXMvTElDRU5TRS0yLjBcbiNcbiMgVW5sZXNzIHJlcXVpcmVkIGJ5IGFwcGxpY2FibGUgbGF3IG9yIGFncmVlZCB0byBpbiB3cml0aW5nLCBzb2Z0d2FyZVxuIyBkaXN0cmlidXRlZCB1bmRlciB0aGUgTGljZW5zZSBpcyBkaXN0cmlidXRlZCBvbiBhbiBcIkFTIElTXCIgQkFTSVMsXG4jIFdJVEhPVVQgV0FSUkFOVElFUyBPUiBDT05ESVRJT05TIE9GIEFOWSBLSU5ELCBlaXRoZXIgZXhwcmVzcyBvciBpbXBsaWVkLlxuIyBTZWUgdGhlIExpY2Vuc2UgZm9yIHRoZSBzcGVjaWZpYyBsYW5ndWFnZSBnb3Zlcm5pbmcgcGVybWlzc2lvbnMgYW5kXG4jIGxpbWl0YXRpb25zIHVuZGVyIHRoZSBMaWNlbnNlLlxuI1xuXG5hbmd1bGFyLm1vZHVsZSgnZmxpbmtBcHAnKVxuXG4uY29udHJvbGxlciAnT3ZlcnZpZXdDb250cm9sbGVyJywgKCRzY29wZSwgT3ZlcnZpZXdTZXJ2aWNlLCBKb2JzU2VydmljZSkgLT5cbiAgJHNjb3BlLmpvYk9ic2VydmVyID0gLT5cbiAgICAkc2NvcGUucnVubmluZ0pvYnMgPSBKb2JzU2VydmljZS5nZXRKb2JzKCdydW5uaW5nJylcbiAgICAkc2NvcGUuZmluaXNoZWRKb2JzID0gSm9ic1NlcnZpY2UuZ2V0Sm9icygnZmluaXNoZWQnKVxuXG4gIEpvYnNTZXJ2aWNlLnJlZ2lzdGVyT2JzZXJ2ZXIoJHNjb3BlLmpvYk9ic2VydmVyKVxuICAkc2NvcGUuJG9uICckZGVzdHJveScsIC0+XG4gICAgSm9ic1NlcnZpY2UudW5SZWdpc3Rlck9ic2VydmVyKCRzY29wZS5qb2JPYnNlcnZlcilcblxuICAkc2NvcGUuam9iT2JzZXJ2ZXIoKVxuIiwiYW5ndWxhci5tb2R1bGUoJ2ZsaW5rQXBwJykuY29udHJvbGxlcignT3ZlcnZpZXdDb250cm9sbGVyJywgZnVuY3Rpb24oJHNjb3BlLCBPdmVydmlld1NlcnZpY2UsIEpvYnNTZXJ2aWNlKSB7XG4gICRzY29wZS5qb2JPYnNlcnZlciA9IGZ1bmN0aW9uKCkge1xuICAgICRzY29wZS5ydW5uaW5nSm9icyA9IEpvYnNTZXJ2aWNlLmdldEpvYnMoJ3J1bm5pbmcnKTtcbiAgICByZXR1cm4gJHNjb3BlLmZpbmlzaGVkSm9icyA9IEpvYnNTZXJ2aWNlLmdldEpvYnMoJ2ZpbmlzaGVkJyk7XG4gIH07XG4gIEpvYnNTZXJ2aWNlLnJlZ2lzdGVyT2JzZXJ2ZXIoJHNjb3BlLmpvYk9ic2VydmVyKTtcbiAgJHNjb3BlLiRvbignJGRlc3Ryb3knLCBmdW5jdGlvbigpIHtcbiAgICByZXR1cm4gSm9ic1NlcnZpY2UudW5SZWdpc3Rlck9ic2VydmVyKCRzY29wZS5qb2JPYnNlcnZlcik7XG4gIH0pO1xuICByZXR1cm4gJHNjb3BlLmpvYk9ic2VydmVyKCk7XG59KTtcbiIsIiNcbiMgTGljZW5zZWQgdG8gdGhlIEFwYWNoZSBTb2Z0d2FyZSBGb3VuZGF0aW9uIChBU0YpIHVuZGVyIG9uZVxuIyBvciBtb3JlIGNvbnRyaWJ1dG9yIGxpY2Vuc2UgYWdyZWVtZW50cy4gIFNlZSB0aGUgTk9USUNFIGZpbGVcbiMgZGlzdHJpYnV0ZWQgd2l0aCB0aGlzIHdvcmsgZm9yIGFkZGl0aW9uYWwgaW5mb3JtYXRpb25cbiMgcmVnYXJkaW5nIGNvcHlyaWdodCBvd25lcnNoaXAuICBUaGUgQVNGIGxpY2Vuc2VzIHRoaXMgZmlsZVxuIyB0byB5b3UgdW5kZXIgdGhlIEFwYWNoZSBMaWNlbnNlLCBWZXJzaW9uIDIuMCAodGhlXG4jIFwiTGljZW5zZVwiKTsgeW91IG1heSBub3QgdXNlIHRoaXMgZmlsZSBleGNlcHQgaW4gY29tcGxpYW5jZVxuIyB3aXRoIHRoZSBMaWNlbnNlLiAgWW91IG1heSBvYnRhaW4gYSBjb3B5IG9mIHRoZSBMaWNlbnNlIGF0XG4jXG4jICAgICBodHRwOi8vd3d3LmFwYWNoZS5vcmcvbGljZW5zZXMvTElDRU5TRS0yLjBcbiNcbiMgVW5sZXNzIHJlcXVpcmVkIGJ5IGFwcGxpY2FibGUgbGF3IG9yIGFncmVlZCB0byBpbiB3cml0aW5nLCBzb2Z0d2FyZVxuIyBkaXN0cmlidXRlZCB1bmRlciB0aGUgTGljZW5zZSBpcyBkaXN0cmlidXRlZCBvbiBhbiBcIkFTIElTXCIgQkFTSVMsXG4jIFdJVEhPVVQgV0FSUkFOVElFUyBPUiBDT05ESVRJT05TIE9GIEFOWSBLSU5ELCBlaXRoZXIgZXhwcmVzcyBvciBpbXBsaWVkLlxuIyBTZWUgdGhlIExpY2Vuc2UgZm9yIHRoZSBzcGVjaWZpYyBsYW5ndWFnZSBnb3Zlcm5pbmcgcGVybWlzc2lvbnMgYW5kXG4jIGxpbWl0YXRpb25zIHVuZGVyIHRoZSBMaWNlbnNlLlxuI1xuXG5hbmd1bGFyLm1vZHVsZSgnZmxpbmtBcHAnKVxuXG4uc2VydmljZSAnT3ZlcnZpZXdTZXJ2aWNlJywgKCRodHRwLCBmbGlua0NvbmZpZywgJGxvZykgLT5cbiAgc2VydmVyU3RhdHVzID0ge31cblxuICBAbG9hZFNlcnZlclN0YXR1cyA9IC0+XG4gICAgJGh0dHAuZ2V0KGZsaW5rQ29uZmlnLmpvYlNlcnZlciArIFwiL21vbml0b3Ivc3RhdHVzXCIpXG4gICAgLnN1Y2Nlc3MgKGRhdGEsIHN0YXR1cywgaGVhZGVycywgY29uZmlnKSAtPlxuICAgICAgJGxvZyBkYXRhXG5cbiAgICAuZXJyb3IgKGRhdGEsIHN0YXR1cywgaGVhZGVycywgY29uZmlnKSAtPlxuICAgICAgcmV0dXJuXG5cbiAgICBzZXJ2ZXJTdGF0dXNcblxuICBAXG4iLCJhbmd1bGFyLm1vZHVsZSgnZmxpbmtBcHAnKS5zZXJ2aWNlKCdPdmVydmlld1NlcnZpY2UnLCBmdW5jdGlvbigkaHR0cCwgZmxpbmtDb25maWcsICRsb2cpIHtcbiAgdmFyIHNlcnZlclN0YXR1cztcbiAgc2VydmVyU3RhdHVzID0ge307XG4gIHRoaXMubG9hZFNlcnZlclN0YXR1cyA9IGZ1bmN0aW9uKCkge1xuICAgICRodHRwLmdldChmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcIi9tb25pdG9yL3N0YXR1c1wiKS5zdWNjZXNzKGZ1bmN0aW9uKGRhdGEsIHN0YXR1cywgaGVhZGVycywgY29uZmlnKSB7XG4gICAgICByZXR1cm4gJGxvZyhkYXRhKTtcbiAgICB9KS5lcnJvcihmdW5jdGlvbihkYXRhLCBzdGF0dXMsIGhlYWRlcnMsIGNvbmZpZykge30pO1xuICAgIHJldHVybiBzZXJ2ZXJTdGF0dXM7XG4gIH07XG4gIHJldHVybiB0aGlzO1xufSk7XG4iXSwic291cmNlUm9vdCI6Ii9zb3VyY2UvIn0=