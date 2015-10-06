angular.module('flinkApp', ['ui.router', 'angularMoment']).run(["$rootScope", function($rootScope) {
  $rootScope.sidebarVisible = false;
  return $rootScope.showSidebar = function() {
    $rootScope.sidebarVisible = !$rootScope.sidebarVisible;
    return $rootScope.sidebarClass = 'force-show';
  };
}]).value('flinkConfig', {
  "refresh-interval": 10000
}).run(["JobsService", "MainService", "flinkConfig", "$interval", function(JobsService, MainService, flinkConfig, $interval) {
  return MainService.loadConfig().then(function(config) {
    angular.extend(flinkConfig, config);
    JobsService.listJobs();
    return $interval(function() {
      return JobsService.listJobs();
    }, flinkConfig["refresh-interval"]);
  });
}]).config(["$uiViewScrollProvider", function($uiViewScrollProvider) {
  return $uiViewScrollProvider.useAnchorScroll();
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
    abstract: true,
    views: {
      details: {
        templateUrl: "partials/jobs/job.plan.html",
        controller: 'JobPlanController'
      }
    }
  }).state("single-job.plan.overview", {
    url: "",
    views: {
      'node-details': {
        templateUrl: "partials/jobs/job.plan.node-list.overview.html",
        controller: 'JobPlanOverviewController'
      }
    }
  }).state("single-job.plan.accumulators", {
    url: "/accumulators",
    views: {
      'node-details': {
        templateUrl: "partials/jobs/job.plan.node-list.accumulators.html",
        controller: 'JobPlanAccumulatorsController'
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
        templateUrl: "partials/jobs/job.exceptions.html",
        controller: 'JobExceptionsController'
      }
    }
  }).state("single-job.properties", {
    url: "/properties",
    views: {
      details: {
        templateUrl: "partials/jobs/job.properties.html",
        controller: 'JobPropertiesController'
      }
    }
  }).state("single-job.config", {
    url: "/config",
    views: {
      details: {
        templateUrl: "partials/jobs/job.config.html"
      }
    }
  }).state("taskmanagers", {
    url: "/taskmanagers",
    views: {
      main: {
        templateUrl: "partials/taskmanagers/index.html",
        controller: 'TaskManagersController'
      }
    }
  }).state("jobmanager", {
    url: "/jobmanager",
    views: {
      main: {
        templateUrl: "partials/jobmanager/index.html"
      }
    }
  }).state("jobmanager.config", {
    url: "/config",
    views: {
      details: {
        templateUrl: "partials/jobmanager/config.html",
        controller: 'JobManagerConfigController'
      }
    }
  }).state("jobmanager.stdout", {
    url: "/stdout",
    views: {
      details: {
        templateUrl: "partials/jobmanager/stdout.html"
      }
    }
  }).state("jobmanager.logfile", {
    url: "/logfile",
    views: {
      details: {
        templateUrl: "partials/jobmanager/logfile.html"
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
}]).filter("humanizeText", function() {
  return function(text) {
    if (text) {
      return text.replace(/&gt;/g, ">").replace(/<br\/>/g, "");
    } else {
      return '';
    }
  };
}).filter("bytes", function() {
  return function(bytes, precision) {
    var number, units;
    if (isNaN(parseFloat(bytes)) || !isFinite(bytes)) {
      return "-";
    }
    if (typeof precision === "undefined") {
      precision = 1;
    }
    units = ["bytes", "kB", "MB", "GB", "TB", "PB"];
    number = Math.floor(Math.log(bytes) / Math.log(1024));
    return (bytes / Math.pow(1024, Math.floor(number))).toFixed(precision) + " " + units[number];
  };
});

angular.module('flinkApp').service('MainService', ["$http", "flinkConfig", "$q", function($http, flinkConfig, $q) {
  this.loadConfig = function() {
    var deferred;
    deferred = $q.defer();
    $http.get("/config").success(function(data, status, headers, config) {
      return deferred.resolve(data);
    });
    return deferred.promise;
  };
  return this;
}]);

angular.module('flinkApp').controller('JobManagerConfigController', ["$scope", "JobManagerConfigService", function($scope, JobManagerConfigService) {
  return JobManagerConfigService.loadConfig().then(function(data) {
    if ($scope.jobmanager == null) {
      $scope.jobmanager = {};
    }
    return $scope.jobmanager['config'] = data;
  });
}]);

angular.module('flinkApp').service('JobManagerConfigService', ["$http", "flinkConfig", "$q", function($http, flinkConfig, $q) {
  var config;
  config = {};
  this.loadConfig = function() {
    var deferred;
    deferred = $q.defer();
    $http.get("/jobmanager/config").success(function(data, status, headers, config) {
      config = data;
      return deferred.resolve(data);
    });
    return deferred.promise;
  };
  return this;
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
}]).controller('SingleJobController', ["$scope", "$state", "$stateParams", "JobsService", "$rootScope", "flinkConfig", "$interval", function($scope, $state, $stateParams, JobsService, $rootScope, flinkConfig, $interval) {
  var refresher;
  console.log('SingleJobController');
  $scope.jobid = $stateParams.jobid;
  $scope.job = null;
  $scope.plan = null;
  $scope.vertices = null;
  JobsService.loadJob($stateParams.jobid).then(function(data) {
    $scope.job = data;
    $scope.plan = data.plan;
    return $scope.vertices = data.vertices;
  });
  refresher = $interval(function() {
    return JobsService.loadJob($stateParams.jobid).then(function(data) {
      $scope.job = data;
      return $scope.$broadcast('reload');
    });
  }, flinkConfig["refresh-interval"]);
  return $scope.$on('$destroy', function() {
    $scope.job = null;
    $scope.plan = null;
    $scope.vertices = null;
    return $interval.cancel(refresher);
  });
}]).controller('JobPlanController', ["$scope", "$state", "$stateParams", "JobsService", function($scope, $state, $stateParams, JobsService) {
  console.log('JobPlanController');
  $scope.nodeid = null;
  $scope.nodeUnfolded = false;
  $scope.stateList = JobsService.stateList();
  $scope.changeNode = function(nodeid) {
    if (nodeid !== $scope.nodeid) {
      $scope.nodeid = nodeid;
      $scope.vertex = null;
      $scope.subtasks = null;
      $scope.accumulators = null;
      return $scope.$broadcast('reload');
    } else {
      $scope.nodeid = null;
      $scope.nodeUnfolded = false;
      $scope.vertex = null;
      $scope.subtasks = null;
      return $scope.accumulators = null;
    }
  };
  $scope.deactivateNode = function() {
    $scope.nodeid = null;
    $scope.nodeUnfolded = false;
    $scope.vertex = null;
    $scope.subtasks = null;
    return $scope.accumulators = null;
  };
  return $scope.toggleFold = function() {
    return $scope.nodeUnfolded = !$scope.nodeUnfolded;
  };
}]).controller('JobPlanOverviewController', ["$scope", "JobsService", function($scope, JobsService) {
  console.log('JobPlanOverviewController');
  if ($scope.nodeid && (!$scope.vertex || !$scope.vertex.st)) {
    JobsService.getSubtasks($scope.nodeid).then(function(data) {
      return $scope.subtasks = data;
    });
  }
  return $scope.$on('reload', function(event) {
    console.log('JobPlanOverviewController');
    if ($scope.nodeid) {
      return JobsService.getSubtasks($scope.nodeid).then(function(data) {
        return $scope.subtasks = data;
      });
    }
  });
}]).controller('JobPlanAccumulatorsController', ["$scope", "JobsService", function($scope, JobsService) {
  console.log('JobPlanAccumulatorsController');
  if ($scope.nodeid && (!$scope.vertex || !$scope.vertex.accumulators)) {
    JobsService.getAccumulators($scope.nodeid).then(function(data) {
      $scope.accumulators = data.main;
      return $scope.subtaskAccumulators = data.subtasks;
    });
  }
  return $scope.$on('reload', function(event) {
    console.log('JobPlanAccumulatorsController');
    if ($scope.nodeid) {
      return JobsService.getAccumulators($scope.nodeid).then(function(data) {
        $scope.accumulators = data.main;
        return $scope.subtaskAccumulators = data.subtasks;
      });
    }
  });
}]).controller('JobTimelineVertexController', ["$scope", "$state", "$stateParams", "JobsService", function($scope, $state, $stateParams, JobsService) {
  console.log('JobTimelineVertexController');
  JobsService.getVertex($stateParams.vertexId).then(function(data) {
    return $scope.vertex = data;
  });
  return $scope.$on('reload', function(event) {
    console.log('JobTimelineVertexController');
    return JobsService.getVertex($stateParams.vertexId).then(function(data) {
      return $scope.vertex = data;
    });
  });
}]).controller('JobExceptionsController', ["$scope", "$state", "$stateParams", "JobsService", function($scope, $state, $stateParams, JobsService) {
  return JobsService.loadExceptions().then(function(data) {
    return $scope.exceptions = data;
  });
}]).controller('JobPropertiesController', ["$scope", "JobsService", function($scope, JobsService) {
  console.log('JobPropertiesController');
  return $scope.changeNode = function(nodeid) {
    if (nodeid !== $scope.nodeid) {
      $scope.nodeid = nodeid;
      return JobsService.getNode(nodeid).then(function(data) {
        return $scope.node = data;
      });
    } else {
      $scope.nodeid = null;
      return $scope.node = null;
    }
  };
}]);

angular.module('flinkApp').directive('vertex', ["$state", function($state) {
  return {
    template: "<svg class='timeline secondary' width='0' height='0'></svg>",
    scope: {
      data: "="
    },
    link: function(scope, elem, attrs) {
      var analyzeTime, containerW, svgEl;
      svgEl = elem.children()[0];
      containerW = elem.width();
      angular.element(svgEl).attr('width', containerW);
      analyzeTime = function(data) {
        var chart, svg, testData;
        d3.select(svgEl).selectAll("*").remove();
        testData = [];
        angular.forEach(data.subtasks, function(subtask, i) {
          var times;
          times = [
            {
              label: "Scheduled",
              color: "#666",
              borderColor: "#555",
              starting_time: subtask.timestamps["SCHEDULED"],
              ending_time: subtask.timestamps["DEPLOYING"],
              type: 'regular'
            }, {
              label: "Deploying",
              color: "#aaa",
              borderColor: "#555",
              starting_time: subtask.timestamps["DEPLOYING"],
              ending_time: subtask.timestamps["RUNNING"],
              type: 'regular'
            }
          ];
          if (subtask.timestamps["FINISHED"] > 0) {
            times.push({
              label: "Running",
              color: "#ddd",
              borderColor: "#555",
              starting_time: subtask.timestamps["RUNNING"],
              ending_time: subtask.timestamps["FINISHED"],
              type: 'regular'
            });
          }
          return testData.push({
            label: "(" + subtask.subtask + ") " + subtask.host,
            times: times
          });
        });
        chart = d3.timeline().stack().tickFormat({
          format: d3.time.format("%L"),
          tickSize: 1
        }).prefix("single").labelFormat(function(label) {
          return label;
        }).margin({
          left: 100,
          right: 0,
          top: 0,
          bottom: 0
        }).itemHeight(30).relativeTime();
        return svg = d3.select(svgEl).datum(testData).call(chart);
      };
      analyzeTime(scope.data);
    }
  };
}]).directive('timeline', ["$state", function($state) {
  return {
    template: "<svg class='timeline' width='0' height='0'></svg>",
    scope: {
      vertices: "=",
      jobid: "="
    },
    link: function(scope, elem, attrs) {
      var analyzeTime, containerW, svgEl, translateLabel;
      svgEl = elem.children()[0];
      containerW = elem.width();
      angular.element(svgEl).attr('width', containerW);
      translateLabel = function(label) {
        return label.replace("&gt;", ">");
      };
      analyzeTime = function(data) {
        var chart, svg, testData;
        d3.select(svgEl).selectAll("*").remove();
        testData = [];
        angular.forEach(data, function(vertex) {
          if (vertex['start-time'] > -1) {
            if (vertex.type === 'scheduled') {
              return testData.push({
                times: [
                  {
                    label: translateLabel(vertex.name),
                    color: "#cccccc",
                    borderColor: "#555555",
                    starting_time: vertex['start-time'],
                    ending_time: vertex['end-time'],
                    type: vertex.type
                  }
                ]
              });
            } else {
              return testData.push({
                times: [
                  {
                    label: translateLabel(vertex.name),
                    color: "#d9f1f7",
                    borderColor: "#62cdea",
                    starting_time: vertex['start-time'],
                    ending_time: vertex['end-time'],
                    link: vertex.id,
                    type: vertex.type
                  }
                ]
              });
            }
          }
        });
        chart = d3.timeline().stack().click(function(d, i, datum) {
          if (d.link) {
            return $state.go("single-job.timeline.vertex", {
              jobid: scope.jobid,
              vertexId: d.link
            });
          }
        }).tickFormat({
          format: d3.time.format("%L"),
          tickSize: 1
        }).prefix("main").margin({
          left: 0,
          right: 0,
          top: 0,
          bottom: 0
        }).itemHeight(30).showBorderLine().showHourTimeline();
        return svg = d3.select(svgEl).datum(testData).call(chart);
      };
      scope.$watch(attrs.vertices, function(data) {
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
      plan: '=',
      setNode: '&'
    },
    link: function(scope, elem, attrs) {
      var containerW, createEdge, createLabelEdge, createLabelNode, createNode, d3mainSvg, d3mainSvgG, d3tmpSvg, drawGraph, extendLabelNodeForIteration, g, getNodeType, isSpecialIterationNode, jobid, loadJsonToDagre, mainG, mainSvgElement, mainTmpElement, mainZoom, searchForNode, shortenString, subgraphs;
      g = null;
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
          return 'node-normal';
        }
      };
      createLabelNode = function(el, info, maxW, maxH) {
        var labelValue, stepName;
        labelValue = "<div href='#/jobs/" + jobid + "/vertex/" + el.id + "' class='node-label " + getNodeType(el, info) + "'>";
        if (info === "mirror") {
          labelValue += "<h3 class='node-name'>Mirror of " + el.operator + "</h3>";
        } else {
          labelValue += "<h3 class='node-name'>" + el.operator + "</h3>";
        }
        if (el.description === "") {
          labelValue += "";
        } else {
          stepName = el.description;
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
          if (el.operator !== undefined) {
            labelValue += "<h5>Operation: " + shortenString(el.operator_strategy) + "</h5>";
          }
        }
        labelValue += "</div>";
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
      createEdge = function(g, data, el, existingNodes, pred, missingNodes) {
        var missingNode;
        if (existingNodes.indexOf(pred.id) !== -1) {
          return g.setEdge(pred.id, el.id, {
            label: createLabelEdge(pred),
            labelType: 'html',
            arrowhead: 'normal'
          });
        } else {
          missingNode = searchForNode(data, pred.id);
          if (!(!missingNode || missingNodes.indexOf(missingNode.id) > -1)) {
            missingNodes.push(missingNode.id);
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
        var el, existingNodes, isParent, k, l, len, len1, maxH, maxW, missingNodes, pred, r, ref, sg, toIterate;
        existingNodes = [];
        missingNodes = [];
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
          if (el.inputs != null) {
            ref = el.inputs;
            for (l = 0, len1 = ref.length; l < len1; l++) {
              pred = ref[l];
              createEdge(g, data, el, existingNodes, pred, missingNodes);
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
        var i, newScale, renderer, sg, xCenterOffset, yCenterOffset;
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
        mainZoom(d3mainSvg);
        return d3mainSvgG.selectAll('.node').on('click', function(d) {
          return scope.setNode({
            nodeid: d
          });
        });
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
  this.setEndTimes = function(list) {
    return angular.forEach(list, function(item, jobKey) {
      if (!(item['end-time'] > -1)) {
        return item['end-time'] = item['start-time'] + item['duration'];
      }
    });
  };
  this.processVertices = function(data) {
    angular.forEach(data.vertices, function(vertex, i) {
      return vertex.type = 'regular';
    });
    return data.vertices.unshift({
      name: 'Scheduled',
      'start-time': data.timestamps['CREATED'],
      'end-time': data.timestamps['CREATED'] + 1,
      type: 'scheduled'
    });
  };
  this.listJobs = function() {
    var deferred;
    deferred = $q.defer();
    $http.get("/joboverview").success((function(_this) {
      return function(data, status, headers, config) {
        angular.forEach(data, function(list, listKey) {
          switch (listKey) {
            case 'running':
              return jobs.running = _this.setEndTimes(list);
            case 'finished':
              return jobs.finished = _this.setEndTimes(list);
            case 'cancelled':
              return jobs.cancelled = _this.setEndTimes(list);
            case 'failed':
              return jobs.failed = _this.setEndTimes(list);
          }
        });
        deferred.resolve(jobs);
        return notifyObservers();
      };
    })(this));
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
    $http.get("/jobs/" + jobid).success((function(_this) {
      return function(data, status, headers, config) {
        _this.setEndTimes(data.vertices);
        _this.processVertices(data);
        return $http.get("/jobs/" + jobid + "/config").success(function(jobConfig) {
          data = angular.extend(data, jobConfig);
          currentJob = data;
          return deferreds.job.resolve(currentJob);
        });
      };
    })(this));
    return deferreds.job.promise;
  };
  this.getNode = function(nodeid) {
    var deferred, seekNode;
    seekNode = function(nodeid, data) {
      var j, len, node, sub;
      for (j = 0, len = data.length; j < len; j++) {
        node = data[j];
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
    deferreds.job.promise.then((function(_this) {
      return function(data) {
        var foundNode;
        foundNode = seekNode(nodeid, currentJob.plan.nodes);
        foundNode.vertex = _this.seekVertex(nodeid);
        return deferred.resolve(foundNode);
      };
    })(this));
    return deferred.promise;
  };
  this.seekVertex = function(nodeid) {
    var j, len, ref, vertex;
    ref = currentJob.vertices;
    for (j = 0, len = ref.length; j < len; j++) {
      vertex = ref[j];
      if (vertex.id === nodeid) {
        return vertex;
      }
    }
    return null;
  };
  this.getVertex = function(vertexid) {
    var deferred;
    deferred = $q.defer();
    deferreds.job.promise.then((function(_this) {
      return function(data) {
        var vertex;
        vertex = _this.seekVertex(vertexid);
        return $http.get("/jobs/" + currentJob.jid + "/vertices/" + vertexid + "/subtasktimes").success(function(data) {
          vertex.subtasks = data.subtasks;
          return deferred.resolve(vertex);
        });
      };
    })(this));
    return deferred.promise;
  };
  this.getSubtasks = function(vertexid) {
    var deferred;
    deferred = $q.defer();
    deferreds.job.promise.then((function(_this) {
      return function(data) {
        return $http.get("/jobs/" + currentJob.jid + "/vertices/" + vertexid).success(function(data) {
          var subtasks;
          subtasks = data.subtasks;
          return deferred.resolve(subtasks);
        });
      };
    })(this));
    return deferred.promise;
  };
  this.getAccumulators = function(vertexid) {
    var deferred;
    deferred = $q.defer();
    deferreds.job.promise.then((function(_this) {
      return function(data) {
        return $http.get("/jobs/" + currentJob.jid + "/vertices/" + vertexid + "/accumulators").success(function(data) {
          var accumulators;
          accumulators = data['user-accumulators'];
          return $http.get("/jobs/" + currentJob.jid + "/vertices/" + vertexid + "/subtasks/accumulators").success(function(data) {
            var subtaskAccumulators;
            subtaskAccumulators = data.subtasks;
            return deferred.resolve({
              main: accumulators,
              subtasks: subtaskAccumulators
            });
          });
        });
      };
    })(this));
    return deferred.promise;
  };
  this.loadExceptions = function() {
    var deferred;
    deferred = $q.defer();
    deferreds.job.promise.then((function(_this) {
      return function(data) {
        return $http.get("/jobs/" + currentJob.jid + "/exceptions").success(function(exceptions) {
          currentJob.exceptions = exceptions;
          return deferred.resolve(exceptions);
        });
      };
    })(this));
    return deferred.promise;
  };
  return this;
}]);

angular.module('flinkApp').controller('OverviewController', ["$scope", "OverviewService", "JobsService", "$interval", "flinkConfig", function($scope, OverviewService, JobsService, $interval, flinkConfig) {
  var refresh;
  $scope.jobObserver = function() {
    $scope.runningJobs = JobsService.getJobs('running');
    return $scope.finishedJobs = JobsService.getJobs('finished');
  };
  JobsService.registerObserver($scope.jobObserver);
  $scope.$on('$destroy', function() {
    return JobsService.unRegisterObserver($scope.jobObserver);
  });
  $scope.jobObserver();
  OverviewService.loadOverview().then(function(data) {
    return $scope.overview = data;
  });
  refresh = $interval(function() {
    return OverviewService.loadOverview().then(function(data) {
      return $scope.overview = data;
    });
  }, flinkConfig["refresh-interval"]);
  return $scope.$on('$destroy', function() {
    return $interval.cancel(refresh);
  });
}]);

angular.module('flinkApp').service('OverviewService', ["$http", "flinkConfig", "$q", function($http, flinkConfig, $q) {
  var overview;
  overview = {};
  this.loadOverview = function() {
    var deferred;
    deferred = $q.defer();
    $http.get("/overview").success(function(data, status, headers, config) {
      overview = data;
      return deferred.resolve(data);
    });
    return deferred.promise;
  };
  return this;
}]);

angular.module('flinkApp').controller('TaskManagersController', ["$scope", "TaskManagersService", "$interval", "flinkConfig", function($scope, TaskManagersService, $interval, flinkConfig) {
  var refresh;
  TaskManagersService.loadManagers().then(function(data) {
    return $scope.managers = data;
  });
  refresh = $interval(function() {
    return TaskManagersService.loadManagers().then(function(data) {
      return $scope.managers = data;
    });
  }, flinkConfig["refresh-interval"]);
  $scope.$on('$destroy', function() {
    return $interval.cancel(refresh);
  });
  $scope.managerId = null;
  return $scope.changeManager = function(managerId) {
    if (managerId !== $scope.managerId) {
      return $scope.managerId = managerId;
    } else {
      return $scope.managerId = null;
    }
  };
}]);

angular.module('flinkApp').service('TaskManagersService', ["$http", "flinkConfig", "$q", function($http, flinkConfig, $q) {
  this.loadManagers = function() {
    var deferred;
    deferred = $q.defer();
    $http.get("/taskmanagers").success(function(data, status, headers, config) {
      return deferred.resolve(data['taskmanagers']);
    });
    return deferred.promise;
  };
  return this;
}]);

//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJzb3VyY2VzIjpbImluZGV4LmNvZmZlZSIsImluZGV4LmpzIiwiY29tbW9uL2RpcmVjdGl2ZXMuY29mZmVlIiwiY29tbW9uL2RpcmVjdGl2ZXMuanMiLCJjb21tb24vZmlsdGVycy5jb2ZmZWUiLCJjb21tb24vZmlsdGVycy5qcyIsImNvbW1vbi9zZXJ2aWNlcy5jb2ZmZWUiLCJjb21tb24vc2VydmljZXMuanMiLCJtb2R1bGVzL2pvYm1hbmFnZXIvam9ibWFuYWdlci5jdHJsLmNvZmZlZSIsIm1vZHVsZXMvam9ibWFuYWdlci9qb2JtYW5hZ2VyLmN0cmwuanMiLCJtb2R1bGVzL2pvYm1hbmFnZXIvam9ibWFuYWdlci5zdmMuY29mZmVlIiwibW9kdWxlcy9qb2JtYW5hZ2VyL2pvYm1hbmFnZXIuc3ZjLmpzIiwibW9kdWxlcy9qb2JzL2pvYnMuY3RybC5jb2ZmZWUiLCJtb2R1bGVzL2pvYnMvam9icy5jdHJsLmpzIiwibW9kdWxlcy9qb2JzL2pvYnMuZGlyLmNvZmZlZSIsIm1vZHVsZXMvam9icy9qb2JzLmRpci5qcyIsIm1vZHVsZXMvam9icy9qb2JzLnN2Yy5jb2ZmZWUiLCJtb2R1bGVzL2pvYnMvam9icy5zdmMuanMiLCJtb2R1bGVzL292ZXJ2aWV3L292ZXJ2aWV3LmN0cmwuY29mZmVlIiwibW9kdWxlcy9vdmVydmlldy9vdmVydmlldy5jdHJsLmpzIiwibW9kdWxlcy9vdmVydmlldy9vdmVydmlldy5zdmMuY29mZmVlIiwibW9kdWxlcy9vdmVydmlldy9vdmVydmlldy5zdmMuanMiLCJtb2R1bGVzL3Rhc2ttYW5hZ2Vycy90YXNrbWFuYWdlcnMuY3RybC5jb2ZmZWUiLCJtb2R1bGVzL3Rhc2ttYW5hZ2Vycy90YXNrbWFuYWdlcnMuY3RybC5qcyIsIm1vZHVsZXMvdGFza21hbmFnZXJzL3Rhc2ttYW5hZ2Vycy5zdmMuY29mZmVlIiwibW9kdWxlcy90YXNrbWFuYWdlcnMvdGFza21hbmFnZXJzLnN2Yy5qcyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiQUFrQkEsUUFBUSxP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ZXguanMiLCJzb3VyY2VzQ29udGVudCI6WyIjXHJcbiMgTGljZW5zZWQgdG8gdGhlIEFwYWNoZSBTb2Z0d2FyZSBGb3VuZGF0aW9uIChBU0YpIHVuZGVyIG9uZVxyXG4jIG9yIG1vcmUgY29udHJpYnV0b3IgbGljZW5zZSBhZ3JlZW1lbnRzLiAgU2VlIHRoZSBOT1RJQ0UgZmlsZVxyXG4jIGRpc3RyaWJ1dGVkIHdpdGggdGhpcyB3b3JrIGZvciBhZGRpdGlvbmFsIGluZm9ybWF0aW9uXHJcbiMgcmVnYXJkaW5nIGNvcHlyaWdodCBvd25lcnNoaXAuICBUaGUgQVNGIGxpY2Vuc2VzIHRoaXMgZmlsZVxyXG4jIHRvIHlvdSB1bmRlciB0aGUgQXBhY2hlIExpY2Vuc2UsIFZlcnNpb24gMi4wICh0aGVcclxuIyBcIkxpY2Vuc2VcIik7IHlvdSBtYXkgbm90IHVzZSB0aGlzIGZpbGUgZXhjZXB0IGluIGNvbXBsaWFuY2VcclxuIyB3aXRoIHRoZSBMaWNlbnNlLiAgWW91IG1heSBvYnRhaW4gYSBjb3B5IG9mIHRoZSBMaWNlbnNlIGF0XHJcbiNcclxuIyAgICAgaHR0cDovL3d3dy5hcGFjaGUub3JnL2xpY2Vuc2VzL0xJQ0VOU0UtMi4wXHJcbiNcclxuIyBVbmxlc3MgcmVxdWlyZWQgYnkgYXBwbGljYWJsZSBsYXcgb3IgYWdyZWVkIHRvIGluIHdyaXRpbmcsIHNvZnR3YXJlXHJcbiMgZGlzdHJpYnV0ZWQgdW5kZXIgdGhlIExpY2Vuc2UgaXMgZGlzdHJpYnV0ZWQgb24gYW4gXCJBUyBJU1wiIEJBU0lTLFxyXG4jIFdJVEhPVVQgV0FSUkFOVElFUyBPUiBDT05ESVRJT05TIE9GIEFOWSBLSU5ELCBlaXRoZXIgZXhwcmVzcyBvciBpbXBsaWVkLlxyXG4jIFNlZSB0aGUgTGljZW5zZSBmb3IgdGhlIHNwZWNpZmljIGxhbmd1YWdlIGdvdmVybmluZyBwZXJtaXNzaW9ucyBhbmRcclxuIyBsaW1pdGF0aW9ucyB1bmRlciB0aGUgTGljZW5zZS5cclxuI1xyXG5cclxuYW5ndWxhci5tb2R1bGUoJ2ZsaW5rQXBwJywgWyd1aS5yb3V0ZXInLCAnYW5ndWxhck1vbWVudCddKVxyXG5cclxuIyAtLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLVxyXG5cclxuLnJ1biAoJHJvb3RTY29wZSkgLT5cclxuICAkcm9vdFNjb3BlLnNpZGViYXJWaXNpYmxlID0gZmFsc2VcclxuICAkcm9vdFNjb3BlLnNob3dTaWRlYmFyID0gLT5cclxuICAgICRyb290U2NvcGUuc2lkZWJhclZpc2libGUgPSAhJHJvb3RTY29wZS5zaWRlYmFyVmlzaWJsZVxyXG4gICAgJHJvb3RTY29wZS5zaWRlYmFyQ2xhc3MgPSAnZm9yY2Utc2hvdydcclxuXHJcbiMgLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS1cclxuXHJcbi52YWx1ZSAnZmxpbmtDb25maWcnLCB7XHJcbiAgXCJyZWZyZXNoLWludGVydmFsXCI6IDEwMDAwXHJcbn1cclxuXHJcbiMgLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS1cclxuXHJcbi5ydW4gKEpvYnNTZXJ2aWNlLCBNYWluU2VydmljZSwgZmxpbmtDb25maWcsICRpbnRlcnZhbCkgLT5cclxuICBNYWluU2VydmljZS5sb2FkQ29uZmlnKCkudGhlbiAoY29uZmlnKSAtPlxyXG4gICAgYW5ndWxhci5leHRlbmQgZmxpbmtDb25maWcsIGNvbmZpZ1xyXG5cclxuICAgIEpvYnNTZXJ2aWNlLmxpc3RKb2JzKClcclxuXHJcbiAgICAkaW50ZXJ2YWwgLT5cclxuICAgICAgSm9ic1NlcnZpY2UubGlzdEpvYnMoKVxyXG4gICAgLCBmbGlua0NvbmZpZ1tcInJlZnJlc2gtaW50ZXJ2YWxcIl1cclxuXHJcblxyXG4jIC0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tXHJcblxyXG4uY29uZmlnICgkdWlWaWV3U2Nyb2xsUHJvdmlkZXIpIC0+XHJcbiAgJHVpVmlld1Njcm9sbFByb3ZpZGVyLnVzZUFuY2hvclNjcm9sbCgpXHJcblxyXG4jIC0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tXHJcblxyXG4uY29uZmlnICgkc3RhdGVQcm92aWRlciwgJHVybFJvdXRlclByb3ZpZGVyKSAtPlxyXG4gICRzdGF0ZVByb3ZpZGVyLnN0YXRlIFwib3ZlcnZpZXdcIixcclxuICAgIHVybDogXCIvb3ZlcnZpZXdcIlxyXG4gICAgdmlld3M6XHJcbiAgICAgIG1haW46XHJcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvb3ZlcnZpZXcuaHRtbFwiXHJcbiAgICAgICAgY29udHJvbGxlcjogJ092ZXJ2aWV3Q29udHJvbGxlcidcclxuXHJcbiAgLnN0YXRlIFwicnVubmluZy1qb2JzXCIsXHJcbiAgICB1cmw6IFwiL3J1bm5pbmctam9ic1wiXHJcbiAgICB2aWV3czpcclxuICAgICAgbWFpbjpcclxuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JzL3J1bm5pbmctam9icy5odG1sXCJcclxuICAgICAgICBjb250cm9sbGVyOiAnUnVubmluZ0pvYnNDb250cm9sbGVyJ1xyXG4gIFxyXG4gIC5zdGF0ZSBcImNvbXBsZXRlZC1qb2JzXCIsXHJcbiAgICB1cmw6IFwiL2NvbXBsZXRlZC1qb2JzXCJcclxuICAgIHZpZXdzOlxyXG4gICAgICBtYWluOlxyXG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYnMvY29tcGxldGVkLWpvYnMuaHRtbFwiXHJcbiAgICAgICAgY29udHJvbGxlcjogJ0NvbXBsZXRlZEpvYnNDb250cm9sbGVyJ1xyXG5cclxuICAuc3RhdGUgXCJzaW5nbGUtam9iXCIsXHJcbiAgICB1cmw6IFwiL2pvYnMve2pvYmlkfVwiXHJcbiAgICBhYnN0cmFjdDogdHJ1ZVxyXG4gICAgdmlld3M6XHJcbiAgICAgIG1haW46XHJcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvam9icy9qb2IuaHRtbFwiXHJcbiAgICAgICAgY29udHJvbGxlcjogJ1NpbmdsZUpvYkNvbnRyb2xsZXInXHJcblxyXG4gIC5zdGF0ZSBcInNpbmdsZS1qb2IucGxhblwiLFxyXG4gICAgdXJsOiBcIlwiXHJcbiAgICBhYnN0cmFjdDogdHJ1ZVxyXG4gICAgdmlld3M6XHJcbiAgICAgIGRldGFpbHM6XHJcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvam9icy9qb2IucGxhbi5odG1sXCJcclxuICAgICAgICBjb250cm9sbGVyOiAnSm9iUGxhbkNvbnRyb2xsZXInXHJcblxyXG4gIC5zdGF0ZSBcInNpbmdsZS1qb2IucGxhbi5vdmVydmlld1wiLFxyXG4gICAgdXJsOiBcIlwiXHJcbiAgICB2aWV3czpcclxuICAgICAgJ25vZGUtZGV0YWlscyc6XHJcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvam9icy9qb2IucGxhbi5ub2RlLWxpc3Qub3ZlcnZpZXcuaHRtbFwiXHJcbiAgICAgICAgY29udHJvbGxlcjogJ0pvYlBsYW5PdmVydmlld0NvbnRyb2xsZXInIFxyXG5cclxuICAuc3RhdGUgXCJzaW5nbGUtam9iLnBsYW4uYWNjdW11bGF0b3JzXCIsXHJcbiAgICB1cmw6IFwiL2FjY3VtdWxhdG9yc1wiXHJcbiAgICB2aWV3czpcclxuICAgICAgJ25vZGUtZGV0YWlscyc6XHJcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvam9icy9qb2IucGxhbi5ub2RlLWxpc3QuYWNjdW11bGF0b3JzLmh0bWxcIlxyXG4gICAgICAgIGNvbnRyb2xsZXI6ICdKb2JQbGFuQWNjdW11bGF0b3JzQ29udHJvbGxlcicgXHJcblxyXG4gIC5zdGF0ZSBcInNpbmdsZS1qb2IudGltZWxpbmVcIixcclxuICAgIHVybDogXCIvdGltZWxpbmVcIlxyXG4gICAgdmlld3M6XHJcbiAgICAgIGRldGFpbHM6XHJcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvam9icy9qb2IudGltZWxpbmUuaHRtbFwiXHJcblxyXG4gIC5zdGF0ZSBcInNpbmdsZS1qb2IudGltZWxpbmUudmVydGV4XCIsXHJcbiAgICB1cmw6IFwiL3t2ZXJ0ZXhJZH1cIlxyXG4gICAgdmlld3M6XHJcbiAgICAgIHZlcnRleDpcclxuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JzL2pvYi50aW1lbGluZS52ZXJ0ZXguaHRtbFwiXHJcbiAgICAgICAgY29udHJvbGxlcjogJ0pvYlRpbWVsaW5lVmVydGV4Q29udHJvbGxlcidcclxuXHJcbiAgLnN0YXRlIFwic2luZ2xlLWpvYi5zdGF0aXN0aWNzXCIsXHJcbiAgICB1cmw6IFwiL3N0YXRpc3RpY3NcIlxyXG4gICAgdmlld3M6XHJcbiAgICAgIGRldGFpbHM6XHJcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvam9icy9qb2Iuc3RhdGlzdGljcy5odG1sXCJcclxuXHJcbiAgLnN0YXRlIFwic2luZ2xlLWpvYi5leGNlcHRpb25zXCIsXHJcbiAgICB1cmw6IFwiL2V4Y2VwdGlvbnNcIlxyXG4gICAgdmlld3M6XHJcbiAgICAgIGRldGFpbHM6XHJcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvam9icy9qb2IuZXhjZXB0aW9ucy5odG1sXCJcclxuICAgICAgICBjb250cm9sbGVyOiAnSm9iRXhjZXB0aW9uc0NvbnRyb2xsZXInXHJcblxyXG4gIC5zdGF0ZSBcInNpbmdsZS1qb2IucHJvcGVydGllc1wiLFxyXG4gICAgdXJsOiBcIi9wcm9wZXJ0aWVzXCJcclxuICAgIHZpZXdzOlxyXG4gICAgICBkZXRhaWxzOlxyXG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYnMvam9iLnByb3BlcnRpZXMuaHRtbFwiXHJcbiAgICAgICAgY29udHJvbGxlcjogJ0pvYlByb3BlcnRpZXNDb250cm9sbGVyJ1xyXG5cclxuICAuc3RhdGUgXCJzaW5nbGUtam9iLmNvbmZpZ1wiLFxyXG4gICAgdXJsOiBcIi9jb25maWdcIlxyXG4gICAgdmlld3M6XHJcbiAgICAgIGRldGFpbHM6XHJcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvam9icy9qb2IuY29uZmlnLmh0bWxcIlxyXG5cclxuICAuc3RhdGUgXCJ0YXNrbWFuYWdlcnNcIixcclxuICAgIHVybDogXCIvdGFza21hbmFnZXJzXCJcclxuICAgIHZpZXdzOlxyXG4gICAgICBtYWluOlxyXG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL3Rhc2ttYW5hZ2Vycy9pbmRleC5odG1sXCJcclxuICAgICAgICBjb250cm9sbGVyOiAnVGFza01hbmFnZXJzQ29udHJvbGxlcidcclxuXHJcbiAgLnN0YXRlIFwiam9ibWFuYWdlclwiLFxyXG4gICAgICB1cmw6IFwiL2pvYm1hbmFnZXJcIlxyXG4gICAgICB2aWV3czpcclxuICAgICAgICBtYWluOlxyXG4gICAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvam9ibWFuYWdlci9pbmRleC5odG1sXCJcclxuXHJcbiAgLnN0YXRlIFwiam9ibWFuYWdlci5jb25maWdcIixcclxuICAgIHVybDogXCIvY29uZmlnXCJcclxuICAgIHZpZXdzOlxyXG4gICAgICBkZXRhaWxzOlxyXG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYm1hbmFnZXIvY29uZmlnLmh0bWxcIlxyXG4gICAgICAgIGNvbnRyb2xsZXI6ICdKb2JNYW5hZ2VyQ29uZmlnQ29udHJvbGxlcidcclxuXHJcbiAgLnN0YXRlIFwiam9ibWFuYWdlci5zdGRvdXRcIixcclxuICAgIHVybDogXCIvc3Rkb3V0XCJcclxuICAgIHZpZXdzOlxyXG4gICAgICBkZXRhaWxzOlxyXG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYm1hbmFnZXIvc3Rkb3V0Lmh0bWxcIlxyXG5cclxuICAuc3RhdGUgXCJqb2JtYW5hZ2VyLmxvZ2ZpbGVcIixcclxuICAgIHVybDogXCIvbG9nZmlsZVwiXHJcbiAgICB2aWV3czpcclxuICAgICAgZGV0YWlsczpcclxuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JtYW5hZ2VyL2xvZ2ZpbGUuaHRtbFwiXHJcblxyXG4gICR1cmxSb3V0ZXJQcm92aWRlci5vdGhlcndpc2UgXCIvb3ZlcnZpZXdcIlxyXG4iLCJhbmd1bGFyLm1vZHVsZSgnZmxpbmtBcHAnLCBbJ3VpLnJvdXRlcicsICdhbmd1bGFyTW9tZW50J10pLnJ1bihmdW5jdGlvbigkcm9vdFNjb3BlKSB7XG4gICRyb290U2NvcGUuc2lkZWJhclZpc2libGUgPSBmYWxzZTtcbiAgcmV0dXJuICRyb290U2NvcGUuc2hvd1NpZGViYXIgPSBmdW5jdGlvbigpIHtcbiAgICAkcm9vdFNjb3BlLnNpZGViYXJWaXNpYmxlID0gISRyb290U2NvcGUuc2lkZWJhclZpc2libGU7XG4gICAgcmV0dXJuICRyb290U2NvcGUuc2lkZWJhckNsYXNzID0gJ2ZvcmNlLXNob3cnO1xuICB9O1xufSkudmFsdWUoJ2ZsaW5rQ29uZmlnJywge1xuICBcInJlZnJlc2gtaW50ZXJ2YWxcIjogMTAwMDBcbn0pLnJ1bihmdW5jdGlvbihKb2JzU2VydmljZSwgTWFpblNlcnZpY2UsIGZsaW5rQ29uZmlnLCAkaW50ZXJ2YWwpIHtcbiAgcmV0dXJuIE1haW5TZXJ2aWNlLmxvYWRDb25maWcoKS50aGVuKGZ1bmN0aW9uKGNvbmZpZykge1xuICAgIGFuZ3VsYXIuZXh0ZW5kKGZsaW5rQ29uZmlnLCBjb25maWcpO1xuICAgIEpvYnNTZXJ2aWNlLmxpc3RKb2JzKCk7XG4gICAgcmV0dXJuICRpbnRlcnZhbChmdW5jdGlvbigpIHtcbiAgICAgIHJldHVybiBKb2JzU2VydmljZS5saXN0Sm9icygpO1xuICAgIH0sIGZsaW5rQ29uZmlnW1wicmVmcmVzaC1pbnRlcnZhbFwiXSk7XG4gIH0pO1xufSkuY29uZmlnKGZ1bmN0aW9uKCR1aVZpZXdTY3JvbGxQcm92aWRlcikge1xuICByZXR1cm4gJHVpVmlld1Njcm9sbFByb3ZpZGVyLnVzZUFuY2hvclNjcm9sbCgpO1xufSkuY29uZmlnKGZ1bmN0aW9uKCRzdGF0ZVByb3ZpZGVyLCAkdXJsUm91dGVyUHJvdmlkZXIpIHtcbiAgJHN0YXRlUHJvdmlkZXIuc3RhdGUoXCJvdmVydmlld1wiLCB7XG4gICAgdXJsOiBcIi9vdmVydmlld1wiLFxuICAgIHZpZXdzOiB7XG4gICAgICBtYWluOiB7XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL292ZXJ2aWV3Lmh0bWxcIixcbiAgICAgICAgY29udHJvbGxlcjogJ092ZXJ2aWV3Q29udHJvbGxlcidcbiAgICAgIH1cbiAgICB9XG4gIH0pLnN0YXRlKFwicnVubmluZy1qb2JzXCIsIHtcbiAgICB1cmw6IFwiL3J1bm5pbmctam9ic1wiLFxuICAgIHZpZXdzOiB7XG4gICAgICBtYWluOiB7XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYnMvcnVubmluZy1qb2JzLmh0bWxcIixcbiAgICAgICAgY29udHJvbGxlcjogJ1J1bm5pbmdKb2JzQ29udHJvbGxlcidcbiAgICAgIH1cbiAgICB9XG4gIH0pLnN0YXRlKFwiY29tcGxldGVkLWpvYnNcIiwge1xuICAgIHVybDogXCIvY29tcGxldGVkLWpvYnNcIixcbiAgICB2aWV3czoge1xuICAgICAgbWFpbjoge1xuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JzL2NvbXBsZXRlZC1qb2JzLmh0bWxcIixcbiAgICAgICAgY29udHJvbGxlcjogJ0NvbXBsZXRlZEpvYnNDb250cm9sbGVyJ1xuICAgICAgfVxuICAgIH1cbiAgfSkuc3RhdGUoXCJzaW5nbGUtam9iXCIsIHtcbiAgICB1cmw6IFwiL2pvYnMve2pvYmlkfVwiLFxuICAgIGFic3RyYWN0OiB0cnVlLFxuICAgIHZpZXdzOiB7XG4gICAgICBtYWluOiB7XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYnMvam9iLmh0bWxcIixcbiAgICAgICAgY29udHJvbGxlcjogJ1NpbmdsZUpvYkNvbnRyb2xsZXInXG4gICAgICB9XG4gICAgfVxuICB9KS5zdGF0ZShcInNpbmdsZS1qb2IucGxhblwiLCB7XG4gICAgdXJsOiBcIlwiLFxuICAgIGFic3RyYWN0OiB0cnVlLFxuICAgIHZpZXdzOiB7XG4gICAgICBkZXRhaWxzOiB7XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYnMvam9iLnBsYW4uaHRtbFwiLFxuICAgICAgICBjb250cm9sbGVyOiAnSm9iUGxhbkNvbnRyb2xsZXInXG4gICAgICB9XG4gICAgfVxuICB9KS5zdGF0ZShcInNpbmdsZS1qb2IucGxhbi5vdmVydmlld1wiLCB7XG4gICAgdXJsOiBcIlwiLFxuICAgIHZpZXdzOiB7XG4gICAgICAnbm9kZS1kZXRhaWxzJzoge1xuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JzL2pvYi5wbGFuLm5vZGUtbGlzdC5vdmVydmlldy5odG1sXCIsXG4gICAgICAgIGNvbnRyb2xsZXI6ICdKb2JQbGFuT3ZlcnZpZXdDb250cm9sbGVyJ1xuICAgICAgfVxuICAgIH1cbiAgfSkuc3RhdGUoXCJzaW5nbGUtam9iLnBsYW4uYWNjdW11bGF0b3JzXCIsIHtcbiAgICB1cmw6IFwiL2FjY3VtdWxhdG9yc1wiLFxuICAgIHZpZXdzOiB7XG4gICAgICAnbm9kZS1kZXRhaWxzJzoge1xuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JzL2pvYi5wbGFuLm5vZGUtbGlzdC5hY2N1bXVsYXRvcnMuaHRtbFwiLFxuICAgICAgICBjb250cm9sbGVyOiAnSm9iUGxhbkFjY3VtdWxhdG9yc0NvbnRyb2xsZXInXG4gICAgICB9XG4gICAgfVxuICB9KS5zdGF0ZShcInNpbmdsZS1qb2IudGltZWxpbmVcIiwge1xuICAgIHVybDogXCIvdGltZWxpbmVcIixcbiAgICB2aWV3czoge1xuICAgICAgZGV0YWlsczoge1xuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JzL2pvYi50aW1lbGluZS5odG1sXCJcbiAgICAgIH1cbiAgICB9XG4gIH0pLnN0YXRlKFwic2luZ2xlLWpvYi50aW1lbGluZS52ZXJ0ZXhcIiwge1xuICAgIHVybDogXCIve3ZlcnRleElkfVwiLFxuICAgIHZpZXdzOiB7XG4gICAgICB2ZXJ0ZXg6IHtcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvam9icy9qb2IudGltZWxpbmUudmVydGV4Lmh0bWxcIixcbiAgICAgICAgY29udHJvbGxlcjogJ0pvYlRpbWVsaW5lVmVydGV4Q29udHJvbGxlcidcbiAgICAgIH1cbiAgICB9XG4gIH0pLnN0YXRlKFwic2luZ2xlLWpvYi5zdGF0aXN0aWNzXCIsIHtcbiAgICB1cmw6IFwiL3N0YXRpc3RpY3NcIixcbiAgICB2aWV3czoge1xuICAgICAgZGV0YWlsczoge1xuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JzL2pvYi5zdGF0aXN0aWNzLmh0bWxcIlxuICAgICAgfVxuICAgIH1cbiAgfSkuc3RhdGUoXCJzaW5nbGUtam9iLmV4Y2VwdGlvbnNcIiwge1xuICAgIHVybDogXCIvZXhjZXB0aW9uc1wiLFxuICAgIHZpZXdzOiB7XG4gICAgICBkZXRhaWxzOiB7XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYnMvam9iLmV4Y2VwdGlvbnMuaHRtbFwiLFxuICAgICAgICBjb250cm9sbGVyOiAnSm9iRXhjZXB0aW9uc0NvbnRyb2xsZXInXG4gICAgICB9XG4gICAgfVxuICB9KS5zdGF0ZShcInNpbmdsZS1qb2IucHJvcGVydGllc1wiLCB7XG4gICAgdXJsOiBcIi9wcm9wZXJ0aWVzXCIsXG4gICAgdmlld3M6IHtcbiAgICAgIGRldGFpbHM6IHtcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvam9icy9qb2IucHJvcGVydGllcy5odG1sXCIsXG4gICAgICAgIGNvbnRyb2xsZXI6ICdKb2JQcm9wZXJ0aWVzQ29udHJvbGxlcidcbiAgICAgIH1cbiAgICB9XG4gIH0pLnN0YXRlKFwic2luZ2xlLWpvYi5jb25maWdcIiwge1xuICAgIHVybDogXCIvY29uZmlnXCIsXG4gICAgdmlld3M6IHtcbiAgICAgIGRldGFpbHM6IHtcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvam9icy9qb2IuY29uZmlnLmh0bWxcIlxuICAgICAgfVxuICAgIH1cbiAgfSkuc3RhdGUoXCJ0YXNrbWFuYWdlcnNcIiwge1xuICAgIHVybDogXCIvdGFza21hbmFnZXJzXCIsXG4gICAgdmlld3M6IHtcbiAgICAgIG1haW46IHtcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvdGFza21hbmFnZXJzL2luZGV4Lmh0bWxcIixcbiAgICAgICAgY29udHJvbGxlcjogJ1Rhc2tNYW5hZ2Vyc0NvbnRyb2xsZXInXG4gICAgICB9XG4gICAgfVxuICB9KS5zdGF0ZShcImpvYm1hbmFnZXJcIiwge1xuICAgIHVybDogXCIvam9ibWFuYWdlclwiLFxuICAgIHZpZXdzOiB7XG4gICAgICBtYWluOiB7XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYm1hbmFnZXIvaW5kZXguaHRtbFwiXG4gICAgICB9XG4gICAgfVxuICB9KS5zdGF0ZShcImpvYm1hbmFnZXIuY29uZmlnXCIsIHtcbiAgICB1cmw6IFwiL2NvbmZpZ1wiLFxuICAgIHZpZXdzOiB7XG4gICAgICBkZXRhaWxzOiB7XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYm1hbmFnZXIvY29uZmlnLmh0bWxcIixcbiAgICAgICAgY29udHJvbGxlcjogJ0pvYk1hbmFnZXJDb25maWdDb250cm9sbGVyJ1xuICAgICAgfVxuICAgIH1cbiAgfSkuc3RhdGUoXCJqb2JtYW5hZ2VyLnN0ZG91dFwiLCB7XG4gICAgdXJsOiBcIi9zdGRvdXRcIixcbiAgICB2aWV3czoge1xuICAgICAgZGV0YWlsczoge1xuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JtYW5hZ2VyL3N0ZG91dC5odG1sXCJcbiAgICAgIH1cbiAgICB9XG4gIH0pLnN0YXRlKFwiam9ibWFuYWdlci5sb2dmaWxlXCIsIHtcbiAgICB1cmw6IFwiL2xvZ2ZpbGVcIixcbiAgICB2aWV3czoge1xuICAgICAgZGV0YWlsczoge1xuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JtYW5hZ2VyL2xvZ2ZpbGUuaHRtbFwiXG4gICAgICB9XG4gICAgfVxuICB9KTtcbiAgcmV0dXJuICR1cmxSb3V0ZXJQcm92aWRlci5vdGhlcndpc2UoXCIvb3ZlcnZpZXdcIik7XG59KTtcbiIsIiNcclxuIyBMaWNlbnNlZCB0byB0aGUgQXBhY2hlIFNvZnR3YXJlIEZvdW5kYXRpb24gKEFTRikgdW5kZXIgb25lXHJcbiMgb3IgbW9yZSBjb250cmlidXRvciBsaWNlbnNlIGFncmVlbWVudHMuICBTZWUgdGhlIE5PVElDRSBmaWxlXHJcbiMgZGlzdHJpYnV0ZWQgd2l0aCB0aGlzIHdvcmsgZm9yIGFkZGl0aW9uYWwgaW5mb3JtYXRpb25cclxuIyByZWdhcmRpbmcgY29weXJpZ2h0IG93bmVyc2hpcC4gIFRoZSBBU0YgbGljZW5zZXMgdGhpcyBmaWxlXHJcbiMgdG8geW91IHVuZGVyIHRoZSBBcGFjaGUgTGljZW5zZSwgVmVyc2lvbiAyLjAgKHRoZVxyXG4jIFwiTGljZW5zZVwiKTsgeW91IG1heSBub3QgdXNlIHRoaXMgZmlsZSBleGNlcHQgaW4gY29tcGxpYW5jZVxyXG4jIHdpdGggdGhlIExpY2Vuc2UuICBZb3UgbWF5IG9idGFpbiBhIGNvcHkgb2YgdGhlIExpY2Vuc2UgYXRcclxuI1xyXG4jICAgICBodHRwOi8vd3d3LmFwYWNoZS5vcmcvbGljZW5zZXMvTElDRU5TRS0yLjBcclxuI1xyXG4jIFVubGVzcyByZXF1aXJlZCBieSBhcHBsaWNhYmxlIGxhdyBvciBhZ3JlZWQgdG8gaW4gd3JpdGluZywgc29mdHdhcmVcclxuIyBkaXN0cmlidXRlZCB1bmRlciB0aGUgTGljZW5zZSBpcyBkaXN0cmlidXRlZCBvbiBhbiBcIkFTIElTXCIgQkFTSVMsXHJcbiMgV0lUSE9VVCBXQVJSQU5USUVTIE9SIENPTkRJVElPTlMgT0YgQU5ZIEtJTkQsIGVpdGhlciBleHByZXNzIG9yIGltcGxpZWQuXHJcbiMgU2VlIHRoZSBMaWNlbnNlIGZvciB0aGUgc3BlY2lmaWMgbGFuZ3VhZ2UgZ292ZXJuaW5nIHBlcm1pc3Npb25zIGFuZFxyXG4jIGxpbWl0YXRpb25zIHVuZGVyIHRoZSBMaWNlbnNlLlxyXG4jXHJcblxyXG5hbmd1bGFyLm1vZHVsZSgnZmxpbmtBcHAnKVxyXG5cclxuIyAtLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tXHJcblxyXG4uZGlyZWN0aXZlICdic0xhYmVsJywgKEpvYnNTZXJ2aWNlKSAtPlxyXG4gIHRyYW5zY2x1ZGU6IHRydWVcclxuICByZXBsYWNlOiB0cnVlXHJcbiAgc2NvcGU6IFxyXG4gICAgZ2V0TGFiZWxDbGFzczogXCImXCJcclxuICAgIHN0YXR1czogXCJAXCJcclxuXHJcbiAgdGVtcGxhdGU6IFwiPHNwYW4gdGl0bGU9J3t7c3RhdHVzfX0nIG5nLWNsYXNzPSdnZXRMYWJlbENsYXNzKCknPjxuZy10cmFuc2NsdWRlPjwvbmctdHJhbnNjbHVkZT48L3NwYW4+XCJcclxuICBcclxuICBsaW5rOiAoc2NvcGUsIGVsZW1lbnQsIGF0dHJzKSAtPlxyXG4gICAgc2NvcGUuZ2V0TGFiZWxDbGFzcyA9IC0+XHJcbiAgICAgICdsYWJlbCBsYWJlbC0nICsgSm9ic1NlcnZpY2UudHJhbnNsYXRlTGFiZWxTdGF0ZShhdHRycy5zdGF0dXMpXHJcblxyXG4jIC0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS1cclxuXHJcbi5kaXJlY3RpdmUgJ2luZGljYXRvclByaW1hcnknLCAoSm9ic1NlcnZpY2UpIC0+XHJcbiAgcmVwbGFjZTogdHJ1ZVxyXG4gIHNjb3BlOiBcclxuICAgIGdldExhYmVsQ2xhc3M6IFwiJlwiXHJcbiAgICBzdGF0dXM6ICdAJ1xyXG5cclxuICB0ZW1wbGF0ZTogXCI8aSB0aXRsZT0ne3tzdGF0dXN9fScgbmctY2xhc3M9J2dldExhYmVsQ2xhc3MoKScgLz5cIlxyXG4gIFxyXG4gIGxpbms6IChzY29wZSwgZWxlbWVudCwgYXR0cnMpIC0+XHJcbiAgICBzY29wZS5nZXRMYWJlbENsYXNzID0gLT5cclxuICAgICAgJ2ZhIGZhLWNpcmNsZSBpbmRpY2F0b3IgaW5kaWNhdG9yLScgKyBKb2JzU2VydmljZS50cmFuc2xhdGVMYWJlbFN0YXRlKGF0dHJzLnN0YXR1cylcclxuXHJcbiMgLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLVxyXG5cclxuLmRpcmVjdGl2ZSAndGFibGVQcm9wZXJ0eScsIC0+XHJcbiAgcmVwbGFjZTogdHJ1ZVxyXG4gIHNjb3BlOlxyXG4gICAgdmFsdWU6ICc9J1xyXG5cclxuICB0ZW1wbGF0ZTogXCI8dGQgdGl0bGU9XFxcInt7dmFsdWUgfHwgJ05vbmUnfX1cXFwiPnt7dmFsdWUgfHwgJ05vbmUnfX08L3RkPlwiXHJcbiIsImFuZ3VsYXIubW9kdWxlKCdmbGlua0FwcCcpLmRpcmVjdGl2ZSgnYnNMYWJlbCcsIGZ1bmN0aW9uKEpvYnNTZXJ2aWNlKSB7XG4gIHJldHVybiB7XG4gICAgdHJhbnNjbHVkZTogdHJ1ZSxcbiAgICByZXBsYWNlOiB0cnVlLFxuICAgIHNjb3BlOiB7XG4gICAgICBnZXRMYWJlbENsYXNzOiBcIiZcIixcbiAgICAgIHN0YXR1czogXCJAXCJcbiAgICB9LFxuICAgIHRlbXBsYXRlOiBcIjxzcGFuIHRpdGxlPSd7e3N0YXR1c319JyBuZy1jbGFzcz0nZ2V0TGFiZWxDbGFzcygpJz48bmctdHJhbnNjbHVkZT48L25nLXRyYW5zY2x1ZGU+PC9zcGFuPlwiLFxuICAgIGxpbms6IGZ1bmN0aW9uKHNjb3BlLCBlbGVtZW50LCBhdHRycykge1xuICAgICAgcmV0dXJuIHNjb3BlLmdldExhYmVsQ2xhc3MgPSBmdW5jdGlvbigpIHtcbiAgICAgICAgcmV0dXJuICdsYWJlbCBsYWJlbC0nICsgSm9ic1NlcnZpY2UudHJhbnNsYXRlTGFiZWxTdGF0ZShhdHRycy5zdGF0dXMpO1xuICAgICAgfTtcbiAgICB9XG4gIH07XG59KS5kaXJlY3RpdmUoJ2luZGljYXRvclByaW1hcnknLCBmdW5jdGlvbihKb2JzU2VydmljZSkge1xuICByZXR1cm4ge1xuICAgIHJlcGxhY2U6IHRydWUsXG4gICAgc2NvcGU6IHtcbiAgICAgIGdldExhYmVsQ2xhc3M6IFwiJlwiLFxuICAgICAgc3RhdHVzOiAnQCdcbiAgICB9LFxuICAgIHRlbXBsYXRlOiBcIjxpIHRpdGxlPSd7e3N0YXR1c319JyBuZy1jbGFzcz0nZ2V0TGFiZWxDbGFzcygpJyAvPlwiLFxuICAgIGxpbms6IGZ1bmN0aW9uKHNjb3BlLCBlbGVtZW50LCBhdHRycykge1xuICAgICAgcmV0dXJuIHNjb3BlLmdldExhYmVsQ2xhc3MgPSBmdW5jdGlvbigpIHtcbiAgICAgICAgcmV0dXJuICdmYSBmYS1jaXJjbGUgaW5kaWNhdG9yIGluZGljYXRvci0nICsgSm9ic1NlcnZpY2UudHJhbnNsYXRlTGFiZWxTdGF0ZShhdHRycy5zdGF0dXMpO1xuICAgICAgfTtcbiAgICB9XG4gIH07XG59KS5kaXJlY3RpdmUoJ3RhYmxlUHJvcGVydHknLCBmdW5jdGlvbigpIHtcbiAgcmV0dXJuIHtcbiAgICByZXBsYWNlOiB0cnVlLFxuICAgIHNjb3BlOiB7XG4gICAgICB2YWx1ZTogJz0nXG4gICAgfSxcbiAgICB0ZW1wbGF0ZTogXCI8dGQgdGl0bGU9XFxcInt7dmFsdWUgfHwgJ05vbmUnfX1cXFwiPnt7dmFsdWUgfHwgJ05vbmUnfX08L3RkPlwiXG4gIH07XG59KTtcbiIsIiNcclxuIyBMaWNlbnNlZCB0byB0aGUgQXBhY2hlIFNvZnR3YXJlIEZvdW5kYXRpb24gKEFTRikgdW5kZXIgb25lXHJcbiMgb3IgbW9yZSBjb250cmlidXRvciBsaWNlbnNlIGFncmVlbWVudHMuICBTZWUgdGhlIE5PVElDRSBmaWxlXHJcbiMgZGlzdHJpYnV0ZWQgd2l0aCB0aGlzIHdvcmsgZm9yIGFkZGl0aW9uYWwgaW5mb3JtYXRpb25cclxuIyByZWdhcmRpbmcgY29weXJpZ2h0IG93bmVyc2hpcC4gIFRoZSBBU0YgbGljZW5zZXMgdGhpcyBmaWxlXHJcbiMgdG8geW91IHVuZGVyIHRoZSBBcGFjaGUgTGljZW5zZSwgVmVyc2lvbiAyLjAgKHRoZVxyXG4jIFwiTGljZW5zZVwiKTsgeW91IG1heSBub3QgdXNlIHRoaXMgZmlsZSBleGNlcHQgaW4gY29tcGxpYW5jZVxyXG4jIHdpdGggdGhlIExpY2Vuc2UuICBZb3UgbWF5IG9idGFpbiBhIGNvcHkgb2YgdGhlIExpY2Vuc2UgYXRcclxuI1xyXG4jICAgICBodHRwOi8vd3d3LmFwYWNoZS5vcmcvbGljZW5zZXMvTElDRU5TRS0yLjBcclxuI1xyXG4jIFVubGVzcyByZXF1aXJlZCBieSBhcHBsaWNhYmxlIGxhdyBvciBhZ3JlZWQgdG8gaW4gd3JpdGluZywgc29mdHdhcmVcclxuIyBkaXN0cmlidXRlZCB1bmRlciB0aGUgTGljZW5zZSBpcyBkaXN0cmlidXRlZCBvbiBhbiBcIkFTIElTXCIgQkFTSVMsXHJcbiMgV0lUSE9VVCBXQVJSQU5USUVTIE9SIENPTkRJVElPTlMgT0YgQU5ZIEtJTkQsIGVpdGhlciBleHByZXNzIG9yIGltcGxpZWQuXHJcbiMgU2VlIHRoZSBMaWNlbnNlIGZvciB0aGUgc3BlY2lmaWMgbGFuZ3VhZ2UgZ292ZXJuaW5nIHBlcm1pc3Npb25zIGFuZFxyXG4jIGxpbWl0YXRpb25zIHVuZGVyIHRoZSBMaWNlbnNlLlxyXG4jXHJcblxyXG5hbmd1bGFyLm1vZHVsZSgnZmxpbmtBcHAnKVxyXG5cclxuLmZpbHRlciBcImFtRHVyYXRpb25Gb3JtYXRFeHRlbmRlZFwiLCAoYW5ndWxhck1vbWVudENvbmZpZykgLT5cclxuICBhbUR1cmF0aW9uRm9ybWF0RXh0ZW5kZWRGaWx0ZXIgPSAodmFsdWUsIGZvcm1hdCwgZHVyYXRpb25Gb3JtYXQpIC0+XHJcbiAgICByZXR1cm4gXCJcIiAgaWYgdHlwZW9mIHZhbHVlIGlzIFwidW5kZWZpbmVkXCIgb3IgdmFsdWUgaXMgbnVsbFxyXG5cclxuICAgIG1vbWVudC5kdXJhdGlvbih2YWx1ZSwgZm9ybWF0KS5mb3JtYXQoZHVyYXRpb25Gb3JtYXQsIHsgdHJpbTogZmFsc2UgfSlcclxuXHJcbiAgYW1EdXJhdGlvbkZvcm1hdEV4dGVuZGVkRmlsdGVyLiRzdGF0ZWZ1bCA9IGFuZ3VsYXJNb21lbnRDb25maWcuc3RhdGVmdWxGaWx0ZXJzXHJcblxyXG4gIGFtRHVyYXRpb25Gb3JtYXRFeHRlbmRlZEZpbHRlclxyXG5cclxuLmZpbHRlciBcImh1bWFuaXplVGV4dFwiLCAtPlxyXG4gICh0ZXh0KSAtPlxyXG4gICAgIyBUT0RPOiBleHRlbmQuLi4gYSBsb3RcclxuICAgIGlmIHRleHQgdGhlbiB0ZXh0LnJlcGxhY2UoLyZndDsvZywgXCI+XCIpLnJlcGxhY2UoLzxiclxcLz4vZyxcIlwiKSBlbHNlICcnXHJcblxyXG4uZmlsdGVyIFwiYnl0ZXNcIiwgLT5cclxuICAoYnl0ZXMsIHByZWNpc2lvbikgLT5cclxuICAgIHJldHVybiBcIi1cIiAgaWYgaXNOYU4ocGFyc2VGbG9hdChieXRlcykpIG9yIG5vdCBpc0Zpbml0ZShieXRlcylcclxuICAgIHByZWNpc2lvbiA9IDEgIGlmIHR5cGVvZiBwcmVjaXNpb24gaXMgXCJ1bmRlZmluZWRcIlxyXG4gICAgdW5pdHMgPSBbIFwiYnl0ZXNcIiwgXCJrQlwiLCBcIk1CXCIsIFwiR0JcIiwgXCJUQlwiLCBcIlBCXCIgXVxyXG4gICAgbnVtYmVyID0gTWF0aC5mbG9vcihNYXRoLmxvZyhieXRlcykgLyBNYXRoLmxvZygxMDI0KSlcclxuICAgIChieXRlcyAvIE1hdGgucG93KDEwMjQsIE1hdGguZmxvb3IobnVtYmVyKSkpLnRvRml4ZWQocHJlY2lzaW9uKSArIFwiIFwiICsgdW5pdHNbbnVtYmVyXVxyXG4iLCJhbmd1bGFyLm1vZHVsZSgnZmxpbmtBcHAnKS5maWx0ZXIoXCJhbUR1cmF0aW9uRm9ybWF0RXh0ZW5kZWRcIiwgZnVuY3Rpb24oYW5ndWxhck1vbWVudENvbmZpZykge1xuICB2YXIgYW1EdXJhdGlvbkZvcm1hdEV4dGVuZGVkRmlsdGVyO1xuICBhbUR1cmF0aW9uRm9ybWF0RXh0ZW5kZWRGaWx0ZXIgPSBmdW5jdGlvbih2YWx1ZSwgZm9ybWF0LCBkdXJhdGlvbkZvcm1hdCkge1xuICAgIGlmICh0eXBlb2YgdmFsdWUgPT09IFwidW5kZWZpbmVkXCIgfHwgdmFsdWUgPT09IG51bGwpIHtcbiAgICAgIHJldHVybiBcIlwiO1xuICAgIH1cbiAgICByZXR1cm4gbW9tZW50LmR1cmF0aW9uKHZhbHVlLCBmb3JtYXQpLmZvcm1hdChkdXJhdGlvbkZvcm1hdCwge1xuICAgICAgdHJpbTogZmFsc2VcbiAgICB9KTtcbiAgfTtcbiAgYW1EdXJhdGlvbkZvcm1hdEV4dGVuZGVkRmlsdGVyLiRzdGF0ZWZ1bCA9IGFuZ3VsYXJNb21lbnRDb25maWcuc3RhdGVmdWxGaWx0ZXJzO1xuICByZXR1cm4gYW1EdXJhdGlvbkZvcm1hdEV4dGVuZGVkRmlsdGVyO1xufSkuZmlsdGVyKFwiaHVtYW5pemVUZXh0XCIsIGZ1bmN0aW9uKCkge1xuICByZXR1cm4gZnVuY3Rpb24odGV4dCkge1xuICAgIGlmICh0ZXh0KSB7XG4gICAgICByZXR1cm4gdGV4dC5yZXBsYWNlKC8mZ3Q7L2csIFwiPlwiKS5yZXBsYWNlKC88YnJcXC8+L2csIFwiXCIpO1xuICAgIH0gZWxzZSB7XG4gICAgICByZXR1cm4gJyc7XG4gICAgfVxuICB9O1xufSkuZmlsdGVyKFwiYnl0ZXNcIiwgZnVuY3Rpb24oKSB7XG4gIHJldHVybiBmdW5jdGlvbihieXRlcywgcHJlY2lzaW9uKSB7XG4gICAgdmFyIG51bWJlciwgdW5pdHM7XG4gICAgaWYgKGlzTmFOKHBhcnNlRmxvYXQoYnl0ZXMpKSB8fCAhaXNGaW5pdGUoYnl0ZXMpKSB7XG4gICAgICByZXR1cm4gXCItXCI7XG4gICAgfVxuICAgIGlmICh0eXBlb2YgcHJlY2lzaW9uID09PSBcInVuZGVmaW5lZFwiKSB7XG4gICAgICBwcmVjaXNpb24gPSAxO1xuICAgIH1cbiAgICB1bml0cyA9IFtcImJ5dGVzXCIsIFwia0JcIiwgXCJNQlwiLCBcIkdCXCIsIFwiVEJcIiwgXCJQQlwiXTtcbiAgICBudW1iZXIgPSBNYXRoLmZsb29yKE1hdGgubG9nKGJ5dGVzKSAvIE1hdGgubG9nKDEwMjQpKTtcbiAgICByZXR1cm4gKGJ5dGVzIC8gTWF0aC5wb3coMTAyNCwgTWF0aC5mbG9vcihudW1iZXIpKSkudG9GaXhlZChwcmVjaXNpb24pICsgXCIgXCIgKyB1bml0c1tudW1iZXJdO1xuICB9O1xufSk7XG4iLCIjXHJcbiMgTGljZW5zZWQgdG8gdGhlIEFwYWNoZSBTb2Z0d2FyZSBGb3VuZGF0aW9uIChBU0YpIHVuZGVyIG9uZVxyXG4jIG9yIG1vcmUgY29udHJpYnV0b3IgbGljZW5zZSBhZ3JlZW1lbnRzLiAgU2VlIHRoZSBOT1RJQ0UgZmlsZVxyXG4jIGRpc3RyaWJ1dGVkIHdpdGggdGhpcyB3b3JrIGZvciBhZGRpdGlvbmFsIGluZm9ybWF0aW9uXHJcbiMgcmVnYXJkaW5nIGNvcHlyaWdodCBvd25lcnNoaXAuICBUaGUgQVNGIGxpY2Vuc2VzIHRoaXMgZmlsZVxyXG4jIHRvIHlvdSB1bmRlciB0aGUgQXBhY2hlIExpY2Vuc2UsIFZlcnNpb24gMi4wICh0aGVcclxuIyBcIkxpY2Vuc2VcIik7IHlvdSBtYXkgbm90IHVzZSB0aGlzIGZpbGUgZXhjZXB0IGluIGNvbXBsaWFuY2VcclxuIyB3aXRoIHRoZSBMaWNlbnNlLiAgWW91IG1heSBvYnRhaW4gYSBjb3B5IG9mIHRoZSBMaWNlbnNlIGF0XHJcbiNcclxuIyAgICAgaHR0cDovL3d3dy5hcGFjaGUub3JnL2xpY2Vuc2VzL0xJQ0VOU0UtMi4wXHJcbiNcclxuIyBVbmxlc3MgcmVxdWlyZWQgYnkgYXBwbGljYWJsZSBsYXcgb3IgYWdyZWVkIHRvIGluIHdyaXRpbmcsIHNvZnR3YXJlXHJcbiMgZGlzdHJpYnV0ZWQgdW5kZXIgdGhlIExpY2Vuc2UgaXMgZGlzdHJpYnV0ZWQgb24gYW4gXCJBUyBJU1wiIEJBU0lTLFxyXG4jIFdJVEhPVVQgV0FSUkFOVElFUyBPUiBDT05ESVRJT05TIE9GIEFOWSBLSU5ELCBlaXRoZXIgZXhwcmVzcyBvciBpbXBsaWVkLlxyXG4jIFNlZSB0aGUgTGljZW5zZSBmb3IgdGhlIHNwZWNpZmljIGxhbmd1YWdlIGdvdmVybmluZyBwZXJtaXNzaW9ucyBhbmRcclxuIyBsaW1pdGF0aW9ucyB1bmRlciB0aGUgTGljZW5zZS5cclxuI1xyXG5cclxuYW5ndWxhci5tb2R1bGUoJ2ZsaW5rQXBwJylcclxuXHJcbi5zZXJ2aWNlICdNYWluU2VydmljZScsICgkaHR0cCwgZmxpbmtDb25maWcsICRxKSAtPlxyXG4gIEBsb2FkQ29uZmlnID0gLT5cclxuICAgIGRlZmVycmVkID0gJHEuZGVmZXIoKVxyXG5cclxuICAgICRodHRwLmdldCBcIi9jb25maWdcIlxyXG4gICAgLnN1Y2Nlc3MgKGRhdGEsIHN0YXR1cywgaGVhZGVycywgY29uZmlnKSAtPlxyXG4gICAgICBkZWZlcnJlZC5yZXNvbHZlKGRhdGEpXHJcblxyXG4gICAgZGVmZXJyZWQucHJvbWlzZVxyXG5cclxuXHJcbiAgQFxyXG4iLCJhbmd1bGFyLm1vZHVsZSgnZmxpbmtBcHAnKS5zZXJ2aWNlKCdNYWluU2VydmljZScsIGZ1bmN0aW9uKCRodHRwLCBmbGlua0NvbmZpZywgJHEpIHtcbiAgdGhpcy5sb2FkQ29uZmlnID0gZnVuY3Rpb24oKSB7XG4gICAgdmFyIGRlZmVycmVkO1xuICAgIGRlZmVycmVkID0gJHEuZGVmZXIoKTtcbiAgICAkaHR0cC5nZXQoXCIvY29uZmlnXCIpLnN1Y2Nlc3MoZnVuY3Rpb24oZGF0YSwgc3RhdHVzLCBoZWFkZXJzLCBjb25maWcpIHtcbiAgICAgIHJldHVybiBkZWZlcnJlZC5yZXNvbHZlKGRhdGEpO1xuICAgIH0pO1xuICAgIHJldHVybiBkZWZlcnJlZC5wcm9taXNlO1xuICB9O1xuICByZXR1cm4gdGhpcztcbn0pO1xuIiwiI1xyXG4jIExpY2Vuc2VkIHRvIHRoZSBBcGFjaGUgU29mdHdhcmUgRm91bmRhdGlvbiAoQVNGKSB1bmRlciBvbmVcclxuIyBvciBtb3JlIGNvbnRyaWJ1dG9yIGxpY2Vuc2UgYWdyZWVtZW50cy4gIFNlZSB0aGUgTk9USUNFIGZpbGVcclxuIyBkaXN0cmlidXRlZCB3aXRoIHRoaXMgd29yayBmb3IgYWRkaXRpb25hbCBpbmZvcm1hdGlvblxyXG4jIHJlZ2FyZGluZyBjb3B5cmlnaHQgb3duZXJzaGlwLiAgVGhlIEFTRiBsaWNlbnNlcyB0aGlzIGZpbGVcclxuIyB0byB5b3UgdW5kZXIgdGhlIEFwYWNoZSBMaWNlbnNlLCBWZXJzaW9uIDIuMCAodGhlXHJcbiMgXCJMaWNlbnNlXCIpOyB5b3UgbWF5IG5vdCB1c2UgdGhpcyBmaWxlIGV4Y2VwdCBpbiBjb21wbGlhbmNlXHJcbiMgd2l0aCB0aGUgTGljZW5zZS4gIFlvdSBtYXkgb2J0YWluIGEgY29weSBvZiB0aGUgTGljZW5zZSBhdFxyXG4jXHJcbiMgICAgIGh0dHA6Ly93d3cuYXBhY2hlLm9yZy9saWNlbnNlcy9MSUNFTlNFLTIuMFxyXG4jXHJcbiMgVW5sZXNzIHJlcXVpcmVkIGJ5IGFwcGxpY2FibGUgbGF3IG9yIGFncmVlZCB0byBpbiB3cml0aW5nLCBzb2Z0d2FyZVxyXG4jIGRpc3RyaWJ1dGVkIHVuZGVyIHRoZSBMaWNlbnNlIGlzIGRpc3RyaWJ1dGVkIG9uIGFuIFwiQVMgSVNcIiBCQVNJUyxcclxuIyBXSVRIT1VUIFdBUlJBTlRJRVMgT1IgQ09ORElUSU9OUyBPRiBBTlkgS0lORCwgZWl0aGVyIGV4cHJlc3Mgb3IgaW1wbGllZC5cclxuIyBTZWUgdGhlIExpY2Vuc2UgZm9yIHRoZSBzcGVjaWZpYyBsYW5ndWFnZSBnb3Zlcm5pbmcgcGVybWlzc2lvbnMgYW5kXHJcbiMgbGltaXRhdGlvbnMgdW5kZXIgdGhlIExpY2Vuc2UuXHJcbiNcclxuXHJcbmFuZ3VsYXIubW9kdWxlKCdmbGlua0FwcCcpXHJcblxyXG4uY29udHJvbGxlciAnSm9iTWFuYWdlckNvbmZpZ0NvbnRyb2xsZXInLCAoJHNjb3BlLCBKb2JNYW5hZ2VyQ29uZmlnU2VydmljZSkgLT5cclxuICBKb2JNYW5hZ2VyQ29uZmlnU2VydmljZS5sb2FkQ29uZmlnKCkudGhlbiAoZGF0YSkgLT5cclxuICAgIGlmICEkc2NvcGUuam9ibWFuYWdlcj9cclxuICAgICAgJHNjb3BlLmpvYm1hbmFnZXIgPSB7fVxyXG4gICAgJHNjb3BlLmpvYm1hbmFnZXJbJ2NvbmZpZyddID0gZGF0YVxyXG5cclxuXHJcbiIsImFuZ3VsYXIubW9kdWxlKCdmbGlua0FwcCcpLmNvbnRyb2xsZXIoJ0pvYk1hbmFnZXJDb25maWdDb250cm9sbGVyJywgZnVuY3Rpb24oJHNjb3BlLCBKb2JNYW5hZ2VyQ29uZmlnU2VydmljZSkge1xuICByZXR1cm4gSm9iTWFuYWdlckNvbmZpZ1NlcnZpY2UubG9hZENvbmZpZygpLnRoZW4oZnVuY3Rpb24oZGF0YSkge1xuICAgIGlmICgkc2NvcGUuam9ibWFuYWdlciA9PSBudWxsKSB7XG4gICAgICAkc2NvcGUuam9ibWFuYWdlciA9IHt9O1xuICAgIH1cbiAgICByZXR1cm4gJHNjb3BlLmpvYm1hbmFnZXJbJ2NvbmZpZyddID0gZGF0YTtcbiAgfSk7XG59KTtcbiIsIiNcclxuIyBMaWNlbnNlZCB0byB0aGUgQXBhY2hlIFNvZnR3YXJlIEZvdW5kYXRpb24gKEFTRikgdW5kZXIgb25lXHJcbiMgb3IgbW9yZSBjb250cmlidXRvciBsaWNlbnNlIGFncmVlbWVudHMuICBTZWUgdGhlIE5PVElDRSBmaWxlXHJcbiMgZGlzdHJpYnV0ZWQgd2l0aCB0aGlzIHdvcmsgZm9yIGFkZGl0aW9uYWwgaW5mb3JtYXRpb25cclxuIyByZWdhcmRpbmcgY29weXJpZ2h0IG93bmVyc2hpcC4gIFRoZSBBU0YgbGljZW5zZXMgdGhpcyBmaWxlXHJcbiMgdG8geW91IHVuZGVyIHRoZSBBcGFjaGUgTGljZW5zZSwgVmVyc2lvbiAyLjAgKHRoZVxyXG4jIFwiTGljZW5zZVwiKTsgeW91IG1heSBub3QgdXNlIHRoaXMgZmlsZSBleGNlcHQgaW4gY29tcGxpYW5jZVxyXG4jIHdpdGggdGhlIExpY2Vuc2UuICBZb3UgbWF5IG9idGFpbiBhIGNvcHkgb2YgdGhlIExpY2Vuc2UgYXRcclxuI1xyXG4jICAgICBodHRwOi8vd3d3LmFwYWNoZS5vcmcvbGljZW5zZXMvTElDRU5TRS0yLjBcclxuI1xyXG4jIFVubGVzcyByZXF1aXJlZCBieSBhcHBsaWNhYmxlIGxhdyBvciBhZ3JlZWQgdG8gaW4gd3JpdGluZywgc29mdHdhcmVcclxuIyBkaXN0cmlidXRlZCB1bmRlciB0aGUgTGljZW5zZSBpcyBkaXN0cmlidXRlZCBvbiBhbiBcIkFTIElTXCIgQkFTSVMsXHJcbiMgV0lUSE9VVCBXQVJSQU5USUVTIE9SIENPTkRJVElPTlMgT0YgQU5ZIEtJTkQsIGVpdGhlciBleHByZXNzIG9yIGltcGxpZWQuXHJcbiMgU2VlIHRoZSBMaWNlbnNlIGZvciB0aGUgc3BlY2lmaWMgbGFuZ3VhZ2UgZ292ZXJuaW5nIHBlcm1pc3Npb25zIGFuZFxyXG4jIGxpbWl0YXRpb25zIHVuZGVyIHRoZSBMaWNlbnNlLlxyXG4jXHJcblxyXG5hbmd1bGFyLm1vZHVsZSgnZmxpbmtBcHAnKVxyXG5cclxuLnNlcnZpY2UgJ0pvYk1hbmFnZXJDb25maWdTZXJ2aWNlJywgKCRodHRwLCBmbGlua0NvbmZpZywgJHEpIC0+XHJcbiAgY29uZmlnID0ge31cclxuXHJcbiAgQGxvYWRDb25maWcgPSAtPlxyXG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpXHJcblxyXG4gICAgJGh0dHAuZ2V0KFwiL2pvYm1hbmFnZXIvY29uZmlnXCIpXHJcbiAgICAuc3VjY2VzcyAoZGF0YSwgc3RhdHVzLCBoZWFkZXJzLCBjb25maWcpIC0+XHJcbiAgICAgIGNvbmZpZyA9IGRhdGFcclxuICAgICAgZGVmZXJyZWQucmVzb2x2ZShkYXRhKVxyXG5cclxuICAgIGRlZmVycmVkLnByb21pc2VcclxuXHJcbiAgQFxyXG4iLCJhbmd1bGFyLm1vZHVsZSgnZmxpbmtBcHAnKS5zZXJ2aWNlKCdKb2JNYW5hZ2VyQ29uZmlnU2VydmljZScsIGZ1bmN0aW9uKCRodHRwLCBmbGlua0NvbmZpZywgJHEpIHtcbiAgdmFyIGNvbmZpZztcbiAgY29uZmlnID0ge307XG4gIHRoaXMubG9hZENvbmZpZyA9IGZ1bmN0aW9uKCkge1xuICAgIHZhciBkZWZlcnJlZDtcbiAgICBkZWZlcnJlZCA9ICRxLmRlZmVyKCk7XG4gICAgJGh0dHAuZ2V0KFwiL2pvYm1hbmFnZXIvY29uZmlnXCIpLnN1Y2Nlc3MoZnVuY3Rpb24oZGF0YSwgc3RhdHVzLCBoZWFkZXJzLCBjb25maWcpIHtcbiAgICAgIGNvbmZpZyA9IGRhdGE7XG4gICAgICByZXR1cm4gZGVmZXJyZWQucmVzb2x2ZShkYXRhKTtcbiAgICB9KTtcbiAgICByZXR1cm4gZGVmZXJyZWQucHJvbWlzZTtcbiAgfTtcbiAgcmV0dXJuIHRoaXM7XG59KTtcbiIsIiNcclxuIyBMaWNlbnNlZCB0byB0aGUgQXBhY2hlIFNvZnR3YXJlIEZvdW5kYXRpb24gKEFTRikgdW5kZXIgb25lXHJcbiMgb3IgbW9yZSBjb250cmlidXRvciBsaWNlbnNlIGFncmVlbWVudHMuICBTZWUgdGhlIE5PVElDRSBmaWxlXHJcbiMgZGlzdHJpYnV0ZWQgd2l0aCB0aGlzIHdvcmsgZm9yIGFkZGl0aW9uYWwgaW5mb3JtYXRpb25cclxuIyByZWdhcmRpbmcgY29weXJpZ2h0IG93bmVyc2hpcC4gIFRoZSBBU0YgbGljZW5zZXMgdGhpcyBmaWxlXHJcbiMgdG8geW91IHVuZGVyIHRoZSBBcGFjaGUgTGljZW5zZSwgVmVyc2lvbiAyLjAgKHRoZVxyXG4jIFwiTGljZW5zZVwiKTsgeW91IG1heSBub3QgdXNlIHRoaXMgZmlsZSBleGNlcHQgaW4gY29tcGxpYW5jZVxyXG4jIHdpdGggdGhlIExpY2Vuc2UuICBZb3UgbWF5IG9idGFpbiBhIGNvcHkgb2YgdGhlIExpY2Vuc2UgYXRcclxuI1xyXG4jICAgICBodHRwOi8vd3d3LmFwYWNoZS5vcmcvbGljZW5zZXMvTElDRU5TRS0yLjBcclxuI1xyXG4jIFVubGVzcyByZXF1aXJlZCBieSBhcHBsaWNhYmxlIGxhdyBvciBhZ3JlZWQgdG8gaW4gd3JpdGluZywgc29mdHdhcmVcclxuIyBkaXN0cmlidXRlZCB1bmRlciB0aGUgTGljZW5zZSBpcyBkaXN0cmlidXRlZCBvbiBhbiBcIkFTIElTXCIgQkFTSVMsXHJcbiMgV0lUSE9VVCBXQVJSQU5USUVTIE9SIENPTkRJVElPTlMgT0YgQU5ZIEtJTkQsIGVpdGhlciBleHByZXNzIG9yIGltcGxpZWQuXHJcbiMgU2VlIHRoZSBMaWNlbnNlIGZvciB0aGUgc3BlY2lmaWMgbGFuZ3VhZ2UgZ292ZXJuaW5nIHBlcm1pc3Npb25zIGFuZFxyXG4jIGxpbWl0YXRpb25zIHVuZGVyIHRoZSBMaWNlbnNlLlxyXG4jXHJcblxyXG5hbmd1bGFyLm1vZHVsZSgnZmxpbmtBcHAnKVxyXG5cclxuLmNvbnRyb2xsZXIgJ1J1bm5pbmdKb2JzQ29udHJvbGxlcicsICgkc2NvcGUsICRzdGF0ZSwgJHN0YXRlUGFyYW1zLCBKb2JzU2VydmljZSkgLT5cclxuICAkc2NvcGUuam9iT2JzZXJ2ZXIgPSAtPlxyXG4gICAgJHNjb3BlLmpvYnMgPSBKb2JzU2VydmljZS5nZXRKb2JzKCdydW5uaW5nJylcclxuXHJcbiAgSm9ic1NlcnZpY2UucmVnaXN0ZXJPYnNlcnZlcigkc2NvcGUuam9iT2JzZXJ2ZXIpXHJcbiAgJHNjb3BlLiRvbiAnJGRlc3Ryb3knLCAtPlxyXG4gICAgSm9ic1NlcnZpY2UudW5SZWdpc3Rlck9ic2VydmVyKCRzY29wZS5qb2JPYnNlcnZlcilcclxuXHJcbiAgJHNjb3BlLmpvYk9ic2VydmVyKClcclxuXHJcbiMgLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS1cclxuXHJcbi5jb250cm9sbGVyICdDb21wbGV0ZWRKb2JzQ29udHJvbGxlcicsICgkc2NvcGUsICRzdGF0ZSwgJHN0YXRlUGFyYW1zLCBKb2JzU2VydmljZSkgLT5cclxuICAkc2NvcGUuam9iT2JzZXJ2ZXIgPSAtPlxyXG4gICAgJHNjb3BlLmpvYnMgPSBKb2JzU2VydmljZS5nZXRKb2JzKCdmaW5pc2hlZCcpXHJcblxyXG4gIEpvYnNTZXJ2aWNlLnJlZ2lzdGVyT2JzZXJ2ZXIoJHNjb3BlLmpvYk9ic2VydmVyKVxyXG4gICRzY29wZS4kb24gJyRkZXN0cm95JywgLT5cclxuICAgIEpvYnNTZXJ2aWNlLnVuUmVnaXN0ZXJPYnNlcnZlcigkc2NvcGUuam9iT2JzZXJ2ZXIpXHJcblxyXG4gICRzY29wZS5qb2JPYnNlcnZlcigpXHJcblxyXG4jIC0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tXHJcblxyXG4uY29udHJvbGxlciAnU2luZ2xlSm9iQ29udHJvbGxlcicsICgkc2NvcGUsICRzdGF0ZSwgJHN0YXRlUGFyYW1zLCBKb2JzU2VydmljZSwgJHJvb3RTY29wZSwgZmxpbmtDb25maWcsICRpbnRlcnZhbCkgLT5cclxuICBjb25zb2xlLmxvZyAnU2luZ2xlSm9iQ29udHJvbGxlcidcclxuXHJcbiAgJHNjb3BlLmpvYmlkID0gJHN0YXRlUGFyYW1zLmpvYmlkXHJcbiAgJHNjb3BlLmpvYiA9IG51bGxcclxuICAkc2NvcGUucGxhbiA9IG51bGxcclxuICAkc2NvcGUudmVydGljZXMgPSBudWxsXHJcblxyXG4gIEpvYnNTZXJ2aWNlLmxvYWRKb2IoJHN0YXRlUGFyYW1zLmpvYmlkKS50aGVuIChkYXRhKSAtPlxyXG4gICAgJHNjb3BlLmpvYiA9IGRhdGFcclxuICAgICRzY29wZS5wbGFuID0gZGF0YS5wbGFuXHJcbiAgICAkc2NvcGUudmVydGljZXMgPSBkYXRhLnZlcnRpY2VzXHJcblxyXG4gIHJlZnJlc2hlciA9ICRpbnRlcnZhbCAtPlxyXG4gICAgSm9ic1NlcnZpY2UubG9hZEpvYigkc3RhdGVQYXJhbXMuam9iaWQpLnRoZW4gKGRhdGEpIC0+XHJcbiAgICAgICRzY29wZS5qb2IgPSBkYXRhXHJcblxyXG4gICAgICAkc2NvcGUuJGJyb2FkY2FzdCAncmVsb2FkJ1xyXG5cclxuICAsIGZsaW5rQ29uZmlnW1wicmVmcmVzaC1pbnRlcnZhbFwiXVxyXG5cclxuICAkc2NvcGUuJG9uICckZGVzdHJveScsIC0+XHJcbiAgICAkc2NvcGUuam9iID0gbnVsbFxyXG4gICAgJHNjb3BlLnBsYW4gPSBudWxsXHJcbiAgICAkc2NvcGUudmVydGljZXMgPSBudWxsXHJcblxyXG4gICAgJGludGVydmFsLmNhbmNlbChyZWZyZXNoZXIpXHJcblxyXG5cclxuIyAtLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLVxyXG5cclxuLmNvbnRyb2xsZXIgJ0pvYlBsYW5Db250cm9sbGVyJywgKCRzY29wZSwgJHN0YXRlLCAkc3RhdGVQYXJhbXMsIEpvYnNTZXJ2aWNlKSAtPlxyXG4gIGNvbnNvbGUubG9nICdKb2JQbGFuQ29udHJvbGxlcidcclxuXHJcbiAgJHNjb3BlLm5vZGVpZCA9IG51bGxcclxuICAkc2NvcGUubm9kZVVuZm9sZGVkID0gZmFsc2VcclxuICAkc2NvcGUuc3RhdGVMaXN0ID0gSm9ic1NlcnZpY2Uuc3RhdGVMaXN0KClcclxuXHJcbiAgJHNjb3BlLmNoYW5nZU5vZGUgPSAobm9kZWlkKSAtPlxyXG4gICAgaWYgbm9kZWlkICE9ICRzY29wZS5ub2RlaWRcclxuICAgICAgJHNjb3BlLm5vZGVpZCA9IG5vZGVpZFxyXG4gICAgICAkc2NvcGUudmVydGV4ID0gbnVsbFxyXG4gICAgICAkc2NvcGUuc3VidGFza3MgPSBudWxsXHJcbiAgICAgICRzY29wZS5hY2N1bXVsYXRvcnMgPSBudWxsXHJcblxyXG4gICAgICAkc2NvcGUuJGJyb2FkY2FzdCAncmVsb2FkJ1xyXG5cclxuICAgIGVsc2VcclxuICAgICAgJHNjb3BlLm5vZGVpZCA9IG51bGxcclxuICAgICAgJHNjb3BlLm5vZGVVbmZvbGRlZCA9IGZhbHNlXHJcbiAgICAgICRzY29wZS52ZXJ0ZXggPSBudWxsXHJcbiAgICAgICRzY29wZS5zdWJ0YXNrcyA9IG51bGxcclxuICAgICAgJHNjb3BlLmFjY3VtdWxhdG9ycyA9IG51bGxcclxuXHJcbiAgJHNjb3BlLmRlYWN0aXZhdGVOb2RlID0gLT5cclxuICAgICRzY29wZS5ub2RlaWQgPSBudWxsXHJcbiAgICAkc2NvcGUubm9kZVVuZm9sZGVkID0gZmFsc2VcclxuICAgICRzY29wZS52ZXJ0ZXggPSBudWxsXHJcbiAgICAkc2NvcGUuc3VidGFza3MgPSBudWxsXHJcbiAgICAkc2NvcGUuYWNjdW11bGF0b3JzID0gbnVsbFxyXG5cclxuICAkc2NvcGUudG9nZ2xlRm9sZCA9IC0+XHJcbiAgICAkc2NvcGUubm9kZVVuZm9sZGVkID0gISRzY29wZS5ub2RlVW5mb2xkZWRcclxuXHJcbiMgLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS1cclxuXHJcbi5jb250cm9sbGVyICdKb2JQbGFuT3ZlcnZpZXdDb250cm9sbGVyJywgKCRzY29wZSwgSm9ic1NlcnZpY2UpIC0+XHJcbiAgY29uc29sZS5sb2cgJ0pvYlBsYW5PdmVydmlld0NvbnRyb2xsZXInXHJcblxyXG4gIGlmICRzY29wZS5ub2RlaWQgYW5kICghJHNjb3BlLnZlcnRleCBvciAhJHNjb3BlLnZlcnRleC5zdClcclxuICAgIEpvYnNTZXJ2aWNlLmdldFN1YnRhc2tzKCRzY29wZS5ub2RlaWQpLnRoZW4gKGRhdGEpIC0+XHJcbiAgICAgICRzY29wZS5zdWJ0YXNrcyA9IGRhdGFcclxuXHJcbiAgJHNjb3BlLiRvbiAncmVsb2FkJywgKGV2ZW50KSAtPlxyXG4gICAgY29uc29sZS5sb2cgJ0pvYlBsYW5PdmVydmlld0NvbnRyb2xsZXInXHJcbiAgICBpZiAkc2NvcGUubm9kZWlkXHJcbiAgICAgIEpvYnNTZXJ2aWNlLmdldFN1YnRhc2tzKCRzY29wZS5ub2RlaWQpLnRoZW4gKGRhdGEpIC0+XHJcbiAgICAgICAgJHNjb3BlLnN1YnRhc2tzID0gZGF0YVxyXG5cclxuIyAtLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLVxyXG5cclxuLmNvbnRyb2xsZXIgJ0pvYlBsYW5BY2N1bXVsYXRvcnNDb250cm9sbGVyJywgKCRzY29wZSwgSm9ic1NlcnZpY2UpIC0+XHJcbiAgY29uc29sZS5sb2cgJ0pvYlBsYW5BY2N1bXVsYXRvcnNDb250cm9sbGVyJ1xyXG5cclxuICBpZiAkc2NvcGUubm9kZWlkIGFuZCAoISRzY29wZS52ZXJ0ZXggb3IgISRzY29wZS52ZXJ0ZXguYWNjdW11bGF0b3JzKVxyXG4gICAgSm9ic1NlcnZpY2UuZ2V0QWNjdW11bGF0b3JzKCRzY29wZS5ub2RlaWQpLnRoZW4gKGRhdGEpIC0+XHJcbiAgICAgICRzY29wZS5hY2N1bXVsYXRvcnMgPSBkYXRhLm1haW5cclxuICAgICAgJHNjb3BlLnN1YnRhc2tBY2N1bXVsYXRvcnMgPSBkYXRhLnN1YnRhc2tzXHJcblxyXG4gICRzY29wZS4kb24gJ3JlbG9hZCcsIChldmVudCkgLT5cclxuICAgIGNvbnNvbGUubG9nICdKb2JQbGFuQWNjdW11bGF0b3JzQ29udHJvbGxlcidcclxuICAgIGlmICRzY29wZS5ub2RlaWRcclxuICAgICAgSm9ic1NlcnZpY2UuZ2V0QWNjdW11bGF0b3JzKCRzY29wZS5ub2RlaWQpLnRoZW4gKGRhdGEpIC0+XHJcbiAgICAgICAgJHNjb3BlLmFjY3VtdWxhdG9ycyA9IGRhdGEubWFpblxyXG4gICAgICAgICRzY29wZS5zdWJ0YXNrQWNjdW11bGF0b3JzID0gZGF0YS5zdWJ0YXNrc1xyXG5cclxuIyAtLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLVxyXG5cclxuLmNvbnRyb2xsZXIgJ0pvYlRpbWVsaW5lVmVydGV4Q29udHJvbGxlcicsICgkc2NvcGUsICRzdGF0ZSwgJHN0YXRlUGFyYW1zLCBKb2JzU2VydmljZSkgLT5cclxuICBjb25zb2xlLmxvZyAnSm9iVGltZWxpbmVWZXJ0ZXhDb250cm9sbGVyJ1xyXG5cclxuICBKb2JzU2VydmljZS5nZXRWZXJ0ZXgoJHN0YXRlUGFyYW1zLnZlcnRleElkKS50aGVuIChkYXRhKSAtPlxyXG4gICAgJHNjb3BlLnZlcnRleCA9IGRhdGFcclxuXHJcbiAgJHNjb3BlLiRvbiAncmVsb2FkJywgKGV2ZW50KSAtPlxyXG4gICAgY29uc29sZS5sb2cgJ0pvYlRpbWVsaW5lVmVydGV4Q29udHJvbGxlcidcclxuICAgIEpvYnNTZXJ2aWNlLmdldFZlcnRleCgkc3RhdGVQYXJhbXMudmVydGV4SWQpLnRoZW4gKGRhdGEpIC0+XHJcbiAgICAgICRzY29wZS52ZXJ0ZXggPSBkYXRhXHJcblxyXG4jIC0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tXHJcblxyXG4uY29udHJvbGxlciAnSm9iRXhjZXB0aW9uc0NvbnRyb2xsZXInLCAoJHNjb3BlLCAkc3RhdGUsICRzdGF0ZVBhcmFtcywgSm9ic1NlcnZpY2UpIC0+XHJcbiAgSm9ic1NlcnZpY2UubG9hZEV4Y2VwdGlvbnMoKS50aGVuIChkYXRhKSAtPlxyXG4gICAgJHNjb3BlLmV4Y2VwdGlvbnMgPSBkYXRhXHJcblxyXG4jIC0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tXHJcblxyXG4uY29udHJvbGxlciAnSm9iUHJvcGVydGllc0NvbnRyb2xsZXInLCAoJHNjb3BlLCBKb2JzU2VydmljZSkgLT5cclxuICBjb25zb2xlLmxvZyAnSm9iUHJvcGVydGllc0NvbnRyb2xsZXInXHJcblxyXG4gICRzY29wZS5jaGFuZ2VOb2RlID0gKG5vZGVpZCkgLT5cclxuICAgIGlmIG5vZGVpZCAhPSAkc2NvcGUubm9kZWlkXHJcbiAgICAgICRzY29wZS5ub2RlaWQgPSBub2RlaWRcclxuXHJcbiAgICAgIEpvYnNTZXJ2aWNlLmdldE5vZGUobm9kZWlkKS50aGVuIChkYXRhKSAtPlxyXG4gICAgICAgICRzY29wZS5ub2RlID0gZGF0YVxyXG5cclxuICAgIGVsc2VcclxuICAgICAgJHNjb3BlLm5vZGVpZCA9IG51bGxcclxuICAgICAgJHNjb3BlLm5vZGUgPSBudWxsXHJcbiIsImFuZ3VsYXIubW9kdWxlKCdmbGlua0FwcCcpLmNvbnRyb2xsZXIoJ1J1bm5pbmdKb2JzQ29udHJvbGxlcicsIGZ1bmN0aW9uKCRzY29wZSwgJHN0YXRlLCAkc3RhdGVQYXJhbXMsIEpvYnNTZXJ2aWNlKSB7XG4gICRzY29wZS5qb2JPYnNlcnZlciA9IGZ1bmN0aW9uKCkge1xuICAgIHJldHVybiAkc2NvcGUuam9icyA9IEpvYnNTZXJ2aWNlLmdldEpvYnMoJ3J1bm5pbmcnKTtcbiAgfTtcbiAgSm9ic1NlcnZpY2UucmVnaXN0ZXJPYnNlcnZlcigkc2NvcGUuam9iT2JzZXJ2ZXIpO1xuICAkc2NvcGUuJG9uKCckZGVzdHJveScsIGZ1bmN0aW9uKCkge1xuICAgIHJldHVybiBKb2JzU2VydmljZS51blJlZ2lzdGVyT2JzZXJ2ZXIoJHNjb3BlLmpvYk9ic2VydmVyKTtcbiAgfSk7XG4gIHJldHVybiAkc2NvcGUuam9iT2JzZXJ2ZXIoKTtcbn0pLmNvbnRyb2xsZXIoJ0NvbXBsZXRlZEpvYnNDb250cm9sbGVyJywgZnVuY3Rpb24oJHNjb3BlLCAkc3RhdGUsICRzdGF0ZVBhcmFtcywgSm9ic1NlcnZpY2UpIHtcbiAgJHNjb3BlLmpvYk9ic2VydmVyID0gZnVuY3Rpb24oKSB7XG4gICAgcmV0dXJuICRzY29wZS5qb2JzID0gSm9ic1NlcnZpY2UuZ2V0Sm9icygnZmluaXNoZWQnKTtcbiAgfTtcbiAgSm9ic1NlcnZpY2UucmVnaXN0ZXJPYnNlcnZlcigkc2NvcGUuam9iT2JzZXJ2ZXIpO1xuICAkc2NvcGUuJG9uKCckZGVzdHJveScsIGZ1bmN0aW9uKCkge1xuICAgIHJldHVybiBKb2JzU2VydmljZS51blJlZ2lzdGVyT2JzZXJ2ZXIoJHNjb3BlLmpvYk9ic2VydmVyKTtcbiAgfSk7XG4gIHJldHVybiAkc2NvcGUuam9iT2JzZXJ2ZXIoKTtcbn0pLmNvbnRyb2xsZXIoJ1NpbmdsZUpvYkNvbnRyb2xsZXInLCBmdW5jdGlvbigkc2NvcGUsICRzdGF0ZSwgJHN0YXRlUGFyYW1zLCBKb2JzU2VydmljZSwgJHJvb3RTY29wZSwgZmxpbmtDb25maWcsICRpbnRlcnZhbCkge1xuICB2YXIgcmVmcmVzaGVyO1xuICBjb25zb2xlLmxvZygnU2luZ2xlSm9iQ29udHJvbGxlcicpO1xuICAkc2NvcGUuam9iaWQgPSAkc3RhdGVQYXJhbXMuam9iaWQ7XG4gICRzY29wZS5qb2IgPSBudWxsO1xuICAkc2NvcGUucGxhbiA9IG51bGw7XG4gICRzY29wZS52ZXJ0aWNlcyA9IG51bGw7XG4gIEpvYnNTZXJ2aWNlLmxvYWRKb2IoJHN0YXRlUGFyYW1zLmpvYmlkKS50aGVuKGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAkc2NvcGUuam9iID0gZGF0YTtcbiAgICAkc2NvcGUucGxhbiA9IGRhdGEucGxhbjtcbiAgICByZXR1cm4gJHNjb3BlLnZlcnRpY2VzID0gZGF0YS52ZXJ0aWNlcztcbiAgfSk7XG4gIHJlZnJlc2hlciA9ICRpbnRlcnZhbChmdW5jdGlvbigpIHtcbiAgICByZXR1cm4gSm9ic1NlcnZpY2UubG9hZEpvYigkc3RhdGVQYXJhbXMuam9iaWQpLnRoZW4oZnVuY3Rpb24oZGF0YSkge1xuICAgICAgJHNjb3BlLmpvYiA9IGRhdGE7XG4gICAgICByZXR1cm4gJHNjb3BlLiRicm9hZGNhc3QoJ3JlbG9hZCcpO1xuICAgIH0pO1xuICB9LCBmbGlua0NvbmZpZ1tcInJlZnJlc2gtaW50ZXJ2YWxcIl0pO1xuICByZXR1cm4gJHNjb3BlLiRvbignJGRlc3Ryb3knLCBmdW5jdGlvbigpIHtcbiAgICAkc2NvcGUuam9iID0gbnVsbDtcbiAgICAkc2NvcGUucGxhbiA9IG51bGw7XG4gICAgJHNjb3BlLnZlcnRpY2VzID0gbnVsbDtcbiAgICByZXR1cm4gJGludGVydmFsLmNhbmNlbChyZWZyZXNoZXIpO1xuICB9KTtcbn0pLmNvbnRyb2xsZXIoJ0pvYlBsYW5Db250cm9sbGVyJywgZnVuY3Rpb24oJHNjb3BlLCAkc3RhdGUsICRzdGF0ZVBhcmFtcywgSm9ic1NlcnZpY2UpIHtcbiAgY29uc29sZS5sb2coJ0pvYlBsYW5Db250cm9sbGVyJyk7XG4gICRzY29wZS5ub2RlaWQgPSBudWxsO1xuICAkc2NvcGUubm9kZVVuZm9sZGVkID0gZmFsc2U7XG4gICRzY29wZS5zdGF0ZUxpc3QgPSBKb2JzU2VydmljZS5zdGF0ZUxpc3QoKTtcbiAgJHNjb3BlLmNoYW5nZU5vZGUgPSBmdW5jdGlvbihub2RlaWQpIHtcbiAgICBpZiAobm9kZWlkICE9PSAkc2NvcGUubm9kZWlkKSB7XG4gICAgICAkc2NvcGUubm9kZWlkID0gbm9kZWlkO1xuICAgICAgJHNjb3BlLnZlcnRleCA9IG51bGw7XG4gICAgICAkc2NvcGUuc3VidGFza3MgPSBudWxsO1xuICAgICAgJHNjb3BlLmFjY3VtdWxhdG9ycyA9IG51bGw7XG4gICAgICByZXR1cm4gJHNjb3BlLiRicm9hZGNhc3QoJ3JlbG9hZCcpO1xuICAgIH0gZWxzZSB7XG4gICAgICAkc2NvcGUubm9kZWlkID0gbnVsbDtcbiAgICAgICRzY29wZS5ub2RlVW5mb2xkZWQgPSBmYWxzZTtcbiAgICAgICRzY29wZS52ZXJ0ZXggPSBudWxsO1xuICAgICAgJHNjb3BlLnN1YnRhc2tzID0gbnVsbDtcbiAgICAgIHJldHVybiAkc2NvcGUuYWNjdW11bGF0b3JzID0gbnVsbDtcbiAgICB9XG4gIH07XG4gICRzY29wZS5kZWFjdGl2YXRlTm9kZSA9IGZ1bmN0aW9uKCkge1xuICAgICRzY29wZS5ub2RlaWQgPSBudWxsO1xuICAgICRzY29wZS5ub2RlVW5mb2xkZWQgPSBmYWxzZTtcbiAgICAkc2NvcGUudmVydGV4ID0gbnVsbDtcbiAgICAkc2NvcGUuc3VidGFza3MgPSBudWxsO1xuICAgIHJldHVybiAkc2NvcGUuYWNjdW11bGF0b3JzID0gbnVsbDtcbiAgfTtcbiAgcmV0dXJuICRzY29wZS50b2dnbGVGb2xkID0gZnVuY3Rpb24oKSB7XG4gICAgcmV0dXJuICRzY29wZS5ub2RlVW5mb2xkZWQgPSAhJHNjb3BlLm5vZGVVbmZvbGRlZDtcbiAgfTtcbn0pLmNvbnRyb2xsZXIoJ0pvYlBsYW5PdmVydmlld0NvbnRyb2xsZXInLCBmdW5jdGlvbigkc2NvcGUsIEpvYnNTZXJ2aWNlKSB7XG4gIGNvbnNvbGUubG9nKCdKb2JQbGFuT3ZlcnZpZXdDb250cm9sbGVyJyk7XG4gIGlmICgkc2NvcGUubm9kZWlkICYmICghJHNjb3BlLnZlcnRleCB8fCAhJHNjb3BlLnZlcnRleC5zdCkpIHtcbiAgICBKb2JzU2VydmljZS5nZXRTdWJ0YXNrcygkc2NvcGUubm9kZWlkKS50aGVuKGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgIHJldHVybiAkc2NvcGUuc3VidGFza3MgPSBkYXRhO1xuICAgIH0pO1xuICB9XG4gIHJldHVybiAkc2NvcGUuJG9uKCdyZWxvYWQnLCBmdW5jdGlvbihldmVudCkge1xuICAgIGNvbnNvbGUubG9nKCdKb2JQbGFuT3ZlcnZpZXdDb250cm9sbGVyJyk7XG4gICAgaWYgKCRzY29wZS5ub2RlaWQpIHtcbiAgICAgIHJldHVybiBKb2JzU2VydmljZS5nZXRTdWJ0YXNrcygkc2NvcGUubm9kZWlkKS50aGVuKGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgICAgcmV0dXJuICRzY29wZS5zdWJ0YXNrcyA9IGRhdGE7XG4gICAgICB9KTtcbiAgICB9XG4gIH0pO1xufSkuY29udHJvbGxlcignSm9iUGxhbkFjY3VtdWxhdG9yc0NvbnRyb2xsZXInLCBmdW5jdGlvbigkc2NvcGUsIEpvYnNTZXJ2aWNlKSB7XG4gIGNvbnNvbGUubG9nKCdKb2JQbGFuQWNjdW11bGF0b3JzQ29udHJvbGxlcicpO1xuICBpZiAoJHNjb3BlLm5vZGVpZCAmJiAoISRzY29wZS52ZXJ0ZXggfHwgISRzY29wZS52ZXJ0ZXguYWNjdW11bGF0b3JzKSkge1xuICAgIEpvYnNTZXJ2aWNlLmdldEFjY3VtdWxhdG9ycygkc2NvcGUubm9kZWlkKS50aGVuKGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgICRzY29wZS5hY2N1bXVsYXRvcnMgPSBkYXRhLm1haW47XG4gICAgICByZXR1cm4gJHNjb3BlLnN1YnRhc2tBY2N1bXVsYXRvcnMgPSBkYXRhLnN1YnRhc2tzO1xuICAgIH0pO1xuICB9XG4gIHJldHVybiAkc2NvcGUuJG9uKCdyZWxvYWQnLCBmdW5jdGlvbihldmVudCkge1xuICAgIGNvbnNvbGUubG9nKCdKb2JQbGFuQWNjdW11bGF0b3JzQ29udHJvbGxlcicpO1xuICAgIGlmICgkc2NvcGUubm9kZWlkKSB7XG4gICAgICByZXR1cm4gSm9ic1NlcnZpY2UuZ2V0QWNjdW11bGF0b3JzKCRzY29wZS5ub2RlaWQpLnRoZW4oZnVuY3Rpb24oZGF0YSkge1xuICAgICAgICAkc2NvcGUuYWNjdW11bGF0b3JzID0gZGF0YS5tYWluO1xuICAgICAgICByZXR1cm4gJHNjb3BlLnN1YnRhc2tBY2N1bXVsYXRvcnMgPSBkYXRhLnN1YnRhc2tzO1xuICAgICAgfSk7XG4gICAgfVxuICB9KTtcbn0pLmNvbnRyb2xsZXIoJ0pvYlRpbWVsaW5lVmVydGV4Q29udHJvbGxlcicsIGZ1bmN0aW9uKCRzY29wZSwgJHN0YXRlLCAkc3RhdGVQYXJhbXMsIEpvYnNTZXJ2aWNlKSB7XG4gIGNvbnNvbGUubG9nKCdKb2JUaW1lbGluZVZlcnRleENvbnRyb2xsZXInKTtcbiAgSm9ic1NlcnZpY2UuZ2V0VmVydGV4KCRzdGF0ZVBhcmFtcy52ZXJ0ZXhJZCkudGhlbihmdW5jdGlvbihkYXRhKSB7XG4gICAgcmV0dXJuICRzY29wZS52ZXJ0ZXggPSBkYXRhO1xuICB9KTtcbiAgcmV0dXJuICRzY29wZS4kb24oJ3JlbG9hZCcsIGZ1bmN0aW9uKGV2ZW50KSB7XG4gICAgY29uc29sZS5sb2coJ0pvYlRpbWVsaW5lVmVydGV4Q29udHJvbGxlcicpO1xuICAgIHJldHVybiBKb2JzU2VydmljZS5nZXRWZXJ0ZXgoJHN0YXRlUGFyYW1zLnZlcnRleElkKS50aGVuKGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgIHJldHVybiAkc2NvcGUudmVydGV4ID0gZGF0YTtcbiAgICB9KTtcbiAgfSk7XG59KS5jb250cm9sbGVyKCdKb2JFeGNlcHRpb25zQ29udHJvbGxlcicsIGZ1bmN0aW9uKCRzY29wZSwgJHN0YXRlLCAkc3RhdGVQYXJhbXMsIEpvYnNTZXJ2aWNlKSB7XG4gIHJldHVybiBKb2JzU2VydmljZS5sb2FkRXhjZXB0aW9ucygpLnRoZW4oZnVuY3Rpb24oZGF0YSkge1xuICAgIHJldHVybiAkc2NvcGUuZXhjZXB0aW9ucyA9IGRhdGE7XG4gIH0pO1xufSkuY29udHJvbGxlcignSm9iUHJvcGVydGllc0NvbnRyb2xsZXInLCBmdW5jdGlvbigkc2NvcGUsIEpvYnNTZXJ2aWNlKSB7XG4gIGNvbnNvbGUubG9nKCdKb2JQcm9wZXJ0aWVzQ29udHJvbGxlcicpO1xuICByZXR1cm4gJHNjb3BlLmNoYW5nZU5vZGUgPSBmdW5jdGlvbihub2RlaWQpIHtcbiAgICBpZiAobm9kZWlkICE9PSAkc2NvcGUubm9kZWlkKSB7XG4gICAgICAkc2NvcGUubm9kZWlkID0gbm9kZWlkO1xuICAgICAgcmV0dXJuIEpvYnNTZXJ2aWNlLmdldE5vZGUobm9kZWlkKS50aGVuKGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgICAgcmV0dXJuICRzY29wZS5ub2RlID0gZGF0YTtcbiAgICAgIH0pO1xuICAgIH0gZWxzZSB7XG4gICAgICAkc2NvcGUubm9kZWlkID0gbnVsbDtcbiAgICAgIHJldHVybiAkc2NvcGUubm9kZSA9IG51bGw7XG4gICAgfVxuICB9O1xufSk7XG4iLCIjXHJcbiMgTGljZW5zZWQgdG8gdGhlIEFwYWNoZSBTb2Z0d2FyZSBGb3VuZGF0aW9uIChBU0YpIHVuZGVyIG9uZVxyXG4jIG9yIG1vcmUgY29udHJpYnV0b3IgbGljZW5zZSBhZ3JlZW1lbnRzLiAgU2VlIHRoZSBOT1RJQ0UgZmlsZVxyXG4jIGRpc3RyaWJ1dGVkIHdpdGggdGhpcyB3b3JrIGZvciBhZGRpdGlvbmFsIGluZm9ybWF0aW9uXHJcbiMgcmVnYXJkaW5nIGNvcHlyaWdodCBvd25lcnNoaXAuICBUaGUgQVNGIGxpY2Vuc2VzIHRoaXMgZmlsZVxyXG4jIHRvIHlvdSB1bmRlciB0aGUgQXBhY2hlIExpY2Vuc2UsIFZlcnNpb24gMi4wICh0aGVcclxuIyBcIkxpY2Vuc2VcIik7IHlvdSBtYXkgbm90IHVzZSB0aGlzIGZpbGUgZXhjZXB0IGluIGNvbXBsaWFuY2VcclxuIyB3aXRoIHRoZSBMaWNlbnNlLiAgWW91IG1heSBvYnRhaW4gYSBjb3B5IG9mIHRoZSBMaWNlbnNlIGF0XHJcbiNcclxuIyAgICAgaHR0cDovL3d3dy5hcGFjaGUub3JnL2xpY2Vuc2VzL0xJQ0VOU0UtMi4wXHJcbiNcclxuIyBVbmxlc3MgcmVxdWlyZWQgYnkgYXBwbGljYWJsZSBsYXcgb3IgYWdyZWVkIHRvIGluIHdyaXRpbmcsIHNvZnR3YXJlXHJcbiMgZGlzdHJpYnV0ZWQgdW5kZXIgdGhlIExpY2Vuc2UgaXMgZGlzdHJpYnV0ZWQgb24gYW4gXCJBUyBJU1wiIEJBU0lTLFxyXG4jIFdJVEhPVVQgV0FSUkFOVElFUyBPUiBDT05ESVRJT05TIE9GIEFOWSBLSU5ELCBlaXRoZXIgZXhwcmVzcyBvciBpbXBsaWVkLlxyXG4jIFNlZSB0aGUgTGljZW5zZSBmb3IgdGhlIHNwZWNpZmljIGxhbmd1YWdlIGdvdmVybmluZyBwZXJtaXNzaW9ucyBhbmRcclxuIyBsaW1pdGF0aW9ucyB1bmRlciB0aGUgTGljZW5zZS5cclxuI1xyXG5cclxuYW5ndWxhci5tb2R1bGUoJ2ZsaW5rQXBwJylcclxuXHJcbiMgLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLVxyXG5cclxuLmRpcmVjdGl2ZSAndmVydGV4JywgKCRzdGF0ZSkgLT5cclxuICB0ZW1wbGF0ZTogXCI8c3ZnIGNsYXNzPSd0aW1lbGluZSBzZWNvbmRhcnknIHdpZHRoPScwJyBoZWlnaHQ9JzAnPjwvc3ZnPlwiXHJcblxyXG4gIHNjb3BlOlxyXG4gICAgZGF0YTogXCI9XCJcclxuXHJcbiAgbGluazogKHNjb3BlLCBlbGVtLCBhdHRycykgLT5cclxuICAgIHN2Z0VsID0gZWxlbS5jaGlsZHJlbigpWzBdXHJcblxyXG4gICAgY29udGFpbmVyVyA9IGVsZW0ud2lkdGgoKVxyXG4gICAgYW5ndWxhci5lbGVtZW50KHN2Z0VsKS5hdHRyKCd3aWR0aCcsIGNvbnRhaW5lclcpXHJcblxyXG4gICAgYW5hbHl6ZVRpbWUgPSAoZGF0YSkgLT5cclxuICAgICAgZDMuc2VsZWN0KHN2Z0VsKS5zZWxlY3RBbGwoXCIqXCIpLnJlbW92ZSgpXHJcblxyXG4gICAgICB0ZXN0RGF0YSA9IFtdXHJcblxyXG4gICAgICBhbmd1bGFyLmZvckVhY2ggZGF0YS5zdWJ0YXNrcywgKHN1YnRhc2ssIGkpIC0+XHJcbiAgICAgICAgdGltZXMgPSBbXHJcbiAgICAgICAgICB7XHJcbiAgICAgICAgICAgIGxhYmVsOiBcIlNjaGVkdWxlZFwiXHJcbiAgICAgICAgICAgIGNvbG9yOiBcIiM2NjZcIlxyXG4gICAgICAgICAgICBib3JkZXJDb2xvcjogXCIjNTU1XCJcclxuICAgICAgICAgICAgc3RhcnRpbmdfdGltZTogc3VidGFzay50aW1lc3RhbXBzW1wiU0NIRURVTEVEXCJdXHJcbiAgICAgICAgICAgIGVuZGluZ190aW1lOiBzdWJ0YXNrLnRpbWVzdGFtcHNbXCJERVBMT1lJTkdcIl1cclxuICAgICAgICAgICAgdHlwZTogJ3JlZ3VsYXInXHJcbiAgICAgICAgICB9XHJcbiAgICAgICAgICB7XHJcbiAgICAgICAgICAgIGxhYmVsOiBcIkRlcGxveWluZ1wiXHJcbiAgICAgICAgICAgIGNvbG9yOiBcIiNhYWFcIlxyXG4gICAgICAgICAgICBib3JkZXJDb2xvcjogXCIjNTU1XCJcclxuICAgICAgICAgICAgc3RhcnRpbmdfdGltZTogc3VidGFzay50aW1lc3RhbXBzW1wiREVQTE9ZSU5HXCJdXHJcbiAgICAgICAgICAgIGVuZGluZ190aW1lOiBzdWJ0YXNrLnRpbWVzdGFtcHNbXCJSVU5OSU5HXCJdXHJcbiAgICAgICAgICAgIHR5cGU6ICdyZWd1bGFyJ1xyXG4gICAgICAgICAgfVxyXG4gICAgICAgIF1cclxuXHJcbiAgICAgICAgaWYgc3VidGFzay50aW1lc3RhbXBzW1wiRklOSVNIRURcIl0gPiAwXHJcbiAgICAgICAgICB0aW1lcy5wdXNoIHtcclxuICAgICAgICAgICAgbGFiZWw6IFwiUnVubmluZ1wiXHJcbiAgICAgICAgICAgIGNvbG9yOiBcIiNkZGRcIlxyXG4gICAgICAgICAgICBib3JkZXJDb2xvcjogXCIjNTU1XCJcclxuICAgICAgICAgICAgc3RhcnRpbmdfdGltZTogc3VidGFzay50aW1lc3RhbXBzW1wiUlVOTklOR1wiXVxyXG4gICAgICAgICAgICBlbmRpbmdfdGltZTogc3VidGFzay50aW1lc3RhbXBzW1wiRklOSVNIRURcIl1cclxuICAgICAgICAgICAgdHlwZTogJ3JlZ3VsYXInXHJcbiAgICAgICAgICB9XHJcblxyXG4gICAgICAgIHRlc3REYXRhLnB1c2gge1xyXG4gICAgICAgICAgbGFiZWw6IFwiKCN7c3VidGFzay5zdWJ0YXNrfSkgI3tzdWJ0YXNrLmhvc3R9XCJcclxuICAgICAgICAgIHRpbWVzOiB0aW1lc1xyXG4gICAgICAgIH1cclxuXHJcbiAgICAgIGNoYXJ0ID0gZDMudGltZWxpbmUoKS5zdGFjaygpXHJcbiAgICAgIC50aWNrRm9ybWF0KHtcclxuICAgICAgICBmb3JtYXQ6IGQzLnRpbWUuZm9ybWF0KFwiJUxcIilcclxuICAgICAgICAjIHRpY2tJbnRlcnZhbDogMVxyXG4gICAgICAgIHRpY2tTaXplOiAxXHJcbiAgICAgIH0pXHJcbiAgICAgIC5wcmVmaXgoXCJzaW5nbGVcIilcclxuICAgICAgLmxhYmVsRm9ybWF0KChsYWJlbCkgLT5cclxuICAgICAgICBsYWJlbFxyXG4gICAgICApXHJcbiAgICAgIC5tYXJnaW4oeyBsZWZ0OiAxMDAsIHJpZ2h0OiAwLCB0b3A6IDAsIGJvdHRvbTogMCB9KVxyXG4gICAgICAuaXRlbUhlaWdodCgzMClcclxuICAgICAgLnJlbGF0aXZlVGltZSgpXHJcblxyXG4gICAgICBzdmcgPSBkMy5zZWxlY3Qoc3ZnRWwpXHJcbiAgICAgIC5kYXR1bSh0ZXN0RGF0YSlcclxuICAgICAgLmNhbGwoY2hhcnQpXHJcblxyXG4gICAgYW5hbHl6ZVRpbWUoc2NvcGUuZGF0YSlcclxuXHJcbiAgICByZXR1cm5cclxuXHJcbiMgLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLVxyXG5cclxuLmRpcmVjdGl2ZSAndGltZWxpbmUnLCAoJHN0YXRlKSAtPlxyXG4gIHRlbXBsYXRlOiBcIjxzdmcgY2xhc3M9J3RpbWVsaW5lJyB3aWR0aD0nMCcgaGVpZ2h0PScwJz48L3N2Zz5cIlxyXG5cclxuICBzY29wZTpcclxuICAgIHZlcnRpY2VzOiBcIj1cIlxyXG4gICAgam9iaWQ6IFwiPVwiXHJcblxyXG4gIGxpbms6IChzY29wZSwgZWxlbSwgYXR0cnMpIC0+XHJcbiAgICBzdmdFbCA9IGVsZW0uY2hpbGRyZW4oKVswXVxyXG5cclxuICAgIGNvbnRhaW5lclcgPSBlbGVtLndpZHRoKClcclxuICAgIGFuZ3VsYXIuZWxlbWVudChzdmdFbCkuYXR0cignd2lkdGgnLCBjb250YWluZXJXKVxyXG5cclxuICAgIHRyYW5zbGF0ZUxhYmVsID0gKGxhYmVsKSAtPlxyXG4gICAgICBsYWJlbC5yZXBsYWNlKFwiJmd0O1wiLCBcIj5cIilcclxuXHJcbiAgICBhbmFseXplVGltZSA9IChkYXRhKSAtPlxyXG4gICAgICBkMy5zZWxlY3Qoc3ZnRWwpLnNlbGVjdEFsbChcIipcIikucmVtb3ZlKClcclxuXHJcbiAgICAgIHRlc3REYXRhID0gW11cclxuXHJcbiAgICAgIGFuZ3VsYXIuZm9yRWFjaCBkYXRhLCAodmVydGV4KSAtPlxyXG4gICAgICAgIGlmIHZlcnRleFsnc3RhcnQtdGltZSddID4gLTFcclxuICAgICAgICAgIGlmIHZlcnRleC50eXBlIGlzICdzY2hlZHVsZWQnXHJcbiAgICAgICAgICAgIHRlc3REYXRhLnB1c2ggXHJcbiAgICAgICAgICAgICAgdGltZXM6IFtcclxuICAgICAgICAgICAgICAgIGxhYmVsOiB0cmFuc2xhdGVMYWJlbCh2ZXJ0ZXgubmFtZSlcclxuICAgICAgICAgICAgICAgIGNvbG9yOiBcIiNjY2NjY2NcIlxyXG4gICAgICAgICAgICAgICAgYm9yZGVyQ29sb3I6IFwiIzU1NTU1NVwiXHJcbiAgICAgICAgICAgICAgICBzdGFydGluZ190aW1lOiB2ZXJ0ZXhbJ3N0YXJ0LXRpbWUnXVxyXG4gICAgICAgICAgICAgICAgZW5kaW5nX3RpbWU6IHZlcnRleFsnZW5kLXRpbWUnXVxyXG4gICAgICAgICAgICAgICAgdHlwZTogdmVydGV4LnR5cGVcclxuICAgICAgICAgICAgICBdXHJcbiAgICAgICAgICBlbHNlXHJcbiAgICAgICAgICAgIHRlc3REYXRhLnB1c2ggXHJcbiAgICAgICAgICAgICAgdGltZXM6IFtcclxuICAgICAgICAgICAgICAgIGxhYmVsOiB0cmFuc2xhdGVMYWJlbCh2ZXJ0ZXgubmFtZSlcclxuICAgICAgICAgICAgICAgIGNvbG9yOiBcIiNkOWYxZjdcIlxyXG4gICAgICAgICAgICAgICAgYm9yZGVyQ29sb3I6IFwiIzYyY2RlYVwiXHJcbiAgICAgICAgICAgICAgICBzdGFydGluZ190aW1lOiB2ZXJ0ZXhbJ3N0YXJ0LXRpbWUnXVxyXG4gICAgICAgICAgICAgICAgZW5kaW5nX3RpbWU6IHZlcnRleFsnZW5kLXRpbWUnXVxyXG4gICAgICAgICAgICAgICAgbGluazogdmVydGV4LmlkXHJcbiAgICAgICAgICAgICAgICB0eXBlOiB2ZXJ0ZXgudHlwZVxyXG4gICAgICAgICAgICAgIF1cclxuXHJcbiAgICAgIGNoYXJ0ID0gZDMudGltZWxpbmUoKS5zdGFjaygpLmNsaWNrKChkLCBpLCBkYXR1bSkgLT5cclxuICAgICAgICBpZiBkLmxpbmtcclxuICAgICAgICAgICRzdGF0ZS5nbyBcInNpbmdsZS1qb2IudGltZWxpbmUudmVydGV4XCIsIHsgam9iaWQ6IHNjb3BlLmpvYmlkLCB2ZXJ0ZXhJZDogZC5saW5rIH1cclxuXHJcbiAgICAgIClcclxuICAgICAgLnRpY2tGb3JtYXQoe1xyXG4gICAgICAgIGZvcm1hdDogZDMudGltZS5mb3JtYXQoXCIlTFwiKVxyXG4gICAgICAgICMgdGlja1RpbWU6IGQzLnRpbWUuc2Vjb25kXHJcbiAgICAgICAgIyB0aWNrSW50ZXJ2YWw6IDAuNVxyXG4gICAgICAgIHRpY2tTaXplOiAxXHJcbiAgICAgIH0pXHJcbiAgICAgIC5wcmVmaXgoXCJtYWluXCIpXHJcbiAgICAgIC5tYXJnaW4oeyBsZWZ0OiAwLCByaWdodDogMCwgdG9wOiAwLCBib3R0b206IDAgfSlcclxuICAgICAgLml0ZW1IZWlnaHQoMzApXHJcbiAgICAgIC5zaG93Qm9yZGVyTGluZSgpXHJcbiAgICAgIC5zaG93SG91clRpbWVsaW5lKClcclxuXHJcbiAgICAgIHN2ZyA9IGQzLnNlbGVjdChzdmdFbClcclxuICAgICAgLmRhdHVtKHRlc3REYXRhKVxyXG4gICAgICAuY2FsbChjaGFydClcclxuXHJcbiAgICBzY29wZS4kd2F0Y2ggYXR0cnMudmVydGljZXMsIChkYXRhKSAtPlxyXG4gICAgICBhbmFseXplVGltZShkYXRhKSBpZiBkYXRhXHJcblxyXG4gICAgcmV0dXJuXHJcblxyXG4jIC0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS1cclxuXHJcbi5kaXJlY3RpdmUgJ2pvYlBsYW4nLCAoJHRpbWVvdXQpIC0+XHJcbiAgdGVtcGxhdGU6IFwiXHJcbiAgICA8c3ZnIGNsYXNzPSdncmFwaCcgd2lkdGg9JzUwMCcgaGVpZ2h0PSc0MDAnPjxnIC8+PC9zdmc+XHJcbiAgICA8c3ZnIGNsYXNzPSd0bXAnIHdpZHRoPScxJyBoZWlnaHQ9JzEnPjxnIC8+PC9zdmc+XHJcbiAgICA8ZGl2IGNsYXNzPSdidG4tZ3JvdXAgem9vbS1idXR0b25zJz5cclxuICAgICAgPGEgY2xhc3M9J2J0biBidG4tZGVmYXVsdCB6b29tLWluJyBuZy1jbGljaz0nem9vbUluKCknPjxpIGNsYXNzPSdmYSBmYS1wbHVzJyAvPjwvYT5cclxuICAgICAgPGEgY2xhc3M9J2J0biBidG4tZGVmYXVsdCB6b29tLW91dCcgbmctY2xpY2s9J3pvb21PdXQoKSc+PGkgY2xhc3M9J2ZhIGZhLW1pbnVzJyAvPjwvYT5cclxuICAgIDwvZGl2PlwiXHJcblxyXG4gIHNjb3BlOlxyXG4gICAgcGxhbjogJz0nXHJcbiAgICBzZXROb2RlOiAnJidcclxuXHJcbiAgbGluazogKHNjb3BlLCBlbGVtLCBhdHRycykgLT5cclxuICAgIGcgPSBudWxsXHJcbiAgICBtYWluWm9vbSA9IGQzLmJlaGF2aW9yLnpvb20oKVxyXG4gICAgc3ViZ3JhcGhzID0gW11cclxuICAgIGpvYmlkID0gYXR0cnMuam9iaWRcclxuXHJcbiAgICBtYWluU3ZnRWxlbWVudCA9IGVsZW0uY2hpbGRyZW4oKVswXVxyXG4gICAgbWFpbkcgPSBlbGVtLmNoaWxkcmVuKCkuY2hpbGRyZW4oKVswXVxyXG4gICAgbWFpblRtcEVsZW1lbnQgPSBlbGVtLmNoaWxkcmVuKClbMV1cclxuXHJcbiAgICBkM21haW5TdmcgPSBkMy5zZWxlY3QobWFpblN2Z0VsZW1lbnQpXHJcbiAgICBkM21haW5TdmdHID0gZDMuc2VsZWN0KG1haW5HKVxyXG4gICAgZDN0bXBTdmcgPSBkMy5zZWxlY3QobWFpblRtcEVsZW1lbnQpXHJcblxyXG4gICAgIyBhbmd1bGFyLmVsZW1lbnQobWFpbkcpLmVtcHR5KClcclxuICAgICMgZDNtYWluU3ZnRy5zZWxlY3RBbGwoXCIqXCIpLnJlbW92ZSgpXHJcblxyXG4gICAgY29udGFpbmVyVyA9IGVsZW0ud2lkdGgoKVxyXG4gICAgYW5ndWxhci5lbGVtZW50KGVsZW0uY2hpbGRyZW4oKVswXSkud2lkdGgoY29udGFpbmVyVylcclxuXHJcbiAgICBzY29wZS56b29tSW4gPSAtPlxyXG4gICAgICBpZiBtYWluWm9vbS5zY2FsZSgpIDwgMi45OVxyXG4gICAgICAgIFxyXG4gICAgICAgICMgQ2FsY3VsYXRlIGFuZCBzdG9yZSBuZXcgdmFsdWVzIGluIHpvb20gb2JqZWN0XHJcbiAgICAgICAgdHJhbnNsYXRlID0gbWFpblpvb20udHJhbnNsYXRlKClcclxuICAgICAgICB2MSA9IHRyYW5zbGF0ZVswXSAqIChtYWluWm9vbS5zY2FsZSgpICsgMC4xIC8gKG1haW5ab29tLnNjYWxlKCkpKVxyXG4gICAgICAgIHYyID0gdHJhbnNsYXRlWzFdICogKG1haW5ab29tLnNjYWxlKCkgKyAwLjEgLyAobWFpblpvb20uc2NhbGUoKSkpXHJcbiAgICAgICAgbWFpblpvb20uc2NhbGUgbWFpblpvb20uc2NhbGUoKSArIDAuMVxyXG4gICAgICAgIG1haW5ab29tLnRyYW5zbGF0ZSBbIHYxLCB2MiBdXHJcbiAgICAgICAgXHJcbiAgICAgICAgIyBUcmFuc2Zvcm0gc3ZnXHJcbiAgICAgICAgZDNtYWluU3ZnRy5hdHRyIFwidHJhbnNmb3JtXCIsIFwidHJhbnNsYXRlKFwiICsgdjEgKyBcIixcIiArIHYyICsgXCIpIHNjYWxlKFwiICsgbWFpblpvb20uc2NhbGUoKSArIFwiKVwiXHJcblxyXG4gICAgc2NvcGUuem9vbU91dCA9IC0+XHJcbiAgICAgIGlmIG1haW5ab29tLnNjYWxlKCkgPiAwLjMxXHJcbiAgICAgICAgXHJcbiAgICAgICAgIyBDYWxjdWxhdGUgYW5kIHN0b3JlIG5ldyB2YWx1ZXMgaW4gbWFpblpvb20gb2JqZWN0XHJcbiAgICAgICAgbWFpblpvb20uc2NhbGUgbWFpblpvb20uc2NhbGUoKSAtIDAuMVxyXG4gICAgICAgIHRyYW5zbGF0ZSA9IG1haW5ab29tLnRyYW5zbGF0ZSgpXHJcbiAgICAgICAgdjEgPSB0cmFuc2xhdGVbMF0gKiAobWFpblpvb20uc2NhbGUoKSAtIDAuMSAvIChtYWluWm9vbS5zY2FsZSgpKSlcclxuICAgICAgICB2MiA9IHRyYW5zbGF0ZVsxXSAqIChtYWluWm9vbS5zY2FsZSgpIC0gMC4xIC8gKG1haW5ab29tLnNjYWxlKCkpKVxyXG4gICAgICAgIG1haW5ab29tLnRyYW5zbGF0ZSBbIHYxLCB2MiBdXHJcbiAgICAgICAgXHJcbiAgICAgICAgIyBUcmFuc2Zvcm0gc3ZnXHJcbiAgICAgICAgZDNtYWluU3ZnRy5hdHRyIFwidHJhbnNmb3JtXCIsIFwidHJhbnNsYXRlKFwiICsgdjEgKyBcIixcIiArIHYyICsgXCIpIHNjYWxlKFwiICsgbWFpblpvb20uc2NhbGUoKSArIFwiKVwiXHJcblxyXG4gICAgI2NyZWF0ZSBhIGxhYmVsIG9mIGFuIGVkZ2VcclxuICAgIGNyZWF0ZUxhYmVsRWRnZSA9IChlbCkgLT5cclxuICAgICAgbGFiZWxWYWx1ZSA9IFwiXCJcclxuICAgICAgaWYgZWwuc2hpcF9zdHJhdGVneT8gb3IgZWwubG9jYWxfc3RyYXRlZ3k/XHJcbiAgICAgICAgbGFiZWxWYWx1ZSArPSBcIjxkaXYgY2xhc3M9J2VkZ2UtbGFiZWwnPlwiXHJcbiAgICAgICAgbGFiZWxWYWx1ZSArPSBlbC5zaGlwX3N0cmF0ZWd5ICBpZiBlbC5zaGlwX3N0cmF0ZWd5P1xyXG4gICAgICAgIGxhYmVsVmFsdWUgKz0gXCIgKFwiICsgZWwudGVtcF9tb2RlICsgXCIpXCIgIHVubGVzcyBlbC50ZW1wX21vZGUgaXMgYHVuZGVmaW5lZGBcclxuICAgICAgICBsYWJlbFZhbHVlICs9IFwiLDxicj5cIiArIGVsLmxvY2FsX3N0cmF0ZWd5ICB1bmxlc3MgZWwubG9jYWxfc3RyYXRlZ3kgaXMgYHVuZGVmaW5lZGBcclxuICAgICAgICBsYWJlbFZhbHVlICs9IFwiPC9kaXY+XCJcclxuICAgICAgbGFiZWxWYWx1ZVxyXG5cclxuXHJcbiAgICAjIHRydWUsIGlmIHRoZSBub2RlIGlzIGEgc3BlY2lhbCBub2RlIGZyb20gYW4gaXRlcmF0aW9uXHJcbiAgICBpc1NwZWNpYWxJdGVyYXRpb25Ob2RlID0gKGluZm8pIC0+XHJcbiAgICAgIChpbmZvIGlzIFwicGFydGlhbFNvbHV0aW9uXCIgb3IgaW5mbyBpcyBcIm5leHRQYXJ0aWFsU29sdXRpb25cIiBvciBpbmZvIGlzIFwid29ya3NldFwiIG9yIGluZm8gaXMgXCJuZXh0V29ya3NldFwiIG9yIGluZm8gaXMgXCJzb2x1dGlvblNldFwiIG9yIGluZm8gaXMgXCJzb2x1dGlvbkRlbHRhXCIpXHJcblxyXG4gICAgZ2V0Tm9kZVR5cGUgPSAoZWwsIGluZm8pIC0+XHJcbiAgICAgIGlmIGluZm8gaXMgXCJtaXJyb3JcIlxyXG4gICAgICAgICdub2RlLW1pcnJvcidcclxuXHJcbiAgICAgIGVsc2UgaWYgaXNTcGVjaWFsSXRlcmF0aW9uTm9kZShpbmZvKVxyXG4gICAgICAgICdub2RlLWl0ZXJhdGlvbidcclxuXHJcbiAgICAgIGVsc2VcclxuICAgICAgICAgICdub2RlLW5vcm1hbCdcclxuICAgICAgXHJcbiAgICAjIGNyZWF0ZXMgdGhlIGxhYmVsIG9mIGEgbm9kZSwgaW4gaW5mbyBpcyBzdG9yZWQsIHdoZXRoZXIgaXQgaXMgYSBzcGVjaWFsIG5vZGUgKGxpa2UgYSBtaXJyb3IgaW4gYW4gaXRlcmF0aW9uKVxyXG4gICAgY3JlYXRlTGFiZWxOb2RlID0gKGVsLCBpbmZvLCBtYXhXLCBtYXhIKSAtPlxyXG4gICAgICAjIGxhYmVsVmFsdWUgPSBcIjxhIGhyZWY9JyMvam9icy9cIiArIGpvYmlkICsgXCIvdmVydGV4L1wiICsgZWwuaWQgKyBcIicgY2xhc3M9J25vZGUtbGFiZWwgXCIgKyBnZXROb2RlVHlwZShlbCwgaW5mbykgKyBcIic+XCJcclxuICAgICAgbGFiZWxWYWx1ZSA9IFwiPGRpdiBocmVmPScjL2pvYnMvXCIgKyBqb2JpZCArIFwiL3ZlcnRleC9cIiArIGVsLmlkICsgXCInIGNsYXNzPSdub2RlLWxhYmVsIFwiICsgZ2V0Tm9kZVR5cGUoZWwsIGluZm8pICsgXCInPlwiXHJcblxyXG4gICAgICAjIE5vZGVuYW1lXHJcbiAgICAgIGlmIGluZm8gaXMgXCJtaXJyb3JcIlxyXG4gICAgICAgIGxhYmVsVmFsdWUgKz0gXCI8aDMgY2xhc3M9J25vZGUtbmFtZSc+TWlycm9yIG9mIFwiICsgZWwub3BlcmF0b3IgKyBcIjwvaDM+XCJcclxuICAgICAgZWxzZVxyXG4gICAgICAgIGxhYmVsVmFsdWUgKz0gXCI8aDMgY2xhc3M9J25vZGUtbmFtZSc+XCIgKyBlbC5vcGVyYXRvciArIFwiPC9oMz5cIlxyXG4gICAgICBpZiBlbC5kZXNjcmlwdGlvbiBpcyBcIlwiXHJcbiAgICAgICAgbGFiZWxWYWx1ZSArPSBcIlwiXHJcbiAgICAgIGVsc2VcclxuICAgICAgICBzdGVwTmFtZSA9IGVsLmRlc2NyaXB0aW9uXHJcbiAgICAgICAgXHJcbiAgICAgICAgIyBjbGVhbiBzdGVwTmFtZVxyXG4gICAgICAgIHN0ZXBOYW1lID0gc2hvcnRlblN0cmluZyhzdGVwTmFtZSlcclxuICAgICAgICBsYWJlbFZhbHVlICs9IFwiPGg0IGNsYXNzPSdzdGVwLW5hbWUnPlwiICsgc3RlcE5hbWUgKyBcIjwvaDQ+XCJcclxuICAgICAgXHJcbiAgICAgICMgSWYgdGhpcyBub2RlIGlzIGFuIFwiaXRlcmF0aW9uXCIgd2UgbmVlZCBhIGRpZmZlcmVudCBwYW5lbC1ib2R5XHJcbiAgICAgIGlmIGVsLnN0ZXBfZnVuY3Rpb24/XHJcbiAgICAgICAgbGFiZWxWYWx1ZSArPSBleHRlbmRMYWJlbE5vZGVGb3JJdGVyYXRpb24oZWwuaWQsIG1heFcsIG1heEgpXHJcbiAgICAgIGVsc2VcclxuICAgICAgICBcclxuICAgICAgICAjIE90aGVyd2lzZSBhZGQgaW5mb3MgICAgXHJcbiAgICAgICAgbGFiZWxWYWx1ZSArPSBcIjxoNT5cIiArIGluZm8gKyBcIiBOb2RlPC9oNT5cIiAgaWYgaXNTcGVjaWFsSXRlcmF0aW9uTm9kZShpbmZvKVxyXG4gICAgICAgIGxhYmVsVmFsdWUgKz0gXCI8aDU+UGFyYWxsZWxpc206IFwiICsgZWwucGFyYWxsZWxpc20gKyBcIjwvaDU+XCIgIHVubGVzcyBlbC5wYXJhbGxlbGlzbSBpcyBcIlwiXHJcbiAgICAgICAgbGFiZWxWYWx1ZSArPSBcIjxoNT5PcGVyYXRpb246IFwiICsgc2hvcnRlblN0cmluZyhlbC5vcGVyYXRvcl9zdHJhdGVneSkgKyBcIjwvaDU+XCIgIHVubGVzcyBlbC5vcGVyYXRvciBpcyBgdW5kZWZpbmVkYFxyXG4gICAgICBcclxuICAgICAgIyBsYWJlbFZhbHVlICs9IFwiPC9hPlwiXHJcbiAgICAgIGxhYmVsVmFsdWUgKz0gXCI8L2Rpdj5cIlxyXG4gICAgICBsYWJlbFZhbHVlXHJcblxyXG4gICAgIyBFeHRlbmRzIHRoZSBsYWJlbCBvZiBhIG5vZGUgd2l0aCBhbiBhZGRpdGlvbmFsIHN2ZyBFbGVtZW50IHRvIHByZXNlbnQgdGhlIGl0ZXJhdGlvbi5cclxuICAgIGV4dGVuZExhYmVsTm9kZUZvckl0ZXJhdGlvbiA9IChpZCwgbWF4VywgbWF4SCkgLT5cclxuICAgICAgc3ZnSUQgPSBcInN2Zy1cIiArIGlkXHJcblxyXG4gICAgICBsYWJlbFZhbHVlID0gXCI8c3ZnIGNsYXNzPSdcIiArIHN2Z0lEICsgXCInIHdpZHRoPVwiICsgbWF4VyArIFwiIGhlaWdodD1cIiArIG1heEggKyBcIj48ZyAvPjwvc3ZnPlwiXHJcbiAgICAgIGxhYmVsVmFsdWVcclxuXHJcbiAgICAjIFNwbGl0IGEgc3RyaW5nIGludG8gbXVsdGlwbGUgbGluZXMgc28gdGhhdCBlYWNoIGxpbmUgaGFzIGxlc3MgdGhhbiAzMCBsZXR0ZXJzLlxyXG4gICAgc2hvcnRlblN0cmluZyA9IChzKSAtPlxyXG4gICAgICAjIG1ha2Ugc3VyZSB0aGF0IG5hbWUgZG9lcyBub3QgY29udGFpbiBhIDwgKGJlY2F1c2Ugb2YgaHRtbClcclxuICAgICAgaWYgcy5jaGFyQXQoMCkgaXMgXCI8XCJcclxuICAgICAgICBzID0gcy5yZXBsYWNlKFwiPFwiLCBcIiZsdDtcIilcclxuICAgICAgICBzID0gcy5yZXBsYWNlKFwiPlwiLCBcIiZndDtcIilcclxuICAgICAgc2JyID0gXCJcIlxyXG4gICAgICB3aGlsZSBzLmxlbmd0aCA+IDMwXHJcbiAgICAgICAgc2JyID0gc2JyICsgcy5zdWJzdHJpbmcoMCwgMzApICsgXCI8YnI+XCJcclxuICAgICAgICBzID0gcy5zdWJzdHJpbmcoMzAsIHMubGVuZ3RoKVxyXG4gICAgICBzYnIgPSBzYnIgKyBzXHJcbiAgICAgIHNiclxyXG5cclxuICAgIGNyZWF0ZU5vZGUgPSAoZywgZGF0YSwgZWwsIGlzUGFyZW50ID0gZmFsc2UsIG1heFcsIG1heEgpIC0+XHJcbiAgICAgICMgY3JlYXRlIG5vZGUsIHNlbmQgYWRkaXRpb25hbCBpbmZvcm1hdGlvbnMgYWJvdXQgdGhlIG5vZGUgaWYgaXQgaXMgYSBzcGVjaWFsIG9uZVxyXG4gICAgICBpZiBlbC5pZCBpcyBkYXRhLnBhcnRpYWxfc29sdXRpb25cclxuICAgICAgICBnLnNldE5vZGUgZWwuaWQsXHJcbiAgICAgICAgICBsYWJlbDogY3JlYXRlTGFiZWxOb2RlKGVsLCBcInBhcnRpYWxTb2x1dGlvblwiLCBtYXhXLCBtYXhIKVxyXG4gICAgICAgICAgbGFiZWxUeXBlOiAnaHRtbCdcclxuICAgICAgICAgIGNsYXNzOiBnZXROb2RlVHlwZShlbCwgXCJwYXJ0aWFsU29sdXRpb25cIilcclxuXHJcbiAgICAgIGVsc2UgaWYgZWwuaWQgaXMgZGF0YS5uZXh0X3BhcnRpYWxfc29sdXRpb25cclxuICAgICAgICBnLnNldE5vZGUgZWwuaWQsXHJcbiAgICAgICAgICBsYWJlbDogY3JlYXRlTGFiZWxOb2RlKGVsLCBcIm5leHRQYXJ0aWFsU29sdXRpb25cIiwgbWF4VywgbWF4SClcclxuICAgICAgICAgIGxhYmVsVHlwZTogJ2h0bWwnXHJcbiAgICAgICAgICBjbGFzczogZ2V0Tm9kZVR5cGUoZWwsIFwibmV4dFBhcnRpYWxTb2x1dGlvblwiKVxyXG5cclxuICAgICAgZWxzZSBpZiBlbC5pZCBpcyBkYXRhLndvcmtzZXRcclxuICAgICAgICBnLnNldE5vZGUgZWwuaWQsXHJcbiAgICAgICAgICBsYWJlbDogY3JlYXRlTGFiZWxOb2RlKGVsLCBcIndvcmtzZXRcIiwgbWF4VywgbWF4SClcclxuICAgICAgICAgIGxhYmVsVHlwZTogJ2h0bWwnXHJcbiAgICAgICAgICBjbGFzczogZ2V0Tm9kZVR5cGUoZWwsIFwid29ya3NldFwiKVxyXG5cclxuICAgICAgZWxzZSBpZiBlbC5pZCBpcyBkYXRhLm5leHRfd29ya3NldFxyXG4gICAgICAgIGcuc2V0Tm9kZSBlbC5pZCxcclxuICAgICAgICAgIGxhYmVsOiBjcmVhdGVMYWJlbE5vZGUoZWwsIFwibmV4dFdvcmtzZXRcIiwgbWF4VywgbWF4SClcclxuICAgICAgICAgIGxhYmVsVHlwZTogJ2h0bWwnXHJcbiAgICAgICAgICBjbGFzczogZ2V0Tm9kZVR5cGUoZWwsIFwibmV4dFdvcmtzZXRcIilcclxuXHJcbiAgICAgIGVsc2UgaWYgZWwuaWQgaXMgZGF0YS5zb2x1dGlvbl9zZXRcclxuICAgICAgICBnLnNldE5vZGUgZWwuaWQsXHJcbiAgICAgICAgICBsYWJlbDogY3JlYXRlTGFiZWxOb2RlKGVsLCBcInNvbHV0aW9uU2V0XCIsIG1heFcsIG1heEgpXHJcbiAgICAgICAgICBsYWJlbFR5cGU6ICdodG1sJ1xyXG4gICAgICAgICAgY2xhc3M6IGdldE5vZGVUeXBlKGVsLCBcInNvbHV0aW9uU2V0XCIpXHJcblxyXG4gICAgICBlbHNlIGlmIGVsLmlkIGlzIGRhdGEuc29sdXRpb25fZGVsdGFcclxuICAgICAgICBnLnNldE5vZGUgZWwuaWQsXHJcbiAgICAgICAgICBsYWJlbDogY3JlYXRlTGFiZWxOb2RlKGVsLCBcInNvbHV0aW9uRGVsdGFcIiwgbWF4VywgbWF4SClcclxuICAgICAgICAgIGxhYmVsVHlwZTogJ2h0bWwnXHJcbiAgICAgICAgICBjbGFzczogZ2V0Tm9kZVR5cGUoZWwsIFwic29sdXRpb25EZWx0YVwiKVxyXG5cclxuICAgICAgZWxzZVxyXG4gICAgICAgIGcuc2V0Tm9kZSBlbC5pZCxcclxuICAgICAgICAgIGxhYmVsOiBjcmVhdGVMYWJlbE5vZGUoZWwsIFwiXCIsIG1heFcsIG1heEgpXHJcbiAgICAgICAgICBsYWJlbFR5cGU6ICdodG1sJ1xyXG4gICAgICAgICAgY2xhc3M6IGdldE5vZGVUeXBlKGVsLCBcIlwiKVxyXG5cclxuICAgIGNyZWF0ZUVkZ2UgPSAoZywgZGF0YSwgZWwsIGV4aXN0aW5nTm9kZXMsIHByZWQsIG1pc3NpbmdOb2RlcykgLT5cclxuICAgICAgdW5sZXNzIGV4aXN0aW5nTm9kZXMuaW5kZXhPZihwcmVkLmlkKSBpcyAtMVxyXG4gICAgICAgIGcuc2V0RWRnZSBwcmVkLmlkLCBlbC5pZCxcclxuICAgICAgICAgIGxhYmVsOiBjcmVhdGVMYWJlbEVkZ2UocHJlZClcclxuICAgICAgICAgIGxhYmVsVHlwZTogJ2h0bWwnXHJcbiAgICAgICAgICBhcnJvd2hlYWQ6ICdub3JtYWwnXHJcblxyXG4gICAgICBlbHNlXHJcbiAgICAgICAgbWlzc2luZ05vZGUgPSBzZWFyY2hGb3JOb2RlKGRhdGEsIHByZWQuaWQpXHJcblxyXG4gICAgICAgIHVubGVzcyAhbWlzc2luZ05vZGUgb3IgbWlzc2luZ05vZGVzLmluZGV4T2YobWlzc2luZ05vZGUuaWQpID4gLTFcclxuICAgICAgICAgIG1pc3NpbmdOb2Rlcy5wdXNoKG1pc3NpbmdOb2RlLmlkKVxyXG4gICAgICAgICAgZy5zZXROb2RlIG1pc3NpbmdOb2RlLmlkLFxyXG4gICAgICAgICAgICBsYWJlbDogY3JlYXRlTGFiZWxOb2RlKG1pc3NpbmdOb2RlLCBcIm1pcnJvclwiKVxyXG4gICAgICAgICAgICBsYWJlbFR5cGU6ICdodG1sJ1xyXG4gICAgICAgICAgICBjbGFzczogZ2V0Tm9kZVR5cGUobWlzc2luZ05vZGUsICdtaXJyb3InKVxyXG5cclxuICAgICAgICAgIGcuc2V0RWRnZSBtaXNzaW5nTm9kZS5pZCwgZWwuaWQsXHJcbiAgICAgICAgICAgIGxhYmVsOiBjcmVhdGVMYWJlbEVkZ2UobWlzc2luZ05vZGUpXHJcbiAgICAgICAgICAgIGxhYmVsVHlwZTogJ2h0bWwnXHJcblxyXG4gICAgbG9hZEpzb25Ub0RhZ3JlID0gKGcsIGRhdGEpIC0+XHJcbiAgICAgIGV4aXN0aW5nTm9kZXMgPSBbXVxyXG4gICAgICBtaXNzaW5nTm9kZXMgPSBbXVxyXG5cclxuICAgICAgaWYgZGF0YS5ub2Rlcz9cclxuICAgICAgICAjIFRoaXMgaXMgdGhlIG5vcm1hbCBqc29uIGRhdGFcclxuICAgICAgICB0b0l0ZXJhdGUgPSBkYXRhLm5vZGVzXHJcblxyXG4gICAgICBlbHNlXHJcbiAgICAgICAgIyBUaGlzIGlzIGFuIGl0ZXJhdGlvbiwgd2Ugbm93IHN0b3JlIHNwZWNpYWwgaXRlcmF0aW9uIG5vZGVzIGlmIHBvc3NpYmxlXHJcbiAgICAgICAgdG9JdGVyYXRlID0gZGF0YS5zdGVwX2Z1bmN0aW9uXHJcbiAgICAgICAgaXNQYXJlbnQgPSB0cnVlXHJcblxyXG4gICAgICBmb3IgZWwgaW4gdG9JdGVyYXRlXHJcbiAgICAgICAgbWF4VyA9IDBcclxuICAgICAgICBtYXhIID0gMFxyXG5cclxuICAgICAgICBpZiBlbC5zdGVwX2Z1bmN0aW9uXHJcbiAgICAgICAgICBzZyA9IG5ldyBkYWdyZUQzLmdyYXBobGliLkdyYXBoKHsgbXVsdGlncmFwaDogdHJ1ZSwgY29tcG91bmQ6IHRydWUgfSkuc2V0R3JhcGgoe1xyXG4gICAgICAgICAgICBub2Rlc2VwOiAyMFxyXG4gICAgICAgICAgICBlZGdlc2VwOiAwXHJcbiAgICAgICAgICAgIHJhbmtzZXA6IDIwXHJcbiAgICAgICAgICAgIHJhbmtkaXI6IFwiTFJcIlxyXG4gICAgICAgICAgICBtYXJnaW54OiAxMFxyXG4gICAgICAgICAgICBtYXJnaW55OiAxMFxyXG4gICAgICAgICAgICB9KVxyXG5cclxuICAgICAgICAgIHN1YmdyYXBoc1tlbC5pZF0gPSBzZ1xyXG5cclxuICAgICAgICAgIGxvYWRKc29uVG9EYWdyZShzZywgZWwpXHJcblxyXG4gICAgICAgICAgciA9IG5ldyBkYWdyZUQzLnJlbmRlcigpXHJcbiAgICAgICAgICBkM3RtcFN2Zy5zZWxlY3QoJ2cnKS5jYWxsKHIsIHNnKVxyXG4gICAgICAgICAgbWF4VyA9IHNnLmdyYXBoKCkud2lkdGhcclxuICAgICAgICAgIG1heEggPSBzZy5ncmFwaCgpLmhlaWdodFxyXG5cclxuICAgICAgICAgIGFuZ3VsYXIuZWxlbWVudChtYWluVG1wRWxlbWVudCkuZW1wdHkoKVxyXG5cclxuICAgICAgICBjcmVhdGVOb2RlKGcsIGRhdGEsIGVsLCBpc1BhcmVudCwgbWF4VywgbWF4SClcclxuXHJcbiAgICAgICAgZXhpc3RpbmdOb2Rlcy5wdXNoIGVsLmlkXHJcbiAgICAgICAgXHJcbiAgICAgICAgIyBjcmVhdGUgZWRnZXMgZnJvbSBpbnB1dHMgdG8gY3VycmVudCBub2RlXHJcbiAgICAgICAgaWYgZWwuaW5wdXRzP1xyXG4gICAgICAgICAgZm9yIHByZWQgaW4gZWwuaW5wdXRzXHJcbiAgICAgICAgICAgIGNyZWF0ZUVkZ2UoZywgZGF0YSwgZWwsIGV4aXN0aW5nTm9kZXMsIHByZWQsIG1pc3NpbmdOb2RlcylcclxuXHJcbiAgICAgIGdcclxuXHJcbiAgICAjIHNlYXJjaGVzIGluIHRoZSBnbG9iYWwgSlNPTkRhdGEgZm9yIHRoZSBub2RlIHdpdGggdGhlIGdpdmVuIGlkXHJcbiAgICBzZWFyY2hGb3JOb2RlID0gKGRhdGEsIG5vZGVJRCkgLT5cclxuICAgICAgZm9yIGkgb2YgZGF0YS5ub2Rlc1xyXG4gICAgICAgIGVsID0gZGF0YS5ub2Rlc1tpXVxyXG4gICAgICAgIHJldHVybiBlbCAgaWYgZWwuaWQgaXMgbm9kZUlEXHJcbiAgICAgICAgXHJcbiAgICAgICAgIyBsb29rIGZvciBub2RlcyB0aGF0IGFyZSBpbiBpdGVyYXRpb25zXHJcbiAgICAgICAgaWYgZWwuc3RlcF9mdW5jdGlvbj9cclxuICAgICAgICAgIGZvciBqIG9mIGVsLnN0ZXBfZnVuY3Rpb25cclxuICAgICAgICAgICAgcmV0dXJuIGVsLnN0ZXBfZnVuY3Rpb25bal0gIGlmIGVsLnN0ZXBfZnVuY3Rpb25bal0uaWQgaXMgbm9kZUlEXHJcblxyXG4gICAgZHJhd0dyYXBoID0gKGRhdGEpIC0+XHJcbiAgICAgIGcgPSBuZXcgZGFncmVEMy5ncmFwaGxpYi5HcmFwaCh7IG11bHRpZ3JhcGg6IHRydWUsIGNvbXBvdW5kOiB0cnVlIH0pLnNldEdyYXBoKHtcclxuICAgICAgICBub2Rlc2VwOiA3MFxyXG4gICAgICAgIGVkZ2VzZXA6IDBcclxuICAgICAgICByYW5rc2VwOiA1MFxyXG4gICAgICAgIHJhbmtkaXI6IFwiTFJcIlxyXG4gICAgICAgIG1hcmdpbng6IDQwXHJcbiAgICAgICAgbWFyZ2lueTogNDBcclxuICAgICAgICB9KVxyXG5cclxuICAgICAgbG9hZEpzb25Ub0RhZ3JlKGcsIGRhdGEpXHJcblxyXG4gICAgICByZW5kZXJlciA9IG5ldyBkYWdyZUQzLnJlbmRlcigpXHJcbiAgICAgIGQzbWFpblN2Z0cuY2FsbChyZW5kZXJlciwgZylcclxuXHJcbiAgICAgIGZvciBpLCBzZyBvZiBzdWJncmFwaHNcclxuICAgICAgICBkM21haW5Tdmcuc2VsZWN0KCdzdmcuc3ZnLScgKyBpICsgJyBnJykuY2FsbChyZW5kZXJlciwgc2cpXHJcblxyXG4gICAgICBuZXdTY2FsZSA9IDAuNVxyXG5cclxuICAgICAgeENlbnRlck9mZnNldCA9IE1hdGguZmxvb3IoKGFuZ3VsYXIuZWxlbWVudChtYWluU3ZnRWxlbWVudCkud2lkdGgoKSAtIGcuZ3JhcGgoKS53aWR0aCAqIG5ld1NjYWxlKSAvIDIpXHJcbiAgICAgIHlDZW50ZXJPZmZzZXQgPSBNYXRoLmZsb29yKChhbmd1bGFyLmVsZW1lbnQobWFpblN2Z0VsZW1lbnQpLmhlaWdodCgpIC0gZy5ncmFwaCgpLmhlaWdodCAqIG5ld1NjYWxlKSAvIDIpXHJcblxyXG4gICAgICBtYWluWm9vbS5zY2FsZShuZXdTY2FsZSkudHJhbnNsYXRlKFt4Q2VudGVyT2Zmc2V0LCB5Q2VudGVyT2Zmc2V0XSlcclxuXHJcbiAgICAgIGQzbWFpblN2Z0cuYXR0cihcInRyYW5zZm9ybVwiLCBcInRyYW5zbGF0ZShcIiArIHhDZW50ZXJPZmZzZXQgKyBcIiwgXCIgKyB5Q2VudGVyT2Zmc2V0ICsgXCIpIHNjYWxlKFwiICsgbWFpblpvb20uc2NhbGUoKSArIFwiKVwiKVxyXG5cclxuICAgICAgbWFpblpvb20ub24oXCJ6b29tXCIsIC0+XHJcbiAgICAgICAgZXYgPSBkMy5ldmVudFxyXG4gICAgICAgIGQzbWFpblN2Z0cuYXR0ciBcInRyYW5zZm9ybVwiLCBcInRyYW5zbGF0ZShcIiArIGV2LnRyYW5zbGF0ZSArIFwiKSBzY2FsZShcIiArIGV2LnNjYWxlICsgXCIpXCJcclxuICAgICAgKVxyXG4gICAgICBtYWluWm9vbShkM21haW5TdmcpXHJcblxyXG4gICAgICBkM21haW5TdmdHLnNlbGVjdEFsbCgnLm5vZGUnKS5vbiAnY2xpY2snLCAoZCkgLT5cclxuICAgICAgICBzY29wZS5zZXROb2RlKHsgbm9kZWlkOiBkIH0pXHJcblxyXG4gICAgc2NvcGUuJHdhdGNoIGF0dHJzLnBsYW4sIChuZXdQbGFuKSAtPlxyXG4gICAgICBkcmF3R3JhcGgobmV3UGxhbikgaWYgbmV3UGxhblxyXG5cclxuICAgIHJldHVyblxyXG4iLCJhbmd1bGFyLm1vZHVsZSgnZmxpbmtBcHAnKS5kaXJlY3RpdmUoJ3ZlcnRleCcsIGZ1bmN0aW9uKCRzdGF0ZSkge1xuICByZXR1cm4ge1xuICAgIHRlbXBsYXRlOiBcIjxzdmcgY2xhc3M9J3RpbWVsaW5lIHNlY29uZGFyeScgd2lkdGg9JzAnIGhlaWdodD0nMCc+PC9zdmc+XCIsXG4gICAgc2NvcGU6IHtcbiAgICAgIGRhdGE6IFwiPVwiXG4gICAgfSxcbiAgICBsaW5rOiBmdW5jdGlvbihzY29wZSwgZWxlbSwgYXR0cnMpIHtcbiAgICAgIHZhciBhbmFseXplVGltZSwgY29udGFpbmVyVywgc3ZnRWw7XG4gICAgICBzdmdFbCA9IGVsZW0uY2hpbGRyZW4oKVswXTtcbiAgICAgIGNvbnRhaW5lclcgPSBlbGVtLndpZHRoKCk7XG4gICAgICBhbmd1bGFyLmVsZW1lbnQoc3ZnRWwpLmF0dHIoJ3dpZHRoJywgY29udGFpbmVyVyk7XG4gICAgICBhbmFseXplVGltZSA9IGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgICAgdmFyIGNoYXJ0LCBzdmcsIHRlc3REYXRhO1xuICAgICAgICBkMy5zZWxlY3Qoc3ZnRWwpLnNlbGVjdEFsbChcIipcIikucmVtb3ZlKCk7XG4gICAgICAgIHRlc3REYXRhID0gW107XG4gICAgICAgIGFuZ3VsYXIuZm9yRWFjaChkYXRhLnN1YnRhc2tzLCBmdW5jdGlvbihzdWJ0YXNrLCBpKSB7XG4gICAgICAgICAgdmFyIHRpbWVzO1xuICAgICAgICAgIHRpbWVzID0gW1xuICAgICAgICAgICAge1xuICAgICAgICAgICAgICBsYWJlbDogXCJTY2hlZHVsZWRcIixcbiAgICAgICAgICAgICAgY29sb3I6IFwiIzY2NlwiLFxuICAgICAgICAgICAgICBib3JkZXJDb2xvcjogXCIjNTU1XCIsXG4gICAgICAgICAgICAgIHN0YXJ0aW5nX3RpbWU6IHN1YnRhc2sudGltZXN0YW1wc1tcIlNDSEVEVUxFRFwiXSxcbiAgICAgICAgICAgICAgZW5kaW5nX3RpbWU6IHN1YnRhc2sudGltZXN0YW1wc1tcIkRFUExPWUlOR1wiXSxcbiAgICAgICAgICAgICAgdHlwZTogJ3JlZ3VsYXInXG4gICAgICAgICAgICB9LCB7XG4gICAgICAgICAgICAgIGxhYmVsOiBcIkRlcGxveWluZ1wiLFxuICAgICAgICAgICAgICBjb2xvcjogXCIjYWFhXCIsXG4gICAgICAgICAgICAgIGJvcmRlckNvbG9yOiBcIiM1NTVcIixcbiAgICAgICAgICAgICAgc3RhcnRpbmdfdGltZTogc3VidGFzay50aW1lc3RhbXBzW1wiREVQTE9ZSU5HXCJdLFxuICAgICAgICAgICAgICBlbmRpbmdfdGltZTogc3VidGFzay50aW1lc3RhbXBzW1wiUlVOTklOR1wiXSxcbiAgICAgICAgICAgICAgdHlwZTogJ3JlZ3VsYXInXG4gICAgICAgICAgICB9XG4gICAgICAgICAgXTtcbiAgICAgICAgICBpZiAoc3VidGFzay50aW1lc3RhbXBzW1wiRklOSVNIRURcIl0gPiAwKSB7XG4gICAgICAgICAgICB0aW1lcy5wdXNoKHtcbiAgICAgICAgICAgICAgbGFiZWw6IFwiUnVubmluZ1wiLFxuICAgICAgICAgICAgICBjb2xvcjogXCIjZGRkXCIsXG4gICAgICAgICAgICAgIGJvcmRlckNvbG9yOiBcIiM1NTVcIixcbiAgICAgICAgICAgICAgc3RhcnRpbmdfdGltZTogc3VidGFzay50aW1lc3RhbXBzW1wiUlVOTklOR1wiXSxcbiAgICAgICAgICAgICAgZW5kaW5nX3RpbWU6IHN1YnRhc2sudGltZXN0YW1wc1tcIkZJTklTSEVEXCJdLFxuICAgICAgICAgICAgICB0eXBlOiAncmVndWxhcidcbiAgICAgICAgICAgIH0pO1xuICAgICAgICAgIH1cbiAgICAgICAgICByZXR1cm4gdGVzdERhdGEucHVzaCh7XG4gICAgICAgICAgICBsYWJlbDogXCIoXCIgKyBzdWJ0YXNrLnN1YnRhc2sgKyBcIikgXCIgKyBzdWJ0YXNrLmhvc3QsXG4gICAgICAgICAgICB0aW1lczogdGltZXNcbiAgICAgICAgICB9KTtcbiAgICAgICAgfSk7XG4gICAgICAgIGNoYXJ0ID0gZDMudGltZWxpbmUoKS5zdGFjaygpLnRpY2tGb3JtYXQoe1xuICAgICAgICAgIGZvcm1hdDogZDMudGltZS5mb3JtYXQoXCIlTFwiKSxcbiAgICAgICAgICB0aWNrU2l6ZTogMVxuICAgICAgICB9KS5wcmVmaXgoXCJzaW5nbGVcIikubGFiZWxGb3JtYXQoZnVuY3Rpb24obGFiZWwpIHtcbiAgICAgICAgICByZXR1cm4gbGFiZWw7XG4gICAgICAgIH0pLm1hcmdpbih7XG4gICAgICAgICAgbGVmdDogMTAwLFxuICAgICAgICAgIHJpZ2h0OiAwLFxuICAgICAgICAgIHRvcDogMCxcbiAgICAgICAgICBib3R0b206IDBcbiAgICAgICAgfSkuaXRlbUhlaWdodCgzMCkucmVsYXRpdmVUaW1lKCk7XG4gICAgICAgIHJldHVybiBzdmcgPSBkMy5zZWxlY3Qoc3ZnRWwpLmRhdHVtKHRlc3REYXRhKS5jYWxsKGNoYXJ0KTtcbiAgICAgIH07XG4gICAgICBhbmFseXplVGltZShzY29wZS5kYXRhKTtcbiAgICB9XG4gIH07XG59KS5kaXJlY3RpdmUoJ3RpbWVsaW5lJywgZnVuY3Rpb24oJHN0YXRlKSB7XG4gIHJldHVybiB7XG4gICAgdGVtcGxhdGU6IFwiPHN2ZyBjbGFzcz0ndGltZWxpbmUnIHdpZHRoPScwJyBoZWlnaHQ9JzAnPjwvc3ZnPlwiLFxuICAgIHNjb3BlOiB7XG4gICAgICB2ZXJ0aWNlczogXCI9XCIsXG4gICAgICBqb2JpZDogXCI9XCJcbiAgICB9LFxuICAgIGxpbms6IGZ1bmN0aW9uKHNjb3BlLCBlbGVtLCBhdHRycykge1xuICAgICAgdmFyIGFuYWx5emVUaW1lLCBjb250YWluZXJXLCBzdmdFbCwgdHJhbnNsYXRlTGFiZWw7XG4gICAgICBzdmdFbCA9IGVsZW0uY2hpbGRyZW4oKVswXTtcbiAgICAgIGNvbnRhaW5lclcgPSBlbGVtLndpZHRoKCk7XG4gICAgICBhbmd1bGFyLmVsZW1lbnQoc3ZnRWwpLmF0dHIoJ3dpZHRoJywgY29udGFpbmVyVyk7XG4gICAgICB0cmFuc2xhdGVMYWJlbCA9IGZ1bmN0aW9uKGxhYmVsKSB7XG4gICAgICAgIHJldHVybiBsYWJlbC5yZXBsYWNlKFwiJmd0O1wiLCBcIj5cIik7XG4gICAgICB9O1xuICAgICAgYW5hbHl6ZVRpbWUgPSBmdW5jdGlvbihkYXRhKSB7XG4gICAgICAgIHZhciBjaGFydCwgc3ZnLCB0ZXN0RGF0YTtcbiAgICAgICAgZDMuc2VsZWN0KHN2Z0VsKS5zZWxlY3RBbGwoXCIqXCIpLnJlbW92ZSgpO1xuICAgICAgICB0ZXN0RGF0YSA9IFtdO1xuICAgICAgICBhbmd1bGFyLmZvckVhY2goZGF0YSwgZnVuY3Rpb24odmVydGV4KSB7XG4gICAgICAgICAgaWYgKHZlcnRleFsnc3RhcnQtdGltZSddID4gLTEpIHtcbiAgICAgICAgICAgIGlmICh2ZXJ0ZXgudHlwZSA9PT0gJ3NjaGVkdWxlZCcpIHtcbiAgICAgICAgICAgICAgcmV0dXJuIHRlc3REYXRhLnB1c2goe1xuICAgICAgICAgICAgICAgIHRpbWVzOiBbXG4gICAgICAgICAgICAgICAgICB7XG4gICAgICAgICAgICAgICAgICAgIGxhYmVsOiB0cmFuc2xhdGVMYWJlbCh2ZXJ0ZXgubmFtZSksXG4gICAgICAgICAgICAgICAgICAgIGNvbG9yOiBcIiNjY2NjY2NcIixcbiAgICAgICAgICAgICAgICAgICAgYm9yZGVyQ29sb3I6IFwiIzU1NTU1NVwiLFxuICAgICAgICAgICAgICAgICAgICBzdGFydGluZ190aW1lOiB2ZXJ0ZXhbJ3N0YXJ0LXRpbWUnXSxcbiAgICAgICAgICAgICAgICAgICAgZW5kaW5nX3RpbWU6IHZlcnRleFsnZW5kLXRpbWUnXSxcbiAgICAgICAgICAgICAgICAgICAgdHlwZTogdmVydGV4LnR5cGVcbiAgICAgICAgICAgICAgICAgIH1cbiAgICAgICAgICAgICAgICBdXG4gICAgICAgICAgICAgIH0pO1xuICAgICAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICAgICAgcmV0dXJuIHRlc3REYXRhLnB1c2goe1xuICAgICAgICAgICAgICAgIHRpbWVzOiBbXG4gICAgICAgICAgICAgICAgICB7XG4gICAgICAgICAgICAgICAgICAgIGxhYmVsOiB0cmFuc2xhdGVMYWJlbCh2ZXJ0ZXgubmFtZSksXG4gICAgICAgICAgICAgICAgICAgIGNvbG9yOiBcIiNkOWYxZjdcIixcbiAgICAgICAgICAgICAgICAgICAgYm9yZGVyQ29sb3I6IFwiIzYyY2RlYVwiLFxuICAgICAgICAgICAgICAgICAgICBzdGFydGluZ190aW1lOiB2ZXJ0ZXhbJ3N0YXJ0LXRpbWUnXSxcbiAgICAgICAgICAgICAgICAgICAgZW5kaW5nX3RpbWU6IHZlcnRleFsnZW5kLXRpbWUnXSxcbiAgICAgICAgICAgICAgICAgICAgbGluazogdmVydGV4LmlkLFxuICAgICAgICAgICAgICAgICAgICB0eXBlOiB2ZXJ0ZXgudHlwZVxuICAgICAgICAgICAgICAgICAgfVxuICAgICAgICAgICAgICAgIF1cbiAgICAgICAgICAgICAgfSk7XG4gICAgICAgICAgICB9XG4gICAgICAgICAgfVxuICAgICAgICB9KTtcbiAgICAgICAgY2hhcnQgPSBkMy50aW1lbGluZSgpLnN0YWNrKCkuY2xpY2soZnVuY3Rpb24oZCwgaSwgZGF0dW0pIHtcbiAgICAgICAgICBpZiAoZC5saW5rKSB7XG4gICAgICAgICAgICByZXR1cm4gJHN0YXRlLmdvKFwic2luZ2xlLWpvYi50aW1lbGluZS52ZXJ0ZXhcIiwge1xuICAgICAgICAgICAgICBqb2JpZDogc2NvcGUuam9iaWQsXG4gICAgICAgICAgICAgIHZlcnRleElkOiBkLmxpbmtcbiAgICAgICAgICAgIH0pO1xuICAgICAgICAgIH1cbiAgICAgICAgfSkudGlja0Zvcm1hdCh7XG4gICAgICAgICAgZm9ybWF0OiBkMy50aW1lLmZvcm1hdChcIiVMXCIpLFxuICAgICAgICAgIHRpY2tTaXplOiAxXG4gICAgICAgIH0pLnByZWZpeChcIm1haW5cIikubWFyZ2luKHtcbiAgICAgICAgICBsZWZ0OiAwLFxuICAgICAgICAgIHJpZ2h0OiAwLFxuICAgICAgICAgIHRvcDogMCxcbiAgICAgICAgICBib3R0b206IDBcbiAgICAgICAgfSkuaXRlbUhlaWdodCgzMCkuc2hvd0JvcmRlckxpbmUoKS5zaG93SG91clRpbWVsaW5lKCk7XG4gICAgICAgIHJldHVybiBzdmcgPSBkMy5zZWxlY3Qoc3ZnRWwpLmRhdHVtKHRlc3REYXRhKS5jYWxsKGNoYXJ0KTtcbiAgICAgIH07XG4gICAgICBzY29wZS4kd2F0Y2goYXR0cnMudmVydGljZXMsIGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgICAgaWYgKGRhdGEpIHtcbiAgICAgICAgICByZXR1cm4gYW5hbHl6ZVRpbWUoZGF0YSk7XG4gICAgICAgIH1cbiAgICAgIH0pO1xuICAgIH1cbiAgfTtcbn0pLmRpcmVjdGl2ZSgnam9iUGxhbicsIGZ1bmN0aW9uKCR0aW1lb3V0KSB7XG4gIHJldHVybiB7XG4gICAgdGVtcGxhdGU6IFwiPHN2ZyBjbGFzcz0nZ3JhcGgnIHdpZHRoPSc1MDAnIGhlaWdodD0nNDAwJz48ZyAvPjwvc3ZnPiA8c3ZnIGNsYXNzPSd0bXAnIHdpZHRoPScxJyBoZWlnaHQ9JzEnPjxnIC8+PC9zdmc+IDxkaXYgY2xhc3M9J2J0bi1ncm91cCB6b29tLWJ1dHRvbnMnPiA8YSBjbGFzcz0nYnRuIGJ0bi1kZWZhdWx0IHpvb20taW4nIG5nLWNsaWNrPSd6b29tSW4oKSc+PGkgY2xhc3M9J2ZhIGZhLXBsdXMnIC8+PC9hPiA8YSBjbGFzcz0nYnRuIGJ0bi1kZWZhdWx0IHpvb20tb3V0JyBuZy1jbGljaz0nem9vbU91dCgpJz48aSBjbGFzcz0nZmEgZmEtbWludXMnIC8+PC9hPiA8L2Rpdj5cIixcbiAgICBzY29wZToge1xuICAgICAgcGxhbjogJz0nLFxuICAgICAgc2V0Tm9kZTogJyYnXG4gICAgfSxcbiAgICBsaW5rOiBmdW5jdGlvbihzY29wZSwgZWxlbSwgYXR0cnMpIHtcbiAgICAgIHZhciBjb250YWluZXJXLCBjcmVhdGVFZGdlLCBjcmVhdGVMYWJlbEVkZ2UsIGNyZWF0ZUxhYmVsTm9kZSwgY3JlYXRlTm9kZSwgZDNtYWluU3ZnLCBkM21haW5TdmdHLCBkM3RtcFN2ZywgZHJhd0dyYXBoLCBleHRlbmRMYWJlbE5vZGVGb3JJdGVyYXRpb24sIGcsIGdldE5vZGVUeXBlLCBpc1NwZWNpYWxJdGVyYXRpb25Ob2RlLCBqb2JpZCwgbG9hZEpzb25Ub0RhZ3JlLCBtYWluRywgbWFpblN2Z0VsZW1lbnQsIG1haW5UbXBFbGVtZW50LCBtYWluWm9vbSwgc2VhcmNoRm9yTm9kZSwgc2hvcnRlblN0cmluZywgc3ViZ3JhcGhzO1xuICAgICAgZyA9IG51bGw7XG4gICAgICBtYWluWm9vbSA9IGQzLmJlaGF2aW9yLnpvb20oKTtcbiAgICAgIHN1YmdyYXBocyA9IFtdO1xuICAgICAgam9iaWQgPSBhdHRycy5qb2JpZDtcbiAgICAgIG1haW5TdmdFbGVtZW50ID0gZWxlbS5jaGlsZHJlbigpWzBdO1xuICAgICAgbWFpbkcgPSBlbGVtLmNoaWxkcmVuKCkuY2hpbGRyZW4oKVswXTtcbiAgICAgIG1haW5UbXBFbGVtZW50ID0gZWxlbS5jaGlsZHJlbigpWzFdO1xuICAgICAgZDNtYWluU3ZnID0gZDMuc2VsZWN0KG1haW5TdmdFbGVtZW50KTtcbiAgICAgIGQzbWFpblN2Z0cgPSBkMy5zZWxlY3QobWFpbkcpO1xuICAgICAgZDN0bXBTdmcgPSBkMy5zZWxlY3QobWFpblRtcEVsZW1lbnQpO1xuICAgICAgY29udGFpbmVyVyA9IGVsZW0ud2lkdGgoKTtcbiAgICAgIGFuZ3VsYXIuZWxlbWVudChlbGVtLmNoaWxkcmVuKClbMF0pLndpZHRoKGNvbnRhaW5lclcpO1xuICAgICAgc2NvcGUuem9vbUluID0gZnVuY3Rpb24oKSB7XG4gICAgICAgIHZhciB0cmFuc2xhdGUsIHYxLCB2MjtcbiAgICAgICAgaWYgKG1haW5ab29tLnNjYWxlKCkgPCAyLjk5KSB7XG4gICAgICAgICAgdHJhbnNsYXRlID0gbWFpblpvb20udHJhbnNsYXRlKCk7XG4gICAgICAgICAgdjEgPSB0cmFuc2xhdGVbMF0gKiAobWFpblpvb20uc2NhbGUoKSArIDAuMSAvIChtYWluWm9vbS5zY2FsZSgpKSk7XG4gICAgICAgICAgdjIgPSB0cmFuc2xhdGVbMV0gKiAobWFpblpvb20uc2NhbGUoKSArIDAuMSAvIChtYWluWm9vbS5zY2FsZSgpKSk7XG4gICAgICAgICAgbWFpblpvb20uc2NhbGUobWFpblpvb20uc2NhbGUoKSArIDAuMSk7XG4gICAgICAgICAgbWFpblpvb20udHJhbnNsYXRlKFt2MSwgdjJdKTtcbiAgICAgICAgICByZXR1cm4gZDNtYWluU3ZnRy5hdHRyKFwidHJhbnNmb3JtXCIsIFwidHJhbnNsYXRlKFwiICsgdjEgKyBcIixcIiArIHYyICsgXCIpIHNjYWxlKFwiICsgbWFpblpvb20uc2NhbGUoKSArIFwiKVwiKTtcbiAgICAgICAgfVxuICAgICAgfTtcbiAgICAgIHNjb3BlLnpvb21PdXQgPSBmdW5jdGlvbigpIHtcbiAgICAgICAgdmFyIHRyYW5zbGF0ZSwgdjEsIHYyO1xuICAgICAgICBpZiAobWFpblpvb20uc2NhbGUoKSA+IDAuMzEpIHtcbiAgICAgICAgICBtYWluWm9vbS5zY2FsZShtYWluWm9vbS5zY2FsZSgpIC0gMC4xKTtcbiAgICAgICAgICB0cmFuc2xhdGUgPSBtYWluWm9vbS50cmFuc2xhdGUoKTtcbiAgICAgICAgICB2MSA9IHRyYW5zbGF0ZVswXSAqIChtYWluWm9vbS5zY2FsZSgpIC0gMC4xIC8gKG1haW5ab29tLnNjYWxlKCkpKTtcbiAgICAgICAgICB2MiA9IHRyYW5zbGF0ZVsxXSAqIChtYWluWm9vbS5zY2FsZSgpIC0gMC4xIC8gKG1haW5ab29tLnNjYWxlKCkpKTtcbiAgICAgICAgICBtYWluWm9vbS50cmFuc2xhdGUoW3YxLCB2Ml0pO1xuICAgICAgICAgIHJldHVybiBkM21haW5TdmdHLmF0dHIoXCJ0cmFuc2Zvcm1cIiwgXCJ0cmFuc2xhdGUoXCIgKyB2MSArIFwiLFwiICsgdjIgKyBcIikgc2NhbGUoXCIgKyBtYWluWm9vbS5zY2FsZSgpICsgXCIpXCIpO1xuICAgICAgICB9XG4gICAgICB9O1xuICAgICAgY3JlYXRlTGFiZWxFZGdlID0gZnVuY3Rpb24oZWwpIHtcbiAgICAgICAgdmFyIGxhYmVsVmFsdWU7XG4gICAgICAgIGxhYmVsVmFsdWUgPSBcIlwiO1xuICAgICAgICBpZiAoKGVsLnNoaXBfc3RyYXRlZ3kgIT0gbnVsbCkgfHwgKGVsLmxvY2FsX3N0cmF0ZWd5ICE9IG51bGwpKSB7XG4gICAgICAgICAgbGFiZWxWYWx1ZSArPSBcIjxkaXYgY2xhc3M9J2VkZ2UtbGFiZWwnPlwiO1xuICAgICAgICAgIGlmIChlbC5zaGlwX3N0cmF0ZWd5ICE9IG51bGwpIHtcbiAgICAgICAgICAgIGxhYmVsVmFsdWUgKz0gZWwuc2hpcF9zdHJhdGVneTtcbiAgICAgICAgICB9XG4gICAgICAgICAgaWYgKGVsLnRlbXBfbW9kZSAhPT0gdW5kZWZpbmVkKSB7XG4gICAgICAgICAgICBsYWJlbFZhbHVlICs9IFwiIChcIiArIGVsLnRlbXBfbW9kZSArIFwiKVwiO1xuICAgICAgICAgIH1cbiAgICAgICAgICBpZiAoZWwubG9jYWxfc3RyYXRlZ3kgIT09IHVuZGVmaW5lZCkge1xuICAgICAgICAgICAgbGFiZWxWYWx1ZSArPSBcIiw8YnI+XCIgKyBlbC5sb2NhbF9zdHJhdGVneTtcbiAgICAgICAgICB9XG4gICAgICAgICAgbGFiZWxWYWx1ZSArPSBcIjwvZGl2PlwiO1xuICAgICAgICB9XG4gICAgICAgIHJldHVybiBsYWJlbFZhbHVlO1xuICAgICAgfTtcbiAgICAgIGlzU3BlY2lhbEl0ZXJhdGlvbk5vZGUgPSBmdW5jdGlvbihpbmZvKSB7XG4gICAgICAgIHJldHVybiBpbmZvID09PSBcInBhcnRpYWxTb2x1dGlvblwiIHx8IGluZm8gPT09IFwibmV4dFBhcnRpYWxTb2x1dGlvblwiIHx8IGluZm8gPT09IFwid29ya3NldFwiIHx8IGluZm8gPT09IFwibmV4dFdvcmtzZXRcIiB8fCBpbmZvID09PSBcInNvbHV0aW9uU2V0XCIgfHwgaW5mbyA9PT0gXCJzb2x1dGlvbkRlbHRhXCI7XG4gICAgICB9O1xuICAgICAgZ2V0Tm9kZVR5cGUgPSBmdW5jdGlvbihlbCwgaW5mbykge1xuICAgICAgICBpZiAoaW5mbyA9PT0gXCJtaXJyb3JcIikge1xuICAgICAgICAgIHJldHVybiAnbm9kZS1taXJyb3InO1xuICAgICAgICB9IGVsc2UgaWYgKGlzU3BlY2lhbEl0ZXJhdGlvbk5vZGUoaW5mbykpIHtcbiAgICAgICAgICByZXR1cm4gJ25vZGUtaXRlcmF0aW9uJztcbiAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICByZXR1cm4gJ25vZGUtbm9ybWFsJztcbiAgICAgICAgfVxuICAgICAgfTtcbiAgICAgIGNyZWF0ZUxhYmVsTm9kZSA9IGZ1bmN0aW9uKGVsLCBpbmZvLCBtYXhXLCBtYXhIKSB7XG4gICAgICAgIHZhciBsYWJlbFZhbHVlLCBzdGVwTmFtZTtcbiAgICAgICAgbGFiZWxWYWx1ZSA9IFwiPGRpdiBocmVmPScjL2pvYnMvXCIgKyBqb2JpZCArIFwiL3ZlcnRleC9cIiArIGVsLmlkICsgXCInIGNsYXNzPSdub2RlLWxhYmVsIFwiICsgZ2V0Tm9kZVR5cGUoZWwsIGluZm8pICsgXCInPlwiO1xuICAgICAgICBpZiAoaW5mbyA9PT0gXCJtaXJyb3JcIikge1xuICAgICAgICAgIGxhYmVsVmFsdWUgKz0gXCI8aDMgY2xhc3M9J25vZGUtbmFtZSc+TWlycm9yIG9mIFwiICsgZWwub3BlcmF0b3IgKyBcIjwvaDM+XCI7XG4gICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgbGFiZWxWYWx1ZSArPSBcIjxoMyBjbGFzcz0nbm9kZS1uYW1lJz5cIiArIGVsLm9wZXJhdG9yICsgXCI8L2gzPlwiO1xuICAgICAgICB9XG4gICAgICAgIGlmIChlbC5kZXNjcmlwdGlvbiA9PT0gXCJcIikge1xuICAgICAgICAgIGxhYmVsVmFsdWUgKz0gXCJcIjtcbiAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICBzdGVwTmFtZSA9IGVsLmRlc2NyaXB0aW9uO1xuICAgICAgICAgIHN0ZXBOYW1lID0gc2hvcnRlblN0cmluZyhzdGVwTmFtZSk7XG4gICAgICAgICAgbGFiZWxWYWx1ZSArPSBcIjxoNCBjbGFzcz0nc3RlcC1uYW1lJz5cIiArIHN0ZXBOYW1lICsgXCI8L2g0PlwiO1xuICAgICAgICB9XG4gICAgICAgIGlmIChlbC5zdGVwX2Z1bmN0aW9uICE9IG51bGwpIHtcbiAgICAgICAgICBsYWJlbFZhbHVlICs9IGV4dGVuZExhYmVsTm9kZUZvckl0ZXJhdGlvbihlbC5pZCwgbWF4VywgbWF4SCk7XG4gICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgaWYgKGlzU3BlY2lhbEl0ZXJhdGlvbk5vZGUoaW5mbykpIHtcbiAgICAgICAgICAgIGxhYmVsVmFsdWUgKz0gXCI8aDU+XCIgKyBpbmZvICsgXCIgTm9kZTwvaDU+XCI7XG4gICAgICAgICAgfVxuICAgICAgICAgIGlmIChlbC5wYXJhbGxlbGlzbSAhPT0gXCJcIikge1xuICAgICAgICAgICAgbGFiZWxWYWx1ZSArPSBcIjxoNT5QYXJhbGxlbGlzbTogXCIgKyBlbC5wYXJhbGxlbGlzbSArIFwiPC9oNT5cIjtcbiAgICAgICAgICB9XG4gICAgICAgICAgaWYgKGVsLm9wZXJhdG9yICE9PSB1bmRlZmluZWQpIHtcbiAgICAgICAgICAgIGxhYmVsVmFsdWUgKz0gXCI8aDU+T3BlcmF0aW9uOiBcIiArIHNob3J0ZW5TdHJpbmcoZWwub3BlcmF0b3Jfc3RyYXRlZ3kpICsgXCI8L2g1PlwiO1xuICAgICAgICAgIH1cbiAgICAgICAgfVxuICAgICAgICBsYWJlbFZhbHVlICs9IFwiPC9kaXY+XCI7XG4gICAgICAgIHJldHVybiBsYWJlbFZhbHVlO1xuICAgICAgfTtcbiAgICAgIGV4dGVuZExhYmVsTm9kZUZvckl0ZXJhdGlvbiA9IGZ1bmN0aW9uKGlkLCBtYXhXLCBtYXhIKSB7XG4gICAgICAgIHZhciBsYWJlbFZhbHVlLCBzdmdJRDtcbiAgICAgICAgc3ZnSUQgPSBcInN2Zy1cIiArIGlkO1xuICAgICAgICBsYWJlbFZhbHVlID0gXCI8c3ZnIGNsYXNzPSdcIiArIHN2Z0lEICsgXCInIHdpZHRoPVwiICsgbWF4VyArIFwiIGhlaWdodD1cIiArIG1heEggKyBcIj48ZyAvPjwvc3ZnPlwiO1xuICAgICAgICByZXR1cm4gbGFiZWxWYWx1ZTtcbiAgICAgIH07XG4gICAgICBzaG9ydGVuU3RyaW5nID0gZnVuY3Rpb24ocykge1xuICAgICAgICB2YXIgc2JyO1xuICAgICAgICBpZiAocy5jaGFyQXQoMCkgPT09IFwiPFwiKSB7XG4gICAgICAgICAgcyA9IHMucmVwbGFjZShcIjxcIiwgXCImbHQ7XCIpO1xuICAgICAgICAgIHMgPSBzLnJlcGxhY2UoXCI+XCIsIFwiJmd0O1wiKTtcbiAgICAgICAgfVxuICAgICAgICBzYnIgPSBcIlwiO1xuICAgICAgICB3aGlsZSAocy5sZW5ndGggPiAzMCkge1xuICAgICAgICAgIHNiciA9IHNiciArIHMuc3Vic3RyaW5nKDAsIDMwKSArIFwiPGJyPlwiO1xuICAgICAgICAgIHMgPSBzLnN1YnN0cmluZygzMCwgcy5sZW5ndGgpO1xuICAgICAgICB9XG4gICAgICAgIHNiciA9IHNiciArIHM7XG4gICAgICAgIHJldHVybiBzYnI7XG4gICAgICB9O1xuICAgICAgY3JlYXRlTm9kZSA9IGZ1bmN0aW9uKGcsIGRhdGEsIGVsLCBpc1BhcmVudCwgbWF4VywgbWF4SCkge1xuICAgICAgICBpZiAoaXNQYXJlbnQgPT0gbnVsbCkge1xuICAgICAgICAgIGlzUGFyZW50ID0gZmFsc2U7XG4gICAgICAgIH1cbiAgICAgICAgaWYgKGVsLmlkID09PSBkYXRhLnBhcnRpYWxfc29sdXRpb24pIHtcbiAgICAgICAgICByZXR1cm4gZy5zZXROb2RlKGVsLmlkLCB7XG4gICAgICAgICAgICBsYWJlbDogY3JlYXRlTGFiZWxOb2RlKGVsLCBcInBhcnRpYWxTb2x1dGlvblwiLCBtYXhXLCBtYXhIKSxcbiAgICAgICAgICAgIGxhYmVsVHlwZTogJ2h0bWwnLFxuICAgICAgICAgICAgXCJjbGFzc1wiOiBnZXROb2RlVHlwZShlbCwgXCJwYXJ0aWFsU29sdXRpb25cIilcbiAgICAgICAgICB9KTtcbiAgICAgICAgfSBlbHNlIGlmIChlbC5pZCA9PT0gZGF0YS5uZXh0X3BhcnRpYWxfc29sdXRpb24pIHtcbiAgICAgICAgICByZXR1cm4gZy5zZXROb2RlKGVsLmlkLCB7XG4gICAgICAgICAgICBsYWJlbDogY3JlYXRlTGFiZWxOb2RlKGVsLCBcIm5leHRQYXJ0aWFsU29sdXRpb25cIiwgbWF4VywgbWF4SCksXG4gICAgICAgICAgICBsYWJlbFR5cGU6ICdodG1sJyxcbiAgICAgICAgICAgIFwiY2xhc3NcIjogZ2V0Tm9kZVR5cGUoZWwsIFwibmV4dFBhcnRpYWxTb2x1dGlvblwiKVxuICAgICAgICAgIH0pO1xuICAgICAgICB9IGVsc2UgaWYgKGVsLmlkID09PSBkYXRhLndvcmtzZXQpIHtcbiAgICAgICAgICByZXR1cm4gZy5zZXROb2RlKGVsLmlkLCB7XG4gICAgICAgICAgICBsYWJlbDogY3JlYXRlTGFiZWxOb2RlKGVsLCBcIndvcmtzZXRcIiwgbWF4VywgbWF4SCksXG4gICAgICAgICAgICBsYWJlbFR5cGU6ICdodG1sJyxcbiAgICAgICAgICAgIFwiY2xhc3NcIjogZ2V0Tm9kZVR5cGUoZWwsIFwid29ya3NldFwiKVxuICAgICAgICAgIH0pO1xuICAgICAgICB9IGVsc2UgaWYgKGVsLmlkID09PSBkYXRhLm5leHRfd29ya3NldCkge1xuICAgICAgICAgIHJldHVybiBnLnNldE5vZGUoZWwuaWQsIHtcbiAgICAgICAgICAgIGxhYmVsOiBjcmVhdGVMYWJlbE5vZGUoZWwsIFwibmV4dFdvcmtzZXRcIiwgbWF4VywgbWF4SCksXG4gICAgICAgICAgICBsYWJlbFR5cGU6ICdodG1sJyxcbiAgICAgICAgICAgIFwiY2xhc3NcIjogZ2V0Tm9kZVR5cGUoZWwsIFwibmV4dFdvcmtzZXRcIilcbiAgICAgICAgICB9KTtcbiAgICAgICAgfSBlbHNlIGlmIChlbC5pZCA9PT0gZGF0YS5zb2x1dGlvbl9zZXQpIHtcbiAgICAgICAgICByZXR1cm4gZy5zZXROb2RlKGVsLmlkLCB7XG4gICAgICAgICAgICBsYWJlbDogY3JlYXRlTGFiZWxOb2RlKGVsLCBcInNvbHV0aW9uU2V0XCIsIG1heFcsIG1heEgpLFxuICAgICAgICAgICAgbGFiZWxUeXBlOiAnaHRtbCcsXG4gICAgICAgICAgICBcImNsYXNzXCI6IGdldE5vZGVUeXBlKGVsLCBcInNvbHV0aW9uU2V0XCIpXG4gICAgICAgICAgfSk7XG4gICAgICAgIH0gZWxzZSBpZiAoZWwuaWQgPT09IGRhdGEuc29sdXRpb25fZGVsdGEpIHtcbiAgICAgICAgICByZXR1cm4gZy5zZXROb2RlKGVsLmlkLCB7XG4gICAgICAgICAgICBsYWJlbDogY3JlYXRlTGFiZWxOb2RlKGVsLCBcInNvbHV0aW9uRGVsdGFcIiwgbWF4VywgbWF4SCksXG4gICAgICAgICAgICBsYWJlbFR5cGU6ICdodG1sJyxcbiAgICAgICAgICAgIFwiY2xhc3NcIjogZ2V0Tm9kZVR5cGUoZWwsIFwic29sdXRpb25EZWx0YVwiKVxuICAgICAgICAgIH0pO1xuICAgICAgICB9IGVsc2Uge1xuICAgICAgICAgIHJldHVybiBnLnNldE5vZGUoZWwuaWQsIHtcbiAgICAgICAgICAgIGxhYmVsOiBjcmVhdGVMYWJlbE5vZGUoZWwsIFwiXCIsIG1heFcsIG1heEgpLFxuICAgICAgICAgICAgbGFiZWxUeXBlOiAnaHRtbCcsXG4gICAgICAgICAgICBcImNsYXNzXCI6IGdldE5vZGVUeXBlKGVsLCBcIlwiKVxuICAgICAgICAgIH0pO1xuICAgICAgICB9XG4gICAgICB9O1xuICAgICAgY3JlYXRlRWRnZSA9IGZ1bmN0aW9uKGcsIGRhdGEsIGVsLCBleGlzdGluZ05vZGVzLCBwcmVkLCBtaXNzaW5nTm9kZXMpIHtcbiAgICAgICAgdmFyIG1pc3NpbmdOb2RlO1xuICAgICAgICBpZiAoZXhpc3RpbmdOb2Rlcy5pbmRleE9mKHByZWQuaWQpICE9PSAtMSkge1xuICAgICAgICAgIHJldHVybiBnLnNldEVkZ2UocHJlZC5pZCwgZWwuaWQsIHtcbiAgICAgICAgICAgIGxhYmVsOiBjcmVhdGVMYWJlbEVkZ2UocHJlZCksXG4gICAgICAgICAgICBsYWJlbFR5cGU6ICdodG1sJyxcbiAgICAgICAgICAgIGFycm93aGVhZDogJ25vcm1hbCdcbiAgICAgICAgICB9KTtcbiAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICBtaXNzaW5nTm9kZSA9IHNlYXJjaEZvck5vZGUoZGF0YSwgcHJlZC5pZCk7XG4gICAgICAgICAgaWYgKCEoIW1pc3NpbmdOb2RlIHx8IG1pc3NpbmdOb2Rlcy5pbmRleE9mKG1pc3NpbmdOb2RlLmlkKSA+IC0xKSkge1xuICAgICAgICAgICAgbWlzc2luZ05vZGVzLnB1c2gobWlzc2luZ05vZGUuaWQpO1xuICAgICAgICAgICAgZy5zZXROb2RlKG1pc3NpbmdOb2RlLmlkLCB7XG4gICAgICAgICAgICAgIGxhYmVsOiBjcmVhdGVMYWJlbE5vZGUobWlzc2luZ05vZGUsIFwibWlycm9yXCIpLFxuICAgICAgICAgICAgICBsYWJlbFR5cGU6ICdodG1sJyxcbiAgICAgICAgICAgICAgXCJjbGFzc1wiOiBnZXROb2RlVHlwZShtaXNzaW5nTm9kZSwgJ21pcnJvcicpXG4gICAgICAgICAgICB9KTtcbiAgICAgICAgICAgIHJldHVybiBnLnNldEVkZ2UobWlzc2luZ05vZGUuaWQsIGVsLmlkLCB7XG4gICAgICAgICAgICAgIGxhYmVsOiBjcmVhdGVMYWJlbEVkZ2UobWlzc2luZ05vZGUpLFxuICAgICAgICAgICAgICBsYWJlbFR5cGU6ICdodG1sJ1xuICAgICAgICAgICAgfSk7XG4gICAgICAgICAgfVxuICAgICAgICB9XG4gICAgICB9O1xuICAgICAgbG9hZEpzb25Ub0RhZ3JlID0gZnVuY3Rpb24oZywgZGF0YSkge1xuICAgICAgICB2YXIgZWwsIGV4aXN0aW5nTm9kZXMsIGlzUGFyZW50LCBrLCBsLCBsZW4sIGxlbjEsIG1heEgsIG1heFcsIG1pc3NpbmdOb2RlcywgcHJlZCwgciwgcmVmLCBzZywgdG9JdGVyYXRlO1xuICAgICAgICBleGlzdGluZ05vZGVzID0gW107XG4gICAgICAgIG1pc3NpbmdOb2RlcyA9IFtdO1xuICAgICAgICBpZiAoZGF0YS5ub2RlcyAhPSBudWxsKSB7XG4gICAgICAgICAgdG9JdGVyYXRlID0gZGF0YS5ub2RlcztcbiAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICB0b0l0ZXJhdGUgPSBkYXRhLnN0ZXBfZnVuY3Rpb247XG4gICAgICAgICAgaXNQYXJlbnQgPSB0cnVlO1xuICAgICAgICB9XG4gICAgICAgIGZvciAoayA9IDAsIGxlbiA9IHRvSXRlcmF0ZS5sZW5ndGg7IGsgPCBsZW47IGsrKykge1xuICAgICAgICAgIGVsID0gdG9JdGVyYXRlW2tdO1xuICAgICAgICAgIG1heFcgPSAwO1xuICAgICAgICAgIG1heEggPSAwO1xuICAgICAgICAgIGlmIChlbC5zdGVwX2Z1bmN0aW9uKSB7XG4gICAgICAgICAgICBzZyA9IG5ldyBkYWdyZUQzLmdyYXBobGliLkdyYXBoKHtcbiAgICAgICAgICAgICAgbXVsdGlncmFwaDogdHJ1ZSxcbiAgICAgICAgICAgICAgY29tcG91bmQ6IHRydWVcbiAgICAgICAgICAgIH0pLnNldEdyYXBoKHtcbiAgICAgICAgICAgICAgbm9kZXNlcDogMjAsXG4gICAgICAgICAgICAgIGVkZ2VzZXA6IDAsXG4gICAgICAgICAgICAgIHJhbmtzZXA6IDIwLFxuICAgICAgICAgICAgICByYW5rZGlyOiBcIkxSXCIsXG4gICAgICAgICAgICAgIG1hcmdpbng6IDEwLFxuICAgICAgICAgICAgICBtYXJnaW55OiAxMFxuICAgICAgICAgICAgfSk7XG4gICAgICAgICAgICBzdWJncmFwaHNbZWwuaWRdID0gc2c7XG4gICAgICAgICAgICBsb2FkSnNvblRvRGFncmUoc2csIGVsKTtcbiAgICAgICAgICAgIHIgPSBuZXcgZGFncmVEMy5yZW5kZXIoKTtcbiAgICAgICAgICAgIGQzdG1wU3ZnLnNlbGVjdCgnZycpLmNhbGwociwgc2cpO1xuICAgICAgICAgICAgbWF4VyA9IHNnLmdyYXBoKCkud2lkdGg7XG4gICAgICAgICAgICBtYXhIID0gc2cuZ3JhcGgoKS5oZWlnaHQ7XG4gICAgICAgICAgICBhbmd1bGFyLmVsZW1lbnQobWFpblRtcEVsZW1lbnQpLmVtcHR5KCk7XG4gICAgICAgICAgfVxuICAgICAgICAgIGNyZWF0ZU5vZGUoZywgZGF0YSwgZWwsIGlzUGFyZW50LCBtYXhXLCBtYXhIKTtcbiAgICAgICAgICBleGlzdGluZ05vZGVzLnB1c2goZWwuaWQpO1xuICAgICAgICAgIGlmIChlbC5pbnB1dHMgIT0gbnVsbCkge1xuICAgICAgICAgICAgcmVmID0gZWwuaW5wdXRzO1xuICAgICAgICAgICAgZm9yIChsID0gMCwgbGVuMSA9IHJlZi5sZW5ndGg7IGwgPCBsZW4xOyBsKyspIHtcbiAgICAgICAgICAgICAgcHJlZCA9IHJlZltsXTtcbiAgICAgICAgICAgICAgY3JlYXRlRWRnZShnLCBkYXRhLCBlbCwgZXhpc3RpbmdOb2RlcywgcHJlZCwgbWlzc2luZ05vZGVzKTtcbiAgICAgICAgICAgIH1cbiAgICAgICAgICB9XG4gICAgICAgIH1cbiAgICAgICAgcmV0dXJuIGc7XG4gICAgICB9O1xuICAgICAgc2VhcmNoRm9yTm9kZSA9IGZ1bmN0aW9uKGRhdGEsIG5vZGVJRCkge1xuICAgICAgICB2YXIgZWwsIGksIGo7XG4gICAgICAgIGZvciAoaSBpbiBkYXRhLm5vZGVzKSB7XG4gICAgICAgICAgZWwgPSBkYXRhLm5vZGVzW2ldO1xuICAgICAgICAgIGlmIChlbC5pZCA9PT0gbm9kZUlEKSB7XG4gICAgICAgICAgICByZXR1cm4gZWw7XG4gICAgICAgICAgfVxuICAgICAgICAgIGlmIChlbC5zdGVwX2Z1bmN0aW9uICE9IG51bGwpIHtcbiAgICAgICAgICAgIGZvciAoaiBpbiBlbC5zdGVwX2Z1bmN0aW9uKSB7XG4gICAgICAgICAgICAgIGlmIChlbC5zdGVwX2Z1bmN0aW9uW2pdLmlkID09PSBub2RlSUQpIHtcbiAgICAgICAgICAgICAgICByZXR1cm4gZWwuc3RlcF9mdW5jdGlvbltqXTtcbiAgICAgICAgICAgICAgfVxuICAgICAgICAgICAgfVxuICAgICAgICAgIH1cbiAgICAgICAgfVxuICAgICAgfTtcbiAgICAgIGRyYXdHcmFwaCA9IGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgICAgdmFyIGksIG5ld1NjYWxlLCByZW5kZXJlciwgc2csIHhDZW50ZXJPZmZzZXQsIHlDZW50ZXJPZmZzZXQ7XG4gICAgICAgIGcgPSBuZXcgZGFncmVEMy5ncmFwaGxpYi5HcmFwaCh7XG4gICAgICAgICAgbXVsdGlncmFwaDogdHJ1ZSxcbiAgICAgICAgICBjb21wb3VuZDogdHJ1ZVxuICAgICAgICB9KS5zZXRHcmFwaCh7XG4gICAgICAgICAgbm9kZXNlcDogNzAsXG4gICAgICAgICAgZWRnZXNlcDogMCxcbiAgICAgICAgICByYW5rc2VwOiA1MCxcbiAgICAgICAgICByYW5rZGlyOiBcIkxSXCIsXG4gICAgICAgICAgbWFyZ2lueDogNDAsXG4gICAgICAgICAgbWFyZ2lueTogNDBcbiAgICAgICAgfSk7XG4gICAgICAgIGxvYWRKc29uVG9EYWdyZShnLCBkYXRhKTtcbiAgICAgICAgcmVuZGVyZXIgPSBuZXcgZGFncmVEMy5yZW5kZXIoKTtcbiAgICAgICAgZDNtYWluU3ZnRy5jYWxsKHJlbmRlcmVyLCBnKTtcbiAgICAgICAgZm9yIChpIGluIHN1YmdyYXBocykge1xuICAgICAgICAgIHNnID0gc3ViZ3JhcGhzW2ldO1xuICAgICAgICAgIGQzbWFpblN2Zy5zZWxlY3QoJ3N2Zy5zdmctJyArIGkgKyAnIGcnKS5jYWxsKHJlbmRlcmVyLCBzZyk7XG4gICAgICAgIH1cbiAgICAgICAgbmV3U2NhbGUgPSAwLjU7XG4gICAgICAgIHhDZW50ZXJPZmZzZXQgPSBNYXRoLmZsb29yKChhbmd1bGFyLmVsZW1lbnQobWFpblN2Z0VsZW1lbnQpLndpZHRoKCkgLSBnLmdyYXBoKCkud2lkdGggKiBuZXdTY2FsZSkgLyAyKTtcbiAgICAgICAgeUNlbnRlck9mZnNldCA9IE1hdGguZmxvb3IoKGFuZ3VsYXIuZWxlbWVudChtYWluU3ZnRWxlbWVudCkuaGVpZ2h0KCkgLSBnLmdyYXBoKCkuaGVpZ2h0ICogbmV3U2NhbGUpIC8gMik7XG4gICAgICAgIG1haW5ab29tLnNjYWxlKG5ld1NjYWxlKS50cmFuc2xhdGUoW3hDZW50ZXJPZmZzZXQsIHlDZW50ZXJPZmZzZXRdKTtcbiAgICAgICAgZDNtYWluU3ZnRy5hdHRyKFwidHJhbnNmb3JtXCIsIFwidHJhbnNsYXRlKFwiICsgeENlbnRlck9mZnNldCArIFwiLCBcIiArIHlDZW50ZXJPZmZzZXQgKyBcIikgc2NhbGUoXCIgKyBtYWluWm9vbS5zY2FsZSgpICsgXCIpXCIpO1xuICAgICAgICBtYWluWm9vbS5vbihcInpvb21cIiwgZnVuY3Rpb24oKSB7XG4gICAgICAgICAgdmFyIGV2O1xuICAgICAgICAgIGV2ID0gZDMuZXZlbnQ7XG4gICAgICAgICAgcmV0dXJuIGQzbWFpblN2Z0cuYXR0cihcInRyYW5zZm9ybVwiLCBcInRyYW5zbGF0ZShcIiArIGV2LnRyYW5zbGF0ZSArIFwiKSBzY2FsZShcIiArIGV2LnNjYWxlICsgXCIpXCIpO1xuICAgICAgICB9KTtcbiAgICAgICAgbWFpblpvb20oZDNtYWluU3ZnKTtcbiAgICAgICAgcmV0dXJuIGQzbWFpblN2Z0cuc2VsZWN0QWxsKCcubm9kZScpLm9uKCdjbGljaycsIGZ1bmN0aW9uKGQpIHtcbiAgICAgICAgICByZXR1cm4gc2NvcGUuc2V0Tm9kZSh7XG4gICAgICAgICAgICBub2RlaWQ6IGRcbiAgICAgICAgICB9KTtcbiAgICAgICAgfSk7XG4gICAgICB9O1xuICAgICAgc2NvcGUuJHdhdGNoKGF0dHJzLnBsYW4sIGZ1bmN0aW9uKG5ld1BsYW4pIHtcbiAgICAgICAgaWYgKG5ld1BsYW4pIHtcbiAgICAgICAgICByZXR1cm4gZHJhd0dyYXBoKG5ld1BsYW4pO1xuICAgICAgICB9XG4gICAgICB9KTtcbiAgICB9XG4gIH07XG59KTtcbiIsIiNcclxuIyBMaWNlbnNlZCB0byB0aGUgQXBhY2hlIFNvZnR3YXJlIEZvdW5kYXRpb24gKEFTRikgdW5kZXIgb25lXHJcbiMgb3IgbW9yZSBjb250cmlidXRvciBsaWNlbnNlIGFncmVlbWVudHMuICBTZWUgdGhlIE5PVElDRSBmaWxlXHJcbiMgZGlzdHJpYnV0ZWQgd2l0aCB0aGlzIHdvcmsgZm9yIGFkZGl0aW9uYWwgaW5mb3JtYXRpb25cclxuIyByZWdhcmRpbmcgY29weXJpZ2h0IG93bmVyc2hpcC4gIFRoZSBBU0YgbGljZW5zZXMgdGhpcyBmaWxlXHJcbiMgdG8geW91IHVuZGVyIHRoZSBBcGFjaGUgTGljZW5zZSwgVmVyc2lvbiAyLjAgKHRoZVxyXG4jIFwiTGljZW5zZVwiKTsgeW91IG1heSBub3QgdXNlIHRoaXMgZmlsZSBleGNlcHQgaW4gY29tcGxpYW5jZVxyXG4jIHdpdGggdGhlIExpY2Vuc2UuICBZb3UgbWF5IG9idGFpbiBhIGNvcHkgb2YgdGhlIExpY2Vuc2UgYXRcclxuI1xyXG4jICAgICBodHRwOi8vd3d3LmFwYWNoZS5vcmcvbGljZW5zZXMvTElDRU5TRS0yLjBcclxuI1xyXG4jIFVubGVzcyByZXF1aXJlZCBieSBhcHBsaWNhYmxlIGxhdyBvciBhZ3JlZWQgdG8gaW4gd3JpdGluZywgc29mdHdhcmVcclxuIyBkaXN0cmlidXRlZCB1bmRlciB0aGUgTGljZW5zZSBpcyBkaXN0cmlidXRlZCBvbiBhbiBcIkFTIElTXCIgQkFTSVMsXHJcbiMgV0lUSE9VVCBXQVJSQU5USUVTIE9SIENPTkRJVElPTlMgT0YgQU5ZIEtJTkQsIGVpdGhlciBleHByZXNzIG9yIGltcGxpZWQuXHJcbiMgU2VlIHRoZSBMaWNlbnNlIGZvciB0aGUgc3BlY2lmaWMgbGFuZ3VhZ2UgZ292ZXJuaW5nIHBlcm1pc3Npb25zIGFuZFxyXG4jIGxpbWl0YXRpb25zIHVuZGVyIHRoZSBMaWNlbnNlLlxyXG4jXHJcblxyXG5hbmd1bGFyLm1vZHVsZSgnZmxpbmtBcHAnKVxyXG5cclxuLnNlcnZpY2UgJ0pvYnNTZXJ2aWNlJywgKCRodHRwLCBmbGlua0NvbmZpZywgJGxvZywgYW1Nb21lbnQsICRxLCAkdGltZW91dCkgLT5cclxuICBjdXJyZW50Sm9iID0gbnVsbFxyXG4gIGN1cnJlbnRQbGFuID0gbnVsbFxyXG5cclxuICBkZWZlcnJlZHMgPSB7fVxyXG4gIGpvYnMgPSB7XHJcbiAgICBydW5uaW5nOiBbXVxyXG4gICAgZmluaXNoZWQ6IFtdXHJcbiAgICBjYW5jZWxsZWQ6IFtdXHJcbiAgICBmYWlsZWQ6IFtdXHJcbiAgfVxyXG5cclxuICBqb2JPYnNlcnZlcnMgPSBbXVxyXG5cclxuICBub3RpZnlPYnNlcnZlcnMgPSAtPlxyXG4gICAgYW5ndWxhci5mb3JFYWNoIGpvYk9ic2VydmVycywgKGNhbGxiYWNrKSAtPlxyXG4gICAgICBjYWxsYmFjaygpXHJcblxyXG4gIEByZWdpc3Rlck9ic2VydmVyID0gKGNhbGxiYWNrKSAtPlxyXG4gICAgam9iT2JzZXJ2ZXJzLnB1c2goY2FsbGJhY2spXHJcblxyXG4gIEB1blJlZ2lzdGVyT2JzZXJ2ZXIgPSAoY2FsbGJhY2spIC0+XHJcbiAgICBpbmRleCA9IGpvYk9ic2VydmVycy5pbmRleE9mKGNhbGxiYWNrKVxyXG4gICAgam9iT2JzZXJ2ZXJzLnNwbGljZShpbmRleCwgMSlcclxuXHJcbiAgQHN0YXRlTGlzdCA9IC0+XHJcbiAgICBbIFxyXG4gICAgICAjICdDUkVBVEVEJ1xyXG4gICAgICAnU0NIRURVTEVEJ1xyXG4gICAgICAnREVQTE9ZSU5HJ1xyXG4gICAgICAnUlVOTklORydcclxuICAgICAgJ0ZJTklTSEVEJ1xyXG4gICAgICAnRkFJTEVEJ1xyXG4gICAgICAnQ0FOQ0VMSU5HJ1xyXG4gICAgICAnQ0FOQ0VMRUQnXHJcbiAgICBdXHJcblxyXG4gIEB0cmFuc2xhdGVMYWJlbFN0YXRlID0gKHN0YXRlKSAtPlxyXG4gICAgc3dpdGNoIHN0YXRlLnRvTG93ZXJDYXNlKClcclxuICAgICAgd2hlbiAnZmluaXNoZWQnIHRoZW4gJ3N1Y2Nlc3MnXHJcbiAgICAgIHdoZW4gJ2ZhaWxlZCcgdGhlbiAnZGFuZ2VyJ1xyXG4gICAgICB3aGVuICdzY2hlZHVsZWQnIHRoZW4gJ2RlZmF1bHQnXHJcbiAgICAgIHdoZW4gJ2RlcGxveWluZycgdGhlbiAnaW5mbydcclxuICAgICAgd2hlbiAncnVubmluZycgdGhlbiAncHJpbWFyeSdcclxuICAgICAgd2hlbiAnY2FuY2VsaW5nJyB0aGVuICd3YXJuaW5nJ1xyXG4gICAgICB3aGVuICdwZW5kaW5nJyB0aGVuICdpbmZvJ1xyXG4gICAgICB3aGVuICd0b3RhbCcgdGhlbiAnYmxhY2snXHJcbiAgICAgIGVsc2UgJ2RlZmF1bHQnXHJcblxyXG4gIEBzZXRFbmRUaW1lcyA9IChsaXN0KSAtPlxyXG4gICAgYW5ndWxhci5mb3JFYWNoIGxpc3QsIChpdGVtLCBqb2JLZXkpIC0+XHJcbiAgICAgIHVubGVzcyBpdGVtWydlbmQtdGltZSddID4gLTFcclxuICAgICAgICBpdGVtWydlbmQtdGltZSddID0gaXRlbVsnc3RhcnQtdGltZSddICsgaXRlbVsnZHVyYXRpb24nXVxyXG5cclxuICBAcHJvY2Vzc1ZlcnRpY2VzID0gKGRhdGEpIC0+XHJcbiAgICBhbmd1bGFyLmZvckVhY2ggZGF0YS52ZXJ0aWNlcywgKHZlcnRleCwgaSkgLT5cclxuICAgICAgdmVydGV4LnR5cGUgPSAncmVndWxhcidcclxuXHJcbiAgICBkYXRhLnZlcnRpY2VzLnVuc2hpZnQoe1xyXG4gICAgICBuYW1lOiAnU2NoZWR1bGVkJ1xyXG4gICAgICAnc3RhcnQtdGltZSc6IGRhdGEudGltZXN0YW1wc1snQ1JFQVRFRCddXHJcbiAgICAgICdlbmQtdGltZSc6IGRhdGEudGltZXN0YW1wc1snQ1JFQVRFRCddICsgMVxyXG4gICAgICB0eXBlOiAnc2NoZWR1bGVkJ1xyXG4gICAgfSlcclxuXHJcbiAgQGxpc3RKb2JzID0gLT5cclxuICAgIGRlZmVycmVkID0gJHEuZGVmZXIoKVxyXG5cclxuICAgICRodHRwLmdldCBcIi9qb2JvdmVydmlld1wiXHJcbiAgICAuc3VjY2VzcyAoZGF0YSwgc3RhdHVzLCBoZWFkZXJzLCBjb25maWcpID0+XHJcbiAgICAgIGFuZ3VsYXIuZm9yRWFjaCBkYXRhLCAobGlzdCwgbGlzdEtleSkgPT5cclxuICAgICAgICBzd2l0Y2ggbGlzdEtleVxyXG4gICAgICAgICAgd2hlbiAncnVubmluZycgdGhlbiBqb2JzLnJ1bm5pbmcgPSBAc2V0RW5kVGltZXMobGlzdClcclxuICAgICAgICAgIHdoZW4gJ2ZpbmlzaGVkJyB0aGVuIGpvYnMuZmluaXNoZWQgPSBAc2V0RW5kVGltZXMobGlzdClcclxuICAgICAgICAgIHdoZW4gJ2NhbmNlbGxlZCcgdGhlbiBqb2JzLmNhbmNlbGxlZCA9IEBzZXRFbmRUaW1lcyhsaXN0KVxyXG4gICAgICAgICAgd2hlbiAnZmFpbGVkJyB0aGVuIGpvYnMuZmFpbGVkID0gQHNldEVuZFRpbWVzKGxpc3QpXHJcblxyXG4gICAgICBkZWZlcnJlZC5yZXNvbHZlKGpvYnMpXHJcbiAgICAgIG5vdGlmeU9ic2VydmVycygpXHJcblxyXG4gICAgZGVmZXJyZWQucHJvbWlzZVxyXG5cclxuICBAZ2V0Sm9icyA9ICh0eXBlKSAtPlxyXG4gICAgam9ic1t0eXBlXVxyXG5cclxuICBAZ2V0QWxsSm9icyA9IC0+XHJcbiAgICBqb2JzXHJcblxyXG4gIEBsb2FkSm9iID0gKGpvYmlkKSAtPlxyXG4gICAgY3VycmVudEpvYiA9IG51bGxcclxuICAgIGRlZmVycmVkcy5qb2IgPSAkcS5kZWZlcigpXHJcblxyXG4gICAgJGh0dHAuZ2V0IFwiL2pvYnMvXCIgKyBqb2JpZFxyXG4gICAgLnN1Y2Nlc3MgKGRhdGEsIHN0YXR1cywgaGVhZGVycywgY29uZmlnKSA9PlxyXG4gICAgICBAc2V0RW5kVGltZXMoZGF0YS52ZXJ0aWNlcylcclxuICAgICAgQHByb2Nlc3NWZXJ0aWNlcyhkYXRhKVxyXG5cclxuICAgICAgJGh0dHAuZ2V0IFwiL2pvYnMvXCIgKyBqb2JpZCArIFwiL2NvbmZpZ1wiXHJcbiAgICAgIC5zdWNjZXNzIChqb2JDb25maWcpIC0+XHJcbiAgICAgICAgZGF0YSA9IGFuZ3VsYXIuZXh0ZW5kKGRhdGEsIGpvYkNvbmZpZylcclxuXHJcbiAgICAgICAgY3VycmVudEpvYiA9IGRhdGFcclxuXHJcbiAgICAgICAgZGVmZXJyZWRzLmpvYi5yZXNvbHZlKGN1cnJlbnRKb2IpXHJcblxyXG4gICAgZGVmZXJyZWRzLmpvYi5wcm9taXNlXHJcblxyXG4gIEBnZXROb2RlID0gKG5vZGVpZCkgLT5cclxuICAgIHNlZWtOb2RlID0gKG5vZGVpZCwgZGF0YSkgLT5cclxuICAgICAgZm9yIG5vZGUgaW4gZGF0YVxyXG4gICAgICAgIHJldHVybiBub2RlIGlmIG5vZGUuaWQgaXMgbm9kZWlkXHJcbiAgICAgICAgc3ViID0gc2Vla05vZGUobm9kZWlkLCBub2RlLnN0ZXBfZnVuY3Rpb24pIGlmIG5vZGUuc3RlcF9mdW5jdGlvblxyXG4gICAgICAgIHJldHVybiBzdWIgaWYgc3ViXHJcblxyXG4gICAgICBudWxsXHJcblxyXG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpXHJcblxyXG4gICAgZGVmZXJyZWRzLmpvYi5wcm9taXNlLnRoZW4gKGRhdGEpID0+XHJcbiAgICAgIGZvdW5kTm9kZSA9IHNlZWtOb2RlKG5vZGVpZCwgY3VycmVudEpvYi5wbGFuLm5vZGVzKVxyXG5cclxuICAgICAgZm91bmROb2RlLnZlcnRleCA9IEBzZWVrVmVydGV4KG5vZGVpZClcclxuXHJcbiAgICAgIGRlZmVycmVkLnJlc29sdmUoZm91bmROb2RlKVxyXG5cclxuICAgIGRlZmVycmVkLnByb21pc2VcclxuXHJcbiAgQHNlZWtWZXJ0ZXggPSAobm9kZWlkKSAtPlxyXG4gICAgZm9yIHZlcnRleCBpbiBjdXJyZW50Sm9iLnZlcnRpY2VzXHJcbiAgICAgIHJldHVybiB2ZXJ0ZXggaWYgdmVydGV4LmlkIGlzIG5vZGVpZFxyXG5cclxuICAgIHJldHVybiBudWxsXHJcblxyXG4gIEBnZXRWZXJ0ZXggPSAodmVydGV4aWQpIC0+XHJcbiAgICBkZWZlcnJlZCA9ICRxLmRlZmVyKClcclxuXHJcbiAgICBkZWZlcnJlZHMuam9iLnByb21pc2UudGhlbiAoZGF0YSkgPT5cclxuICAgICAgdmVydGV4ID0gQHNlZWtWZXJ0ZXgodmVydGV4aWQpXHJcblxyXG4gICAgICAkaHR0cC5nZXQgXCIvam9icy9cIiArIGN1cnJlbnRKb2IuamlkICsgXCIvdmVydGljZXMvXCIgKyB2ZXJ0ZXhpZCArIFwiL3N1YnRhc2t0aW1lc1wiXHJcbiAgICAgIC5zdWNjZXNzIChkYXRhKSA9PlxyXG4gICAgICAgICMgVE9ETzogY2hhbmdlIHRvIHN1YnRhc2t0aW1lc1xyXG4gICAgICAgIHZlcnRleC5zdWJ0YXNrcyA9IGRhdGEuc3VidGFza3NcclxuXHJcbiAgICAgICAgZGVmZXJyZWQucmVzb2x2ZSh2ZXJ0ZXgpXHJcblxyXG4gICAgZGVmZXJyZWQucHJvbWlzZVxyXG5cclxuICBAZ2V0U3VidGFza3MgPSAodmVydGV4aWQpIC0+XHJcbiAgICBkZWZlcnJlZCA9ICRxLmRlZmVyKClcclxuXHJcbiAgICBkZWZlcnJlZHMuam9iLnByb21pc2UudGhlbiAoZGF0YSkgPT5cclxuICAgICAgIyB2ZXJ0ZXggPSBAc2Vla1ZlcnRleCh2ZXJ0ZXhpZClcclxuXHJcbiAgICAgICRodHRwLmdldCBcIi9qb2JzL1wiICsgY3VycmVudEpvYi5qaWQgKyBcIi92ZXJ0aWNlcy9cIiArIHZlcnRleGlkXHJcbiAgICAgIC5zdWNjZXNzIChkYXRhKSAtPlxyXG4gICAgICAgIHN1YnRhc2tzID0gZGF0YS5zdWJ0YXNrc1xyXG5cclxuICAgICAgICBkZWZlcnJlZC5yZXNvbHZlKHN1YnRhc2tzKVxyXG5cclxuICAgIGRlZmVycmVkLnByb21pc2VcclxuXHJcbiAgQGdldEFjY3VtdWxhdG9ycyA9ICh2ZXJ0ZXhpZCkgLT5cclxuICAgIGRlZmVycmVkID0gJHEuZGVmZXIoKVxyXG5cclxuICAgIGRlZmVycmVkcy5qb2IucHJvbWlzZS50aGVuIChkYXRhKSA9PlxyXG4gICAgICAjIHZlcnRleCA9IEBzZWVrVmVydGV4KHZlcnRleGlkKVxyXG5cclxuICAgICAgJGh0dHAuZ2V0IFwiL2pvYnMvXCIgKyBjdXJyZW50Sm9iLmppZCArIFwiL3ZlcnRpY2VzL1wiICsgdmVydGV4aWQgKyBcIi9hY2N1bXVsYXRvcnNcIlxyXG4gICAgICAuc3VjY2VzcyAoZGF0YSkgLT5cclxuICAgICAgICBhY2N1bXVsYXRvcnMgPSBkYXRhWyd1c2VyLWFjY3VtdWxhdG9ycyddXHJcblxyXG4gICAgICAgICRodHRwLmdldCBcIi9qb2JzL1wiICsgY3VycmVudEpvYi5qaWQgKyBcIi92ZXJ0aWNlcy9cIiArIHZlcnRleGlkICsgXCIvc3VidGFza3MvYWNjdW11bGF0b3JzXCJcclxuICAgICAgICAuc3VjY2VzcyAoZGF0YSkgLT5cclxuICAgICAgICAgIHN1YnRhc2tBY2N1bXVsYXRvcnMgPSBkYXRhLnN1YnRhc2tzXHJcblxyXG4gICAgICAgICAgZGVmZXJyZWQucmVzb2x2ZSh7IG1haW46IGFjY3VtdWxhdG9ycywgc3VidGFza3M6IHN1YnRhc2tBY2N1bXVsYXRvcnMgfSlcclxuXHJcbiAgICBkZWZlcnJlZC5wcm9taXNlXHJcblxyXG4gIEBsb2FkRXhjZXB0aW9ucyA9IC0+XHJcbiAgICBkZWZlcnJlZCA9ICRxLmRlZmVyKClcclxuXHJcbiAgICBkZWZlcnJlZHMuam9iLnByb21pc2UudGhlbiAoZGF0YSkgPT5cclxuXHJcbiAgICAgICRodHRwLmdldCBcIi9qb2JzL1wiICsgY3VycmVudEpvYi5qaWQgKyBcIi9leGNlcHRpb25zXCJcclxuICAgICAgLnN1Y2Nlc3MgKGV4Y2VwdGlvbnMpIC0+XHJcbiAgICAgICAgY3VycmVudEpvYi5leGNlcHRpb25zID0gZXhjZXB0aW9uc1xyXG5cclxuICAgICAgICBkZWZlcnJlZC5yZXNvbHZlKGV4Y2VwdGlvbnMpXHJcblxyXG4gICAgZGVmZXJyZWQucHJvbWlzZVxyXG5cclxuICBAXHJcbiIsImFuZ3VsYXIubW9kdWxlKCdmbGlua0FwcCcpLnNlcnZpY2UoJ0pvYnNTZXJ2aWNlJywgZnVuY3Rpb24oJGh0dHAsIGZsaW5rQ29uZmlnLCAkbG9nLCBhbU1vbWVudCwgJHEsICR0aW1lb3V0KSB7XG4gIHZhciBjdXJyZW50Sm9iLCBjdXJyZW50UGxhbiwgZGVmZXJyZWRzLCBqb2JPYnNlcnZlcnMsIGpvYnMsIG5vdGlmeU9ic2VydmVycztcbiAgY3VycmVudEpvYiA9IG51bGw7XG4gIGN1cnJlbnRQbGFuID0gbnVsbDtcbiAgZGVmZXJyZWRzID0ge307XG4gIGpvYnMgPSB7XG4gICAgcnVubmluZzogW10sXG4gICAgZmluaXNoZWQ6IFtdLFxuICAgIGNhbmNlbGxlZDogW10sXG4gICAgZmFpbGVkOiBbXVxuICB9O1xuICBqb2JPYnNlcnZlcnMgPSBbXTtcbiAgbm90aWZ5T2JzZXJ2ZXJzID0gZnVuY3Rpb24oKSB7XG4gICAgcmV0dXJuIGFuZ3VsYXIuZm9yRWFjaChqb2JPYnNlcnZlcnMsIGZ1bmN0aW9uKGNhbGxiYWNrKSB7XG4gICAgICByZXR1cm4gY2FsbGJhY2soKTtcbiAgICB9KTtcbiAgfTtcbiAgdGhpcy5yZWdpc3Rlck9ic2VydmVyID0gZnVuY3Rpb24oY2FsbGJhY2spIHtcbiAgICByZXR1cm4gam9iT2JzZXJ2ZXJzLnB1c2goY2FsbGJhY2spO1xuICB9O1xuICB0aGlzLnVuUmVnaXN0ZXJPYnNlcnZlciA9IGZ1bmN0aW9uKGNhbGxiYWNrKSB7XG4gICAgdmFyIGluZGV4O1xuICAgIGluZGV4ID0gam9iT2JzZXJ2ZXJzLmluZGV4T2YoY2FsbGJhY2spO1xuICAgIHJldHVybiBqb2JPYnNlcnZlcnMuc3BsaWNlKGluZGV4LCAxKTtcbiAgfTtcbiAgdGhpcy5zdGF0ZUxpc3QgPSBmdW5jdGlvbigpIHtcbiAgICByZXR1cm4gWydTQ0hFRFVMRUQnLCAnREVQTE9ZSU5HJywgJ1JVTk5JTkcnLCAnRklOSVNIRUQnLCAnRkFJTEVEJywgJ0NBTkNFTElORycsICdDQU5DRUxFRCddO1xuICB9O1xuICB0aGlzLnRyYW5zbGF0ZUxhYmVsU3RhdGUgPSBmdW5jdGlvbihzdGF0ZSkge1xuICAgIHN3aXRjaCAoc3RhdGUudG9Mb3dlckNhc2UoKSkge1xuICAgICAgY2FzZSAnZmluaXNoZWQnOlxuICAgICAgICByZXR1cm4gJ3N1Y2Nlc3MnO1xuICAgICAgY2FzZSAnZmFpbGVkJzpcbiAgICAgICAgcmV0dXJuICdkYW5nZXInO1xuICAgICAgY2FzZSAnc2NoZWR1bGVkJzpcbiAgICAgICAgcmV0dXJuICdkZWZhdWx0JztcbiAgICAgIGNhc2UgJ2RlcGxveWluZyc6XG4gICAgICAgIHJldHVybiAnaW5mbyc7XG4gICAgICBjYXNlICdydW5uaW5nJzpcbiAgICAgICAgcmV0dXJuICdwcmltYXJ5JztcbiAgICAgIGNhc2UgJ2NhbmNlbGluZyc6XG4gICAgICAgIHJldHVybiAnd2FybmluZyc7XG4gICAgICBjYXNlICdwZW5kaW5nJzpcbiAgICAgICAgcmV0dXJuICdpbmZvJztcbiAgICAgIGNhc2UgJ3RvdGFsJzpcbiAgICAgICAgcmV0dXJuICdibGFjayc7XG4gICAgICBkZWZhdWx0OlxuICAgICAgICByZXR1cm4gJ2RlZmF1bHQnO1xuICAgIH1cbiAgfTtcbiAgdGhpcy5zZXRFbmRUaW1lcyA9IGZ1bmN0aW9uKGxpc3QpIHtcbiAgICByZXR1cm4gYW5ndWxhci5mb3JFYWNoKGxpc3QsIGZ1bmN0aW9uKGl0ZW0sIGpvYktleSkge1xuICAgICAgaWYgKCEoaXRlbVsnZW5kLXRpbWUnXSA+IC0xKSkge1xuICAgICAgICByZXR1cm4gaXRlbVsnZW5kLXRpbWUnXSA9IGl0ZW1bJ3N0YXJ0LXRpbWUnXSArIGl0ZW1bJ2R1cmF0aW9uJ107XG4gICAgICB9XG4gICAgfSk7XG4gIH07XG4gIHRoaXMucHJvY2Vzc1ZlcnRpY2VzID0gZnVuY3Rpb24oZGF0YSkge1xuICAgIGFuZ3VsYXIuZm9yRWFjaChkYXRhLnZlcnRpY2VzLCBmdW5jdGlvbih2ZXJ0ZXgsIGkpIHtcbiAgICAgIHJldHVybiB2ZXJ0ZXgudHlwZSA9ICdyZWd1bGFyJztcbiAgICB9KTtcbiAgICByZXR1cm4gZGF0YS52ZXJ0aWNlcy51bnNoaWZ0KHtcbiAgICAgIG5hbWU6ICdTY2hlZHVsZWQnLFxuICAgICAgJ3N0YXJ0LXRpbWUnOiBkYXRhLnRpbWVzdGFtcHNbJ0NSRUFURUQnXSxcbiAgICAgICdlbmQtdGltZSc6IGRhdGEudGltZXN0YW1wc1snQ1JFQVRFRCddICsgMSxcbiAgICAgIHR5cGU6ICdzY2hlZHVsZWQnXG4gICAgfSk7XG4gIH07XG4gIHRoaXMubGlzdEpvYnMgPSBmdW5jdGlvbigpIHtcbiAgICB2YXIgZGVmZXJyZWQ7XG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpO1xuICAgICRodHRwLmdldChcIi9qb2JvdmVydmlld1wiKS5zdWNjZXNzKChmdW5jdGlvbihfdGhpcykge1xuICAgICAgcmV0dXJuIGZ1bmN0aW9uKGRhdGEsIHN0YXR1cywgaGVhZGVycywgY29uZmlnKSB7XG4gICAgICAgIGFuZ3VsYXIuZm9yRWFjaChkYXRhLCBmdW5jdGlvbihsaXN0LCBsaXN0S2V5KSB7XG4gICAgICAgICAgc3dpdGNoIChsaXN0S2V5KSB7XG4gICAgICAgICAgICBjYXNlICdydW5uaW5nJzpcbiAgICAgICAgICAgICAgcmV0dXJuIGpvYnMucnVubmluZyA9IF90aGlzLnNldEVuZFRpbWVzKGxpc3QpO1xuICAgICAgICAgICAgY2FzZSAnZmluaXNoZWQnOlxuICAgICAgICAgICAgICByZXR1cm4gam9icy5maW5pc2hlZCA9IF90aGlzLnNldEVuZFRpbWVzKGxpc3QpO1xuICAgICAgICAgICAgY2FzZSAnY2FuY2VsbGVkJzpcbiAgICAgICAgICAgICAgcmV0dXJuIGpvYnMuY2FuY2VsbGVkID0gX3RoaXMuc2V0RW5kVGltZXMobGlzdCk7XG4gICAgICAgICAgICBjYXNlICdmYWlsZWQnOlxuICAgICAgICAgICAgICByZXR1cm4gam9icy5mYWlsZWQgPSBfdGhpcy5zZXRFbmRUaW1lcyhsaXN0KTtcbiAgICAgICAgICB9XG4gICAgICAgIH0pO1xuICAgICAgICBkZWZlcnJlZC5yZXNvbHZlKGpvYnMpO1xuICAgICAgICByZXR1cm4gbm90aWZ5T2JzZXJ2ZXJzKCk7XG4gICAgICB9O1xuICAgIH0pKHRoaXMpKTtcbiAgICByZXR1cm4gZGVmZXJyZWQucHJvbWlzZTtcbiAgfTtcbiAgdGhpcy5nZXRKb2JzID0gZnVuY3Rpb24odHlwZSkge1xuICAgIHJldHVybiBqb2JzW3R5cGVdO1xuICB9O1xuICB0aGlzLmdldEFsbEpvYnMgPSBmdW5jdGlvbigpIHtcbiAgICByZXR1cm4gam9icztcbiAgfTtcbiAgdGhpcy5sb2FkSm9iID0gZnVuY3Rpb24oam9iaWQpIHtcbiAgICBjdXJyZW50Sm9iID0gbnVsbDtcbiAgICBkZWZlcnJlZHMuam9iID0gJHEuZGVmZXIoKTtcbiAgICAkaHR0cC5nZXQoXCIvam9icy9cIiArIGpvYmlkKS5zdWNjZXNzKChmdW5jdGlvbihfdGhpcykge1xuICAgICAgcmV0dXJuIGZ1bmN0aW9uKGRhdGEsIHN0YXR1cywgaGVhZGVycywgY29uZmlnKSB7XG4gICAgICAgIF90aGlzLnNldEVuZFRpbWVzKGRhdGEudmVydGljZXMpO1xuICAgICAgICBfdGhpcy5wcm9jZXNzVmVydGljZXMoZGF0YSk7XG4gICAgICAgIHJldHVybiAkaHR0cC5nZXQoXCIvam9icy9cIiArIGpvYmlkICsgXCIvY29uZmlnXCIpLnN1Y2Nlc3MoZnVuY3Rpb24oam9iQ29uZmlnKSB7XG4gICAgICAgICAgZGF0YSA9IGFuZ3VsYXIuZXh0ZW5kKGRhdGEsIGpvYkNvbmZpZyk7XG4gICAgICAgICAgY3VycmVudEpvYiA9IGRhdGE7XG4gICAgICAgICAgcmV0dXJuIGRlZmVycmVkcy5qb2IucmVzb2x2ZShjdXJyZW50Sm9iKTtcbiAgICAgICAgfSk7XG4gICAgICB9O1xuICAgIH0pKHRoaXMpKTtcbiAgICByZXR1cm4gZGVmZXJyZWRzLmpvYi5wcm9taXNlO1xuICB9O1xuICB0aGlzLmdldE5vZGUgPSBmdW5jdGlvbihub2RlaWQpIHtcbiAgICB2YXIgZGVmZXJyZWQsIHNlZWtOb2RlO1xuICAgIHNlZWtOb2RlID0gZnVuY3Rpb24obm9kZWlkLCBkYXRhKSB7XG4gICAgICB2YXIgaiwgbGVuLCBub2RlLCBzdWI7XG4gICAgICBmb3IgKGogPSAwLCBsZW4gPSBkYXRhLmxlbmd0aDsgaiA8IGxlbjsgaisrKSB7XG4gICAgICAgIG5vZGUgPSBkYXRhW2pdO1xuICAgICAgICBpZiAobm9kZS5pZCA9PT0gbm9kZWlkKSB7XG4gICAgICAgICAgcmV0dXJuIG5vZGU7XG4gICAgICAgIH1cbiAgICAgICAgaWYgKG5vZGUuc3RlcF9mdW5jdGlvbikge1xuICAgICAgICAgIHN1YiA9IHNlZWtOb2RlKG5vZGVpZCwgbm9kZS5zdGVwX2Z1bmN0aW9uKTtcbiAgICAgICAgfVxuICAgICAgICBpZiAoc3ViKSB7XG4gICAgICAgICAgcmV0dXJuIHN1YjtcbiAgICAgICAgfVxuICAgICAgfVxuICAgICAgcmV0dXJuIG51bGw7XG4gICAgfTtcbiAgICBkZWZlcnJlZCA9ICRxLmRlZmVyKCk7XG4gICAgZGVmZXJyZWRzLmpvYi5wcm9taXNlLnRoZW4oKGZ1bmN0aW9uKF90aGlzKSB7XG4gICAgICByZXR1cm4gZnVuY3Rpb24oZGF0YSkge1xuICAgICAgICB2YXIgZm91bmROb2RlO1xuICAgICAgICBmb3VuZE5vZGUgPSBzZWVrTm9kZShub2RlaWQsIGN1cnJlbnRKb2IucGxhbi5ub2Rlcyk7XG4gICAgICAgIGZvdW5kTm9kZS52ZXJ0ZXggPSBfdGhpcy5zZWVrVmVydGV4KG5vZGVpZCk7XG4gICAgICAgIHJldHVybiBkZWZlcnJlZC5yZXNvbHZlKGZvdW5kTm9kZSk7XG4gICAgICB9O1xuICAgIH0pKHRoaXMpKTtcbiAgICByZXR1cm4gZGVmZXJyZWQucHJvbWlzZTtcbiAgfTtcbiAgdGhpcy5zZWVrVmVydGV4ID0gZnVuY3Rpb24obm9kZWlkKSB7XG4gICAgdmFyIGosIGxlbiwgcmVmLCB2ZXJ0ZXg7XG4gICAgcmVmID0gY3VycmVudEpvYi52ZXJ0aWNlcztcbiAgICBmb3IgKGogPSAwLCBsZW4gPSByZWYubGVuZ3RoOyBqIDwgbGVuOyBqKyspIHtcbiAgICAgIHZlcnRleCA9IHJlZltqXTtcbiAgICAgIGlmICh2ZXJ0ZXguaWQgPT09IG5vZGVpZCkge1xuICAgICAgICByZXR1cm4gdmVydGV4O1xuICAgICAgfVxuICAgIH1cbiAgICByZXR1cm4gbnVsbDtcbiAgfTtcbiAgdGhpcy5nZXRWZXJ0ZXggPSBmdW5jdGlvbih2ZXJ0ZXhpZCkge1xuICAgIHZhciBkZWZlcnJlZDtcbiAgICBkZWZlcnJlZCA9ICRxLmRlZmVyKCk7XG4gICAgZGVmZXJyZWRzLmpvYi5wcm9taXNlLnRoZW4oKGZ1bmN0aW9uKF90aGlzKSB7XG4gICAgICByZXR1cm4gZnVuY3Rpb24oZGF0YSkge1xuICAgICAgICB2YXIgdmVydGV4O1xuICAgICAgICB2ZXJ0ZXggPSBfdGhpcy5zZWVrVmVydGV4KHZlcnRleGlkKTtcbiAgICAgICAgcmV0dXJuICRodHRwLmdldChcIi9qb2JzL1wiICsgY3VycmVudEpvYi5qaWQgKyBcIi92ZXJ0aWNlcy9cIiArIHZlcnRleGlkICsgXCIvc3VidGFza3RpbWVzXCIpLnN1Y2Nlc3MoZnVuY3Rpb24oZGF0YSkge1xuICAgICAgICAgIHZlcnRleC5zdWJ0YXNrcyA9IGRhdGEuc3VidGFza3M7XG4gICAgICAgICAgcmV0dXJuIGRlZmVycmVkLnJlc29sdmUodmVydGV4KTtcbiAgICAgICAgfSk7XG4gICAgICB9O1xuICAgIH0pKHRoaXMpKTtcbiAgICByZXR1cm4gZGVmZXJyZWQucHJvbWlzZTtcbiAgfTtcbiAgdGhpcy5nZXRTdWJ0YXNrcyA9IGZ1bmN0aW9uKHZlcnRleGlkKSB7XG4gICAgdmFyIGRlZmVycmVkO1xuICAgIGRlZmVycmVkID0gJHEuZGVmZXIoKTtcbiAgICBkZWZlcnJlZHMuam9iLnByb21pc2UudGhlbigoZnVuY3Rpb24oX3RoaXMpIHtcbiAgICAgIHJldHVybiBmdW5jdGlvbihkYXRhKSB7XG4gICAgICAgIHJldHVybiAkaHR0cC5nZXQoXCIvam9icy9cIiArIGN1cnJlbnRKb2IuamlkICsgXCIvdmVydGljZXMvXCIgKyB2ZXJ0ZXhpZCkuc3VjY2VzcyhmdW5jdGlvbihkYXRhKSB7XG4gICAgICAgICAgdmFyIHN1YnRhc2tzO1xuICAgICAgICAgIHN1YnRhc2tzID0gZGF0YS5zdWJ0YXNrcztcbiAgICAgICAgICByZXR1cm4gZGVmZXJyZWQucmVzb2x2ZShzdWJ0YXNrcyk7XG4gICAgICAgIH0pO1xuICAgICAgfTtcbiAgICB9KSh0aGlzKSk7XG4gICAgcmV0dXJuIGRlZmVycmVkLnByb21pc2U7XG4gIH07XG4gIHRoaXMuZ2V0QWNjdW11bGF0b3JzID0gZnVuY3Rpb24odmVydGV4aWQpIHtcbiAgICB2YXIgZGVmZXJyZWQ7XG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpO1xuICAgIGRlZmVycmVkcy5qb2IucHJvbWlzZS50aGVuKChmdW5jdGlvbihfdGhpcykge1xuICAgICAgcmV0dXJuIGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgICAgcmV0dXJuICRodHRwLmdldChcIi9qb2JzL1wiICsgY3VycmVudEpvYi5qaWQgKyBcIi92ZXJ0aWNlcy9cIiArIHZlcnRleGlkICsgXCIvYWNjdW11bGF0b3JzXCIpLnN1Y2Nlc3MoZnVuY3Rpb24oZGF0YSkge1xuICAgICAgICAgIHZhciBhY2N1bXVsYXRvcnM7XG4gICAgICAgICAgYWNjdW11bGF0b3JzID0gZGF0YVsndXNlci1hY2N1bXVsYXRvcnMnXTtcbiAgICAgICAgICByZXR1cm4gJGh0dHAuZ2V0KFwiL2pvYnMvXCIgKyBjdXJyZW50Sm9iLmppZCArIFwiL3ZlcnRpY2VzL1wiICsgdmVydGV4aWQgKyBcIi9zdWJ0YXNrcy9hY2N1bXVsYXRvcnNcIikuc3VjY2VzcyhmdW5jdGlvbihkYXRhKSB7XG4gICAgICAgICAgICB2YXIgc3VidGFza0FjY3VtdWxhdG9ycztcbiAgICAgICAgICAgIHN1YnRhc2tBY2N1bXVsYXRvcnMgPSBkYXRhLnN1YnRhc2tzO1xuICAgICAgICAgICAgcmV0dXJuIGRlZmVycmVkLnJlc29sdmUoe1xuICAgICAgICAgICAgICBtYWluOiBhY2N1bXVsYXRvcnMsXG4gICAgICAgICAgICAgIHN1YnRhc2tzOiBzdWJ0YXNrQWNjdW11bGF0b3JzXG4gICAgICAgICAgICB9KTtcbiAgICAgICAgICB9KTtcbiAgICAgICAgfSk7XG4gICAgICB9O1xuICAgIH0pKHRoaXMpKTtcbiAgICByZXR1cm4gZGVmZXJyZWQucHJvbWlzZTtcbiAgfTtcbiAgdGhpcy5sb2FkRXhjZXB0aW9ucyA9IGZ1bmN0aW9uKCkge1xuICAgIHZhciBkZWZlcnJlZDtcbiAgICBkZWZlcnJlZCA9ICRxLmRlZmVyKCk7XG4gICAgZGVmZXJyZWRzLmpvYi5wcm9taXNlLnRoZW4oKGZ1bmN0aW9uKF90aGlzKSB7XG4gICAgICByZXR1cm4gZnVuY3Rpb24oZGF0YSkge1xuICAgICAgICByZXR1cm4gJGh0dHAuZ2V0KFwiL2pvYnMvXCIgKyBjdXJyZW50Sm9iLmppZCArIFwiL2V4Y2VwdGlvbnNcIikuc3VjY2VzcyhmdW5jdGlvbihleGNlcHRpb25zKSB7XG4gICAgICAgICAgY3VycmVudEpvYi5leGNlcHRpb25zID0gZXhjZXB0aW9ucztcbiAgICAgICAgICByZXR1cm4gZGVmZXJyZWQucmVzb2x2ZShleGNlcHRpb25zKTtcbiAgICAgICAgfSk7XG4gICAgICB9O1xuICAgIH0pKHRoaXMpKTtcbiAgICByZXR1cm4gZGVmZXJyZWQucHJvbWlzZTtcbiAgfTtcbiAgcmV0dXJuIHRoaXM7XG59KTtcbiIsIiNcclxuIyBMaWNlbnNlZCB0byB0aGUgQXBhY2hlIFNvZnR3YXJlIEZvdW5kYXRpb24gKEFTRikgdW5kZXIgb25lXHJcbiMgb3IgbW9yZSBjb250cmlidXRvciBsaWNlbnNlIGFncmVlbWVudHMuICBTZWUgdGhlIE5PVElDRSBmaWxlXHJcbiMgZGlzdHJpYnV0ZWQgd2l0aCB0aGlzIHdvcmsgZm9yIGFkZGl0aW9uYWwgaW5mb3JtYXRpb25cclxuIyByZWdhcmRpbmcgY29weXJpZ2h0IG93bmVyc2hpcC4gIFRoZSBBU0YgbGljZW5zZXMgdGhpcyBmaWxlXHJcbiMgdG8geW91IHVuZGVyIHRoZSBBcGFjaGUgTGljZW5zZSwgVmVyc2lvbiAyLjAgKHRoZVxyXG4jIFwiTGljZW5zZVwiKTsgeW91IG1heSBub3QgdXNlIHRoaXMgZmlsZSBleGNlcHQgaW4gY29tcGxpYW5jZVxyXG4jIHdpdGggdGhlIExpY2Vuc2UuICBZb3UgbWF5IG9idGFpbiBhIGNvcHkgb2YgdGhlIExpY2Vuc2UgYXRcclxuI1xyXG4jICAgICBodHRwOi8vd3d3LmFwYWNoZS5vcmcvbGljZW5zZXMvTElDRU5TRS0yLjBcclxuI1xyXG4jIFVubGVzcyByZXF1aXJlZCBieSBhcHBsaWNhYmxlIGxhdyBvciBhZ3JlZWQgdG8gaW4gd3JpdGluZywgc29mdHdhcmVcclxuIyBkaXN0cmlidXRlZCB1bmRlciB0aGUgTGljZW5zZSBpcyBkaXN0cmlidXRlZCBvbiBhbiBcIkFTIElTXCIgQkFTSVMsXHJcbiMgV0lUSE9VVCBXQVJSQU5USUVTIE9SIENPTkRJVElPTlMgT0YgQU5ZIEtJTkQsIGVpdGhlciBleHByZXNzIG9yIGltcGxpZWQuXHJcbiMgU2VlIHRoZSBMaWNlbnNlIGZvciB0aGUgc3BlY2lmaWMgbGFuZ3VhZ2UgZ292ZXJuaW5nIHBlcm1pc3Npb25zIGFuZFxyXG4jIGxpbWl0YXRpb25zIHVuZGVyIHRoZSBMaWNlbnNlLlxyXG4jXHJcblxyXG5hbmd1bGFyLm1vZHVsZSgnZmxpbmtBcHAnKVxyXG5cclxuLmNvbnRyb2xsZXIgJ092ZXJ2aWV3Q29udHJvbGxlcicsICgkc2NvcGUsIE92ZXJ2aWV3U2VydmljZSwgSm9ic1NlcnZpY2UsICRpbnRlcnZhbCwgZmxpbmtDb25maWcpIC0+XHJcbiAgJHNjb3BlLmpvYk9ic2VydmVyID0gLT5cclxuICAgICRzY29wZS5ydW5uaW5nSm9icyA9IEpvYnNTZXJ2aWNlLmdldEpvYnMoJ3J1bm5pbmcnKVxyXG4gICAgJHNjb3BlLmZpbmlzaGVkSm9icyA9IEpvYnNTZXJ2aWNlLmdldEpvYnMoJ2ZpbmlzaGVkJylcclxuXHJcbiAgSm9ic1NlcnZpY2UucmVnaXN0ZXJPYnNlcnZlcigkc2NvcGUuam9iT2JzZXJ2ZXIpXHJcbiAgJHNjb3BlLiRvbiAnJGRlc3Ryb3knLCAtPlxyXG4gICAgSm9ic1NlcnZpY2UudW5SZWdpc3Rlck9ic2VydmVyKCRzY29wZS5qb2JPYnNlcnZlcilcclxuXHJcbiAgJHNjb3BlLmpvYk9ic2VydmVyKClcclxuXHJcbiAgT3ZlcnZpZXdTZXJ2aWNlLmxvYWRPdmVydmlldygpLnRoZW4gKGRhdGEpIC0+XHJcbiAgICAkc2NvcGUub3ZlcnZpZXcgPSBkYXRhXHJcblxyXG4gIHJlZnJlc2ggPSAkaW50ZXJ2YWwgLT5cclxuICAgIE92ZXJ2aWV3U2VydmljZS5sb2FkT3ZlcnZpZXcoKS50aGVuIChkYXRhKSAtPlxyXG4gICAgICAkc2NvcGUub3ZlcnZpZXcgPSBkYXRhXHJcbiAgLCBmbGlua0NvbmZpZ1tcInJlZnJlc2gtaW50ZXJ2YWxcIl1cclxuXHJcbiAgJHNjb3BlLiRvbiAnJGRlc3Ryb3knLCAtPlxyXG4gICAgJGludGVydmFsLmNhbmNlbChyZWZyZXNoKVxyXG4iLCJhbmd1bGFyLm1vZHVsZSgnZmxpbmtBcHAnKS5jb250cm9sbGVyKCdPdmVydmlld0NvbnRyb2xsZXInLCBmdW5jdGlvbigkc2NvcGUsIE92ZXJ2aWV3U2VydmljZSwgSm9ic1NlcnZpY2UsICRpbnRlcnZhbCwgZmxpbmtDb25maWcpIHtcbiAgdmFyIHJlZnJlc2g7XG4gICRzY29wZS5qb2JPYnNlcnZlciA9IGZ1bmN0aW9uKCkge1xuICAgICRzY29wZS5ydW5uaW5nSm9icyA9IEpvYnNTZXJ2aWNlLmdldEpvYnMoJ3J1bm5pbmcnKTtcbiAgICByZXR1cm4gJHNjb3BlLmZpbmlzaGVkSm9icyA9IEpvYnNTZXJ2aWNlLmdldEpvYnMoJ2ZpbmlzaGVkJyk7XG4gIH07XG4gIEpvYnNTZXJ2aWNlLnJlZ2lzdGVyT2JzZXJ2ZXIoJHNjb3BlLmpvYk9ic2VydmVyKTtcbiAgJHNjb3BlLiRvbignJGRlc3Ryb3knLCBmdW5jdGlvbigpIHtcbiAgICByZXR1cm4gSm9ic1NlcnZpY2UudW5SZWdpc3Rlck9ic2VydmVyKCRzY29wZS5qb2JPYnNlcnZlcik7XG4gIH0pO1xuICAkc2NvcGUuam9iT2JzZXJ2ZXIoKTtcbiAgT3ZlcnZpZXdTZXJ2aWNlLmxvYWRPdmVydmlldygpLnRoZW4oZnVuY3Rpb24oZGF0YSkge1xuICAgIHJldHVybiAkc2NvcGUub3ZlcnZpZXcgPSBkYXRhO1xuICB9KTtcbiAgcmVmcmVzaCA9ICRpbnRlcnZhbChmdW5jdGlvbigpIHtcbiAgICByZXR1cm4gT3ZlcnZpZXdTZXJ2aWNlLmxvYWRPdmVydmlldygpLnRoZW4oZnVuY3Rpb24oZGF0YSkge1xuICAgICAgcmV0dXJuICRzY29wZS5vdmVydmlldyA9IGRhdGE7XG4gICAgfSk7XG4gIH0sIGZsaW5rQ29uZmlnW1wicmVmcmVzaC1pbnRlcnZhbFwiXSk7XG4gIHJldHVybiAkc2NvcGUuJG9uKCckZGVzdHJveScsIGZ1bmN0aW9uKCkge1xuICAgIHJldHVybiAkaW50ZXJ2YWwuY2FuY2VsKHJlZnJlc2gpO1xuICB9KTtcbn0pO1xuIiwiI1xyXG4jIExpY2Vuc2VkIHRvIHRoZSBBcGFjaGUgU29mdHdhcmUgRm91bmRhdGlvbiAoQVNGKSB1bmRlciBvbmVcclxuIyBvciBtb3JlIGNvbnRyaWJ1dG9yIGxpY2Vuc2UgYWdyZWVtZW50cy4gIFNlZSB0aGUgTk9USUNFIGZpbGVcclxuIyBkaXN0cmlidXRlZCB3aXRoIHRoaXMgd29yayBmb3IgYWRkaXRpb25hbCBpbmZvcm1hdGlvblxyXG4jIHJlZ2FyZGluZyBjb3B5cmlnaHQgb3duZXJzaGlwLiAgVGhlIEFTRiBsaWNlbnNlcyB0aGlzIGZpbGVcclxuIyB0byB5b3UgdW5kZXIgdGhlIEFwYWNoZSBMaWNlbnNlLCBWZXJzaW9uIDIuMCAodGhlXHJcbiMgXCJMaWNlbnNlXCIpOyB5b3UgbWF5IG5vdCB1c2UgdGhpcyBmaWxlIGV4Y2VwdCBpbiBjb21wbGlhbmNlXHJcbiMgd2l0aCB0aGUgTGljZW5zZS4gIFlvdSBtYXkgb2J0YWluIGEgY29weSBvZiB0aGUgTGljZW5zZSBhdFxyXG4jXHJcbiMgICAgIGh0dHA6Ly93d3cuYXBhY2hlLm9yZy9saWNlbnNlcy9MSUNFTlNFLTIuMFxyXG4jXHJcbiMgVW5sZXNzIHJlcXVpcmVkIGJ5IGFwcGxpY2FibGUgbGF3IG9yIGFncmVlZCB0byBpbiB3cml0aW5nLCBzb2Z0d2FyZVxyXG4jIGRpc3RyaWJ1dGVkIHVuZGVyIHRoZSBMaWNlbnNlIGlzIGRpc3RyaWJ1dGVkIG9uIGFuIFwiQVMgSVNcIiBCQVNJUyxcclxuIyBXSVRIT1VUIFdBUlJBTlRJRVMgT1IgQ09ORElUSU9OUyBPRiBBTlkgS0lORCwgZWl0aGVyIGV4cHJlc3Mgb3IgaW1wbGllZC5cclxuIyBTZWUgdGhlIExpY2Vuc2UgZm9yIHRoZSBzcGVjaWZpYyBsYW5ndWFnZSBnb3Zlcm5pbmcgcGVybWlzc2lvbnMgYW5kXHJcbiMgbGltaXRhdGlvbnMgdW5kZXIgdGhlIExpY2Vuc2UuXHJcbiNcclxuXHJcbmFuZ3VsYXIubW9kdWxlKCdmbGlua0FwcCcpXHJcblxyXG4uc2VydmljZSAnT3ZlcnZpZXdTZXJ2aWNlJywgKCRodHRwLCBmbGlua0NvbmZpZywgJHEpIC0+XHJcbiAgb3ZlcnZpZXcgPSB7fVxyXG5cclxuICBAbG9hZE92ZXJ2aWV3ID0gLT5cclxuICAgIGRlZmVycmVkID0gJHEuZGVmZXIoKVxyXG5cclxuICAgICRodHRwLmdldChcIi9vdmVydmlld1wiKVxyXG4gICAgLnN1Y2Nlc3MgKGRhdGEsIHN0YXR1cywgaGVhZGVycywgY29uZmlnKSAtPlxyXG4gICAgICBvdmVydmlldyA9IGRhdGFcclxuICAgICAgZGVmZXJyZWQucmVzb2x2ZShkYXRhKVxyXG5cclxuICAgIGRlZmVycmVkLnByb21pc2VcclxuXHJcbiAgQFxyXG4iLCJhbmd1bGFyLm1vZHVsZSgnZmxpbmtBcHAnKS5zZXJ2aWNlKCdPdmVydmlld1NlcnZpY2UnLCBmdW5jdGlvbigkaHR0cCwgZmxpbmtDb25maWcsICRxKSB7XG4gIHZhciBvdmVydmlldztcbiAgb3ZlcnZpZXcgPSB7fTtcbiAgdGhpcy5sb2FkT3ZlcnZpZXcgPSBmdW5jdGlvbigpIHtcbiAgICB2YXIgZGVmZXJyZWQ7XG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpO1xuICAgICRodHRwLmdldChcIi9vdmVydmlld1wiKS5zdWNjZXNzKGZ1bmN0aW9uKGRhdGEsIHN0YXR1cywgaGVhZGVycywgY29uZmlnKSB7XG4gICAgICBvdmVydmlldyA9IGRhdGE7XG4gICAgICByZXR1cm4gZGVmZXJyZWQucmVzb2x2ZShkYXRhKTtcbiAgICB9KTtcbiAgICByZXR1cm4gZGVmZXJyZWQucHJvbWlzZTtcbiAgfTtcbiAgcmV0dXJuIHRoaXM7XG59KTtcbiIsIiNcclxuIyBMaWNlbnNlZCB0byB0aGUgQXBhY2hlIFNvZnR3YXJlIEZvdW5kYXRpb24gKEFTRikgdW5kZXIgb25lXHJcbiMgb3IgbW9yZSBjb250cmlidXRvciBsaWNlbnNlIGFncmVlbWVudHMuICBTZWUgdGhlIE5PVElDRSBmaWxlXHJcbiMgZGlzdHJpYnV0ZWQgd2l0aCB0aGlzIHdvcmsgZm9yIGFkZGl0aW9uYWwgaW5mb3JtYXRpb25cclxuIyByZWdhcmRpbmcgY29weXJpZ2h0IG93bmVyc2hpcC4gIFRoZSBBU0YgbGljZW5zZXMgdGhpcyBmaWxlXHJcbiMgdG8geW91IHVuZGVyIHRoZSBBcGFjaGUgTGljZW5zZSwgVmVyc2lvbiAyLjAgKHRoZVxyXG4jIFwiTGljZW5zZVwiKTsgeW91IG1heSBub3QgdXNlIHRoaXMgZmlsZSBleGNlcHQgaW4gY29tcGxpYW5jZVxyXG4jIHdpdGggdGhlIExpY2Vuc2UuICBZb3UgbWF5IG9idGFpbiBhIGNvcHkgb2YgdGhlIExpY2Vuc2UgYXRcclxuI1xyXG4jICAgICBodHRwOi8vd3d3LmFwYWNoZS5vcmcvbGljZW5zZXMvTElDRU5TRS0yLjBcclxuI1xyXG4jIFVubGVzcyByZXF1aXJlZCBieSBhcHBsaWNhYmxlIGxhdyBvciBhZ3JlZWQgdG8gaW4gd3JpdGluZywgc29mdHdhcmVcclxuIyBkaXN0cmlidXRlZCB1bmRlciB0aGUgTGljZW5zZSBpcyBkaXN0cmlidXRlZCBvbiBhbiBcIkFTIElTXCIgQkFTSVMsXHJcbiMgV0lUSE9VVCBXQVJSQU5USUVTIE9SIENPTkRJVElPTlMgT0YgQU5ZIEtJTkQsIGVpdGhlciBleHByZXNzIG9yIGltcGxpZWQuXHJcbiMgU2VlIHRoZSBMaWNlbnNlIGZvciB0aGUgc3BlY2lmaWMgbGFuZ3VhZ2UgZ292ZXJuaW5nIHBlcm1pc3Npb25zIGFuZFxyXG4jIGxpbWl0YXRpb25zIHVuZGVyIHRoZSBMaWNlbnNlLlxyXG4jXHJcblxyXG5hbmd1bGFyLm1vZHVsZSgnZmxpbmtBcHAnKVxyXG5cclxuLmNvbnRyb2xsZXIgJ1Rhc2tNYW5hZ2Vyc0NvbnRyb2xsZXInLCAoJHNjb3BlLCBUYXNrTWFuYWdlcnNTZXJ2aWNlLCAkaW50ZXJ2YWwsIGZsaW5rQ29uZmlnKSAtPlxyXG4gIFRhc2tNYW5hZ2Vyc1NlcnZpY2UubG9hZE1hbmFnZXJzKCkudGhlbiAoZGF0YSkgLT5cclxuICAgICRzY29wZS5tYW5hZ2VycyA9IGRhdGFcclxuXHJcbiAgcmVmcmVzaCA9ICRpbnRlcnZhbCAtPlxyXG4gICAgVGFza01hbmFnZXJzU2VydmljZS5sb2FkTWFuYWdlcnMoKS50aGVuIChkYXRhKSAtPlxyXG4gICAgICAkc2NvcGUubWFuYWdlcnMgPSBkYXRhXHJcbiAgLCBmbGlua0NvbmZpZ1tcInJlZnJlc2gtaW50ZXJ2YWxcIl1cclxuXHJcbiAgJHNjb3BlLiRvbiAnJGRlc3Ryb3knLCAtPlxyXG4gICAgJGludGVydmFsLmNhbmNlbChyZWZyZXNoKVxyXG5cclxuICAkc2NvcGUubWFuYWdlcklkID0gbnVsbFxyXG5cclxuICAkc2NvcGUuY2hhbmdlTWFuYWdlciA9IChtYW5hZ2VySWQpIC0+XHJcbiAgICBpZiBtYW5hZ2VySWQgIT0gJHNjb3BlLm1hbmFnZXJJZFxyXG4gICAgICAkc2NvcGUubWFuYWdlcklkID0gbWFuYWdlcklkXHJcbiAgICBlbHNlXHJcbiAgICAgICRzY29wZS5tYW5hZ2VySWQgPSBudWxsXHJcbiIsImFuZ3VsYXIubW9kdWxlKCdmbGlua0FwcCcpLmNvbnRyb2xsZXIoJ1Rhc2tNYW5hZ2Vyc0NvbnRyb2xsZXInLCBmdW5jdGlvbigkc2NvcGUsIFRhc2tNYW5hZ2Vyc1NlcnZpY2UsICRpbnRlcnZhbCwgZmxpbmtDb25maWcpIHtcbiAgdmFyIHJlZnJlc2g7XG4gIFRhc2tNYW5hZ2Vyc1NlcnZpY2UubG9hZE1hbmFnZXJzKCkudGhlbihmdW5jdGlvbihkYXRhKSB7XG4gICAgcmV0dXJuICRzY29wZS5tYW5hZ2VycyA9IGRhdGE7XG4gIH0pO1xuICByZWZyZXNoID0gJGludGVydmFsKGZ1bmN0aW9uKCkge1xuICAgIHJldHVybiBUYXNrTWFuYWdlcnNTZXJ2aWNlLmxvYWRNYW5hZ2VycygpLnRoZW4oZnVuY3Rpb24oZGF0YSkge1xuICAgICAgcmV0dXJuICRzY29wZS5tYW5hZ2VycyA9IGRhdGE7XG4gICAgfSk7XG4gIH0sIGZsaW5rQ29uZmlnW1wicmVmcmVzaC1pbnRlcnZhbFwiXSk7XG4gICRzY29wZS4kb24oJyRkZXN0cm95JywgZnVuY3Rpb24oKSB7XG4gICAgcmV0dXJuICRpbnRlcnZhbC5jYW5jZWwocmVmcmVzaCk7XG4gIH0pO1xuICAkc2NvcGUubWFuYWdlcklkID0gbnVsbDtcbiAgcmV0dXJuICRzY29wZS5jaGFuZ2VNYW5hZ2VyID0gZnVuY3Rpb24obWFuYWdlcklkKSB7XG4gICAgaWYgKG1hbmFnZXJJZCAhPT0gJHNjb3BlLm1hbmFnZXJJZCkge1xuICAgICAgcmV0dXJuICRzY29wZS5tYW5hZ2VySWQgPSBtYW5hZ2VySWQ7XG4gICAgfSBlbHNlIHtcbiAgICAgIHJldHVybiAkc2NvcGUubWFuYWdlcklkID0gbnVsbDtcbiAgICB9XG4gIH07XG59KTtcbiIsIiNcclxuIyBMaWNlbnNlZCB0byB0aGUgQXBhY2hlIFNvZnR3YXJlIEZvdW5kYXRpb24gKEFTRikgdW5kZXIgb25lXHJcbiMgb3IgbW9yZSBjb250cmlidXRvciBsaWNlbnNlIGFncmVlbWVudHMuICBTZWUgdGhlIE5PVElDRSBmaWxlXHJcbiMgZGlzdHJpYnV0ZWQgd2l0aCB0aGlzIHdvcmsgZm9yIGFkZGl0aW9uYWwgaW5mb3JtYXRpb25cclxuIyByZWdhcmRpbmcgY29weXJpZ2h0IG93bmVyc2hpcC4gIFRoZSBBU0YgbGljZW5zZXMgdGhpcyBmaWxlXHJcbiMgdG8geW91IHVuZGVyIHRoZSBBcGFjaGUgTGljZW5zZSwgVmVyc2lvbiAyLjAgKHRoZVxyXG4jIFwiTGljZW5zZVwiKTsgeW91IG1heSBub3QgdXNlIHRoaXMgZmlsZSBleGNlcHQgaW4gY29tcGxpYW5jZVxyXG4jIHdpdGggdGhlIExpY2Vuc2UuICBZb3UgbWF5IG9idGFpbiBhIGNvcHkgb2YgdGhlIExpY2Vuc2UgYXRcclxuI1xyXG4jICAgICBodHRwOi8vd3d3LmFwYWNoZS5vcmcvbGljZW5zZXMvTElDRU5TRS0yLjBcclxuI1xyXG4jIFVubGVzcyByZXF1aXJlZCBieSBhcHBsaWNhYmxlIGxhdyBvciBhZ3JlZWQgdG8gaW4gd3JpdGluZywgc29mdHdhcmVcclxuIyBkaXN0cmlidXRlZCB1bmRlciB0aGUgTGljZW5zZSBpcyBkaXN0cmlidXRlZCBvbiBhbiBcIkFTIElTXCIgQkFTSVMsXHJcbiMgV0lUSE9VVCBXQVJSQU5USUVTIE9SIENPTkRJVElPTlMgT0YgQU5ZIEtJTkQsIGVpdGhlciBleHByZXNzIG9yIGltcGxpZWQuXHJcbiMgU2VlIHRoZSBMaWNlbnNlIGZvciB0aGUgc3BlY2lmaWMgbGFuZ3VhZ2UgZ292ZXJuaW5nIHBlcm1pc3Npb25zIGFuZFxyXG4jIGxpbWl0YXRpb25zIHVuZGVyIHRoZSBMaWNlbnNlLlxyXG4jXHJcblxyXG5hbmd1bGFyLm1vZHVsZSgnZmxpbmtBcHAnKVxyXG5cclxuLnNlcnZpY2UgJ1Rhc2tNYW5hZ2Vyc1NlcnZpY2UnLCAoJGh0dHAsIGZsaW5rQ29uZmlnLCAkcSkgLT5cclxuICBAbG9hZE1hbmFnZXJzID0gLT5cclxuICAgIGRlZmVycmVkID0gJHEuZGVmZXIoKVxyXG5cclxuICAgICRodHRwLmdldChcIi90YXNrbWFuYWdlcnNcIilcclxuICAgIC5zdWNjZXNzIChkYXRhLCBzdGF0dXMsIGhlYWRlcnMsIGNvbmZpZykgLT5cclxuICAgICAgZGVmZXJyZWQucmVzb2x2ZShkYXRhWyd0YXNrbWFuYWdlcnMnXSlcclxuXHJcbiAgICBkZWZlcnJlZC5wcm9taXNlXHJcblxyXG4gIEBcclxuIiwiYW5ndWxhci5tb2R1bGUoJ2ZsaW5rQXBwJykuc2VydmljZSgnVGFza01hbmFnZXJzU2VydmljZScsIGZ1bmN0aW9uKCRodHRwLCBmbGlua0NvbmZpZywgJHEpIHtcbiAgdGhpcy5sb2FkTWFuYWdlcnMgPSBmdW5jdGlvbigpIHtcbiAgICB2YXIgZGVmZXJyZWQ7XG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpO1xuICAgICRodHRwLmdldChcIi90YXNrbWFuYWdlcnNcIikuc3VjY2VzcyhmdW5jdGlvbihkYXRhLCBzdGF0dXMsIGhlYWRlcnMsIGNvbmZpZykge1xuICAgICAgcmV0dXJuIGRlZmVycmVkLnJlc29sdmUoZGF0YVsndGFza21hbmFnZXJzJ10pO1xuICAgIH0pO1xuICAgIHJldHVybiBkZWZlcnJlZC5wcm9taXNlO1xuICB9O1xuICByZXR1cm4gdGhpcztcbn0pO1xuIl0sInNvdXJjZVJvb3QiOiIvc291cmNlLyJ9
