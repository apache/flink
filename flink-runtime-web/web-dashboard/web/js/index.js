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
  }).state("all-manager", {
    url: "/taskmanagers",
    views: {
      main: {
        templateUrl: "partials/taskmanager/index.html",
        controller: 'AllTaskManagersController'
      }
    }
  }).state("single-manager", {
    url: "/taskmanager/{taskmanagerid}",
    views: {
      main: {
        templateUrl: "partials/taskmanager/taskmanager.html",
        controller: 'SingleTaskManagerController'
      }
    }
  }).state("single-manager.metrics", {
    url: "/metrics",
    views: {
      details: {
        templateUrl: "partials/taskmanager/taskmanager.metrics.html"
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
        templateUrl: "partials/jobmanager/stdout.html",
        controller: 'JobManagerStdoutController'
      }
    }
  }).state("jobmanager.log", {
    url: "/log",
    views: {
      details: {
        templateUrl: "partials/jobmanager/log.html",
        controller: 'JobManagerLogsController'
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
}]).filter("humanizeDuration", function() {
  return function(value, short) {
    var days, hours, minutes, ms, seconds, x;
    if (typeof value === "undefined" || value === null) {
      return "";
    }
    ms = value % 1000;
    x = Math.floor(value / 1000);
    seconds = x % 60;
    x = Math.floor(x / 60);
    minutes = x % 60;
    x = Math.floor(x / 60);
    hours = x % 24;
    x = Math.floor(x / 24);
    days = x;
    if (days === 0) {
      if (hours === 0) {
        if (minutes === 0) {
          if (seconds === 0) {
            return ms + "ms";
          } else {
            return seconds + "s ";
          }
        } else {
          return minutes + "m " + seconds + "s";
        }
      } else {
        if (short) {
          return hours + "h " + minutes + "m";
        } else {
          return hours + "h " + minutes + "m " + seconds + "s";
        }
      }
    } else {
      if (short) {
        return days + "d " + hours + "h";
      } else {
        return days + "d " + hours + "h " + minutes + "m " + seconds + "s";
      }
    }
  };
}).filter("humanizeText", function() {
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
    $http.get("config").success(function(data, status, headers, config) {
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
}]).controller('JobManagerLogsController', ["$scope", "JobManagerLogsService", function($scope, JobManagerLogsService) {
  JobManagerLogsService.loadLogs().then(function(data) {
    if ($scope.jobmanager == null) {
      $scope.jobmanager = {};
    }
    return $scope.jobmanager['log'] = data;
  });
  return $scope.reloadData = function() {
    return JobManagerLogsService.loadLogs().then(function(data) {
      return $scope.jobmanager['log'] = data;
    });
  };
}]).controller('JobManagerStdoutController', ["$scope", "JobManagerStdoutService", function($scope, JobManagerStdoutService) {
  JobManagerStdoutService.loadStdout().then(function(data) {
    if ($scope.jobmanager == null) {
      $scope.jobmanager = {};
    }
    return $scope.jobmanager['stdout'] = data;
  });
  return $scope.reloadData = function() {
    return JobManagerStdoutService.loadStdout().then(function(data) {
      return $scope.jobmanager['stdout'] = data;
    });
  };
}]);

angular.module('flinkApp').service('JobManagerConfigService', ["$http", "flinkConfig", "$q", function($http, flinkConfig, $q) {
  var config;
  config = {};
  this.loadConfig = function() {
    var deferred;
    deferred = $q.defer();
    $http.get("jobmanager/config").success(function(data, status, headers, config) {
      config = data;
      return deferred.resolve(data);
    });
    return deferred.promise;
  };
  return this;
}]).service('JobManagerLogsService', ["$http", "flinkConfig", "$q", function($http, flinkConfig, $q) {
  var logs;
  logs = {};
  this.loadLogs = function() {
    var deferred;
    deferred = $q.defer();
    $http.get("jobmanager/log").success(function(data, status, headers, config) {
      logs = data;
      return deferred.resolve(data);
    });
    return deferred.promise;
  };
  return this;
}]).service('JobManagerStdoutService', ["$http", "flinkConfig", "$q", function($http, flinkConfig, $q) {
  var stdout;
  stdout = {};
  this.loadStdout = function() {
    var deferred;
    deferred = $q.defer();
    $http.get("jobmanager/stdout").success(function(data, status, headers, config) {
      stdout = data;
      return deferred.resolve(data);
    });
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
    $http.get("overview").success(function(data, status, headers, config) {
      overview = data;
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
  $scope.$on('$destroy', function() {
    $scope.job = null;
    $scope.plan = null;
    $scope.vertices = null;
    return $interval.cancel(refresher);
  });
  return $scope.cancelJob = function(cancelEvent) {
    angular.element(cancelEvent.currentTarget).removeClass('label-danger').addClass('label-info').html('Cancelling...');
    return JobsService.cancelJob($stateParams.jobid).then(function(data) {
      return {};
    });
  };
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
          if (!!missingNode) {
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
          if (el.inputs != null) {
            ref = el.inputs;
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
    $http.get("joboverview").success((function(_this) {
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
    $http.get("jobs/" + jobid).success((function(_this) {
      return function(data, status, headers, config) {
        _this.setEndTimes(data.vertices);
        _this.processVertices(data);
        return $http.get("jobs/" + jobid + "/config").success(function(jobConfig) {
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
        return $http.get("jobs/" + currentJob.jid + "/vertices/" + vertexid + "/subtasktimes").success(function(data) {
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
        return $http.get("jobs/" + currentJob.jid + "/vertices/" + vertexid).success(function(data) {
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
        return $http.get("jobs/" + currentJob.jid + "/vertices/" + vertexid + "/accumulators").success(function(data) {
          var accumulators;
          accumulators = data['user-accumulators'];
          return $http.get("jobs/" + currentJob.jid + "/vertices/" + vertexid + "/subtasks/accumulators").success(function(data) {
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
        return $http.get("jobs/" + currentJob.jid + "/exceptions").success(function(exceptions) {
          currentJob.exceptions = exceptions;
          return deferred.resolve(exceptions);
        });
      };
    })(this));
    return deferred.promise;
  };
  this.cancelJob = function(jobid) {
    return $http.get("jobs/" + jobid + "/yarn-cancel");
  };
  return this;
}]);

angular.module('flinkApp').controller('AllTaskManagersController', ["$scope", "TaskManagersService", "$interval", "flinkConfig", function($scope, TaskManagersService, $interval, flinkConfig) {
  var refresh;
  TaskManagersService.loadManagers().then(function(data) {
    return $scope.managers = data;
  });
  refresh = $interval(function() {
    return TaskManagersService.loadManagers().then(function(data) {
      return $scope.managers = data;
    });
  }, flinkConfig["refresh-interval"]);
  return $scope.$on('$destroy', function() {
    return $interval.cancel(refresh);
  });
}]).controller('SingleTaskManagerController', ["$scope", "$stateParams", "SingleTaskManagerService", "$interval", "flinkConfig", function($scope, $stateParams, SingleTaskManagerService, $interval, flinkConfig) {
  var refresh;
  $scope.metrics = {};
  SingleTaskManagerService.loadMetrics($stateParams.taskmanagerid).then(function(data) {
    return $scope.metrics = data[0];
  });
  refresh = $interval(function() {
    return SingleTaskManagerService.loadMetrics($stateParams.taskmanagerid).then(function(data) {
      return $scope.metrics = data[0];
    });
  }, flinkConfig["refresh-interval"]);
  return $scope.$on('$destroy', function() {
    return $interval.cancel(refresh);
  });
}]);

angular.module('flinkApp').service('TaskManagersService', ["$http", "flinkConfig", "$q", function($http, flinkConfig, $q) {
  this.loadManagers = function() {
    var deferred;
    deferred = $q.defer();
    $http.get("taskmanagers").success(function(data, status, headers, config) {
      return deferred.resolve(data['taskmanagers']);
    });
    return deferred.promise;
  };
  return this;
}]).service('SingleTaskManagerService', ["$http", "flinkConfig", "$q", function($http, flinkConfig, $q) {
  this.loadMetrics = function(taskmanagerid) {
    var deferred;
    deferred = $q.defer();
    $http.get("taskmanagers/" + taskmanagerid).success(function(data, status, headers, config) {
      return deferred.resolve(data['taskmanagers']);
    });
    return deferred.promise;
  };
  return this;
}]);

//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJzb3VyY2VzIjpbImluZGV4LmNvZmZlZSIsImluZGV4LmpzIiwiY29tbW9uL2RpcmVjdGl2ZXMuY29mZmVlIiwiY29tbW9uL2RpcmVjdGl2ZXMuanMiLCJjb21tb24vZmlsdGVycy5jb2ZmZWUiLCJjb21tb24vZmlsdGVycy5qcyIsImNvbW1vbi9zZXJ2aWNlcy5jb2ZmZWUiLCJjb21tb24vc2VydmljZXMuanMiLCJtb2R1bGVzL2pvYm1hbmFnZXIvam9ibWFuYWdlci5jdHJsLmNvZmZlZSIsIm1vZHVsZXMvam9ibWFuYWdlci9qb2JtYW5hZ2VyLmN0cmwuanMiLCJtb2R1bGVzL2pvYm1hbmFnZXIvam9ibWFuYWdlci5zdmMuY29mZmVlIiwibW9kdWxlcy9qb2JtYW5hZ2VyL2pvYm1hbmFnZXIuc3ZjLmpzIiwibW9kdWxlcy9vdmVydmlldy9vdmVydmlldy5jdHJsLmNvZmZlZSIsIm1vZHVsZXMvb3ZlcnZpZXcvb3ZlcnZpZXcuY3RybC5qcyIsIm1vZHVsZXMvb3ZlcnZpZXcvb3ZlcnZpZXcuc3ZjLmNvZmZlZSIsIm1vZHVsZXMvb3ZlcnZpZXcvb3ZlcnZpZXcuc3ZjLmpzIiwibW9kdWxlcy9qb2JzL2pvYnMuY3RybC5jb2ZmZWUiLCJtb2R1bGVzL2pvYnMvam9icy5jdHJsLmpzIiwibW9kdWxlcy9qb2JzL2pvYnMuZGlyLmNvZmZlZSIsIm1vZHVsZXMvam9icy9qb2JzLmRpci5qcyIsIm1vZHVsZXMvam9icy9qb2JzLnN2Yy5jb2ZmZWUiLCJtb2R1bGVzL2pvYnMvam9icy5zdmMuanMiLCJtb2R1bGVzL3Rhc2ttYW5hZ2VyL3Rhc2ttYW5hZ2VyLmN0cmwuY29mZmVlIiwibW9kdWxlcy90YXNrbWFuYWdlci90YXNrbWFuYWdlci5jdHJsLmpzIiwibW9kdWxlcy90YXNrbWFuYWdlci90YXNrbWFuYWdlci5zdmMuY29mZmVlIiwibW9kdWxlcy90YXNrbWFuYWdlci90YXNrbWFuYWdlci5zdmMuanMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6IkFBa0JBLFFBQVEsT0FBTyxZQUFZ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UUFBUSxLQUFLOztJQ25CeEIsT0RxQkEsU0FBUzs7RUNuQlgsT0RxQkE7SUFFRCxRQUFRLDJEQUE0QixTQUFDLE9BQU8sYUFBYSxJQUFyQjtFQUNuQyxLQUFDLGNBQWMsU0FBQyxlQUFEO0lBQ2IsSUFBQTtJQUFBLFdBQVcsR0FBRztJQUVkLE1BQU0sSUFBSSxrQkFBa0IsZUFDM0IsUUFBUSxTQUFDLE1BQU0sUUFBUSxTQUFTLFFBQXhCO01DdEJQLE9EdUJBLFNBQVMsUUFBUSxLQUFLOztJQ3JCeEIsT0R1QkEsU0FBUzs7RUNyQlgsT0R1QkE7O0FDckJGIiwiZmlsZSI6ImluZGV4LmpzIiwic291cmNlc0NvbnRlbnQiOlsiI1xuIyBMaWNlbnNlZCB0byB0aGUgQXBhY2hlIFNvZnR3YXJlIEZvdW5kYXRpb24gKEFTRikgdW5kZXIgb25lXG4jIG9yIG1vcmUgY29udHJpYnV0b3IgbGljZW5zZSBhZ3JlZW1lbnRzLiAgU2VlIHRoZSBOT1RJQ0UgZmlsZVxuIyBkaXN0cmlidXRlZCB3aXRoIHRoaXMgd29yayBmb3IgYWRkaXRpb25hbCBpbmZvcm1hdGlvblxuIyByZWdhcmRpbmcgY29weXJpZ2h0IG93bmVyc2hpcC4gIFRoZSBBU0YgbGljZW5zZXMgdGhpcyBmaWxlXG4jIHRvIHlvdSB1bmRlciB0aGUgQXBhY2hlIExpY2Vuc2UsIFZlcnNpb24gMi4wICh0aGVcbiMgXCJMaWNlbnNlXCIpOyB5b3UgbWF5IG5vdCB1c2UgdGhpcyBmaWxlIGV4Y2VwdCBpbiBjb21wbGlhbmNlXG4jIHdpdGggdGhlIExpY2Vuc2UuICBZb3UgbWF5IG9idGFpbiBhIGNvcHkgb2YgdGhlIExpY2Vuc2UgYXRcbiNcbiMgICAgIGh0dHA6Ly93d3cuYXBhY2hlLm9yZy9saWNlbnNlcy9MSUNFTlNFLTIuMFxuI1xuIyBVbmxlc3MgcmVxdWlyZWQgYnkgYXBwbGljYWJsZSBsYXcgb3IgYWdyZWVkIHRvIGluIHdyaXRpbmcsIHNvZnR3YXJlXG4jIGRpc3RyaWJ1dGVkIHVuZGVyIHRoZSBMaWNlbnNlIGlzIGRpc3RyaWJ1dGVkIG9uIGFuIFwiQVMgSVNcIiBCQVNJUyxcbiMgV0lUSE9VVCBXQVJSQU5USUVTIE9SIENPTkRJVElPTlMgT0YgQU5ZIEtJTkQsIGVpdGhlciBleHByZXNzIG9yIGltcGxpZWQuXG4jIFNlZSB0aGUgTGljZW5zZSBmb3IgdGhlIHNwZWNpZmljIGxhbmd1YWdlIGdvdmVybmluZyBwZXJtaXNzaW9ucyBhbmRcbiMgbGltaXRhdGlvbnMgdW5kZXIgdGhlIExpY2Vuc2UuXG4jXG5cbmFuZ3VsYXIubW9kdWxlKCdmbGlua0FwcCcsIFsndWkucm91dGVyJywgJ2FuZ3VsYXJNb21lbnQnXSlcblxuIyAtLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLVxuXG4ucnVuICgkcm9vdFNjb3BlKSAtPlxuICAkcm9vdFNjb3BlLnNpZGViYXJWaXNpYmxlID0gZmFsc2VcbiAgJHJvb3RTY29wZS5zaG93U2lkZWJhciA9IC0+XG4gICAgJHJvb3RTY29wZS5zaWRlYmFyVmlzaWJsZSA9ICEkcm9vdFNjb3BlLnNpZGViYXJWaXNpYmxlXG4gICAgJHJvb3RTY29wZS5zaWRlYmFyQ2xhc3MgPSAnZm9yY2Utc2hvdydcblxuIyAtLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLVxuXG4udmFsdWUgJ2ZsaW5rQ29uZmlnJywge1xuICBcInJlZnJlc2gtaW50ZXJ2YWxcIjogMTAwMDBcbn1cblxuIyAtLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLVxuXG4ucnVuIChKb2JzU2VydmljZSwgTWFpblNlcnZpY2UsIGZsaW5rQ29uZmlnLCAkaW50ZXJ2YWwpIC0+XG4gIE1haW5TZXJ2aWNlLmxvYWRDb25maWcoKS50aGVuIChjb25maWcpIC0+XG4gICAgYW5ndWxhci5leHRlbmQgZmxpbmtDb25maWcsIGNvbmZpZ1xuXG4gICAgSm9ic1NlcnZpY2UubGlzdEpvYnMoKVxuXG4gICAgJGludGVydmFsIC0+XG4gICAgICBKb2JzU2VydmljZS5saXN0Sm9icygpXG4gICAgLCBmbGlua0NvbmZpZ1tcInJlZnJlc2gtaW50ZXJ2YWxcIl1cblxuXG4jIC0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tXG5cbi5jb25maWcgKCR1aVZpZXdTY3JvbGxQcm92aWRlcikgLT5cbiAgJHVpVmlld1Njcm9sbFByb3ZpZGVyLnVzZUFuY2hvclNjcm9sbCgpXG5cbiMgLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS1cblxuLmNvbmZpZyAoJHN0YXRlUHJvdmlkZXIsICR1cmxSb3V0ZXJQcm92aWRlcikgLT5cbiAgJHN0YXRlUHJvdmlkZXIuc3RhdGUgXCJvdmVydmlld1wiLFxuICAgIHVybDogXCIvb3ZlcnZpZXdcIlxuICAgIHZpZXdzOlxuICAgICAgbWFpbjpcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvb3ZlcnZpZXcuaHRtbFwiXG4gICAgICAgIGNvbnRyb2xsZXI6ICdPdmVydmlld0NvbnRyb2xsZXInXG5cbiAgLnN0YXRlIFwicnVubmluZy1qb2JzXCIsXG4gICAgdXJsOiBcIi9ydW5uaW5nLWpvYnNcIlxuICAgIHZpZXdzOlxuICAgICAgbWFpbjpcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvam9icy9ydW5uaW5nLWpvYnMuaHRtbFwiXG4gICAgICAgIGNvbnRyb2xsZXI6ICdSdW5uaW5nSm9ic0NvbnRyb2xsZXInXG5cbiAgLnN0YXRlIFwiY29tcGxldGVkLWpvYnNcIixcbiAgICB1cmw6IFwiL2NvbXBsZXRlZC1qb2JzXCJcbiAgICB2aWV3czpcbiAgICAgIG1haW46XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYnMvY29tcGxldGVkLWpvYnMuaHRtbFwiXG4gICAgICAgIGNvbnRyb2xsZXI6ICdDb21wbGV0ZWRKb2JzQ29udHJvbGxlcidcblxuICAuc3RhdGUgXCJzaW5nbGUtam9iXCIsXG4gICAgdXJsOiBcIi9qb2JzL3tqb2JpZH1cIlxuICAgIGFic3RyYWN0OiB0cnVlXG4gICAgdmlld3M6XG4gICAgICBtYWluOlxuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JzL2pvYi5odG1sXCJcbiAgICAgICAgY29udHJvbGxlcjogJ1NpbmdsZUpvYkNvbnRyb2xsZXInXG5cbiAgLnN0YXRlIFwic2luZ2xlLWpvYi5wbGFuXCIsXG4gICAgdXJsOiBcIlwiXG4gICAgYWJzdHJhY3Q6IHRydWVcbiAgICB2aWV3czpcbiAgICAgIGRldGFpbHM6XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYnMvam9iLnBsYW4uaHRtbFwiXG4gICAgICAgIGNvbnRyb2xsZXI6ICdKb2JQbGFuQ29udHJvbGxlcidcblxuICAuc3RhdGUgXCJzaW5nbGUtam9iLnBsYW4ub3ZlcnZpZXdcIixcbiAgICB1cmw6IFwiXCJcbiAgICB2aWV3czpcbiAgICAgICdub2RlLWRldGFpbHMnOlxuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JzL2pvYi5wbGFuLm5vZGUtbGlzdC5vdmVydmlldy5odG1sXCJcbiAgICAgICAgY29udHJvbGxlcjogJ0pvYlBsYW5PdmVydmlld0NvbnRyb2xsZXInXG5cbiAgLnN0YXRlIFwic2luZ2xlLWpvYi5wbGFuLmFjY3VtdWxhdG9yc1wiLFxuICAgIHVybDogXCIvYWNjdW11bGF0b3JzXCJcbiAgICB2aWV3czpcbiAgICAgICdub2RlLWRldGFpbHMnOlxuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JzL2pvYi5wbGFuLm5vZGUtbGlzdC5hY2N1bXVsYXRvcnMuaHRtbFwiXG4gICAgICAgIGNvbnRyb2xsZXI6ICdKb2JQbGFuQWNjdW11bGF0b3JzQ29udHJvbGxlcidcblxuICAuc3RhdGUgXCJzaW5nbGUtam9iLnRpbWVsaW5lXCIsXG4gICAgdXJsOiBcIi90aW1lbGluZVwiXG4gICAgdmlld3M6XG4gICAgICBkZXRhaWxzOlxuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JzL2pvYi50aW1lbGluZS5odG1sXCJcblxuICAuc3RhdGUgXCJzaW5nbGUtam9iLnRpbWVsaW5lLnZlcnRleFwiLFxuICAgIHVybDogXCIve3ZlcnRleElkfVwiXG4gICAgdmlld3M6XG4gICAgICB2ZXJ0ZXg6XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYnMvam9iLnRpbWVsaW5lLnZlcnRleC5odG1sXCJcbiAgICAgICAgY29udHJvbGxlcjogJ0pvYlRpbWVsaW5lVmVydGV4Q29udHJvbGxlcidcblxuICAuc3RhdGUgXCJzaW5nbGUtam9iLmV4Y2VwdGlvbnNcIixcbiAgICB1cmw6IFwiL2V4Y2VwdGlvbnNcIlxuICAgIHZpZXdzOlxuICAgICAgZGV0YWlsczpcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvam9icy9qb2IuZXhjZXB0aW9ucy5odG1sXCJcbiAgICAgICAgY29udHJvbGxlcjogJ0pvYkV4Y2VwdGlvbnNDb250cm9sbGVyJ1xuXG4gIC5zdGF0ZSBcInNpbmdsZS1qb2IucHJvcGVydGllc1wiLFxuICAgIHVybDogXCIvcHJvcGVydGllc1wiXG4gICAgdmlld3M6XG4gICAgICBkZXRhaWxzOlxuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JzL2pvYi5wcm9wZXJ0aWVzLmh0bWxcIlxuICAgICAgICBjb250cm9sbGVyOiAnSm9iUHJvcGVydGllc0NvbnRyb2xsZXInXG5cbiAgLnN0YXRlIFwic2luZ2xlLWpvYi5jb25maWdcIixcbiAgICB1cmw6IFwiL2NvbmZpZ1wiXG4gICAgdmlld3M6XG4gICAgICBkZXRhaWxzOlxuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JzL2pvYi5jb25maWcuaHRtbFwiXG5cbiAgLnN0YXRlIFwiYWxsLW1hbmFnZXJcIixcbiAgICB1cmw6IFwiL3Rhc2ttYW5hZ2Vyc1wiXG4gICAgdmlld3M6XG4gICAgICBtYWluOlxuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy90YXNrbWFuYWdlci9pbmRleC5odG1sXCJcbiAgICAgICAgY29udHJvbGxlcjogJ0FsbFRhc2tNYW5hZ2Vyc0NvbnRyb2xsZXInXG5cbiAgLnN0YXRlIFwic2luZ2xlLW1hbmFnZXJcIixcbiAgICAgIHVybDogXCIvdGFza21hbmFnZXIve3Rhc2ttYW5hZ2VyaWR9XCJcbiAgICAgIHZpZXdzOlxuICAgICAgICBtYWluOlxuICAgICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL3Rhc2ttYW5hZ2VyL3Rhc2ttYW5hZ2VyLmh0bWxcIlxuICAgICAgICAgIGNvbnRyb2xsZXI6ICdTaW5nbGVUYXNrTWFuYWdlckNvbnRyb2xsZXInXG5cbiAgLnN0YXRlIFwic2luZ2xlLW1hbmFnZXIubWV0cmljc1wiLFxuICAgIHVybDogXCIvbWV0cmljc1wiXG4gICAgdmlld3M6XG4gICAgICBkZXRhaWxzOlxuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy90YXNrbWFuYWdlci90YXNrbWFuYWdlci5tZXRyaWNzLmh0bWxcIlxuXG4gIC5zdGF0ZSBcImpvYm1hbmFnZXJcIixcbiAgICAgIHVybDogXCIvam9ibWFuYWdlclwiXG4gICAgICB2aWV3czpcbiAgICAgICAgbWFpbjpcbiAgICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JtYW5hZ2VyL2luZGV4Lmh0bWxcIlxuXG4gIC5zdGF0ZSBcImpvYm1hbmFnZXIuY29uZmlnXCIsXG4gICAgdXJsOiBcIi9jb25maWdcIlxuICAgIHZpZXdzOlxuICAgICAgZGV0YWlsczpcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvam9ibWFuYWdlci9jb25maWcuaHRtbFwiXG4gICAgICAgIGNvbnRyb2xsZXI6ICdKb2JNYW5hZ2VyQ29uZmlnQ29udHJvbGxlcidcblxuICAuc3RhdGUgXCJqb2JtYW5hZ2VyLnN0ZG91dFwiLFxuICAgIHVybDogXCIvc3Rkb3V0XCJcbiAgICB2aWV3czpcbiAgICAgIGRldGFpbHM6XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYm1hbmFnZXIvc3Rkb3V0Lmh0bWxcIlxuICAgICAgICBjb250cm9sbGVyOiAnSm9iTWFuYWdlclN0ZG91dENvbnRyb2xsZXInXG5cbiAgLnN0YXRlIFwiam9ibWFuYWdlci5sb2dcIixcbiAgICB1cmw6IFwiL2xvZ1wiXG4gICAgdmlld3M6XG4gICAgICBkZXRhaWxzOlxuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JtYW5hZ2VyL2xvZy5odG1sXCJcbiAgICAgICAgY29udHJvbGxlcjogJ0pvYk1hbmFnZXJMb2dzQ29udHJvbGxlcidcblxuICAkdXJsUm91dGVyUHJvdmlkZXIub3RoZXJ3aXNlIFwiL292ZXJ2aWV3XCJcbiIsImFuZ3VsYXIubW9kdWxlKCdmbGlua0FwcCcsIFsndWkucm91dGVyJywgJ2FuZ3VsYXJNb21lbnQnXSkucnVuKGZ1bmN0aW9uKCRyb290U2NvcGUpIHtcbiAgJHJvb3RTY29wZS5zaWRlYmFyVmlzaWJsZSA9IGZhbHNlO1xuICByZXR1cm4gJHJvb3RTY29wZS5zaG93U2lkZWJhciA9IGZ1bmN0aW9uKCkge1xuICAgICRyb290U2NvcGUuc2lkZWJhclZpc2libGUgPSAhJHJvb3RTY29wZS5zaWRlYmFyVmlzaWJsZTtcbiAgICByZXR1cm4gJHJvb3RTY29wZS5zaWRlYmFyQ2xhc3MgPSAnZm9yY2Utc2hvdyc7XG4gIH07XG59KS52YWx1ZSgnZmxpbmtDb25maWcnLCB7XG4gIFwicmVmcmVzaC1pbnRlcnZhbFwiOiAxMDAwMFxufSkucnVuKGZ1bmN0aW9uKEpvYnNTZXJ2aWNlLCBNYWluU2VydmljZSwgZmxpbmtDb25maWcsICRpbnRlcnZhbCkge1xuICByZXR1cm4gTWFpblNlcnZpY2UubG9hZENvbmZpZygpLnRoZW4oZnVuY3Rpb24oY29uZmlnKSB7XG4gICAgYW5ndWxhci5leHRlbmQoZmxpbmtDb25maWcsIGNvbmZpZyk7XG4gICAgSm9ic1NlcnZpY2UubGlzdEpvYnMoKTtcbiAgICByZXR1cm4gJGludGVydmFsKGZ1bmN0aW9uKCkge1xuICAgICAgcmV0dXJuIEpvYnNTZXJ2aWNlLmxpc3RKb2JzKCk7XG4gICAgfSwgZmxpbmtDb25maWdbXCJyZWZyZXNoLWludGVydmFsXCJdKTtcbiAgfSk7XG59KS5jb25maWcoZnVuY3Rpb24oJHVpVmlld1Njcm9sbFByb3ZpZGVyKSB7XG4gIHJldHVybiAkdWlWaWV3U2Nyb2xsUHJvdmlkZXIudXNlQW5jaG9yU2Nyb2xsKCk7XG59KS5jb25maWcoZnVuY3Rpb24oJHN0YXRlUHJvdmlkZXIsICR1cmxSb3V0ZXJQcm92aWRlcikge1xuICAkc3RhdGVQcm92aWRlci5zdGF0ZShcIm92ZXJ2aWV3XCIsIHtcbiAgICB1cmw6IFwiL292ZXJ2aWV3XCIsXG4gICAgdmlld3M6IHtcbiAgICAgIG1haW46IHtcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvb3ZlcnZpZXcuaHRtbFwiLFxuICAgICAgICBjb250cm9sbGVyOiAnT3ZlcnZpZXdDb250cm9sbGVyJ1xuICAgICAgfVxuICAgIH1cbiAgfSkuc3RhdGUoXCJydW5uaW5nLWpvYnNcIiwge1xuICAgIHVybDogXCIvcnVubmluZy1qb2JzXCIsXG4gICAgdmlld3M6IHtcbiAgICAgIG1haW46IHtcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvam9icy9ydW5uaW5nLWpvYnMuaHRtbFwiLFxuICAgICAgICBjb250cm9sbGVyOiAnUnVubmluZ0pvYnNDb250cm9sbGVyJ1xuICAgICAgfVxuICAgIH1cbiAgfSkuc3RhdGUoXCJjb21wbGV0ZWQtam9ic1wiLCB7XG4gICAgdXJsOiBcIi9jb21wbGV0ZWQtam9ic1wiLFxuICAgIHZpZXdzOiB7XG4gICAgICBtYWluOiB7XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYnMvY29tcGxldGVkLWpvYnMuaHRtbFwiLFxuICAgICAgICBjb250cm9sbGVyOiAnQ29tcGxldGVkSm9ic0NvbnRyb2xsZXInXG4gICAgICB9XG4gICAgfVxuICB9KS5zdGF0ZShcInNpbmdsZS1qb2JcIiwge1xuICAgIHVybDogXCIvam9icy97am9iaWR9XCIsXG4gICAgYWJzdHJhY3Q6IHRydWUsXG4gICAgdmlld3M6IHtcbiAgICAgIG1haW46IHtcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvam9icy9qb2IuaHRtbFwiLFxuICAgICAgICBjb250cm9sbGVyOiAnU2luZ2xlSm9iQ29udHJvbGxlcidcbiAgICAgIH1cbiAgICB9XG4gIH0pLnN0YXRlKFwic2luZ2xlLWpvYi5wbGFuXCIsIHtcbiAgICB1cmw6IFwiXCIsXG4gICAgYWJzdHJhY3Q6IHRydWUsXG4gICAgdmlld3M6IHtcbiAgICAgIGRldGFpbHM6IHtcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvam9icy9qb2IucGxhbi5odG1sXCIsXG4gICAgICAgIGNvbnRyb2xsZXI6ICdKb2JQbGFuQ29udHJvbGxlcidcbiAgICAgIH1cbiAgICB9XG4gIH0pLnN0YXRlKFwic2luZ2xlLWpvYi5wbGFuLm92ZXJ2aWV3XCIsIHtcbiAgICB1cmw6IFwiXCIsXG4gICAgdmlld3M6IHtcbiAgICAgICdub2RlLWRldGFpbHMnOiB7XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYnMvam9iLnBsYW4ubm9kZS1saXN0Lm92ZXJ2aWV3Lmh0bWxcIixcbiAgICAgICAgY29udHJvbGxlcjogJ0pvYlBsYW5PdmVydmlld0NvbnRyb2xsZXInXG4gICAgICB9XG4gICAgfVxuICB9KS5zdGF0ZShcInNpbmdsZS1qb2IucGxhbi5hY2N1bXVsYXRvcnNcIiwge1xuICAgIHVybDogXCIvYWNjdW11bGF0b3JzXCIsXG4gICAgdmlld3M6IHtcbiAgICAgICdub2RlLWRldGFpbHMnOiB7XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYnMvam9iLnBsYW4ubm9kZS1saXN0LmFjY3VtdWxhdG9ycy5odG1sXCIsXG4gICAgICAgIGNvbnRyb2xsZXI6ICdKb2JQbGFuQWNjdW11bGF0b3JzQ29udHJvbGxlcidcbiAgICAgIH1cbiAgICB9XG4gIH0pLnN0YXRlKFwic2luZ2xlLWpvYi50aW1lbGluZVwiLCB7XG4gICAgdXJsOiBcIi90aW1lbGluZVwiLFxuICAgIHZpZXdzOiB7XG4gICAgICBkZXRhaWxzOiB7XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYnMvam9iLnRpbWVsaW5lLmh0bWxcIlxuICAgICAgfVxuICAgIH1cbiAgfSkuc3RhdGUoXCJzaW5nbGUtam9iLnRpbWVsaW5lLnZlcnRleFwiLCB7XG4gICAgdXJsOiBcIi97dmVydGV4SWR9XCIsXG4gICAgdmlld3M6IHtcbiAgICAgIHZlcnRleDoge1xuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JzL2pvYi50aW1lbGluZS52ZXJ0ZXguaHRtbFwiLFxuICAgICAgICBjb250cm9sbGVyOiAnSm9iVGltZWxpbmVWZXJ0ZXhDb250cm9sbGVyJ1xuICAgICAgfVxuICAgIH1cbiAgfSkuc3RhdGUoXCJzaW5nbGUtam9iLmV4Y2VwdGlvbnNcIiwge1xuICAgIHVybDogXCIvZXhjZXB0aW9uc1wiLFxuICAgIHZpZXdzOiB7XG4gICAgICBkZXRhaWxzOiB7XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYnMvam9iLmV4Y2VwdGlvbnMuaHRtbFwiLFxuICAgICAgICBjb250cm9sbGVyOiAnSm9iRXhjZXB0aW9uc0NvbnRyb2xsZXInXG4gICAgICB9XG4gICAgfVxuICB9KS5zdGF0ZShcInNpbmdsZS1qb2IucHJvcGVydGllc1wiLCB7XG4gICAgdXJsOiBcIi9wcm9wZXJ0aWVzXCIsXG4gICAgdmlld3M6IHtcbiAgICAgIGRldGFpbHM6IHtcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvam9icy9qb2IucHJvcGVydGllcy5odG1sXCIsXG4gICAgICAgIGNvbnRyb2xsZXI6ICdKb2JQcm9wZXJ0aWVzQ29udHJvbGxlcidcbiAgICAgIH1cbiAgICB9XG4gIH0pLnN0YXRlKFwic2luZ2xlLWpvYi5jb25maWdcIiwge1xuICAgIHVybDogXCIvY29uZmlnXCIsXG4gICAgdmlld3M6IHtcbiAgICAgIGRldGFpbHM6IHtcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvam9icy9qb2IuY29uZmlnLmh0bWxcIlxuICAgICAgfVxuICAgIH1cbiAgfSkuc3RhdGUoXCJhbGwtbWFuYWdlclwiLCB7XG4gICAgdXJsOiBcIi90YXNrbWFuYWdlcnNcIixcbiAgICB2aWV3czoge1xuICAgICAgbWFpbjoge1xuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy90YXNrbWFuYWdlci9pbmRleC5odG1sXCIsXG4gICAgICAgIGNvbnRyb2xsZXI6ICdBbGxUYXNrTWFuYWdlcnNDb250cm9sbGVyJ1xuICAgICAgfVxuICAgIH1cbiAgfSkuc3RhdGUoXCJzaW5nbGUtbWFuYWdlclwiLCB7XG4gICAgdXJsOiBcIi90YXNrbWFuYWdlci97dGFza21hbmFnZXJpZH1cIixcbiAgICB2aWV3czoge1xuICAgICAgbWFpbjoge1xuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy90YXNrbWFuYWdlci90YXNrbWFuYWdlci5odG1sXCIsXG4gICAgICAgIGNvbnRyb2xsZXI6ICdTaW5nbGVUYXNrTWFuYWdlckNvbnRyb2xsZXInXG4gICAgICB9XG4gICAgfVxuICB9KS5zdGF0ZShcInNpbmdsZS1tYW5hZ2VyLm1ldHJpY3NcIiwge1xuICAgIHVybDogXCIvbWV0cmljc1wiLFxuICAgIHZpZXdzOiB7XG4gICAgICBkZXRhaWxzOiB7XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL3Rhc2ttYW5hZ2VyL3Rhc2ttYW5hZ2VyLm1ldHJpY3MuaHRtbFwiXG4gICAgICB9XG4gICAgfVxuICB9KS5zdGF0ZShcImpvYm1hbmFnZXJcIiwge1xuICAgIHVybDogXCIvam9ibWFuYWdlclwiLFxuICAgIHZpZXdzOiB7XG4gICAgICBtYWluOiB7XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYm1hbmFnZXIvaW5kZXguaHRtbFwiXG4gICAgICB9XG4gICAgfVxuICB9KS5zdGF0ZShcImpvYm1hbmFnZXIuY29uZmlnXCIsIHtcbiAgICB1cmw6IFwiL2NvbmZpZ1wiLFxuICAgIHZpZXdzOiB7XG4gICAgICBkZXRhaWxzOiB7XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYm1hbmFnZXIvY29uZmlnLmh0bWxcIixcbiAgICAgICAgY29udHJvbGxlcjogJ0pvYk1hbmFnZXJDb25maWdDb250cm9sbGVyJ1xuICAgICAgfVxuICAgIH1cbiAgfSkuc3RhdGUoXCJqb2JtYW5hZ2VyLnN0ZG91dFwiLCB7XG4gICAgdXJsOiBcIi9zdGRvdXRcIixcbiAgICB2aWV3czoge1xuICAgICAgZGV0YWlsczoge1xuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JtYW5hZ2VyL3N0ZG91dC5odG1sXCIsXG4gICAgICAgIGNvbnRyb2xsZXI6ICdKb2JNYW5hZ2VyU3Rkb3V0Q29udHJvbGxlcidcbiAgICAgIH1cbiAgICB9XG4gIH0pLnN0YXRlKFwiam9ibWFuYWdlci5sb2dcIiwge1xuICAgIHVybDogXCIvbG9nXCIsXG4gICAgdmlld3M6IHtcbiAgICAgIGRldGFpbHM6IHtcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvam9ibWFuYWdlci9sb2cuaHRtbFwiLFxuICAgICAgICBjb250cm9sbGVyOiAnSm9iTWFuYWdlckxvZ3NDb250cm9sbGVyJ1xuICAgICAgfVxuICAgIH1cbiAgfSk7XG4gIHJldHVybiAkdXJsUm91dGVyUHJvdmlkZXIub3RoZXJ3aXNlKFwiL292ZXJ2aWV3XCIpO1xufSk7XG4iLCIjXG4jIExpY2Vuc2VkIHRvIHRoZSBBcGFjaGUgU29mdHdhcmUgRm91bmRhdGlvbiAoQVNGKSB1bmRlciBvbmVcbiMgb3IgbW9yZSBjb250cmlidXRvciBsaWNlbnNlIGFncmVlbWVudHMuICBTZWUgdGhlIE5PVElDRSBmaWxlXG4jIGRpc3RyaWJ1dGVkIHdpdGggdGhpcyB3b3JrIGZvciBhZGRpdGlvbmFsIGluZm9ybWF0aW9uXG4jIHJlZ2FyZGluZyBjb3B5cmlnaHQgb3duZXJzaGlwLiAgVGhlIEFTRiBsaWNlbnNlcyB0aGlzIGZpbGVcbiMgdG8geW91IHVuZGVyIHRoZSBBcGFjaGUgTGljZW5zZSwgVmVyc2lvbiAyLjAgKHRoZVxuIyBcIkxpY2Vuc2VcIik7IHlvdSBtYXkgbm90IHVzZSB0aGlzIGZpbGUgZXhjZXB0IGluIGNvbXBsaWFuY2VcbiMgd2l0aCB0aGUgTGljZW5zZS4gIFlvdSBtYXkgb2J0YWluIGEgY29weSBvZiB0aGUgTGljZW5zZSBhdFxuI1xuIyAgICAgaHR0cDovL3d3dy5hcGFjaGUub3JnL2xpY2Vuc2VzL0xJQ0VOU0UtMi4wXG4jXG4jIFVubGVzcyByZXF1aXJlZCBieSBhcHBsaWNhYmxlIGxhdyBvciBhZ3JlZWQgdG8gaW4gd3JpdGluZywgc29mdHdhcmVcbiMgZGlzdHJpYnV0ZWQgdW5kZXIgdGhlIExpY2Vuc2UgaXMgZGlzdHJpYnV0ZWQgb24gYW4gXCJBUyBJU1wiIEJBU0lTLFxuIyBXSVRIT1VUIFdBUlJBTlRJRVMgT1IgQ09ORElUSU9OUyBPRiBBTlkgS0lORCwgZWl0aGVyIGV4cHJlc3Mgb3IgaW1wbGllZC5cbiMgU2VlIHRoZSBMaWNlbnNlIGZvciB0aGUgc3BlY2lmaWMgbGFuZ3VhZ2UgZ292ZXJuaW5nIHBlcm1pc3Npb25zIGFuZFxuIyBsaW1pdGF0aW9ucyB1bmRlciB0aGUgTGljZW5zZS5cbiNcblxuYW5ndWxhci5tb2R1bGUoJ2ZsaW5rQXBwJylcblxuIyAtLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tXG5cbi5kaXJlY3RpdmUgJ2JzTGFiZWwnLCAoSm9ic1NlcnZpY2UpIC0+XG4gIHRyYW5zY2x1ZGU6IHRydWVcbiAgcmVwbGFjZTogdHJ1ZVxuICBzY29wZTogXG4gICAgZ2V0TGFiZWxDbGFzczogXCImXCJcbiAgICBzdGF0dXM6IFwiQFwiXG5cbiAgdGVtcGxhdGU6IFwiPHNwYW4gdGl0bGU9J3t7c3RhdHVzfX0nIG5nLWNsYXNzPSdnZXRMYWJlbENsYXNzKCknPjxuZy10cmFuc2NsdWRlPjwvbmctdHJhbnNjbHVkZT48L3NwYW4+XCJcbiAgXG4gIGxpbms6IChzY29wZSwgZWxlbWVudCwgYXR0cnMpIC0+XG4gICAgc2NvcGUuZ2V0TGFiZWxDbGFzcyA9IC0+XG4gICAgICAnbGFiZWwgbGFiZWwtJyArIEpvYnNTZXJ2aWNlLnRyYW5zbGF0ZUxhYmVsU3RhdGUoYXR0cnMuc3RhdHVzKVxuXG4jIC0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS1cblxuLmRpcmVjdGl2ZSAnaW5kaWNhdG9yUHJpbWFyeScsIChKb2JzU2VydmljZSkgLT5cbiAgcmVwbGFjZTogdHJ1ZVxuICBzY29wZTogXG4gICAgZ2V0TGFiZWxDbGFzczogXCImXCJcbiAgICBzdGF0dXM6ICdAJ1xuXG4gIHRlbXBsYXRlOiBcIjxpIHRpdGxlPSd7e3N0YXR1c319JyBuZy1jbGFzcz0nZ2V0TGFiZWxDbGFzcygpJyAvPlwiXG4gIFxuICBsaW5rOiAoc2NvcGUsIGVsZW1lbnQsIGF0dHJzKSAtPlxuICAgIHNjb3BlLmdldExhYmVsQ2xhc3MgPSAtPlxuICAgICAgJ2ZhIGZhLWNpcmNsZSBpbmRpY2F0b3IgaW5kaWNhdG9yLScgKyBKb2JzU2VydmljZS50cmFuc2xhdGVMYWJlbFN0YXRlKGF0dHJzLnN0YXR1cylcblxuIyAtLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tXG5cbi5kaXJlY3RpdmUgJ3RhYmxlUHJvcGVydHknLCAtPlxuICByZXBsYWNlOiB0cnVlXG4gIHNjb3BlOlxuICAgIHZhbHVlOiAnPSdcblxuICB0ZW1wbGF0ZTogXCI8dGQgdGl0bGU9XFxcInt7dmFsdWUgfHwgJ05vbmUnfX1cXFwiPnt7dmFsdWUgfHwgJ05vbmUnfX08L3RkPlwiXG4iLCJhbmd1bGFyLm1vZHVsZSgnZmxpbmtBcHAnKS5kaXJlY3RpdmUoJ2JzTGFiZWwnLCBmdW5jdGlvbihKb2JzU2VydmljZSkge1xuICByZXR1cm4ge1xuICAgIHRyYW5zY2x1ZGU6IHRydWUsXG4gICAgcmVwbGFjZTogdHJ1ZSxcbiAgICBzY29wZToge1xuICAgICAgZ2V0TGFiZWxDbGFzczogXCImXCIsXG4gICAgICBzdGF0dXM6IFwiQFwiXG4gICAgfSxcbiAgICB0ZW1wbGF0ZTogXCI8c3BhbiB0aXRsZT0ne3tzdGF0dXN9fScgbmctY2xhc3M9J2dldExhYmVsQ2xhc3MoKSc+PG5nLXRyYW5zY2x1ZGU+PC9uZy10cmFuc2NsdWRlPjwvc3Bhbj5cIixcbiAgICBsaW5rOiBmdW5jdGlvbihzY29wZSwgZWxlbWVudCwgYXR0cnMpIHtcbiAgICAgIHJldHVybiBzY29wZS5nZXRMYWJlbENsYXNzID0gZnVuY3Rpb24oKSB7XG4gICAgICAgIHJldHVybiAnbGFiZWwgbGFiZWwtJyArIEpvYnNTZXJ2aWNlLnRyYW5zbGF0ZUxhYmVsU3RhdGUoYXR0cnMuc3RhdHVzKTtcbiAgICAgIH07XG4gICAgfVxuICB9O1xufSkuZGlyZWN0aXZlKCdpbmRpY2F0b3JQcmltYXJ5JywgZnVuY3Rpb24oSm9ic1NlcnZpY2UpIHtcbiAgcmV0dXJuIHtcbiAgICByZXBsYWNlOiB0cnVlLFxuICAgIHNjb3BlOiB7XG4gICAgICBnZXRMYWJlbENsYXNzOiBcIiZcIixcbiAgICAgIHN0YXR1czogJ0AnXG4gICAgfSxcbiAgICB0ZW1wbGF0ZTogXCI8aSB0aXRsZT0ne3tzdGF0dXN9fScgbmctY2xhc3M9J2dldExhYmVsQ2xhc3MoKScgLz5cIixcbiAgICBsaW5rOiBmdW5jdGlvbihzY29wZSwgZWxlbWVudCwgYXR0cnMpIHtcbiAgICAgIHJldHVybiBzY29wZS5nZXRMYWJlbENsYXNzID0gZnVuY3Rpb24oKSB7XG4gICAgICAgIHJldHVybiAnZmEgZmEtY2lyY2xlIGluZGljYXRvciBpbmRpY2F0b3ItJyArIEpvYnNTZXJ2aWNlLnRyYW5zbGF0ZUxhYmVsU3RhdGUoYXR0cnMuc3RhdHVzKTtcbiAgICAgIH07XG4gICAgfVxuICB9O1xufSkuZGlyZWN0aXZlKCd0YWJsZVByb3BlcnR5JywgZnVuY3Rpb24oKSB7XG4gIHJldHVybiB7XG4gICAgcmVwbGFjZTogdHJ1ZSxcbiAgICBzY29wZToge1xuICAgICAgdmFsdWU6ICc9J1xuICAgIH0sXG4gICAgdGVtcGxhdGU6IFwiPHRkIHRpdGxlPVxcXCJ7e3ZhbHVlIHx8ICdOb25lJ319XFxcIj57e3ZhbHVlIHx8ICdOb25lJ319PC90ZD5cIlxuICB9O1xufSk7XG4iLCIjXG4jIExpY2Vuc2VkIHRvIHRoZSBBcGFjaGUgU29mdHdhcmUgRm91bmRhdGlvbiAoQVNGKSB1bmRlciBvbmVcbiMgb3IgbW9yZSBjb250cmlidXRvciBsaWNlbnNlIGFncmVlbWVudHMuICBTZWUgdGhlIE5PVElDRSBmaWxlXG4jIGRpc3RyaWJ1dGVkIHdpdGggdGhpcyB3b3JrIGZvciBhZGRpdGlvbmFsIGluZm9ybWF0aW9uXG4jIHJlZ2FyZGluZyBjb3B5cmlnaHQgb3duZXJzaGlwLiAgVGhlIEFTRiBsaWNlbnNlcyB0aGlzIGZpbGVcbiMgdG8geW91IHVuZGVyIHRoZSBBcGFjaGUgTGljZW5zZSwgVmVyc2lvbiAyLjAgKHRoZVxuIyBcIkxpY2Vuc2VcIik7IHlvdSBtYXkgbm90IHVzZSB0aGlzIGZpbGUgZXhjZXB0IGluIGNvbXBsaWFuY2VcbiMgd2l0aCB0aGUgTGljZW5zZS4gIFlvdSBtYXkgb2J0YWluIGEgY29weSBvZiB0aGUgTGljZW5zZSBhdFxuI1xuIyAgICAgaHR0cDovL3d3dy5hcGFjaGUub3JnL2xpY2Vuc2VzL0xJQ0VOU0UtMi4wXG4jXG4jIFVubGVzcyByZXF1aXJlZCBieSBhcHBsaWNhYmxlIGxhdyBvciBhZ3JlZWQgdG8gaW4gd3JpdGluZywgc29mdHdhcmVcbiMgZGlzdHJpYnV0ZWQgdW5kZXIgdGhlIExpY2Vuc2UgaXMgZGlzdHJpYnV0ZWQgb24gYW4gXCJBUyBJU1wiIEJBU0lTLFxuIyBXSVRIT1VUIFdBUlJBTlRJRVMgT1IgQ09ORElUSU9OUyBPRiBBTlkgS0lORCwgZWl0aGVyIGV4cHJlc3Mgb3IgaW1wbGllZC5cbiMgU2VlIHRoZSBMaWNlbnNlIGZvciB0aGUgc3BlY2lmaWMgbGFuZ3VhZ2UgZ292ZXJuaW5nIHBlcm1pc3Npb25zIGFuZFxuIyBsaW1pdGF0aW9ucyB1bmRlciB0aGUgTGljZW5zZS5cbiNcblxuYW5ndWxhci5tb2R1bGUoJ2ZsaW5rQXBwJylcblxuLmZpbHRlciBcImFtRHVyYXRpb25Gb3JtYXRFeHRlbmRlZFwiLCAoYW5ndWxhck1vbWVudENvbmZpZykgLT5cbiAgYW1EdXJhdGlvbkZvcm1hdEV4dGVuZGVkRmlsdGVyID0gKHZhbHVlLCBmb3JtYXQsIGR1cmF0aW9uRm9ybWF0KSAtPlxuICAgIHJldHVybiBcIlwiICBpZiB0eXBlb2YgdmFsdWUgaXMgXCJ1bmRlZmluZWRcIiBvciB2YWx1ZSBpcyBudWxsXG5cbiAgICBtb21lbnQuZHVyYXRpb24odmFsdWUsIGZvcm1hdCkuZm9ybWF0KGR1cmF0aW9uRm9ybWF0LCB7IHRyaW06IGZhbHNlIH0pXG5cbiAgYW1EdXJhdGlvbkZvcm1hdEV4dGVuZGVkRmlsdGVyLiRzdGF0ZWZ1bCA9IGFuZ3VsYXJNb21lbnRDb25maWcuc3RhdGVmdWxGaWx0ZXJzXG5cbiAgYW1EdXJhdGlvbkZvcm1hdEV4dGVuZGVkRmlsdGVyXG5cbi5maWx0ZXIgXCJodW1hbml6ZUR1cmF0aW9uXCIsIC0+XG4gICh2YWx1ZSwgc2hvcnQpIC0+XG4gICAgcmV0dXJuIFwiXCIgaWYgdHlwZW9mIHZhbHVlIGlzIFwidW5kZWZpbmVkXCIgb3IgdmFsdWUgaXMgbnVsbFxuICAgIG1zID0gdmFsdWUgJSAxMDAwXG4gICAgeCA9IE1hdGguZmxvb3IodmFsdWUgLyAxMDAwKVxuICAgIHNlY29uZHMgPSB4ICUgNjBcbiAgICB4ID0gTWF0aC5mbG9vcih4IC8gNjApXG4gICAgbWludXRlcyA9IHggJSA2MFxuICAgIHggPSBNYXRoLmZsb29yKHggLyA2MClcbiAgICBob3VycyA9IHggJSAyNFxuICAgIHggPSBNYXRoLmZsb29yKHggLyAyNClcbiAgICBkYXlzID0geFxuICAgIGlmIGRheXMgPT0gMFxuICAgICAgaWYgaG91cnMgPT0gMFxuICAgICAgICBpZiBtaW51dGVzID09IDBcbiAgICAgICAgICBpZiBzZWNvbmRzID09IDBcbiAgICAgICAgICAgIHJldHVybiBtcyArIFwibXNcIlxuICAgICAgICAgIGVsc2VcbiAgICAgICAgICAgIHJldHVybiBzZWNvbmRzICsgXCJzIFwiXG4gICAgICAgIGVsc2VcbiAgICAgICAgICByZXR1cm4gbWludXRlcyArIFwibSBcIiArIHNlY29uZHMgKyBcInNcIlxuICAgICAgZWxzZVxuICAgICAgICBpZiBzaG9ydCB0aGVuIHJldHVybiBob3VycyArIFwiaCBcIiArIG1pbnV0ZXMgKyBcIm1cIiBlbHNlIHJldHVybiBob3VycyArIFwiaCBcIiArIG1pbnV0ZXMgKyBcIm0gXCIgKyBzZWNvbmRzICsgXCJzXCJcbiAgICBlbHNlXG4gICAgICBpZiBzaG9ydCB0aGVuIHJldHVybiBkYXlzICsgXCJkIFwiICsgaG91cnMgKyBcImhcIiBlbHNlIHJldHVybiBkYXlzICsgXCJkIFwiICsgaG91cnMgKyBcImggXCIgKyBtaW51dGVzICsgXCJtIFwiICsgc2Vjb25kcyArIFwic1wiXG5cbi5maWx0ZXIgXCJodW1hbml6ZVRleHRcIiwgLT5cbiAgKHRleHQpIC0+XG4gICAgIyBUT0RPOiBleHRlbmQuLi4gYSBsb3RcbiAgICBpZiB0ZXh0IHRoZW4gdGV4dC5yZXBsYWNlKC8mZ3Q7L2csIFwiPlwiKS5yZXBsYWNlKC88YnJcXC8+L2csXCJcIikgZWxzZSAnJ1xuXG4uZmlsdGVyIFwiYnl0ZXNcIiwgLT5cbiAgKGJ5dGVzLCBwcmVjaXNpb24pIC0+XG4gICAgcmV0dXJuIFwiLVwiICBpZiBpc05hTihwYXJzZUZsb2F0KGJ5dGVzKSkgb3Igbm90IGlzRmluaXRlKGJ5dGVzKVxuICAgIHByZWNpc2lvbiA9IDEgIGlmIHR5cGVvZiBwcmVjaXNpb24gaXMgXCJ1bmRlZmluZWRcIlxuICAgIHVuaXRzID0gWyBcImJ5dGVzXCIsIFwia0JcIiwgXCJNQlwiLCBcIkdCXCIsIFwiVEJcIiwgXCJQQlwiIF1cbiAgICBudW1iZXIgPSBNYXRoLmZsb29yKE1hdGgubG9nKGJ5dGVzKSAvIE1hdGgubG9nKDEwMjQpKVxuICAgIChieXRlcyAvIE1hdGgucG93KDEwMjQsIE1hdGguZmxvb3IobnVtYmVyKSkpLnRvRml4ZWQocHJlY2lzaW9uKSArIFwiIFwiICsgdW5pdHNbbnVtYmVyXVxuIiwiYW5ndWxhci5tb2R1bGUoJ2ZsaW5rQXBwJykuZmlsdGVyKFwiYW1EdXJhdGlvbkZvcm1hdEV4dGVuZGVkXCIsIGZ1bmN0aW9uKGFuZ3VsYXJNb21lbnRDb25maWcpIHtcbiAgdmFyIGFtRHVyYXRpb25Gb3JtYXRFeHRlbmRlZEZpbHRlcjtcbiAgYW1EdXJhdGlvbkZvcm1hdEV4dGVuZGVkRmlsdGVyID0gZnVuY3Rpb24odmFsdWUsIGZvcm1hdCwgZHVyYXRpb25Gb3JtYXQpIHtcbiAgICBpZiAodHlwZW9mIHZhbHVlID09PSBcInVuZGVmaW5lZFwiIHx8IHZhbHVlID09PSBudWxsKSB7XG4gICAgICByZXR1cm4gXCJcIjtcbiAgICB9XG4gICAgcmV0dXJuIG1vbWVudC5kdXJhdGlvbih2YWx1ZSwgZm9ybWF0KS5mb3JtYXQoZHVyYXRpb25Gb3JtYXQsIHtcbiAgICAgIHRyaW06IGZhbHNlXG4gICAgfSk7XG4gIH07XG4gIGFtRHVyYXRpb25Gb3JtYXRFeHRlbmRlZEZpbHRlci4kc3RhdGVmdWwgPSBhbmd1bGFyTW9tZW50Q29uZmlnLnN0YXRlZnVsRmlsdGVycztcbiAgcmV0dXJuIGFtRHVyYXRpb25Gb3JtYXRFeHRlbmRlZEZpbHRlcjtcbn0pLmZpbHRlcihcImh1bWFuaXplRHVyYXRpb25cIiwgZnVuY3Rpb24oKSB7XG4gIHJldHVybiBmdW5jdGlvbih2YWx1ZSwgc2hvcnQpIHtcbiAgICB2YXIgZGF5cywgaG91cnMsIG1pbnV0ZXMsIG1zLCBzZWNvbmRzLCB4O1xuICAgIGlmICh0eXBlb2YgdmFsdWUgPT09IFwidW5kZWZpbmVkXCIgfHwgdmFsdWUgPT09IG51bGwpIHtcbiAgICAgIHJldHVybiBcIlwiO1xuICAgIH1cbiAgICBtcyA9IHZhbHVlICUgMTAwMDtcbiAgICB4ID0gTWF0aC5mbG9vcih2YWx1ZSAvIDEwMDApO1xuICAgIHNlY29uZHMgPSB4ICUgNjA7XG4gICAgeCA9IE1hdGguZmxvb3IoeCAvIDYwKTtcbiAgICBtaW51dGVzID0geCAlIDYwO1xuICAgIHggPSBNYXRoLmZsb29yKHggLyA2MCk7XG4gICAgaG91cnMgPSB4ICUgMjQ7XG4gICAgeCA9IE1hdGguZmxvb3IoeCAvIDI0KTtcbiAgICBkYXlzID0geDtcbiAgICBpZiAoZGF5cyA9PT0gMCkge1xuICAgICAgaWYgKGhvdXJzID09PSAwKSB7XG4gICAgICAgIGlmIChtaW51dGVzID09PSAwKSB7XG4gICAgICAgICAgaWYgKHNlY29uZHMgPT09IDApIHtcbiAgICAgICAgICAgIHJldHVybiBtcyArIFwibXNcIjtcbiAgICAgICAgICB9IGVsc2Uge1xuICAgICAgICAgICAgcmV0dXJuIHNlY29uZHMgKyBcInMgXCI7XG4gICAgICAgICAgfVxuICAgICAgICB9IGVsc2Uge1xuICAgICAgICAgIHJldHVybiBtaW51dGVzICsgXCJtIFwiICsgc2Vjb25kcyArIFwic1wiO1xuICAgICAgICB9XG4gICAgICB9IGVsc2Uge1xuICAgICAgICBpZiAoc2hvcnQpIHtcbiAgICAgICAgICByZXR1cm4gaG91cnMgKyBcImggXCIgKyBtaW51dGVzICsgXCJtXCI7XG4gICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgcmV0dXJuIGhvdXJzICsgXCJoIFwiICsgbWludXRlcyArIFwibSBcIiArIHNlY29uZHMgKyBcInNcIjtcbiAgICAgICAgfVxuICAgICAgfVxuICAgIH0gZWxzZSB7XG4gICAgICBpZiAoc2hvcnQpIHtcbiAgICAgICAgcmV0dXJuIGRheXMgKyBcImQgXCIgKyBob3VycyArIFwiaFwiO1xuICAgICAgfSBlbHNlIHtcbiAgICAgICAgcmV0dXJuIGRheXMgKyBcImQgXCIgKyBob3VycyArIFwiaCBcIiArIG1pbnV0ZXMgKyBcIm0gXCIgKyBzZWNvbmRzICsgXCJzXCI7XG4gICAgICB9XG4gICAgfVxuICB9O1xufSkuZmlsdGVyKFwiaHVtYW5pemVUZXh0XCIsIGZ1bmN0aW9uKCkge1xuICByZXR1cm4gZnVuY3Rpb24odGV4dCkge1xuICAgIGlmICh0ZXh0KSB7XG4gICAgICByZXR1cm4gdGV4dC5yZXBsYWNlKC8mZ3Q7L2csIFwiPlwiKS5yZXBsYWNlKC88YnJcXC8+L2csIFwiXCIpO1xuICAgIH0gZWxzZSB7XG4gICAgICByZXR1cm4gJyc7XG4gICAgfVxuICB9O1xufSkuZmlsdGVyKFwiYnl0ZXNcIiwgZnVuY3Rpb24oKSB7XG4gIHJldHVybiBmdW5jdGlvbihieXRlcywgcHJlY2lzaW9uKSB7XG4gICAgdmFyIG51bWJlciwgdW5pdHM7XG4gICAgaWYgKGlzTmFOKHBhcnNlRmxvYXQoYnl0ZXMpKSB8fCAhaXNGaW5pdGUoYnl0ZXMpKSB7XG4gICAgICByZXR1cm4gXCItXCI7XG4gICAgfVxuICAgIGlmICh0eXBlb2YgcHJlY2lzaW9uID09PSBcInVuZGVmaW5lZFwiKSB7XG4gICAgICBwcmVjaXNpb24gPSAxO1xuICAgIH1cbiAgICB1bml0cyA9IFtcImJ5dGVzXCIsIFwia0JcIiwgXCJNQlwiLCBcIkdCXCIsIFwiVEJcIiwgXCJQQlwiXTtcbiAgICBudW1iZXIgPSBNYXRoLmZsb29yKE1hdGgubG9nKGJ5dGVzKSAvIE1hdGgubG9nKDEwMjQpKTtcbiAgICByZXR1cm4gKGJ5dGVzIC8gTWF0aC5wb3coMTAyNCwgTWF0aC5mbG9vcihudW1iZXIpKSkudG9GaXhlZChwcmVjaXNpb24pICsgXCIgXCIgKyB1bml0c1tudW1iZXJdO1xuICB9O1xufSk7XG4iLCIjXG4jIExpY2Vuc2VkIHRvIHRoZSBBcGFjaGUgU29mdHdhcmUgRm91bmRhdGlvbiAoQVNGKSB1bmRlciBvbmVcbiMgb3IgbW9yZSBjb250cmlidXRvciBsaWNlbnNlIGFncmVlbWVudHMuICBTZWUgdGhlIE5PVElDRSBmaWxlXG4jIGRpc3RyaWJ1dGVkIHdpdGggdGhpcyB3b3JrIGZvciBhZGRpdGlvbmFsIGluZm9ybWF0aW9uXG4jIHJlZ2FyZGluZyBjb3B5cmlnaHQgb3duZXJzaGlwLiAgVGhlIEFTRiBsaWNlbnNlcyB0aGlzIGZpbGVcbiMgdG8geW91IHVuZGVyIHRoZSBBcGFjaGUgTGljZW5zZSwgVmVyc2lvbiAyLjAgKHRoZVxuIyBcIkxpY2Vuc2VcIik7IHlvdSBtYXkgbm90IHVzZSB0aGlzIGZpbGUgZXhjZXB0IGluIGNvbXBsaWFuY2VcbiMgd2l0aCB0aGUgTGljZW5zZS4gIFlvdSBtYXkgb2J0YWluIGEgY29weSBvZiB0aGUgTGljZW5zZSBhdFxuI1xuIyAgICAgaHR0cDovL3d3dy5hcGFjaGUub3JnL2xpY2Vuc2VzL0xJQ0VOU0UtMi4wXG4jXG4jIFVubGVzcyByZXF1aXJlZCBieSBhcHBsaWNhYmxlIGxhdyBvciBhZ3JlZWQgdG8gaW4gd3JpdGluZywgc29mdHdhcmVcbiMgZGlzdHJpYnV0ZWQgdW5kZXIgdGhlIExpY2Vuc2UgaXMgZGlzdHJpYnV0ZWQgb24gYW4gXCJBUyBJU1wiIEJBU0lTLFxuIyBXSVRIT1VUIFdBUlJBTlRJRVMgT1IgQ09ORElUSU9OUyBPRiBBTlkgS0lORCwgZWl0aGVyIGV4cHJlc3Mgb3IgaW1wbGllZC5cbiMgU2VlIHRoZSBMaWNlbnNlIGZvciB0aGUgc3BlY2lmaWMgbGFuZ3VhZ2UgZ292ZXJuaW5nIHBlcm1pc3Npb25zIGFuZFxuIyBsaW1pdGF0aW9ucyB1bmRlciB0aGUgTGljZW5zZS5cbiNcblxuYW5ndWxhci5tb2R1bGUoJ2ZsaW5rQXBwJylcblxuLnNlcnZpY2UgJ01haW5TZXJ2aWNlJywgKCRodHRwLCBmbGlua0NvbmZpZywgJHEpIC0+XG4gIEBsb2FkQ29uZmlnID0gLT5cbiAgICBkZWZlcnJlZCA9ICRxLmRlZmVyKClcblxuICAgICRodHRwLmdldCBcImNvbmZpZ1wiXG4gICAgLnN1Y2Nlc3MgKGRhdGEsIHN0YXR1cywgaGVhZGVycywgY29uZmlnKSAtPlxuICAgICAgZGVmZXJyZWQucmVzb2x2ZShkYXRhKVxuXG4gICAgZGVmZXJyZWQucHJvbWlzZVxuXG5cbiAgQFxuIiwiYW5ndWxhci5tb2R1bGUoJ2ZsaW5rQXBwJykuc2VydmljZSgnTWFpblNlcnZpY2UnLCBmdW5jdGlvbigkaHR0cCwgZmxpbmtDb25maWcsICRxKSB7XG4gIHRoaXMubG9hZENvbmZpZyA9IGZ1bmN0aW9uKCkge1xuICAgIHZhciBkZWZlcnJlZDtcbiAgICBkZWZlcnJlZCA9ICRxLmRlZmVyKCk7XG4gICAgJGh0dHAuZ2V0KFwiY29uZmlnXCIpLnN1Y2Nlc3MoZnVuY3Rpb24oZGF0YSwgc3RhdHVzLCBoZWFkZXJzLCBjb25maWcpIHtcbiAgICAgIHJldHVybiBkZWZlcnJlZC5yZXNvbHZlKGRhdGEpO1xuICAgIH0pO1xuICAgIHJldHVybiBkZWZlcnJlZC5wcm9taXNlO1xuICB9O1xuICByZXR1cm4gdGhpcztcbn0pO1xuIiwiI1xuIyBMaWNlbnNlZCB0byB0aGUgQXBhY2hlIFNvZnR3YXJlIEZvdW5kYXRpb24gKEFTRikgdW5kZXIgb25lXG4jIG9yIG1vcmUgY29udHJpYnV0b3IgbGljZW5zZSBhZ3JlZW1lbnRzLiAgU2VlIHRoZSBOT1RJQ0UgZmlsZVxuIyBkaXN0cmlidXRlZCB3aXRoIHRoaXMgd29yayBmb3IgYWRkaXRpb25hbCBpbmZvcm1hdGlvblxuIyByZWdhcmRpbmcgY29weXJpZ2h0IG93bmVyc2hpcC4gIFRoZSBBU0YgbGljZW5zZXMgdGhpcyBmaWxlXG4jIHRvIHlvdSB1bmRlciB0aGUgQXBhY2hlIExpY2Vuc2UsIFZlcnNpb24gMi4wICh0aGVcbiMgXCJMaWNlbnNlXCIpOyB5b3UgbWF5IG5vdCB1c2UgdGhpcyBmaWxlIGV4Y2VwdCBpbiBjb21wbGlhbmNlXG4jIHdpdGggdGhlIExpY2Vuc2UuICBZb3UgbWF5IG9idGFpbiBhIGNvcHkgb2YgdGhlIExpY2Vuc2UgYXRcbiNcbiMgICAgIGh0dHA6Ly93d3cuYXBhY2hlLm9yZy9saWNlbnNlcy9MSUNFTlNFLTIuMFxuI1xuIyBVbmxlc3MgcmVxdWlyZWQgYnkgYXBwbGljYWJsZSBsYXcgb3IgYWdyZWVkIHRvIGluIHdyaXRpbmcsIHNvZnR3YXJlXG4jIGRpc3RyaWJ1dGVkIHVuZGVyIHRoZSBMaWNlbnNlIGlzIGRpc3RyaWJ1dGVkIG9uIGFuIFwiQVMgSVNcIiBCQVNJUyxcbiMgV0lUSE9VVCBXQVJSQU5USUVTIE9SIENPTkRJVElPTlMgT0YgQU5ZIEtJTkQsIGVpdGhlciBleHByZXNzIG9yIGltcGxpZWQuXG4jIFNlZSB0aGUgTGljZW5zZSBmb3IgdGhlIHNwZWNpZmljIGxhbmd1YWdlIGdvdmVybmluZyBwZXJtaXNzaW9ucyBhbmRcbiMgbGltaXRhdGlvbnMgdW5kZXIgdGhlIExpY2Vuc2UuXG4jXG5cbmFuZ3VsYXIubW9kdWxlKCdmbGlua0FwcCcpXG5cbi5jb250cm9sbGVyICdKb2JNYW5hZ2VyQ29uZmlnQ29udHJvbGxlcicsICgkc2NvcGUsIEpvYk1hbmFnZXJDb25maWdTZXJ2aWNlKSAtPlxuICBKb2JNYW5hZ2VyQ29uZmlnU2VydmljZS5sb2FkQ29uZmlnKCkudGhlbiAoZGF0YSkgLT5cbiAgICBpZiAhJHNjb3BlLmpvYm1hbmFnZXI/XG4gICAgICAkc2NvcGUuam9ibWFuYWdlciA9IHt9XG4gICAgJHNjb3BlLmpvYm1hbmFnZXJbJ2NvbmZpZyddID0gZGF0YVxuXG4uY29udHJvbGxlciAnSm9iTWFuYWdlckxvZ3NDb250cm9sbGVyJywgKCRzY29wZSwgSm9iTWFuYWdlckxvZ3NTZXJ2aWNlKSAtPlxuICBKb2JNYW5hZ2VyTG9nc1NlcnZpY2UubG9hZExvZ3MoKS50aGVuIChkYXRhKSAtPlxuICAgIGlmICEkc2NvcGUuam9ibWFuYWdlcj9cbiAgICAgICRzY29wZS5qb2JtYW5hZ2VyID0ge31cbiAgICAkc2NvcGUuam9ibWFuYWdlclsnbG9nJ10gPSBkYXRhXG5cbiAgJHNjb3BlLnJlbG9hZERhdGEgPSAoKSAtPlxuICAgIEpvYk1hbmFnZXJMb2dzU2VydmljZS5sb2FkTG9ncygpLnRoZW4gKGRhdGEpIC0+XG4gICAgICAkc2NvcGUuam9ibWFuYWdlclsnbG9nJ10gPSBkYXRhXG5cbi5jb250cm9sbGVyICdKb2JNYW5hZ2VyU3Rkb3V0Q29udHJvbGxlcicsICgkc2NvcGUsIEpvYk1hbmFnZXJTdGRvdXRTZXJ2aWNlKSAtPlxuICBKb2JNYW5hZ2VyU3Rkb3V0U2VydmljZS5sb2FkU3Rkb3V0KCkudGhlbiAoZGF0YSkgLT5cbiAgICBpZiAhJHNjb3BlLmpvYm1hbmFnZXI/XG4gICAgICAkc2NvcGUuam9ibWFuYWdlciA9IHt9XG4gICAgJHNjb3BlLmpvYm1hbmFnZXJbJ3N0ZG91dCddID0gZGF0YVxuXG4gICRzY29wZS5yZWxvYWREYXRhID0gKCkgLT5cbiAgICBKb2JNYW5hZ2VyU3Rkb3V0U2VydmljZS5sb2FkU3Rkb3V0KCkudGhlbiAoZGF0YSkgLT5cbiAgICAgICRzY29wZS5qb2JtYW5hZ2VyWydzdGRvdXQnXSA9IGRhdGFcbiIsImFuZ3VsYXIubW9kdWxlKCdmbGlua0FwcCcpLmNvbnRyb2xsZXIoJ0pvYk1hbmFnZXJDb25maWdDb250cm9sbGVyJywgZnVuY3Rpb24oJHNjb3BlLCBKb2JNYW5hZ2VyQ29uZmlnU2VydmljZSkge1xuICByZXR1cm4gSm9iTWFuYWdlckNvbmZpZ1NlcnZpY2UubG9hZENvbmZpZygpLnRoZW4oZnVuY3Rpb24oZGF0YSkge1xuICAgIGlmICgkc2NvcGUuam9ibWFuYWdlciA9PSBudWxsKSB7XG4gICAgICAkc2NvcGUuam9ibWFuYWdlciA9IHt9O1xuICAgIH1cbiAgICByZXR1cm4gJHNjb3BlLmpvYm1hbmFnZXJbJ2NvbmZpZyddID0gZGF0YTtcbiAgfSk7XG59KS5jb250cm9sbGVyKCdKb2JNYW5hZ2VyTG9nc0NvbnRyb2xsZXInLCBmdW5jdGlvbigkc2NvcGUsIEpvYk1hbmFnZXJMb2dzU2VydmljZSkge1xuICBKb2JNYW5hZ2VyTG9nc1NlcnZpY2UubG9hZExvZ3MoKS50aGVuKGZ1bmN0aW9uKGRhdGEpIHtcbiAgICBpZiAoJHNjb3BlLmpvYm1hbmFnZXIgPT0gbnVsbCkge1xuICAgICAgJHNjb3BlLmpvYm1hbmFnZXIgPSB7fTtcbiAgICB9XG4gICAgcmV0dXJuICRzY29wZS5qb2JtYW5hZ2VyWydsb2cnXSA9IGRhdGE7XG4gIH0pO1xuICByZXR1cm4gJHNjb3BlLnJlbG9hZERhdGEgPSBmdW5jdGlvbigpIHtcbiAgICByZXR1cm4gSm9iTWFuYWdlckxvZ3NTZXJ2aWNlLmxvYWRMb2dzKCkudGhlbihmdW5jdGlvbihkYXRhKSB7XG4gICAgICByZXR1cm4gJHNjb3BlLmpvYm1hbmFnZXJbJ2xvZyddID0gZGF0YTtcbiAgICB9KTtcbiAgfTtcbn0pLmNvbnRyb2xsZXIoJ0pvYk1hbmFnZXJTdGRvdXRDb250cm9sbGVyJywgZnVuY3Rpb24oJHNjb3BlLCBKb2JNYW5hZ2VyU3Rkb3V0U2VydmljZSkge1xuICBKb2JNYW5hZ2VyU3Rkb3V0U2VydmljZS5sb2FkU3Rkb3V0KCkudGhlbihmdW5jdGlvbihkYXRhKSB7XG4gICAgaWYgKCRzY29wZS5qb2JtYW5hZ2VyID09IG51bGwpIHtcbiAgICAgICRzY29wZS5qb2JtYW5hZ2VyID0ge307XG4gICAgfVxuICAgIHJldHVybiAkc2NvcGUuam9ibWFuYWdlclsnc3Rkb3V0J10gPSBkYXRhO1xuICB9KTtcbiAgcmV0dXJuICRzY29wZS5yZWxvYWREYXRhID0gZnVuY3Rpb24oKSB7XG4gICAgcmV0dXJuIEpvYk1hbmFnZXJTdGRvdXRTZXJ2aWNlLmxvYWRTdGRvdXQoKS50aGVuKGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgIHJldHVybiAkc2NvcGUuam9ibWFuYWdlclsnc3Rkb3V0J10gPSBkYXRhO1xuICAgIH0pO1xuICB9O1xufSk7XG4iLCIjXG4jIExpY2Vuc2VkIHRvIHRoZSBBcGFjaGUgU29mdHdhcmUgRm91bmRhdGlvbiAoQVNGKSB1bmRlciBvbmVcbiMgb3IgbW9yZSBjb250cmlidXRvciBsaWNlbnNlIGFncmVlbWVudHMuICBTZWUgdGhlIE5PVElDRSBmaWxlXG4jIGRpc3RyaWJ1dGVkIHdpdGggdGhpcyB3b3JrIGZvciBhZGRpdGlvbmFsIGluZm9ybWF0aW9uXG4jIHJlZ2FyZGluZyBjb3B5cmlnaHQgb3duZXJzaGlwLiAgVGhlIEFTRiBsaWNlbnNlcyB0aGlzIGZpbGVcbiMgdG8geW91IHVuZGVyIHRoZSBBcGFjaGUgTGljZW5zZSwgVmVyc2lvbiAyLjAgKHRoZVxuIyBcIkxpY2Vuc2VcIik7IHlvdSBtYXkgbm90IHVzZSB0aGlzIGZpbGUgZXhjZXB0IGluIGNvbXBsaWFuY2VcbiMgd2l0aCB0aGUgTGljZW5zZS4gIFlvdSBtYXkgb2J0YWluIGEgY29weSBvZiB0aGUgTGljZW5zZSBhdFxuI1xuIyAgICAgaHR0cDovL3d3dy5hcGFjaGUub3JnL2xpY2Vuc2VzL0xJQ0VOU0UtMi4wXG4jXG4jIFVubGVzcyByZXF1aXJlZCBieSBhcHBsaWNhYmxlIGxhdyBvciBhZ3JlZWQgdG8gaW4gd3JpdGluZywgc29mdHdhcmVcbiMgZGlzdHJpYnV0ZWQgdW5kZXIgdGhlIExpY2Vuc2UgaXMgZGlzdHJpYnV0ZWQgb24gYW4gXCJBUyBJU1wiIEJBU0lTLFxuIyBXSVRIT1VUIFdBUlJBTlRJRVMgT1IgQ09ORElUSU9OUyBPRiBBTlkgS0lORCwgZWl0aGVyIGV4cHJlc3Mgb3IgaW1wbGllZC5cbiMgU2VlIHRoZSBMaWNlbnNlIGZvciB0aGUgc3BlY2lmaWMgbGFuZ3VhZ2UgZ292ZXJuaW5nIHBlcm1pc3Npb25zIGFuZFxuIyBsaW1pdGF0aW9ucyB1bmRlciB0aGUgTGljZW5zZS5cbiNcblxuYW5ndWxhci5tb2R1bGUoJ2ZsaW5rQXBwJylcblxuLnNlcnZpY2UgJ0pvYk1hbmFnZXJDb25maWdTZXJ2aWNlJywgKCRodHRwLCBmbGlua0NvbmZpZywgJHEpIC0+XG4gIGNvbmZpZyA9IHt9XG5cbiAgQGxvYWRDb25maWcgPSAtPlxuICAgIGRlZmVycmVkID0gJHEuZGVmZXIoKVxuXG4gICAgJGh0dHAuZ2V0KFwiam9ibWFuYWdlci9jb25maWdcIilcbiAgICAuc3VjY2VzcyAoZGF0YSwgc3RhdHVzLCBoZWFkZXJzLCBjb25maWcpIC0+XG4gICAgICBjb25maWcgPSBkYXRhXG4gICAgICBkZWZlcnJlZC5yZXNvbHZlKGRhdGEpXG5cbiAgICBkZWZlcnJlZC5wcm9taXNlXG5cbiAgQFxuXG4uc2VydmljZSAnSm9iTWFuYWdlckxvZ3NTZXJ2aWNlJywgKCRodHRwLCBmbGlua0NvbmZpZywgJHEpIC0+XG4gIGxvZ3MgPSB7fVxuXG4gIEBsb2FkTG9ncyA9IC0+XG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpXG5cbiAgICAkaHR0cC5nZXQoXCJqb2JtYW5hZ2VyL2xvZ1wiKVxuICAgIC5zdWNjZXNzIChkYXRhLCBzdGF0dXMsIGhlYWRlcnMsIGNvbmZpZykgLT5cbiAgICAgIGxvZ3MgPSBkYXRhXG4gICAgICBkZWZlcnJlZC5yZXNvbHZlKGRhdGEpXG5cbiAgICBkZWZlcnJlZC5wcm9taXNlXG5cbiAgQFxuXG4uc2VydmljZSAnSm9iTWFuYWdlclN0ZG91dFNlcnZpY2UnLCAoJGh0dHAsIGZsaW5rQ29uZmlnLCAkcSkgLT5cbiAgc3Rkb3V0ID0ge31cblxuICBAbG9hZFN0ZG91dCA9IC0+XG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpXG5cbiAgICAkaHR0cC5nZXQoXCJqb2JtYW5hZ2VyL3N0ZG91dFwiKVxuICAgIC5zdWNjZXNzIChkYXRhLCBzdGF0dXMsIGhlYWRlcnMsIGNvbmZpZykgLT5cbiAgICAgIHN0ZG91dCA9IGRhdGFcbiAgICAgIGRlZmVycmVkLnJlc29sdmUoZGF0YSlcblxuICAgIGRlZmVycmVkLnByb21pc2VcblxuICBAXG4iLCJhbmd1bGFyLm1vZHVsZSgnZmxpbmtBcHAnKS5zZXJ2aWNlKCdKb2JNYW5hZ2VyQ29uZmlnU2VydmljZScsIGZ1bmN0aW9uKCRodHRwLCBmbGlua0NvbmZpZywgJHEpIHtcbiAgdmFyIGNvbmZpZztcbiAgY29uZmlnID0ge307XG4gIHRoaXMubG9hZENvbmZpZyA9IGZ1bmN0aW9uKCkge1xuICAgIHZhciBkZWZlcnJlZDtcbiAgICBkZWZlcnJlZCA9ICRxLmRlZmVyKCk7XG4gICAgJGh0dHAuZ2V0KFwiam9ibWFuYWdlci9jb25maWdcIikuc3VjY2VzcyhmdW5jdGlvbihkYXRhLCBzdGF0dXMsIGhlYWRlcnMsIGNvbmZpZykge1xuICAgICAgY29uZmlnID0gZGF0YTtcbiAgICAgIHJldHVybiBkZWZlcnJlZC5yZXNvbHZlKGRhdGEpO1xuICAgIH0pO1xuICAgIHJldHVybiBkZWZlcnJlZC5wcm9taXNlO1xuICB9O1xuICByZXR1cm4gdGhpcztcbn0pLnNlcnZpY2UoJ0pvYk1hbmFnZXJMb2dzU2VydmljZScsIGZ1bmN0aW9uKCRodHRwLCBmbGlua0NvbmZpZywgJHEpIHtcbiAgdmFyIGxvZ3M7XG4gIGxvZ3MgPSB7fTtcbiAgdGhpcy5sb2FkTG9ncyA9IGZ1bmN0aW9uKCkge1xuICAgIHZhciBkZWZlcnJlZDtcbiAgICBkZWZlcnJlZCA9ICRxLmRlZmVyKCk7XG4gICAgJGh0dHAuZ2V0KFwiam9ibWFuYWdlci9sb2dcIikuc3VjY2VzcyhmdW5jdGlvbihkYXRhLCBzdGF0dXMsIGhlYWRlcnMsIGNvbmZpZykge1xuICAgICAgbG9ncyA9IGRhdGE7XG4gICAgICByZXR1cm4gZGVmZXJyZWQucmVzb2x2ZShkYXRhKTtcbiAgICB9KTtcbiAgICByZXR1cm4gZGVmZXJyZWQucHJvbWlzZTtcbiAgfTtcbiAgcmV0dXJuIHRoaXM7XG59KS5zZXJ2aWNlKCdKb2JNYW5hZ2VyU3Rkb3V0U2VydmljZScsIGZ1bmN0aW9uKCRodHRwLCBmbGlua0NvbmZpZywgJHEpIHtcbiAgdmFyIHN0ZG91dDtcbiAgc3Rkb3V0ID0ge307XG4gIHRoaXMubG9hZFN0ZG91dCA9IGZ1bmN0aW9uKCkge1xuICAgIHZhciBkZWZlcnJlZDtcbiAgICBkZWZlcnJlZCA9ICRxLmRlZmVyKCk7XG4gICAgJGh0dHAuZ2V0KFwiam9ibWFuYWdlci9zdGRvdXRcIikuc3VjY2VzcyhmdW5jdGlvbihkYXRhLCBzdGF0dXMsIGhlYWRlcnMsIGNvbmZpZykge1xuICAgICAgc3Rkb3V0ID0gZGF0YTtcbiAgICAgIHJldHVybiBkZWZlcnJlZC5yZXNvbHZlKGRhdGEpO1xuICAgIH0pO1xuICAgIHJldHVybiBkZWZlcnJlZC5wcm9taXNlO1xuICB9O1xuICByZXR1cm4gdGhpcztcbn0pO1xuIiwiI1xuIyBMaWNlbnNlZCB0byB0aGUgQXBhY2hlIFNvZnR3YXJlIEZvdW5kYXRpb24gKEFTRikgdW5kZXIgb25lXG4jIG9yIG1vcmUgY29udHJpYnV0b3IgbGljZW5zZSBhZ3JlZW1lbnRzLiAgU2VlIHRoZSBOT1RJQ0UgZmlsZVxuIyBkaXN0cmlidXRlZCB3aXRoIHRoaXMgd29yayBmb3IgYWRkaXRpb25hbCBpbmZvcm1hdGlvblxuIyByZWdhcmRpbmcgY29weXJpZ2h0IG93bmVyc2hpcC4gIFRoZSBBU0YgbGljZW5zZXMgdGhpcyBmaWxlXG4jIHRvIHlvdSB1bmRlciB0aGUgQXBhY2hlIExpY2Vuc2UsIFZlcnNpb24gMi4wICh0aGVcbiMgXCJMaWNlbnNlXCIpOyB5b3UgbWF5IG5vdCB1c2UgdGhpcyBmaWxlIGV4Y2VwdCBpbiBjb21wbGlhbmNlXG4jIHdpdGggdGhlIExpY2Vuc2UuICBZb3UgbWF5IG9idGFpbiBhIGNvcHkgb2YgdGhlIExpY2Vuc2UgYXRcbiNcbiMgICAgIGh0dHA6Ly93d3cuYXBhY2hlLm9yZy9saWNlbnNlcy9MSUNFTlNFLTIuMFxuI1xuIyBVbmxlc3MgcmVxdWlyZWQgYnkgYXBwbGljYWJsZSBsYXcgb3IgYWdyZWVkIHRvIGluIHdyaXRpbmcsIHNvZnR3YXJlXG4jIGRpc3RyaWJ1dGVkIHVuZGVyIHRoZSBMaWNlbnNlIGlzIGRpc3RyaWJ1dGVkIG9uIGFuIFwiQVMgSVNcIiBCQVNJUyxcbiMgV0lUSE9VVCBXQVJSQU5USUVTIE9SIENPTkRJVElPTlMgT0YgQU5ZIEtJTkQsIGVpdGhlciBleHByZXNzIG9yIGltcGxpZWQuXG4jIFNlZSB0aGUgTGljZW5zZSBmb3IgdGhlIHNwZWNpZmljIGxhbmd1YWdlIGdvdmVybmluZyBwZXJtaXNzaW9ucyBhbmRcbiMgbGltaXRhdGlvbnMgdW5kZXIgdGhlIExpY2Vuc2UuXG4jXG5cbmFuZ3VsYXIubW9kdWxlKCdmbGlua0FwcCcpXG5cbi5jb250cm9sbGVyICdPdmVydmlld0NvbnRyb2xsZXInLCAoJHNjb3BlLCBPdmVydmlld1NlcnZpY2UsIEpvYnNTZXJ2aWNlLCAkaW50ZXJ2YWwsIGZsaW5rQ29uZmlnKSAtPlxuICAkc2NvcGUuam9iT2JzZXJ2ZXIgPSAtPlxuICAgICRzY29wZS5ydW5uaW5nSm9icyA9IEpvYnNTZXJ2aWNlLmdldEpvYnMoJ3J1bm5pbmcnKVxuICAgICRzY29wZS5maW5pc2hlZEpvYnMgPSBKb2JzU2VydmljZS5nZXRKb2JzKCdmaW5pc2hlZCcpXG5cbiAgSm9ic1NlcnZpY2UucmVnaXN0ZXJPYnNlcnZlcigkc2NvcGUuam9iT2JzZXJ2ZXIpXG4gICRzY29wZS4kb24gJyRkZXN0cm95JywgLT5cbiAgICBKb2JzU2VydmljZS51blJlZ2lzdGVyT2JzZXJ2ZXIoJHNjb3BlLmpvYk9ic2VydmVyKVxuXG4gICRzY29wZS5qb2JPYnNlcnZlcigpXG5cbiAgT3ZlcnZpZXdTZXJ2aWNlLmxvYWRPdmVydmlldygpLnRoZW4gKGRhdGEpIC0+XG4gICAgJHNjb3BlLm92ZXJ2aWV3ID0gZGF0YVxuXG4gIHJlZnJlc2ggPSAkaW50ZXJ2YWwgLT5cbiAgICBPdmVydmlld1NlcnZpY2UubG9hZE92ZXJ2aWV3KCkudGhlbiAoZGF0YSkgLT5cbiAgICAgICRzY29wZS5vdmVydmlldyA9IGRhdGFcbiAgLCBmbGlua0NvbmZpZ1tcInJlZnJlc2gtaW50ZXJ2YWxcIl1cblxuICAkc2NvcGUuJG9uICckZGVzdHJveScsIC0+XG4gICAgJGludGVydmFsLmNhbmNlbChyZWZyZXNoKVxuIiwiYW5ndWxhci5tb2R1bGUoJ2ZsaW5rQXBwJykuY29udHJvbGxlcignT3ZlcnZpZXdDb250cm9sbGVyJywgZnVuY3Rpb24oJHNjb3BlLCBPdmVydmlld1NlcnZpY2UsIEpvYnNTZXJ2aWNlLCAkaW50ZXJ2YWwsIGZsaW5rQ29uZmlnKSB7XG4gIHZhciByZWZyZXNoO1xuICAkc2NvcGUuam9iT2JzZXJ2ZXIgPSBmdW5jdGlvbigpIHtcbiAgICAkc2NvcGUucnVubmluZ0pvYnMgPSBKb2JzU2VydmljZS5nZXRKb2JzKCdydW5uaW5nJyk7XG4gICAgcmV0dXJuICRzY29wZS5maW5pc2hlZEpvYnMgPSBKb2JzU2VydmljZS5nZXRKb2JzKCdmaW5pc2hlZCcpO1xuICB9O1xuICBKb2JzU2VydmljZS5yZWdpc3Rlck9ic2VydmVyKCRzY29wZS5qb2JPYnNlcnZlcik7XG4gICRzY29wZS4kb24oJyRkZXN0cm95JywgZnVuY3Rpb24oKSB7XG4gICAgcmV0dXJuIEpvYnNTZXJ2aWNlLnVuUmVnaXN0ZXJPYnNlcnZlcigkc2NvcGUuam9iT2JzZXJ2ZXIpO1xuICB9KTtcbiAgJHNjb3BlLmpvYk9ic2VydmVyKCk7XG4gIE92ZXJ2aWV3U2VydmljZS5sb2FkT3ZlcnZpZXcoKS50aGVuKGZ1bmN0aW9uKGRhdGEpIHtcbiAgICByZXR1cm4gJHNjb3BlLm92ZXJ2aWV3ID0gZGF0YTtcbiAgfSk7XG4gIHJlZnJlc2ggPSAkaW50ZXJ2YWwoZnVuY3Rpb24oKSB7XG4gICAgcmV0dXJuIE92ZXJ2aWV3U2VydmljZS5sb2FkT3ZlcnZpZXcoKS50aGVuKGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgIHJldHVybiAkc2NvcGUub3ZlcnZpZXcgPSBkYXRhO1xuICAgIH0pO1xuICB9LCBmbGlua0NvbmZpZ1tcInJlZnJlc2gtaW50ZXJ2YWxcIl0pO1xuICByZXR1cm4gJHNjb3BlLiRvbignJGRlc3Ryb3knLCBmdW5jdGlvbigpIHtcbiAgICByZXR1cm4gJGludGVydmFsLmNhbmNlbChyZWZyZXNoKTtcbiAgfSk7XG59KTtcbiIsIiNcbiMgTGljZW5zZWQgdG8gdGhlIEFwYWNoZSBTb2Z0d2FyZSBGb3VuZGF0aW9uIChBU0YpIHVuZGVyIG9uZVxuIyBvciBtb3JlIGNvbnRyaWJ1dG9yIGxpY2Vuc2UgYWdyZWVtZW50cy4gIFNlZSB0aGUgTk9USUNFIGZpbGVcbiMgZGlzdHJpYnV0ZWQgd2l0aCB0aGlzIHdvcmsgZm9yIGFkZGl0aW9uYWwgaW5mb3JtYXRpb25cbiMgcmVnYXJkaW5nIGNvcHlyaWdodCBvd25lcnNoaXAuICBUaGUgQVNGIGxpY2Vuc2VzIHRoaXMgZmlsZVxuIyB0byB5b3UgdW5kZXIgdGhlIEFwYWNoZSBMaWNlbnNlLCBWZXJzaW9uIDIuMCAodGhlXG4jIFwiTGljZW5zZVwiKTsgeW91IG1heSBub3QgdXNlIHRoaXMgZmlsZSBleGNlcHQgaW4gY29tcGxpYW5jZVxuIyB3aXRoIHRoZSBMaWNlbnNlLiAgWW91IG1heSBvYnRhaW4gYSBjb3B5IG9mIHRoZSBMaWNlbnNlIGF0XG4jXG4jICAgICBodHRwOi8vd3d3LmFwYWNoZS5vcmcvbGljZW5zZXMvTElDRU5TRS0yLjBcbiNcbiMgVW5sZXNzIHJlcXVpcmVkIGJ5IGFwcGxpY2FibGUgbGF3IG9yIGFncmVlZCB0byBpbiB3cml0aW5nLCBzb2Z0d2FyZVxuIyBkaXN0cmlidXRlZCB1bmRlciB0aGUgTGljZW5zZSBpcyBkaXN0cmlidXRlZCBvbiBhbiBcIkFTIElTXCIgQkFTSVMsXG4jIFdJVEhPVVQgV0FSUkFOVElFUyBPUiBDT05ESVRJT05TIE9GIEFOWSBLSU5ELCBlaXRoZXIgZXhwcmVzcyBvciBpbXBsaWVkLlxuIyBTZWUgdGhlIExpY2Vuc2UgZm9yIHRoZSBzcGVjaWZpYyBsYW5ndWFnZSBnb3Zlcm5pbmcgcGVybWlzc2lvbnMgYW5kXG4jIGxpbWl0YXRpb25zIHVuZGVyIHRoZSBMaWNlbnNlLlxuI1xuXG5hbmd1bGFyLm1vZHVsZSgnZmxpbmtBcHAnKVxuXG4uc2VydmljZSAnT3ZlcnZpZXdTZXJ2aWNlJywgKCRodHRwLCBmbGlua0NvbmZpZywgJHEpIC0+XG4gIG92ZXJ2aWV3ID0ge31cblxuICBAbG9hZE92ZXJ2aWV3ID0gLT5cbiAgICBkZWZlcnJlZCA9ICRxLmRlZmVyKClcblxuICAgICRodHRwLmdldChcIm92ZXJ2aWV3XCIpXG4gICAgLnN1Y2Nlc3MgKGRhdGEsIHN0YXR1cywgaGVhZGVycywgY29uZmlnKSAtPlxuICAgICAgb3ZlcnZpZXcgPSBkYXRhXG4gICAgICBkZWZlcnJlZC5yZXNvbHZlKGRhdGEpXG5cbiAgICBkZWZlcnJlZC5wcm9taXNlXG5cbiAgQFxuIiwiYW5ndWxhci5tb2R1bGUoJ2ZsaW5rQXBwJykuc2VydmljZSgnT3ZlcnZpZXdTZXJ2aWNlJywgZnVuY3Rpb24oJGh0dHAsIGZsaW5rQ29uZmlnLCAkcSkge1xuICB2YXIgb3ZlcnZpZXc7XG4gIG92ZXJ2aWV3ID0ge307XG4gIHRoaXMubG9hZE92ZXJ2aWV3ID0gZnVuY3Rpb24oKSB7XG4gICAgdmFyIGRlZmVycmVkO1xuICAgIGRlZmVycmVkID0gJHEuZGVmZXIoKTtcbiAgICAkaHR0cC5nZXQoXCJvdmVydmlld1wiKS5zdWNjZXNzKGZ1bmN0aW9uKGRhdGEsIHN0YXR1cywgaGVhZGVycywgY29uZmlnKSB7XG4gICAgICBvdmVydmlldyA9IGRhdGE7XG4gICAgICByZXR1cm4gZGVmZXJyZWQucmVzb2x2ZShkYXRhKTtcbiAgICB9KTtcbiAgICByZXR1cm4gZGVmZXJyZWQucHJvbWlzZTtcbiAgfTtcbiAgcmV0dXJuIHRoaXM7XG59KTtcbiIsIiNcbiMgTGljZW5zZWQgdG8gdGhlIEFwYWNoZSBTb2Z0d2FyZSBGb3VuZGF0aW9uIChBU0YpIHVuZGVyIG9uZVxuIyBvciBtb3JlIGNvbnRyaWJ1dG9yIGxpY2Vuc2UgYWdyZWVtZW50cy4gIFNlZSB0aGUgTk9USUNFIGZpbGVcbiMgZGlzdHJpYnV0ZWQgd2l0aCB0aGlzIHdvcmsgZm9yIGFkZGl0aW9uYWwgaW5mb3JtYXRpb25cbiMgcmVnYXJkaW5nIGNvcHlyaWdodCBvd25lcnNoaXAuICBUaGUgQVNGIGxpY2Vuc2VzIHRoaXMgZmlsZVxuIyB0byB5b3UgdW5kZXIgdGhlIEFwYWNoZSBMaWNlbnNlLCBWZXJzaW9uIDIuMCAodGhlXG4jIFwiTGljZW5zZVwiKTsgeW91IG1heSBub3QgdXNlIHRoaXMgZmlsZSBleGNlcHQgaW4gY29tcGxpYW5jZVxuIyB3aXRoIHRoZSBMaWNlbnNlLiAgWW91IG1heSBvYnRhaW4gYSBjb3B5IG9mIHRoZSBMaWNlbnNlIGF0XG4jXG4jICAgICBodHRwOi8vd3d3LmFwYWNoZS5vcmcvbGljZW5zZXMvTElDRU5TRS0yLjBcbiNcbiMgVW5sZXNzIHJlcXVpcmVkIGJ5IGFwcGxpY2FibGUgbGF3IG9yIGFncmVlZCB0byBpbiB3cml0aW5nLCBzb2Z0d2FyZVxuIyBkaXN0cmlidXRlZCB1bmRlciB0aGUgTGljZW5zZSBpcyBkaXN0cmlidXRlZCBvbiBhbiBcIkFTIElTXCIgQkFTSVMsXG4jIFdJVEhPVVQgV0FSUkFOVElFUyBPUiBDT05ESVRJT05TIE9GIEFOWSBLSU5ELCBlaXRoZXIgZXhwcmVzcyBvciBpbXBsaWVkLlxuIyBTZWUgdGhlIExpY2Vuc2UgZm9yIHRoZSBzcGVjaWZpYyBsYW5ndWFnZSBnb3Zlcm5pbmcgcGVybWlzc2lvbnMgYW5kXG4jIGxpbWl0YXRpb25zIHVuZGVyIHRoZSBMaWNlbnNlLlxuI1xuXG5hbmd1bGFyLm1vZHVsZSgnZmxpbmtBcHAnKVxuXG4uY29udHJvbGxlciAnUnVubmluZ0pvYnNDb250cm9sbGVyJywgKCRzY29wZSwgJHN0YXRlLCAkc3RhdGVQYXJhbXMsIEpvYnNTZXJ2aWNlKSAtPlxuICAkc2NvcGUuam9iT2JzZXJ2ZXIgPSAtPlxuICAgICRzY29wZS5qb2JzID0gSm9ic1NlcnZpY2UuZ2V0Sm9icygncnVubmluZycpXG5cbiAgSm9ic1NlcnZpY2UucmVnaXN0ZXJPYnNlcnZlcigkc2NvcGUuam9iT2JzZXJ2ZXIpXG4gICRzY29wZS4kb24gJyRkZXN0cm95JywgLT5cbiAgICBKb2JzU2VydmljZS51blJlZ2lzdGVyT2JzZXJ2ZXIoJHNjb3BlLmpvYk9ic2VydmVyKVxuXG4gICRzY29wZS5qb2JPYnNlcnZlcigpXG5cbiMgLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS1cblxuLmNvbnRyb2xsZXIgJ0NvbXBsZXRlZEpvYnNDb250cm9sbGVyJywgKCRzY29wZSwgJHN0YXRlLCAkc3RhdGVQYXJhbXMsIEpvYnNTZXJ2aWNlKSAtPlxuICAkc2NvcGUuam9iT2JzZXJ2ZXIgPSAtPlxuICAgICRzY29wZS5qb2JzID0gSm9ic1NlcnZpY2UuZ2V0Sm9icygnZmluaXNoZWQnKVxuXG4gIEpvYnNTZXJ2aWNlLnJlZ2lzdGVyT2JzZXJ2ZXIoJHNjb3BlLmpvYk9ic2VydmVyKVxuICAkc2NvcGUuJG9uICckZGVzdHJveScsIC0+XG4gICAgSm9ic1NlcnZpY2UudW5SZWdpc3Rlck9ic2VydmVyKCRzY29wZS5qb2JPYnNlcnZlcilcblxuICAkc2NvcGUuam9iT2JzZXJ2ZXIoKVxuXG4jIC0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tXG5cbi5jb250cm9sbGVyICdTaW5nbGVKb2JDb250cm9sbGVyJywgKCRzY29wZSwgJHN0YXRlLCAkc3RhdGVQYXJhbXMsIEpvYnNTZXJ2aWNlLCAkcm9vdFNjb3BlLCBmbGlua0NvbmZpZywgJGludGVydmFsKSAtPlxuICBjb25zb2xlLmxvZyAnU2luZ2xlSm9iQ29udHJvbGxlcidcblxuICAkc2NvcGUuam9iaWQgPSAkc3RhdGVQYXJhbXMuam9iaWRcbiAgJHNjb3BlLmpvYiA9IG51bGxcbiAgJHNjb3BlLnBsYW4gPSBudWxsXG4gICRzY29wZS52ZXJ0aWNlcyA9IG51bGxcblxuICBKb2JzU2VydmljZS5sb2FkSm9iKCRzdGF0ZVBhcmFtcy5qb2JpZCkudGhlbiAoZGF0YSkgLT5cbiAgICAkc2NvcGUuam9iID0gZGF0YVxuICAgICRzY29wZS5wbGFuID0gZGF0YS5wbGFuXG4gICAgJHNjb3BlLnZlcnRpY2VzID0gZGF0YS52ZXJ0aWNlc1xuXG4gIHJlZnJlc2hlciA9ICRpbnRlcnZhbCAtPlxuICAgIEpvYnNTZXJ2aWNlLmxvYWRKb2IoJHN0YXRlUGFyYW1zLmpvYmlkKS50aGVuIChkYXRhKSAtPlxuICAgICAgJHNjb3BlLmpvYiA9IGRhdGFcblxuICAgICAgJHNjb3BlLiRicm9hZGNhc3QgJ3JlbG9hZCdcblxuICAsIGZsaW5rQ29uZmlnW1wicmVmcmVzaC1pbnRlcnZhbFwiXVxuXG4gICRzY29wZS4kb24gJyRkZXN0cm95JywgLT5cbiAgICAkc2NvcGUuam9iID0gbnVsbFxuICAgICRzY29wZS5wbGFuID0gbnVsbFxuICAgICRzY29wZS52ZXJ0aWNlcyA9IG51bGxcblxuICAgICRpbnRlcnZhbC5jYW5jZWwocmVmcmVzaGVyKVxuXG4gICRzY29wZS5jYW5jZWxKb2IgPSAoY2FuY2VsRXZlbnQpIC0+XG4gICAgYW5ndWxhci5lbGVtZW50KGNhbmNlbEV2ZW50LmN1cnJlbnRUYXJnZXQpLnJlbW92ZUNsYXNzKCdsYWJlbC1kYW5nZXInKS5hZGRDbGFzcygnbGFiZWwtaW5mbycpLmh0bWwoJ0NhbmNlbGxpbmcuLi4nKVxuICAgIEpvYnNTZXJ2aWNlLmNhbmNlbEpvYigkc3RhdGVQYXJhbXMuam9iaWQpLnRoZW4gKGRhdGEpIC0+XG4gICAgICB7fVxuXG4jIC0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tXG5cbi5jb250cm9sbGVyICdKb2JQbGFuQ29udHJvbGxlcicsICgkc2NvcGUsICRzdGF0ZSwgJHN0YXRlUGFyYW1zLCBKb2JzU2VydmljZSkgLT5cbiAgY29uc29sZS5sb2cgJ0pvYlBsYW5Db250cm9sbGVyJ1xuXG4gICRzY29wZS5ub2RlaWQgPSBudWxsXG4gICRzY29wZS5ub2RlVW5mb2xkZWQgPSBmYWxzZVxuICAkc2NvcGUuc3RhdGVMaXN0ID0gSm9ic1NlcnZpY2Uuc3RhdGVMaXN0KClcblxuICAkc2NvcGUuY2hhbmdlTm9kZSA9IChub2RlaWQpIC0+XG4gICAgaWYgbm9kZWlkICE9ICRzY29wZS5ub2RlaWRcbiAgICAgICRzY29wZS5ub2RlaWQgPSBub2RlaWRcbiAgICAgICRzY29wZS52ZXJ0ZXggPSBudWxsXG4gICAgICAkc2NvcGUuc3VidGFza3MgPSBudWxsXG4gICAgICAkc2NvcGUuYWNjdW11bGF0b3JzID0gbnVsbFxuXG4gICAgICAkc2NvcGUuJGJyb2FkY2FzdCAncmVsb2FkJ1xuXG4gICAgZWxzZVxuICAgICAgJHNjb3BlLm5vZGVpZCA9IG51bGxcbiAgICAgICRzY29wZS5ub2RlVW5mb2xkZWQgPSBmYWxzZVxuICAgICAgJHNjb3BlLnZlcnRleCA9IG51bGxcbiAgICAgICRzY29wZS5zdWJ0YXNrcyA9IG51bGxcbiAgICAgICRzY29wZS5hY2N1bXVsYXRvcnMgPSBudWxsXG5cbiAgJHNjb3BlLmRlYWN0aXZhdGVOb2RlID0gLT5cbiAgICAkc2NvcGUubm9kZWlkID0gbnVsbFxuICAgICRzY29wZS5ub2RlVW5mb2xkZWQgPSBmYWxzZVxuICAgICRzY29wZS52ZXJ0ZXggPSBudWxsXG4gICAgJHNjb3BlLnN1YnRhc2tzID0gbnVsbFxuICAgICRzY29wZS5hY2N1bXVsYXRvcnMgPSBudWxsXG5cbiAgJHNjb3BlLnRvZ2dsZUZvbGQgPSAtPlxuICAgICRzY29wZS5ub2RlVW5mb2xkZWQgPSAhJHNjb3BlLm5vZGVVbmZvbGRlZFxuXG4jIC0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tXG5cbi5jb250cm9sbGVyICdKb2JQbGFuT3ZlcnZpZXdDb250cm9sbGVyJywgKCRzY29wZSwgSm9ic1NlcnZpY2UpIC0+XG4gIGNvbnNvbGUubG9nICdKb2JQbGFuT3ZlcnZpZXdDb250cm9sbGVyJ1xuXG4gIGlmICRzY29wZS5ub2RlaWQgYW5kICghJHNjb3BlLnZlcnRleCBvciAhJHNjb3BlLnZlcnRleC5zdClcbiAgICBKb2JzU2VydmljZS5nZXRTdWJ0YXNrcygkc2NvcGUubm9kZWlkKS50aGVuIChkYXRhKSAtPlxuICAgICAgJHNjb3BlLnN1YnRhc2tzID0gZGF0YVxuXG4gICRzY29wZS4kb24gJ3JlbG9hZCcsIChldmVudCkgLT5cbiAgICBjb25zb2xlLmxvZyAnSm9iUGxhbk92ZXJ2aWV3Q29udHJvbGxlcidcbiAgICBpZiAkc2NvcGUubm9kZWlkXG4gICAgICBKb2JzU2VydmljZS5nZXRTdWJ0YXNrcygkc2NvcGUubm9kZWlkKS50aGVuIChkYXRhKSAtPlxuICAgICAgICAkc2NvcGUuc3VidGFza3MgPSBkYXRhXG5cbiMgLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS1cblxuLmNvbnRyb2xsZXIgJ0pvYlBsYW5BY2N1bXVsYXRvcnNDb250cm9sbGVyJywgKCRzY29wZSwgSm9ic1NlcnZpY2UpIC0+XG4gIGNvbnNvbGUubG9nICdKb2JQbGFuQWNjdW11bGF0b3JzQ29udHJvbGxlcidcblxuICBpZiAkc2NvcGUubm9kZWlkIGFuZCAoISRzY29wZS52ZXJ0ZXggb3IgISRzY29wZS52ZXJ0ZXguYWNjdW11bGF0b3JzKVxuICAgIEpvYnNTZXJ2aWNlLmdldEFjY3VtdWxhdG9ycygkc2NvcGUubm9kZWlkKS50aGVuIChkYXRhKSAtPlxuICAgICAgJHNjb3BlLmFjY3VtdWxhdG9ycyA9IGRhdGEubWFpblxuICAgICAgJHNjb3BlLnN1YnRhc2tBY2N1bXVsYXRvcnMgPSBkYXRhLnN1YnRhc2tzXG5cbiAgJHNjb3BlLiRvbiAncmVsb2FkJywgKGV2ZW50KSAtPlxuICAgIGNvbnNvbGUubG9nICdKb2JQbGFuQWNjdW11bGF0b3JzQ29udHJvbGxlcidcbiAgICBpZiAkc2NvcGUubm9kZWlkXG4gICAgICBKb2JzU2VydmljZS5nZXRBY2N1bXVsYXRvcnMoJHNjb3BlLm5vZGVpZCkudGhlbiAoZGF0YSkgLT5cbiAgICAgICAgJHNjb3BlLmFjY3VtdWxhdG9ycyA9IGRhdGEubWFpblxuICAgICAgICAkc2NvcGUuc3VidGFza0FjY3VtdWxhdG9ycyA9IGRhdGEuc3VidGFza3NcblxuIyAtLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLVxuXG4uY29udHJvbGxlciAnSm9iVGltZWxpbmVWZXJ0ZXhDb250cm9sbGVyJywgKCRzY29wZSwgJHN0YXRlLCAkc3RhdGVQYXJhbXMsIEpvYnNTZXJ2aWNlKSAtPlxuICBjb25zb2xlLmxvZyAnSm9iVGltZWxpbmVWZXJ0ZXhDb250cm9sbGVyJ1xuXG4gIEpvYnNTZXJ2aWNlLmdldFZlcnRleCgkc3RhdGVQYXJhbXMudmVydGV4SWQpLnRoZW4gKGRhdGEpIC0+XG4gICAgJHNjb3BlLnZlcnRleCA9IGRhdGFcblxuICAkc2NvcGUuJG9uICdyZWxvYWQnLCAoZXZlbnQpIC0+XG4gICAgY29uc29sZS5sb2cgJ0pvYlRpbWVsaW5lVmVydGV4Q29udHJvbGxlcidcbiAgICBKb2JzU2VydmljZS5nZXRWZXJ0ZXgoJHN0YXRlUGFyYW1zLnZlcnRleElkKS50aGVuIChkYXRhKSAtPlxuICAgICAgJHNjb3BlLnZlcnRleCA9IGRhdGFcblxuIyAtLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLVxuXG4uY29udHJvbGxlciAnSm9iRXhjZXB0aW9uc0NvbnRyb2xsZXInLCAoJHNjb3BlLCAkc3RhdGUsICRzdGF0ZVBhcmFtcywgSm9ic1NlcnZpY2UpIC0+XG4gIEpvYnNTZXJ2aWNlLmxvYWRFeGNlcHRpb25zKCkudGhlbiAoZGF0YSkgLT5cbiAgICAkc2NvcGUuZXhjZXB0aW9ucyA9IGRhdGFcblxuIyAtLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLVxuXG4uY29udHJvbGxlciAnSm9iUHJvcGVydGllc0NvbnRyb2xsZXInLCAoJHNjb3BlLCBKb2JzU2VydmljZSkgLT5cbiAgY29uc29sZS5sb2cgJ0pvYlByb3BlcnRpZXNDb250cm9sbGVyJ1xuXG4gICRzY29wZS5jaGFuZ2VOb2RlID0gKG5vZGVpZCkgLT5cbiAgICBpZiBub2RlaWQgIT0gJHNjb3BlLm5vZGVpZFxuICAgICAgJHNjb3BlLm5vZGVpZCA9IG5vZGVpZFxuXG4gICAgICBKb2JzU2VydmljZS5nZXROb2RlKG5vZGVpZCkudGhlbiAoZGF0YSkgLT5cbiAgICAgICAgJHNjb3BlLm5vZGUgPSBkYXRhXG5cbiAgICBlbHNlXG4gICAgICAkc2NvcGUubm9kZWlkID0gbnVsbFxuICAgICAgJHNjb3BlLm5vZGUgPSBudWxsXG4iLCJhbmd1bGFyLm1vZHVsZSgnZmxpbmtBcHAnKS5jb250cm9sbGVyKCdSdW5uaW5nSm9ic0NvbnRyb2xsZXInLCBmdW5jdGlvbigkc2NvcGUsICRzdGF0ZSwgJHN0YXRlUGFyYW1zLCBKb2JzU2VydmljZSkge1xuICAkc2NvcGUuam9iT2JzZXJ2ZXIgPSBmdW5jdGlvbigpIHtcbiAgICByZXR1cm4gJHNjb3BlLmpvYnMgPSBKb2JzU2VydmljZS5nZXRKb2JzKCdydW5uaW5nJyk7XG4gIH07XG4gIEpvYnNTZXJ2aWNlLnJlZ2lzdGVyT2JzZXJ2ZXIoJHNjb3BlLmpvYk9ic2VydmVyKTtcbiAgJHNjb3BlLiRvbignJGRlc3Ryb3knLCBmdW5jdGlvbigpIHtcbiAgICByZXR1cm4gSm9ic1NlcnZpY2UudW5SZWdpc3Rlck9ic2VydmVyKCRzY29wZS5qb2JPYnNlcnZlcik7XG4gIH0pO1xuICByZXR1cm4gJHNjb3BlLmpvYk9ic2VydmVyKCk7XG59KS5jb250cm9sbGVyKCdDb21wbGV0ZWRKb2JzQ29udHJvbGxlcicsIGZ1bmN0aW9uKCRzY29wZSwgJHN0YXRlLCAkc3RhdGVQYXJhbXMsIEpvYnNTZXJ2aWNlKSB7XG4gICRzY29wZS5qb2JPYnNlcnZlciA9IGZ1bmN0aW9uKCkge1xuICAgIHJldHVybiAkc2NvcGUuam9icyA9IEpvYnNTZXJ2aWNlLmdldEpvYnMoJ2ZpbmlzaGVkJyk7XG4gIH07XG4gIEpvYnNTZXJ2aWNlLnJlZ2lzdGVyT2JzZXJ2ZXIoJHNjb3BlLmpvYk9ic2VydmVyKTtcbiAgJHNjb3BlLiRvbignJGRlc3Ryb3knLCBmdW5jdGlvbigpIHtcbiAgICByZXR1cm4gSm9ic1NlcnZpY2UudW5SZWdpc3Rlck9ic2VydmVyKCRzY29wZS5qb2JPYnNlcnZlcik7XG4gIH0pO1xuICByZXR1cm4gJHNjb3BlLmpvYk9ic2VydmVyKCk7XG59KS5jb250cm9sbGVyKCdTaW5nbGVKb2JDb250cm9sbGVyJywgZnVuY3Rpb24oJHNjb3BlLCAkc3RhdGUsICRzdGF0ZVBhcmFtcywgSm9ic1NlcnZpY2UsICRyb290U2NvcGUsIGZsaW5rQ29uZmlnLCAkaW50ZXJ2YWwpIHtcbiAgdmFyIHJlZnJlc2hlcjtcbiAgY29uc29sZS5sb2coJ1NpbmdsZUpvYkNvbnRyb2xsZXInKTtcbiAgJHNjb3BlLmpvYmlkID0gJHN0YXRlUGFyYW1zLmpvYmlkO1xuICAkc2NvcGUuam9iID0gbnVsbDtcbiAgJHNjb3BlLnBsYW4gPSBudWxsO1xuICAkc2NvcGUudmVydGljZXMgPSBudWxsO1xuICBKb2JzU2VydmljZS5sb2FkSm9iKCRzdGF0ZVBhcmFtcy5qb2JpZCkudGhlbihmdW5jdGlvbihkYXRhKSB7XG4gICAgJHNjb3BlLmpvYiA9IGRhdGE7XG4gICAgJHNjb3BlLnBsYW4gPSBkYXRhLnBsYW47XG4gICAgcmV0dXJuICRzY29wZS52ZXJ0aWNlcyA9IGRhdGEudmVydGljZXM7XG4gIH0pO1xuICByZWZyZXNoZXIgPSAkaW50ZXJ2YWwoZnVuY3Rpb24oKSB7XG4gICAgcmV0dXJuIEpvYnNTZXJ2aWNlLmxvYWRKb2IoJHN0YXRlUGFyYW1zLmpvYmlkKS50aGVuKGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgICRzY29wZS5qb2IgPSBkYXRhO1xuICAgICAgcmV0dXJuICRzY29wZS4kYnJvYWRjYXN0KCdyZWxvYWQnKTtcbiAgICB9KTtcbiAgfSwgZmxpbmtDb25maWdbXCJyZWZyZXNoLWludGVydmFsXCJdKTtcbiAgJHNjb3BlLiRvbignJGRlc3Ryb3knLCBmdW5jdGlvbigpIHtcbiAgICAkc2NvcGUuam9iID0gbnVsbDtcbiAgICAkc2NvcGUucGxhbiA9IG51bGw7XG4gICAgJHNjb3BlLnZlcnRpY2VzID0gbnVsbDtcbiAgICByZXR1cm4gJGludGVydmFsLmNhbmNlbChyZWZyZXNoZXIpO1xuICB9KTtcbiAgcmV0dXJuICRzY29wZS5jYW5jZWxKb2IgPSBmdW5jdGlvbihjYW5jZWxFdmVudCkge1xuICAgIGFuZ3VsYXIuZWxlbWVudChjYW5jZWxFdmVudC5jdXJyZW50VGFyZ2V0KS5yZW1vdmVDbGFzcygnbGFiZWwtZGFuZ2VyJykuYWRkQ2xhc3MoJ2xhYmVsLWluZm8nKS5odG1sKCdDYW5jZWxsaW5nLi4uJyk7XG4gICAgcmV0dXJuIEpvYnNTZXJ2aWNlLmNhbmNlbEpvYigkc3RhdGVQYXJhbXMuam9iaWQpLnRoZW4oZnVuY3Rpb24oZGF0YSkge1xuICAgICAgcmV0dXJuIHt9O1xuICAgIH0pO1xuICB9O1xufSkuY29udHJvbGxlcignSm9iUGxhbkNvbnRyb2xsZXInLCBmdW5jdGlvbigkc2NvcGUsICRzdGF0ZSwgJHN0YXRlUGFyYW1zLCBKb2JzU2VydmljZSkge1xuICBjb25zb2xlLmxvZygnSm9iUGxhbkNvbnRyb2xsZXInKTtcbiAgJHNjb3BlLm5vZGVpZCA9IG51bGw7XG4gICRzY29wZS5ub2RlVW5mb2xkZWQgPSBmYWxzZTtcbiAgJHNjb3BlLnN0YXRlTGlzdCA9IEpvYnNTZXJ2aWNlLnN0YXRlTGlzdCgpO1xuICAkc2NvcGUuY2hhbmdlTm9kZSA9IGZ1bmN0aW9uKG5vZGVpZCkge1xuICAgIGlmIChub2RlaWQgIT09ICRzY29wZS5ub2RlaWQpIHtcbiAgICAgICRzY29wZS5ub2RlaWQgPSBub2RlaWQ7XG4gICAgICAkc2NvcGUudmVydGV4ID0gbnVsbDtcbiAgICAgICRzY29wZS5zdWJ0YXNrcyA9IG51bGw7XG4gICAgICAkc2NvcGUuYWNjdW11bGF0b3JzID0gbnVsbDtcbiAgICAgIHJldHVybiAkc2NvcGUuJGJyb2FkY2FzdCgncmVsb2FkJyk7XG4gICAgfSBlbHNlIHtcbiAgICAgICRzY29wZS5ub2RlaWQgPSBudWxsO1xuICAgICAgJHNjb3BlLm5vZGVVbmZvbGRlZCA9IGZhbHNlO1xuICAgICAgJHNjb3BlLnZlcnRleCA9IG51bGw7XG4gICAgICAkc2NvcGUuc3VidGFza3MgPSBudWxsO1xuICAgICAgcmV0dXJuICRzY29wZS5hY2N1bXVsYXRvcnMgPSBudWxsO1xuICAgIH1cbiAgfTtcbiAgJHNjb3BlLmRlYWN0aXZhdGVOb2RlID0gZnVuY3Rpb24oKSB7XG4gICAgJHNjb3BlLm5vZGVpZCA9IG51bGw7XG4gICAgJHNjb3BlLm5vZGVVbmZvbGRlZCA9IGZhbHNlO1xuICAgICRzY29wZS52ZXJ0ZXggPSBudWxsO1xuICAgICRzY29wZS5zdWJ0YXNrcyA9IG51bGw7XG4gICAgcmV0dXJuICRzY29wZS5hY2N1bXVsYXRvcnMgPSBudWxsO1xuICB9O1xuICByZXR1cm4gJHNjb3BlLnRvZ2dsZUZvbGQgPSBmdW5jdGlvbigpIHtcbiAgICByZXR1cm4gJHNjb3BlLm5vZGVVbmZvbGRlZCA9ICEkc2NvcGUubm9kZVVuZm9sZGVkO1xuICB9O1xufSkuY29udHJvbGxlcignSm9iUGxhbk92ZXJ2aWV3Q29udHJvbGxlcicsIGZ1bmN0aW9uKCRzY29wZSwgSm9ic1NlcnZpY2UpIHtcbiAgY29uc29sZS5sb2coJ0pvYlBsYW5PdmVydmlld0NvbnRyb2xsZXInKTtcbiAgaWYgKCRzY29wZS5ub2RlaWQgJiYgKCEkc2NvcGUudmVydGV4IHx8ICEkc2NvcGUudmVydGV4LnN0KSkge1xuICAgIEpvYnNTZXJ2aWNlLmdldFN1YnRhc2tzKCRzY29wZS5ub2RlaWQpLnRoZW4oZnVuY3Rpb24oZGF0YSkge1xuICAgICAgcmV0dXJuICRzY29wZS5zdWJ0YXNrcyA9IGRhdGE7XG4gICAgfSk7XG4gIH1cbiAgcmV0dXJuICRzY29wZS4kb24oJ3JlbG9hZCcsIGZ1bmN0aW9uKGV2ZW50KSB7XG4gICAgY29uc29sZS5sb2coJ0pvYlBsYW5PdmVydmlld0NvbnRyb2xsZXInKTtcbiAgICBpZiAoJHNjb3BlLm5vZGVpZCkge1xuICAgICAgcmV0dXJuIEpvYnNTZXJ2aWNlLmdldFN1YnRhc2tzKCRzY29wZS5ub2RlaWQpLnRoZW4oZnVuY3Rpb24oZGF0YSkge1xuICAgICAgICByZXR1cm4gJHNjb3BlLnN1YnRhc2tzID0gZGF0YTtcbiAgICAgIH0pO1xuICAgIH1cbiAgfSk7XG59KS5jb250cm9sbGVyKCdKb2JQbGFuQWNjdW11bGF0b3JzQ29udHJvbGxlcicsIGZ1bmN0aW9uKCRzY29wZSwgSm9ic1NlcnZpY2UpIHtcbiAgY29uc29sZS5sb2coJ0pvYlBsYW5BY2N1bXVsYXRvcnNDb250cm9sbGVyJyk7XG4gIGlmICgkc2NvcGUubm9kZWlkICYmICghJHNjb3BlLnZlcnRleCB8fCAhJHNjb3BlLnZlcnRleC5hY2N1bXVsYXRvcnMpKSB7XG4gICAgSm9ic1NlcnZpY2UuZ2V0QWNjdW11bGF0b3JzKCRzY29wZS5ub2RlaWQpLnRoZW4oZnVuY3Rpb24oZGF0YSkge1xuICAgICAgJHNjb3BlLmFjY3VtdWxhdG9ycyA9IGRhdGEubWFpbjtcbiAgICAgIHJldHVybiAkc2NvcGUuc3VidGFza0FjY3VtdWxhdG9ycyA9IGRhdGEuc3VidGFza3M7XG4gICAgfSk7XG4gIH1cbiAgcmV0dXJuICRzY29wZS4kb24oJ3JlbG9hZCcsIGZ1bmN0aW9uKGV2ZW50KSB7XG4gICAgY29uc29sZS5sb2coJ0pvYlBsYW5BY2N1bXVsYXRvcnNDb250cm9sbGVyJyk7XG4gICAgaWYgKCRzY29wZS5ub2RlaWQpIHtcbiAgICAgIHJldHVybiBKb2JzU2VydmljZS5nZXRBY2N1bXVsYXRvcnMoJHNjb3BlLm5vZGVpZCkudGhlbihmdW5jdGlvbihkYXRhKSB7XG4gICAgICAgICRzY29wZS5hY2N1bXVsYXRvcnMgPSBkYXRhLm1haW47XG4gICAgICAgIHJldHVybiAkc2NvcGUuc3VidGFza0FjY3VtdWxhdG9ycyA9IGRhdGEuc3VidGFza3M7XG4gICAgICB9KTtcbiAgICB9XG4gIH0pO1xufSkuY29udHJvbGxlcignSm9iVGltZWxpbmVWZXJ0ZXhDb250cm9sbGVyJywgZnVuY3Rpb24oJHNjb3BlLCAkc3RhdGUsICRzdGF0ZVBhcmFtcywgSm9ic1NlcnZpY2UpIHtcbiAgY29uc29sZS5sb2coJ0pvYlRpbWVsaW5lVmVydGV4Q29udHJvbGxlcicpO1xuICBKb2JzU2VydmljZS5nZXRWZXJ0ZXgoJHN0YXRlUGFyYW1zLnZlcnRleElkKS50aGVuKGZ1bmN0aW9uKGRhdGEpIHtcbiAgICByZXR1cm4gJHNjb3BlLnZlcnRleCA9IGRhdGE7XG4gIH0pO1xuICByZXR1cm4gJHNjb3BlLiRvbigncmVsb2FkJywgZnVuY3Rpb24oZXZlbnQpIHtcbiAgICBjb25zb2xlLmxvZygnSm9iVGltZWxpbmVWZXJ0ZXhDb250cm9sbGVyJyk7XG4gICAgcmV0dXJuIEpvYnNTZXJ2aWNlLmdldFZlcnRleCgkc3RhdGVQYXJhbXMudmVydGV4SWQpLnRoZW4oZnVuY3Rpb24oZGF0YSkge1xuICAgICAgcmV0dXJuICRzY29wZS52ZXJ0ZXggPSBkYXRhO1xuICAgIH0pO1xuICB9KTtcbn0pLmNvbnRyb2xsZXIoJ0pvYkV4Y2VwdGlvbnNDb250cm9sbGVyJywgZnVuY3Rpb24oJHNjb3BlLCAkc3RhdGUsICRzdGF0ZVBhcmFtcywgSm9ic1NlcnZpY2UpIHtcbiAgcmV0dXJuIEpvYnNTZXJ2aWNlLmxvYWRFeGNlcHRpb25zKCkudGhlbihmdW5jdGlvbihkYXRhKSB7XG4gICAgcmV0dXJuICRzY29wZS5leGNlcHRpb25zID0gZGF0YTtcbiAgfSk7XG59KS5jb250cm9sbGVyKCdKb2JQcm9wZXJ0aWVzQ29udHJvbGxlcicsIGZ1bmN0aW9uKCRzY29wZSwgSm9ic1NlcnZpY2UpIHtcbiAgY29uc29sZS5sb2coJ0pvYlByb3BlcnRpZXNDb250cm9sbGVyJyk7XG4gIHJldHVybiAkc2NvcGUuY2hhbmdlTm9kZSA9IGZ1bmN0aW9uKG5vZGVpZCkge1xuICAgIGlmIChub2RlaWQgIT09ICRzY29wZS5ub2RlaWQpIHtcbiAgICAgICRzY29wZS5ub2RlaWQgPSBub2RlaWQ7XG4gICAgICByZXR1cm4gSm9ic1NlcnZpY2UuZ2V0Tm9kZShub2RlaWQpLnRoZW4oZnVuY3Rpb24oZGF0YSkge1xuICAgICAgICByZXR1cm4gJHNjb3BlLm5vZGUgPSBkYXRhO1xuICAgICAgfSk7XG4gICAgfSBlbHNlIHtcbiAgICAgICRzY29wZS5ub2RlaWQgPSBudWxsO1xuICAgICAgcmV0dXJuICRzY29wZS5ub2RlID0gbnVsbDtcbiAgICB9XG4gIH07XG59KTtcbiIsIiNcbiMgTGljZW5zZWQgdG8gdGhlIEFwYWNoZSBTb2Z0d2FyZSBGb3VuZGF0aW9uIChBU0YpIHVuZGVyIG9uZVxuIyBvciBtb3JlIGNvbnRyaWJ1dG9yIGxpY2Vuc2UgYWdyZWVtZW50cy4gIFNlZSB0aGUgTk9USUNFIGZpbGVcbiMgZGlzdHJpYnV0ZWQgd2l0aCB0aGlzIHdvcmsgZm9yIGFkZGl0aW9uYWwgaW5mb3JtYXRpb25cbiMgcmVnYXJkaW5nIGNvcHlyaWdodCBvd25lcnNoaXAuICBUaGUgQVNGIGxpY2Vuc2VzIHRoaXMgZmlsZVxuIyB0byB5b3UgdW5kZXIgdGhlIEFwYWNoZSBMaWNlbnNlLCBWZXJzaW9uIDIuMCAodGhlXG4jIFwiTGljZW5zZVwiKTsgeW91IG1heSBub3QgdXNlIHRoaXMgZmlsZSBleGNlcHQgaW4gY29tcGxpYW5jZVxuIyB3aXRoIHRoZSBMaWNlbnNlLiAgWW91IG1heSBvYnRhaW4gYSBjb3B5IG9mIHRoZSBMaWNlbnNlIGF0XG4jXG4jICAgICBodHRwOi8vd3d3LmFwYWNoZS5vcmcvbGljZW5zZXMvTElDRU5TRS0yLjBcbiNcbiMgVW5sZXNzIHJlcXVpcmVkIGJ5IGFwcGxpY2FibGUgbGF3IG9yIGFncmVlZCB0byBpbiB3cml0aW5nLCBzb2Z0d2FyZVxuIyBkaXN0cmlidXRlZCB1bmRlciB0aGUgTGljZW5zZSBpcyBkaXN0cmlidXRlZCBvbiBhbiBcIkFTIElTXCIgQkFTSVMsXG4jIFdJVEhPVVQgV0FSUkFOVElFUyBPUiBDT05ESVRJT05TIE9GIEFOWSBLSU5ELCBlaXRoZXIgZXhwcmVzcyBvciBpbXBsaWVkLlxuIyBTZWUgdGhlIExpY2Vuc2UgZm9yIHRoZSBzcGVjaWZpYyBsYW5ndWFnZSBnb3Zlcm5pbmcgcGVybWlzc2lvbnMgYW5kXG4jIGxpbWl0YXRpb25zIHVuZGVyIHRoZSBMaWNlbnNlLlxuI1xuXG5hbmd1bGFyLm1vZHVsZSgnZmxpbmtBcHAnKVxuXG4jIC0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS1cblxuLmRpcmVjdGl2ZSAndmVydGV4JywgKCRzdGF0ZSkgLT5cbiAgdGVtcGxhdGU6IFwiPHN2ZyBjbGFzcz0ndGltZWxpbmUgc2Vjb25kYXJ5JyB3aWR0aD0nMCcgaGVpZ2h0PScwJz48L3N2Zz5cIlxuXG4gIHNjb3BlOlxuICAgIGRhdGE6IFwiPVwiXG5cbiAgbGluazogKHNjb3BlLCBlbGVtLCBhdHRycykgLT5cbiAgICBzdmdFbCA9IGVsZW0uY2hpbGRyZW4oKVswXVxuXG4gICAgY29udGFpbmVyVyA9IGVsZW0ud2lkdGgoKVxuICAgIGFuZ3VsYXIuZWxlbWVudChzdmdFbCkuYXR0cignd2lkdGgnLCBjb250YWluZXJXKVxuXG4gICAgYW5hbHl6ZVRpbWUgPSAoZGF0YSkgLT5cbiAgICAgIGQzLnNlbGVjdChzdmdFbCkuc2VsZWN0QWxsKFwiKlwiKS5yZW1vdmUoKVxuXG4gICAgICB0ZXN0RGF0YSA9IFtdXG5cbiAgICAgIGFuZ3VsYXIuZm9yRWFjaCBkYXRhLnN1YnRhc2tzLCAoc3VidGFzaywgaSkgLT5cbiAgICAgICAgdGltZXMgPSBbXG4gICAgICAgICAge1xuICAgICAgICAgICAgbGFiZWw6IFwiU2NoZWR1bGVkXCJcbiAgICAgICAgICAgIGNvbG9yOiBcIiM2NjZcIlxuICAgICAgICAgICAgYm9yZGVyQ29sb3I6IFwiIzU1NVwiXG4gICAgICAgICAgICBzdGFydGluZ190aW1lOiBzdWJ0YXNrLnRpbWVzdGFtcHNbXCJTQ0hFRFVMRURcIl1cbiAgICAgICAgICAgIGVuZGluZ190aW1lOiBzdWJ0YXNrLnRpbWVzdGFtcHNbXCJERVBMT1lJTkdcIl1cbiAgICAgICAgICAgIHR5cGU6ICdyZWd1bGFyJ1xuICAgICAgICAgIH1cbiAgICAgICAgICB7XG4gICAgICAgICAgICBsYWJlbDogXCJEZXBsb3lpbmdcIlxuICAgICAgICAgICAgY29sb3I6IFwiI2FhYVwiXG4gICAgICAgICAgICBib3JkZXJDb2xvcjogXCIjNTU1XCJcbiAgICAgICAgICAgIHN0YXJ0aW5nX3RpbWU6IHN1YnRhc2sudGltZXN0YW1wc1tcIkRFUExPWUlOR1wiXVxuICAgICAgICAgICAgZW5kaW5nX3RpbWU6IHN1YnRhc2sudGltZXN0YW1wc1tcIlJVTk5JTkdcIl1cbiAgICAgICAgICAgIHR5cGU6ICdyZWd1bGFyJ1xuICAgICAgICAgIH1cbiAgICAgICAgXVxuXG4gICAgICAgIGlmIHN1YnRhc2sudGltZXN0YW1wc1tcIkZJTklTSEVEXCJdID4gMFxuICAgICAgICAgIHRpbWVzLnB1c2gge1xuICAgICAgICAgICAgbGFiZWw6IFwiUnVubmluZ1wiXG4gICAgICAgICAgICBjb2xvcjogXCIjZGRkXCJcbiAgICAgICAgICAgIGJvcmRlckNvbG9yOiBcIiM1NTVcIlxuICAgICAgICAgICAgc3RhcnRpbmdfdGltZTogc3VidGFzay50aW1lc3RhbXBzW1wiUlVOTklOR1wiXVxuICAgICAgICAgICAgZW5kaW5nX3RpbWU6IHN1YnRhc2sudGltZXN0YW1wc1tcIkZJTklTSEVEXCJdXG4gICAgICAgICAgICB0eXBlOiAncmVndWxhcidcbiAgICAgICAgICB9XG5cbiAgICAgICAgdGVzdERhdGEucHVzaCB7XG4gICAgICAgICAgbGFiZWw6IFwiKCN7c3VidGFzay5zdWJ0YXNrfSkgI3tzdWJ0YXNrLmhvc3R9XCJcbiAgICAgICAgICB0aW1lczogdGltZXNcbiAgICAgICAgfVxuXG4gICAgICBjaGFydCA9IGQzLnRpbWVsaW5lKCkuc3RhY2soKVxuICAgICAgLnRpY2tGb3JtYXQoe1xuICAgICAgICBmb3JtYXQ6IGQzLnRpbWUuZm9ybWF0KFwiJUxcIilcbiAgICAgICAgIyB0aWNrSW50ZXJ2YWw6IDFcbiAgICAgICAgdGlja1NpemU6IDFcbiAgICAgIH0pXG4gICAgICAucHJlZml4KFwic2luZ2xlXCIpXG4gICAgICAubGFiZWxGb3JtYXQoKGxhYmVsKSAtPlxuICAgICAgICBsYWJlbFxuICAgICAgKVxuICAgICAgLm1hcmdpbih7IGxlZnQ6IDEwMCwgcmlnaHQ6IDAsIHRvcDogMCwgYm90dG9tOiAwIH0pXG4gICAgICAuaXRlbUhlaWdodCgzMClcbiAgICAgIC5yZWxhdGl2ZVRpbWUoKVxuXG4gICAgICBzdmcgPSBkMy5zZWxlY3Qoc3ZnRWwpXG4gICAgICAuZGF0dW0odGVzdERhdGEpXG4gICAgICAuY2FsbChjaGFydClcblxuICAgIGFuYWx5emVUaW1lKHNjb3BlLmRhdGEpXG5cbiAgICByZXR1cm5cblxuIyAtLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tXG5cbi5kaXJlY3RpdmUgJ3RpbWVsaW5lJywgKCRzdGF0ZSkgLT5cbiAgdGVtcGxhdGU6IFwiPHN2ZyBjbGFzcz0ndGltZWxpbmUnIHdpZHRoPScwJyBoZWlnaHQ9JzAnPjwvc3ZnPlwiXG5cbiAgc2NvcGU6XG4gICAgdmVydGljZXM6IFwiPVwiXG4gICAgam9iaWQ6IFwiPVwiXG5cbiAgbGluazogKHNjb3BlLCBlbGVtLCBhdHRycykgLT5cbiAgICBzdmdFbCA9IGVsZW0uY2hpbGRyZW4oKVswXVxuXG4gICAgY29udGFpbmVyVyA9IGVsZW0ud2lkdGgoKVxuICAgIGFuZ3VsYXIuZWxlbWVudChzdmdFbCkuYXR0cignd2lkdGgnLCBjb250YWluZXJXKVxuXG4gICAgdHJhbnNsYXRlTGFiZWwgPSAobGFiZWwpIC0+XG4gICAgICBsYWJlbC5yZXBsYWNlKFwiJmd0O1wiLCBcIj5cIilcblxuICAgIGFuYWx5emVUaW1lID0gKGRhdGEpIC0+XG4gICAgICBkMy5zZWxlY3Qoc3ZnRWwpLnNlbGVjdEFsbChcIipcIikucmVtb3ZlKClcblxuICAgICAgdGVzdERhdGEgPSBbXVxuXG4gICAgICBhbmd1bGFyLmZvckVhY2ggZGF0YSwgKHZlcnRleCkgLT5cbiAgICAgICAgaWYgdmVydGV4WydzdGFydC10aW1lJ10gPiAtMVxuICAgICAgICAgIGlmIHZlcnRleC50eXBlIGlzICdzY2hlZHVsZWQnXG4gICAgICAgICAgICB0ZXN0RGF0YS5wdXNoIFxuICAgICAgICAgICAgICB0aW1lczogW1xuICAgICAgICAgICAgICAgIGxhYmVsOiB0cmFuc2xhdGVMYWJlbCh2ZXJ0ZXgubmFtZSlcbiAgICAgICAgICAgICAgICBjb2xvcjogXCIjY2NjY2NjXCJcbiAgICAgICAgICAgICAgICBib3JkZXJDb2xvcjogXCIjNTU1NTU1XCJcbiAgICAgICAgICAgICAgICBzdGFydGluZ190aW1lOiB2ZXJ0ZXhbJ3N0YXJ0LXRpbWUnXVxuICAgICAgICAgICAgICAgIGVuZGluZ190aW1lOiB2ZXJ0ZXhbJ2VuZC10aW1lJ11cbiAgICAgICAgICAgICAgICB0eXBlOiB2ZXJ0ZXgudHlwZVxuICAgICAgICAgICAgICBdXG4gICAgICAgICAgZWxzZVxuICAgICAgICAgICAgdGVzdERhdGEucHVzaCBcbiAgICAgICAgICAgICAgdGltZXM6IFtcbiAgICAgICAgICAgICAgICBsYWJlbDogdHJhbnNsYXRlTGFiZWwodmVydGV4Lm5hbWUpXG4gICAgICAgICAgICAgICAgY29sb3I6IFwiI2Q5ZjFmN1wiXG4gICAgICAgICAgICAgICAgYm9yZGVyQ29sb3I6IFwiIzYyY2RlYVwiXG4gICAgICAgICAgICAgICAgc3RhcnRpbmdfdGltZTogdmVydGV4WydzdGFydC10aW1lJ11cbiAgICAgICAgICAgICAgICBlbmRpbmdfdGltZTogdmVydGV4WydlbmQtdGltZSddXG4gICAgICAgICAgICAgICAgbGluazogdmVydGV4LmlkXG4gICAgICAgICAgICAgICAgdHlwZTogdmVydGV4LnR5cGVcbiAgICAgICAgICAgICAgXVxuXG4gICAgICBjaGFydCA9IGQzLnRpbWVsaW5lKCkuc3RhY2soKS5jbGljaygoZCwgaSwgZGF0dW0pIC0+XG4gICAgICAgIGlmIGQubGlua1xuICAgICAgICAgICRzdGF0ZS5nbyBcInNpbmdsZS1qb2IudGltZWxpbmUudmVydGV4XCIsIHsgam9iaWQ6IHNjb3BlLmpvYmlkLCB2ZXJ0ZXhJZDogZC5saW5rIH1cblxuICAgICAgKVxuICAgICAgLnRpY2tGb3JtYXQoe1xuICAgICAgICBmb3JtYXQ6IGQzLnRpbWUuZm9ybWF0KFwiJUxcIilcbiAgICAgICAgIyB0aWNrVGltZTogZDMudGltZS5zZWNvbmRcbiAgICAgICAgIyB0aWNrSW50ZXJ2YWw6IDAuNVxuICAgICAgICB0aWNrU2l6ZTogMVxuICAgICAgfSlcbiAgICAgIC5wcmVmaXgoXCJtYWluXCIpXG4gICAgICAubWFyZ2luKHsgbGVmdDogMCwgcmlnaHQ6IDAsIHRvcDogMCwgYm90dG9tOiAwIH0pXG4gICAgICAuaXRlbUhlaWdodCgzMClcbiAgICAgIC5zaG93Qm9yZGVyTGluZSgpXG4gICAgICAuc2hvd0hvdXJUaW1lbGluZSgpXG5cbiAgICAgIHN2ZyA9IGQzLnNlbGVjdChzdmdFbClcbiAgICAgIC5kYXR1bSh0ZXN0RGF0YSlcbiAgICAgIC5jYWxsKGNoYXJ0KVxuXG4gICAgc2NvcGUuJHdhdGNoIGF0dHJzLnZlcnRpY2VzLCAoZGF0YSkgLT5cbiAgICAgIGFuYWx5emVUaW1lKGRhdGEpIGlmIGRhdGFcblxuICAgIHJldHVyblxuXG4jIC0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS1cblxuLmRpcmVjdGl2ZSAnam9iUGxhbicsICgkdGltZW91dCkgLT5cbiAgdGVtcGxhdGU6IFwiXG4gICAgPHN2ZyBjbGFzcz0nZ3JhcGgnIHdpZHRoPSc1MDAnIGhlaWdodD0nNDAwJz48ZyAvPjwvc3ZnPlxuICAgIDxzdmcgY2xhc3M9J3RtcCcgd2lkdGg9JzEnIGhlaWdodD0nMSc+PGcgLz48L3N2Zz5cbiAgICA8ZGl2IGNsYXNzPSdidG4tZ3JvdXAgem9vbS1idXR0b25zJz5cbiAgICAgIDxhIGNsYXNzPSdidG4gYnRuLWRlZmF1bHQgem9vbS1pbicgbmctY2xpY2s9J3pvb21JbigpJz48aSBjbGFzcz0nZmEgZmEtcGx1cycgLz48L2E+XG4gICAgICA8YSBjbGFzcz0nYnRuIGJ0bi1kZWZhdWx0IHpvb20tb3V0JyBuZy1jbGljaz0nem9vbU91dCgpJz48aSBjbGFzcz0nZmEgZmEtbWludXMnIC8+PC9hPlxuICAgIDwvZGl2PlwiXG5cbiAgc2NvcGU6XG4gICAgcGxhbjogJz0nXG4gICAgc2V0Tm9kZTogJyYnXG5cbiAgbGluazogKHNjb3BlLCBlbGVtLCBhdHRycykgLT5cbiAgICBnID0gbnVsbFxuICAgIG1haW5ab29tID0gZDMuYmVoYXZpb3Iuem9vbSgpXG4gICAgc3ViZ3JhcGhzID0gW11cbiAgICBqb2JpZCA9IGF0dHJzLmpvYmlkXG5cbiAgICBtYWluU3ZnRWxlbWVudCA9IGVsZW0uY2hpbGRyZW4oKVswXVxuICAgIG1haW5HID0gZWxlbS5jaGlsZHJlbigpLmNoaWxkcmVuKClbMF1cbiAgICBtYWluVG1wRWxlbWVudCA9IGVsZW0uY2hpbGRyZW4oKVsxXVxuXG4gICAgZDNtYWluU3ZnID0gZDMuc2VsZWN0KG1haW5TdmdFbGVtZW50KVxuICAgIGQzbWFpblN2Z0cgPSBkMy5zZWxlY3QobWFpbkcpXG4gICAgZDN0bXBTdmcgPSBkMy5zZWxlY3QobWFpblRtcEVsZW1lbnQpXG5cbiAgICAjIGFuZ3VsYXIuZWxlbWVudChtYWluRykuZW1wdHkoKVxuICAgICMgZDNtYWluU3ZnRy5zZWxlY3RBbGwoXCIqXCIpLnJlbW92ZSgpXG5cbiAgICBjb250YWluZXJXID0gZWxlbS53aWR0aCgpXG4gICAgYW5ndWxhci5lbGVtZW50KGVsZW0uY2hpbGRyZW4oKVswXSkud2lkdGgoY29udGFpbmVyVylcblxuICAgIHNjb3BlLnpvb21JbiA9IC0+XG4gICAgICBpZiBtYWluWm9vbS5zY2FsZSgpIDwgMi45OVxuICAgICAgICBcbiAgICAgICAgIyBDYWxjdWxhdGUgYW5kIHN0b3JlIG5ldyB2YWx1ZXMgaW4gem9vbSBvYmplY3RcbiAgICAgICAgdHJhbnNsYXRlID0gbWFpblpvb20udHJhbnNsYXRlKClcbiAgICAgICAgdjEgPSB0cmFuc2xhdGVbMF0gKiAobWFpblpvb20uc2NhbGUoKSArIDAuMSAvIChtYWluWm9vbS5zY2FsZSgpKSlcbiAgICAgICAgdjIgPSB0cmFuc2xhdGVbMV0gKiAobWFpblpvb20uc2NhbGUoKSArIDAuMSAvIChtYWluWm9vbS5zY2FsZSgpKSlcbiAgICAgICAgbWFpblpvb20uc2NhbGUgbWFpblpvb20uc2NhbGUoKSArIDAuMVxuICAgICAgICBtYWluWm9vbS50cmFuc2xhdGUgWyB2MSwgdjIgXVxuICAgICAgICBcbiAgICAgICAgIyBUcmFuc2Zvcm0gc3ZnXG4gICAgICAgIGQzbWFpblN2Z0cuYXR0ciBcInRyYW5zZm9ybVwiLCBcInRyYW5zbGF0ZShcIiArIHYxICsgXCIsXCIgKyB2MiArIFwiKSBzY2FsZShcIiArIG1haW5ab29tLnNjYWxlKCkgKyBcIilcIlxuXG4gICAgc2NvcGUuem9vbU91dCA9IC0+XG4gICAgICBpZiBtYWluWm9vbS5zY2FsZSgpID4gMC4zMVxuICAgICAgICBcbiAgICAgICAgIyBDYWxjdWxhdGUgYW5kIHN0b3JlIG5ldyB2YWx1ZXMgaW4gbWFpblpvb20gb2JqZWN0XG4gICAgICAgIG1haW5ab29tLnNjYWxlIG1haW5ab29tLnNjYWxlKCkgLSAwLjFcbiAgICAgICAgdHJhbnNsYXRlID0gbWFpblpvb20udHJhbnNsYXRlKClcbiAgICAgICAgdjEgPSB0cmFuc2xhdGVbMF0gKiAobWFpblpvb20uc2NhbGUoKSAtIDAuMSAvIChtYWluWm9vbS5zY2FsZSgpKSlcbiAgICAgICAgdjIgPSB0cmFuc2xhdGVbMV0gKiAobWFpblpvb20uc2NhbGUoKSAtIDAuMSAvIChtYWluWm9vbS5zY2FsZSgpKSlcbiAgICAgICAgbWFpblpvb20udHJhbnNsYXRlIFsgdjEsIHYyIF1cbiAgICAgICAgXG4gICAgICAgICMgVHJhbnNmb3JtIHN2Z1xuICAgICAgICBkM21haW5TdmdHLmF0dHIgXCJ0cmFuc2Zvcm1cIiwgXCJ0cmFuc2xhdGUoXCIgKyB2MSArIFwiLFwiICsgdjIgKyBcIikgc2NhbGUoXCIgKyBtYWluWm9vbS5zY2FsZSgpICsgXCIpXCJcblxuICAgICNjcmVhdGUgYSBsYWJlbCBvZiBhbiBlZGdlXG4gICAgY3JlYXRlTGFiZWxFZGdlID0gKGVsKSAtPlxuICAgICAgbGFiZWxWYWx1ZSA9IFwiXCJcbiAgICAgIGlmIGVsLnNoaXBfc3RyYXRlZ3k/IG9yIGVsLmxvY2FsX3N0cmF0ZWd5P1xuICAgICAgICBsYWJlbFZhbHVlICs9IFwiPGRpdiBjbGFzcz0nZWRnZS1sYWJlbCc+XCJcbiAgICAgICAgbGFiZWxWYWx1ZSArPSBlbC5zaGlwX3N0cmF0ZWd5ICBpZiBlbC5zaGlwX3N0cmF0ZWd5P1xuICAgICAgICBsYWJlbFZhbHVlICs9IFwiIChcIiArIGVsLnRlbXBfbW9kZSArIFwiKVwiICB1bmxlc3MgZWwudGVtcF9tb2RlIGlzIGB1bmRlZmluZWRgXG4gICAgICAgIGxhYmVsVmFsdWUgKz0gXCIsPGJyPlwiICsgZWwubG9jYWxfc3RyYXRlZ3kgIHVubGVzcyBlbC5sb2NhbF9zdHJhdGVneSBpcyBgdW5kZWZpbmVkYFxuICAgICAgICBsYWJlbFZhbHVlICs9IFwiPC9kaXY+XCJcbiAgICAgIGxhYmVsVmFsdWVcblxuXG4gICAgIyB0cnVlLCBpZiB0aGUgbm9kZSBpcyBhIHNwZWNpYWwgbm9kZSBmcm9tIGFuIGl0ZXJhdGlvblxuICAgIGlzU3BlY2lhbEl0ZXJhdGlvbk5vZGUgPSAoaW5mbykgLT5cbiAgICAgIChpbmZvIGlzIFwicGFydGlhbFNvbHV0aW9uXCIgb3IgaW5mbyBpcyBcIm5leHRQYXJ0aWFsU29sdXRpb25cIiBvciBpbmZvIGlzIFwid29ya3NldFwiIG9yIGluZm8gaXMgXCJuZXh0V29ya3NldFwiIG9yIGluZm8gaXMgXCJzb2x1dGlvblNldFwiIG9yIGluZm8gaXMgXCJzb2x1dGlvbkRlbHRhXCIpXG5cbiAgICBnZXROb2RlVHlwZSA9IChlbCwgaW5mbykgLT5cbiAgICAgIGlmIGluZm8gaXMgXCJtaXJyb3JcIlxuICAgICAgICAnbm9kZS1taXJyb3InXG5cbiAgICAgIGVsc2UgaWYgaXNTcGVjaWFsSXRlcmF0aW9uTm9kZShpbmZvKVxuICAgICAgICAnbm9kZS1pdGVyYXRpb24nXG5cbiAgICAgIGVsc2VcbiAgICAgICAgICAnbm9kZS1ub3JtYWwnXG4gICAgICBcbiAgICAjIGNyZWF0ZXMgdGhlIGxhYmVsIG9mIGEgbm9kZSwgaW4gaW5mbyBpcyBzdG9yZWQsIHdoZXRoZXIgaXQgaXMgYSBzcGVjaWFsIG5vZGUgKGxpa2UgYSBtaXJyb3IgaW4gYW4gaXRlcmF0aW9uKVxuICAgIGNyZWF0ZUxhYmVsTm9kZSA9IChlbCwgaW5mbywgbWF4VywgbWF4SCkgLT5cbiAgICAgICMgbGFiZWxWYWx1ZSA9IFwiPGEgaHJlZj0nIy9qb2JzL1wiICsgam9iaWQgKyBcIi92ZXJ0ZXgvXCIgKyBlbC5pZCArIFwiJyBjbGFzcz0nbm9kZS1sYWJlbCBcIiArIGdldE5vZGVUeXBlKGVsLCBpbmZvKSArIFwiJz5cIlxuICAgICAgbGFiZWxWYWx1ZSA9IFwiPGRpdiBocmVmPScjL2pvYnMvXCIgKyBqb2JpZCArIFwiL3ZlcnRleC9cIiArIGVsLmlkICsgXCInIGNsYXNzPSdub2RlLWxhYmVsIFwiICsgZ2V0Tm9kZVR5cGUoZWwsIGluZm8pICsgXCInPlwiXG5cbiAgICAgICMgTm9kZW5hbWVcbiAgICAgIGlmIGluZm8gaXMgXCJtaXJyb3JcIlxuICAgICAgICBsYWJlbFZhbHVlICs9IFwiPGgzIGNsYXNzPSdub2RlLW5hbWUnPk1pcnJvciBvZiBcIiArIGVsLm9wZXJhdG9yICsgXCI8L2gzPlwiXG4gICAgICBlbHNlXG4gICAgICAgIGxhYmVsVmFsdWUgKz0gXCI8aDMgY2xhc3M9J25vZGUtbmFtZSc+XCIgKyBlbC5vcGVyYXRvciArIFwiPC9oMz5cIlxuICAgICAgaWYgZWwuZGVzY3JpcHRpb24gaXMgXCJcIlxuICAgICAgICBsYWJlbFZhbHVlICs9IFwiXCJcbiAgICAgIGVsc2VcbiAgICAgICAgc3RlcE5hbWUgPSBlbC5kZXNjcmlwdGlvblxuICAgICAgICBcbiAgICAgICAgIyBjbGVhbiBzdGVwTmFtZVxuICAgICAgICBzdGVwTmFtZSA9IHNob3J0ZW5TdHJpbmcoc3RlcE5hbWUpXG4gICAgICAgIGxhYmVsVmFsdWUgKz0gXCI8aDQgY2xhc3M9J3N0ZXAtbmFtZSc+XCIgKyBzdGVwTmFtZSArIFwiPC9oND5cIlxuICAgICAgXG4gICAgICAjIElmIHRoaXMgbm9kZSBpcyBhbiBcIml0ZXJhdGlvblwiIHdlIG5lZWQgYSBkaWZmZXJlbnQgcGFuZWwtYm9keVxuICAgICAgaWYgZWwuc3RlcF9mdW5jdGlvbj9cbiAgICAgICAgbGFiZWxWYWx1ZSArPSBleHRlbmRMYWJlbE5vZGVGb3JJdGVyYXRpb24oZWwuaWQsIG1heFcsIG1heEgpXG4gICAgICBlbHNlXG4gICAgICAgIFxuICAgICAgICAjIE90aGVyd2lzZSBhZGQgaW5mb3MgICAgXG4gICAgICAgIGxhYmVsVmFsdWUgKz0gXCI8aDU+XCIgKyBpbmZvICsgXCIgTm9kZTwvaDU+XCIgIGlmIGlzU3BlY2lhbEl0ZXJhdGlvbk5vZGUoaW5mbylcbiAgICAgICAgbGFiZWxWYWx1ZSArPSBcIjxoNT5QYXJhbGxlbGlzbTogXCIgKyBlbC5wYXJhbGxlbGlzbSArIFwiPC9oNT5cIiAgdW5sZXNzIGVsLnBhcmFsbGVsaXNtIGlzIFwiXCJcbiAgICAgICAgbGFiZWxWYWx1ZSArPSBcIjxoNT5PcGVyYXRpb246IFwiICsgc2hvcnRlblN0cmluZyhlbC5vcGVyYXRvcl9zdHJhdGVneSkgKyBcIjwvaDU+XCIgIHVubGVzcyBlbC5vcGVyYXRvciBpcyBgdW5kZWZpbmVkYFxuICAgICAgXG4gICAgICAjIGxhYmVsVmFsdWUgKz0gXCI8L2E+XCJcbiAgICAgIGxhYmVsVmFsdWUgKz0gXCI8L2Rpdj5cIlxuICAgICAgbGFiZWxWYWx1ZVxuXG4gICAgIyBFeHRlbmRzIHRoZSBsYWJlbCBvZiBhIG5vZGUgd2l0aCBhbiBhZGRpdGlvbmFsIHN2ZyBFbGVtZW50IHRvIHByZXNlbnQgdGhlIGl0ZXJhdGlvbi5cbiAgICBleHRlbmRMYWJlbE5vZGVGb3JJdGVyYXRpb24gPSAoaWQsIG1heFcsIG1heEgpIC0+XG4gICAgICBzdmdJRCA9IFwic3ZnLVwiICsgaWRcblxuICAgICAgbGFiZWxWYWx1ZSA9IFwiPHN2ZyBjbGFzcz0nXCIgKyBzdmdJRCArIFwiJyB3aWR0aD1cIiArIG1heFcgKyBcIiBoZWlnaHQ9XCIgKyBtYXhIICsgXCI+PGcgLz48L3N2Zz5cIlxuICAgICAgbGFiZWxWYWx1ZVxuXG4gICAgIyBTcGxpdCBhIHN0cmluZyBpbnRvIG11bHRpcGxlIGxpbmVzIHNvIHRoYXQgZWFjaCBsaW5lIGhhcyBsZXNzIHRoYW4gMzAgbGV0dGVycy5cbiAgICBzaG9ydGVuU3RyaW5nID0gKHMpIC0+XG4gICAgICAjIG1ha2Ugc3VyZSB0aGF0IG5hbWUgZG9lcyBub3QgY29udGFpbiBhIDwgKGJlY2F1c2Ugb2YgaHRtbClcbiAgICAgIGlmIHMuY2hhckF0KDApIGlzIFwiPFwiXG4gICAgICAgIHMgPSBzLnJlcGxhY2UoXCI8XCIsIFwiJmx0O1wiKVxuICAgICAgICBzID0gcy5yZXBsYWNlKFwiPlwiLCBcIiZndDtcIilcbiAgICAgIHNiciA9IFwiXCJcbiAgICAgIHdoaWxlIHMubGVuZ3RoID4gMzBcbiAgICAgICAgc2JyID0gc2JyICsgcy5zdWJzdHJpbmcoMCwgMzApICsgXCI8YnI+XCJcbiAgICAgICAgcyA9IHMuc3Vic3RyaW5nKDMwLCBzLmxlbmd0aClcbiAgICAgIHNiciA9IHNiciArIHNcbiAgICAgIHNiclxuXG4gICAgY3JlYXRlTm9kZSA9IChnLCBkYXRhLCBlbCwgaXNQYXJlbnQgPSBmYWxzZSwgbWF4VywgbWF4SCkgLT5cbiAgICAgICMgY3JlYXRlIG5vZGUsIHNlbmQgYWRkaXRpb25hbCBpbmZvcm1hdGlvbnMgYWJvdXQgdGhlIG5vZGUgaWYgaXQgaXMgYSBzcGVjaWFsIG9uZVxuICAgICAgaWYgZWwuaWQgaXMgZGF0YS5wYXJ0aWFsX3NvbHV0aW9uXG4gICAgICAgIGcuc2V0Tm9kZSBlbC5pZCxcbiAgICAgICAgICBsYWJlbDogY3JlYXRlTGFiZWxOb2RlKGVsLCBcInBhcnRpYWxTb2x1dGlvblwiLCBtYXhXLCBtYXhIKVxuICAgICAgICAgIGxhYmVsVHlwZTogJ2h0bWwnXG4gICAgICAgICAgY2xhc3M6IGdldE5vZGVUeXBlKGVsLCBcInBhcnRpYWxTb2x1dGlvblwiKVxuXG4gICAgICBlbHNlIGlmIGVsLmlkIGlzIGRhdGEubmV4dF9wYXJ0aWFsX3NvbHV0aW9uXG4gICAgICAgIGcuc2V0Tm9kZSBlbC5pZCxcbiAgICAgICAgICBsYWJlbDogY3JlYXRlTGFiZWxOb2RlKGVsLCBcIm5leHRQYXJ0aWFsU29sdXRpb25cIiwgbWF4VywgbWF4SClcbiAgICAgICAgICBsYWJlbFR5cGU6ICdodG1sJ1xuICAgICAgICAgIGNsYXNzOiBnZXROb2RlVHlwZShlbCwgXCJuZXh0UGFydGlhbFNvbHV0aW9uXCIpXG5cbiAgICAgIGVsc2UgaWYgZWwuaWQgaXMgZGF0YS53b3Jrc2V0XG4gICAgICAgIGcuc2V0Tm9kZSBlbC5pZCxcbiAgICAgICAgICBsYWJlbDogY3JlYXRlTGFiZWxOb2RlKGVsLCBcIndvcmtzZXRcIiwgbWF4VywgbWF4SClcbiAgICAgICAgICBsYWJlbFR5cGU6ICdodG1sJ1xuICAgICAgICAgIGNsYXNzOiBnZXROb2RlVHlwZShlbCwgXCJ3b3Jrc2V0XCIpXG5cbiAgICAgIGVsc2UgaWYgZWwuaWQgaXMgZGF0YS5uZXh0X3dvcmtzZXRcbiAgICAgICAgZy5zZXROb2RlIGVsLmlkLFxuICAgICAgICAgIGxhYmVsOiBjcmVhdGVMYWJlbE5vZGUoZWwsIFwibmV4dFdvcmtzZXRcIiwgbWF4VywgbWF4SClcbiAgICAgICAgICBsYWJlbFR5cGU6ICdodG1sJ1xuICAgICAgICAgIGNsYXNzOiBnZXROb2RlVHlwZShlbCwgXCJuZXh0V29ya3NldFwiKVxuXG4gICAgICBlbHNlIGlmIGVsLmlkIGlzIGRhdGEuc29sdXRpb25fc2V0XG4gICAgICAgIGcuc2V0Tm9kZSBlbC5pZCxcbiAgICAgICAgICBsYWJlbDogY3JlYXRlTGFiZWxOb2RlKGVsLCBcInNvbHV0aW9uU2V0XCIsIG1heFcsIG1heEgpXG4gICAgICAgICAgbGFiZWxUeXBlOiAnaHRtbCdcbiAgICAgICAgICBjbGFzczogZ2V0Tm9kZVR5cGUoZWwsIFwic29sdXRpb25TZXRcIilcblxuICAgICAgZWxzZSBpZiBlbC5pZCBpcyBkYXRhLnNvbHV0aW9uX2RlbHRhXG4gICAgICAgIGcuc2V0Tm9kZSBlbC5pZCxcbiAgICAgICAgICBsYWJlbDogY3JlYXRlTGFiZWxOb2RlKGVsLCBcInNvbHV0aW9uRGVsdGFcIiwgbWF4VywgbWF4SClcbiAgICAgICAgICBsYWJlbFR5cGU6ICdodG1sJ1xuICAgICAgICAgIGNsYXNzOiBnZXROb2RlVHlwZShlbCwgXCJzb2x1dGlvbkRlbHRhXCIpXG5cbiAgICAgIGVsc2VcbiAgICAgICAgZy5zZXROb2RlIGVsLmlkLFxuICAgICAgICAgIGxhYmVsOiBjcmVhdGVMYWJlbE5vZGUoZWwsIFwiXCIsIG1heFcsIG1heEgpXG4gICAgICAgICAgbGFiZWxUeXBlOiAnaHRtbCdcbiAgICAgICAgICBjbGFzczogZ2V0Tm9kZVR5cGUoZWwsIFwiXCIpXG5cbiAgICBjcmVhdGVFZGdlID0gKGcsIGRhdGEsIGVsLCBleGlzdGluZ05vZGVzLCBwcmVkKSAtPlxuICAgICAgdW5sZXNzIGV4aXN0aW5nTm9kZXMuaW5kZXhPZihwcmVkLmlkKSBpcyAtMVxuICAgICAgICBnLnNldEVkZ2UgcHJlZC5pZCwgZWwuaWQsXG4gICAgICAgICAgbGFiZWw6IGNyZWF0ZUxhYmVsRWRnZShwcmVkKVxuICAgICAgICAgIGxhYmVsVHlwZTogJ2h0bWwnXG4gICAgICAgICAgYXJyb3doZWFkOiAnbm9ybWFsJ1xuXG4gICAgICBlbHNlXG4gICAgICAgIG1pc3NpbmdOb2RlID0gc2VhcmNoRm9yTm9kZShkYXRhLCBwcmVkLmlkKVxuICAgICAgICB1bmxlc3MgIW1pc3NpbmdOb2RlXG4gICAgICAgICAgZy5zZXRFZGdlIG1pc3NpbmdOb2RlLmlkLCBlbC5pZCxcbiAgICAgICAgICAgIGxhYmVsOiBjcmVhdGVMYWJlbEVkZ2UobWlzc2luZ05vZGUpXG4gICAgICAgICAgICBsYWJlbFR5cGU6ICdodG1sJ1xuXG4gICAgbG9hZEpzb25Ub0RhZ3JlID0gKGcsIGRhdGEpIC0+XG4gICAgICBleGlzdGluZ05vZGVzID0gW11cblxuICAgICAgaWYgZGF0YS5ub2Rlcz9cbiAgICAgICAgIyBUaGlzIGlzIHRoZSBub3JtYWwganNvbiBkYXRhXG4gICAgICAgIHRvSXRlcmF0ZSA9IGRhdGEubm9kZXNcblxuICAgICAgZWxzZVxuICAgICAgICAjIFRoaXMgaXMgYW4gaXRlcmF0aW9uLCB3ZSBub3cgc3RvcmUgc3BlY2lhbCBpdGVyYXRpb24gbm9kZXMgaWYgcG9zc2libGVcbiAgICAgICAgdG9JdGVyYXRlID0gZGF0YS5zdGVwX2Z1bmN0aW9uXG4gICAgICAgIGlzUGFyZW50ID0gdHJ1ZVxuXG4gICAgICBmb3IgZWwgaW4gdG9JdGVyYXRlXG4gICAgICAgIG1heFcgPSAwXG4gICAgICAgIG1heEggPSAwXG5cbiAgICAgICAgaWYgZWwuc3RlcF9mdW5jdGlvblxuICAgICAgICAgIHNnID0gbmV3IGRhZ3JlRDMuZ3JhcGhsaWIuR3JhcGgoeyBtdWx0aWdyYXBoOiB0cnVlLCBjb21wb3VuZDogdHJ1ZSB9KS5zZXRHcmFwaCh7XG4gICAgICAgICAgICBub2Rlc2VwOiAyMFxuICAgICAgICAgICAgZWRnZXNlcDogMFxuICAgICAgICAgICAgcmFua3NlcDogMjBcbiAgICAgICAgICAgIHJhbmtkaXI6IFwiTFJcIlxuICAgICAgICAgICAgbWFyZ2lueDogMTBcbiAgICAgICAgICAgIG1hcmdpbnk6IDEwXG4gICAgICAgICAgICB9KVxuXG4gICAgICAgICAgc3ViZ3JhcGhzW2VsLmlkXSA9IHNnXG5cbiAgICAgICAgICBsb2FkSnNvblRvRGFncmUoc2csIGVsKVxuXG4gICAgICAgICAgciA9IG5ldyBkYWdyZUQzLnJlbmRlcigpXG4gICAgICAgICAgZDN0bXBTdmcuc2VsZWN0KCdnJykuY2FsbChyLCBzZylcbiAgICAgICAgICBtYXhXID0gc2cuZ3JhcGgoKS53aWR0aFxuICAgICAgICAgIG1heEggPSBzZy5ncmFwaCgpLmhlaWdodFxuXG4gICAgICAgICAgYW5ndWxhci5lbGVtZW50KG1haW5UbXBFbGVtZW50KS5lbXB0eSgpXG5cbiAgICAgICAgY3JlYXRlTm9kZShnLCBkYXRhLCBlbCwgaXNQYXJlbnQsIG1heFcsIG1heEgpXG5cbiAgICAgICAgZXhpc3RpbmdOb2Rlcy5wdXNoIGVsLmlkXG4gICAgICAgIFxuICAgICAgICAjIGNyZWF0ZSBlZGdlcyBmcm9tIGlucHV0cyB0byBjdXJyZW50IG5vZGVcbiAgICAgICAgaWYgZWwuaW5wdXRzP1xuICAgICAgICAgIGZvciBwcmVkIGluIGVsLmlucHV0c1xuICAgICAgICAgICAgY3JlYXRlRWRnZShnLCBkYXRhLCBlbCwgZXhpc3RpbmdOb2RlcywgcHJlZClcblxuICAgICAgZ1xuXG4gICAgIyBzZWFyY2hlcyBpbiB0aGUgZ2xvYmFsIEpTT05EYXRhIGZvciB0aGUgbm9kZSB3aXRoIHRoZSBnaXZlbiBpZFxuICAgIHNlYXJjaEZvck5vZGUgPSAoZGF0YSwgbm9kZUlEKSAtPlxuICAgICAgZm9yIGkgb2YgZGF0YS5ub2Rlc1xuICAgICAgICBlbCA9IGRhdGEubm9kZXNbaV1cbiAgICAgICAgcmV0dXJuIGVsICBpZiBlbC5pZCBpcyBub2RlSURcbiAgICAgICAgXG4gICAgICAgICMgbG9vayBmb3Igbm9kZXMgdGhhdCBhcmUgaW4gaXRlcmF0aW9uc1xuICAgICAgICBpZiBlbC5zdGVwX2Z1bmN0aW9uP1xuICAgICAgICAgIGZvciBqIG9mIGVsLnN0ZXBfZnVuY3Rpb25cbiAgICAgICAgICAgIHJldHVybiBlbC5zdGVwX2Z1bmN0aW9uW2pdICBpZiBlbC5zdGVwX2Z1bmN0aW9uW2pdLmlkIGlzIG5vZGVJRFxuXG4gICAgZHJhd0dyYXBoID0gKGRhdGEpIC0+XG4gICAgICBnID0gbmV3IGRhZ3JlRDMuZ3JhcGhsaWIuR3JhcGgoeyBtdWx0aWdyYXBoOiB0cnVlLCBjb21wb3VuZDogdHJ1ZSB9KS5zZXRHcmFwaCh7XG4gICAgICAgIG5vZGVzZXA6IDcwXG4gICAgICAgIGVkZ2VzZXA6IDBcbiAgICAgICAgcmFua3NlcDogNTBcbiAgICAgICAgcmFua2RpcjogXCJMUlwiXG4gICAgICAgIG1hcmdpbng6IDQwXG4gICAgICAgIG1hcmdpbnk6IDQwXG4gICAgICAgIH0pXG5cbiAgICAgIGxvYWRKc29uVG9EYWdyZShnLCBkYXRhKVxuXG4gICAgICByZW5kZXJlciA9IG5ldyBkYWdyZUQzLnJlbmRlcigpXG4gICAgICBkM21haW5TdmdHLmNhbGwocmVuZGVyZXIsIGcpXG5cbiAgICAgIGZvciBpLCBzZyBvZiBzdWJncmFwaHNcbiAgICAgICAgZDNtYWluU3ZnLnNlbGVjdCgnc3ZnLnN2Zy0nICsgaSArICcgZycpLmNhbGwocmVuZGVyZXIsIHNnKVxuXG4gICAgICBuZXdTY2FsZSA9IDAuNVxuXG4gICAgICB4Q2VudGVyT2Zmc2V0ID0gTWF0aC5mbG9vcigoYW5ndWxhci5lbGVtZW50KG1haW5TdmdFbGVtZW50KS53aWR0aCgpIC0gZy5ncmFwaCgpLndpZHRoICogbmV3U2NhbGUpIC8gMilcbiAgICAgIHlDZW50ZXJPZmZzZXQgPSBNYXRoLmZsb29yKChhbmd1bGFyLmVsZW1lbnQobWFpblN2Z0VsZW1lbnQpLmhlaWdodCgpIC0gZy5ncmFwaCgpLmhlaWdodCAqIG5ld1NjYWxlKSAvIDIpXG5cbiAgICAgIG1haW5ab29tLnNjYWxlKG5ld1NjYWxlKS50cmFuc2xhdGUoW3hDZW50ZXJPZmZzZXQsIHlDZW50ZXJPZmZzZXRdKVxuXG4gICAgICBkM21haW5TdmdHLmF0dHIoXCJ0cmFuc2Zvcm1cIiwgXCJ0cmFuc2xhdGUoXCIgKyB4Q2VudGVyT2Zmc2V0ICsgXCIsIFwiICsgeUNlbnRlck9mZnNldCArIFwiKSBzY2FsZShcIiArIG1haW5ab29tLnNjYWxlKCkgKyBcIilcIilcblxuICAgICAgbWFpblpvb20ub24oXCJ6b29tXCIsIC0+XG4gICAgICAgIGV2ID0gZDMuZXZlbnRcbiAgICAgICAgZDNtYWluU3ZnRy5hdHRyIFwidHJhbnNmb3JtXCIsIFwidHJhbnNsYXRlKFwiICsgZXYudHJhbnNsYXRlICsgXCIpIHNjYWxlKFwiICsgZXYuc2NhbGUgKyBcIilcIlxuICAgICAgKVxuICAgICAgbWFpblpvb20oZDNtYWluU3ZnKVxuXG4gICAgICBkM21haW5TdmdHLnNlbGVjdEFsbCgnLm5vZGUnKS5vbiAnY2xpY2snLCAoZCkgLT5cbiAgICAgICAgc2NvcGUuc2V0Tm9kZSh7IG5vZGVpZDogZCB9KVxuXG4gICAgc2NvcGUuJHdhdGNoIGF0dHJzLnBsYW4sIChuZXdQbGFuKSAtPlxuICAgICAgZHJhd0dyYXBoKG5ld1BsYW4pIGlmIG5ld1BsYW5cblxuICAgIHJldHVyblxuIiwiYW5ndWxhci5tb2R1bGUoJ2ZsaW5rQXBwJykuZGlyZWN0aXZlKCd2ZXJ0ZXgnLCBmdW5jdGlvbigkc3RhdGUpIHtcbiAgcmV0dXJuIHtcbiAgICB0ZW1wbGF0ZTogXCI8c3ZnIGNsYXNzPSd0aW1lbGluZSBzZWNvbmRhcnknIHdpZHRoPScwJyBoZWlnaHQ9JzAnPjwvc3ZnPlwiLFxuICAgIHNjb3BlOiB7XG4gICAgICBkYXRhOiBcIj1cIlxuICAgIH0sXG4gICAgbGluazogZnVuY3Rpb24oc2NvcGUsIGVsZW0sIGF0dHJzKSB7XG4gICAgICB2YXIgYW5hbHl6ZVRpbWUsIGNvbnRhaW5lclcsIHN2Z0VsO1xuICAgICAgc3ZnRWwgPSBlbGVtLmNoaWxkcmVuKClbMF07XG4gICAgICBjb250YWluZXJXID0gZWxlbS53aWR0aCgpO1xuICAgICAgYW5ndWxhci5lbGVtZW50KHN2Z0VsKS5hdHRyKCd3aWR0aCcsIGNvbnRhaW5lclcpO1xuICAgICAgYW5hbHl6ZVRpbWUgPSBmdW5jdGlvbihkYXRhKSB7XG4gICAgICAgIHZhciBjaGFydCwgc3ZnLCB0ZXN0RGF0YTtcbiAgICAgICAgZDMuc2VsZWN0KHN2Z0VsKS5zZWxlY3RBbGwoXCIqXCIpLnJlbW92ZSgpO1xuICAgICAgICB0ZXN0RGF0YSA9IFtdO1xuICAgICAgICBhbmd1bGFyLmZvckVhY2goZGF0YS5zdWJ0YXNrcywgZnVuY3Rpb24oc3VidGFzaywgaSkge1xuICAgICAgICAgIHZhciB0aW1lcztcbiAgICAgICAgICB0aW1lcyA9IFtcbiAgICAgICAgICAgIHtcbiAgICAgICAgICAgICAgbGFiZWw6IFwiU2NoZWR1bGVkXCIsXG4gICAgICAgICAgICAgIGNvbG9yOiBcIiM2NjZcIixcbiAgICAgICAgICAgICAgYm9yZGVyQ29sb3I6IFwiIzU1NVwiLFxuICAgICAgICAgICAgICBzdGFydGluZ190aW1lOiBzdWJ0YXNrLnRpbWVzdGFtcHNbXCJTQ0hFRFVMRURcIl0sXG4gICAgICAgICAgICAgIGVuZGluZ190aW1lOiBzdWJ0YXNrLnRpbWVzdGFtcHNbXCJERVBMT1lJTkdcIl0sXG4gICAgICAgICAgICAgIHR5cGU6ICdyZWd1bGFyJ1xuICAgICAgICAgICAgfSwge1xuICAgICAgICAgICAgICBsYWJlbDogXCJEZXBsb3lpbmdcIixcbiAgICAgICAgICAgICAgY29sb3I6IFwiI2FhYVwiLFxuICAgICAgICAgICAgICBib3JkZXJDb2xvcjogXCIjNTU1XCIsXG4gICAgICAgICAgICAgIHN0YXJ0aW5nX3RpbWU6IHN1YnRhc2sudGltZXN0YW1wc1tcIkRFUExPWUlOR1wiXSxcbiAgICAgICAgICAgICAgZW5kaW5nX3RpbWU6IHN1YnRhc2sudGltZXN0YW1wc1tcIlJVTk5JTkdcIl0sXG4gICAgICAgICAgICAgIHR5cGU6ICdyZWd1bGFyJ1xuICAgICAgICAgICAgfVxuICAgICAgICAgIF07XG4gICAgICAgICAgaWYgKHN1YnRhc2sudGltZXN0YW1wc1tcIkZJTklTSEVEXCJdID4gMCkge1xuICAgICAgICAgICAgdGltZXMucHVzaCh7XG4gICAgICAgICAgICAgIGxhYmVsOiBcIlJ1bm5pbmdcIixcbiAgICAgICAgICAgICAgY29sb3I6IFwiI2RkZFwiLFxuICAgICAgICAgICAgICBib3JkZXJDb2xvcjogXCIjNTU1XCIsXG4gICAgICAgICAgICAgIHN0YXJ0aW5nX3RpbWU6IHN1YnRhc2sudGltZXN0YW1wc1tcIlJVTk5JTkdcIl0sXG4gICAgICAgICAgICAgIGVuZGluZ190aW1lOiBzdWJ0YXNrLnRpbWVzdGFtcHNbXCJGSU5JU0hFRFwiXSxcbiAgICAgICAgICAgICAgdHlwZTogJ3JlZ3VsYXInXG4gICAgICAgICAgICB9KTtcbiAgICAgICAgICB9XG4gICAgICAgICAgcmV0dXJuIHRlc3REYXRhLnB1c2goe1xuICAgICAgICAgICAgbGFiZWw6IFwiKFwiICsgc3VidGFzay5zdWJ0YXNrICsgXCIpIFwiICsgc3VidGFzay5ob3N0LFxuICAgICAgICAgICAgdGltZXM6IHRpbWVzXG4gICAgICAgICAgfSk7XG4gICAgICAgIH0pO1xuICAgICAgICBjaGFydCA9IGQzLnRpbWVsaW5lKCkuc3RhY2soKS50aWNrRm9ybWF0KHtcbiAgICAgICAgICBmb3JtYXQ6IGQzLnRpbWUuZm9ybWF0KFwiJUxcIiksXG4gICAgICAgICAgdGlja1NpemU6IDFcbiAgICAgICAgfSkucHJlZml4KFwic2luZ2xlXCIpLmxhYmVsRm9ybWF0KGZ1bmN0aW9uKGxhYmVsKSB7XG4gICAgICAgICAgcmV0dXJuIGxhYmVsO1xuICAgICAgICB9KS5tYXJnaW4oe1xuICAgICAgICAgIGxlZnQ6IDEwMCxcbiAgICAgICAgICByaWdodDogMCxcbiAgICAgICAgICB0b3A6IDAsXG4gICAgICAgICAgYm90dG9tOiAwXG4gICAgICAgIH0pLml0ZW1IZWlnaHQoMzApLnJlbGF0aXZlVGltZSgpO1xuICAgICAgICByZXR1cm4gc3ZnID0gZDMuc2VsZWN0KHN2Z0VsKS5kYXR1bSh0ZXN0RGF0YSkuY2FsbChjaGFydCk7XG4gICAgICB9O1xuICAgICAgYW5hbHl6ZVRpbWUoc2NvcGUuZGF0YSk7XG4gICAgfVxuICB9O1xufSkuZGlyZWN0aXZlKCd0aW1lbGluZScsIGZ1bmN0aW9uKCRzdGF0ZSkge1xuICByZXR1cm4ge1xuICAgIHRlbXBsYXRlOiBcIjxzdmcgY2xhc3M9J3RpbWVsaW5lJyB3aWR0aD0nMCcgaGVpZ2h0PScwJz48L3N2Zz5cIixcbiAgICBzY29wZToge1xuICAgICAgdmVydGljZXM6IFwiPVwiLFxuICAgICAgam9iaWQ6IFwiPVwiXG4gICAgfSxcbiAgICBsaW5rOiBmdW5jdGlvbihzY29wZSwgZWxlbSwgYXR0cnMpIHtcbiAgICAgIHZhciBhbmFseXplVGltZSwgY29udGFpbmVyVywgc3ZnRWwsIHRyYW5zbGF0ZUxhYmVsO1xuICAgICAgc3ZnRWwgPSBlbGVtLmNoaWxkcmVuKClbMF07XG4gICAgICBjb250YWluZXJXID0gZWxlbS53aWR0aCgpO1xuICAgICAgYW5ndWxhci5lbGVtZW50KHN2Z0VsKS5hdHRyKCd3aWR0aCcsIGNvbnRhaW5lclcpO1xuICAgICAgdHJhbnNsYXRlTGFiZWwgPSBmdW5jdGlvbihsYWJlbCkge1xuICAgICAgICByZXR1cm4gbGFiZWwucmVwbGFjZShcIiZndDtcIiwgXCI+XCIpO1xuICAgICAgfTtcbiAgICAgIGFuYWx5emVUaW1lID0gZnVuY3Rpb24oZGF0YSkge1xuICAgICAgICB2YXIgY2hhcnQsIHN2ZywgdGVzdERhdGE7XG4gICAgICAgIGQzLnNlbGVjdChzdmdFbCkuc2VsZWN0QWxsKFwiKlwiKS5yZW1vdmUoKTtcbiAgICAgICAgdGVzdERhdGEgPSBbXTtcbiAgICAgICAgYW5ndWxhci5mb3JFYWNoKGRhdGEsIGZ1bmN0aW9uKHZlcnRleCkge1xuICAgICAgICAgIGlmICh2ZXJ0ZXhbJ3N0YXJ0LXRpbWUnXSA+IC0xKSB7XG4gICAgICAgICAgICBpZiAodmVydGV4LnR5cGUgPT09ICdzY2hlZHVsZWQnKSB7XG4gICAgICAgICAgICAgIHJldHVybiB0ZXN0RGF0YS5wdXNoKHtcbiAgICAgICAgICAgICAgICB0aW1lczogW1xuICAgICAgICAgICAgICAgICAge1xuICAgICAgICAgICAgICAgICAgICBsYWJlbDogdHJhbnNsYXRlTGFiZWwodmVydGV4Lm5hbWUpLFxuICAgICAgICAgICAgICAgICAgICBjb2xvcjogXCIjY2NjY2NjXCIsXG4gICAgICAgICAgICAgICAgICAgIGJvcmRlckNvbG9yOiBcIiM1NTU1NTVcIixcbiAgICAgICAgICAgICAgICAgICAgc3RhcnRpbmdfdGltZTogdmVydGV4WydzdGFydC10aW1lJ10sXG4gICAgICAgICAgICAgICAgICAgIGVuZGluZ190aW1lOiB2ZXJ0ZXhbJ2VuZC10aW1lJ10sXG4gICAgICAgICAgICAgICAgICAgIHR5cGU6IHZlcnRleC50eXBlXG4gICAgICAgICAgICAgICAgICB9XG4gICAgICAgICAgICAgICAgXVxuICAgICAgICAgICAgICB9KTtcbiAgICAgICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgICAgIHJldHVybiB0ZXN0RGF0YS5wdXNoKHtcbiAgICAgICAgICAgICAgICB0aW1lczogW1xuICAgICAgICAgICAgICAgICAge1xuICAgICAgICAgICAgICAgICAgICBsYWJlbDogdHJhbnNsYXRlTGFiZWwodmVydGV4Lm5hbWUpLFxuICAgICAgICAgICAgICAgICAgICBjb2xvcjogXCIjZDlmMWY3XCIsXG4gICAgICAgICAgICAgICAgICAgIGJvcmRlckNvbG9yOiBcIiM2MmNkZWFcIixcbiAgICAgICAgICAgICAgICAgICAgc3RhcnRpbmdfdGltZTogdmVydGV4WydzdGFydC10aW1lJ10sXG4gICAgICAgICAgICAgICAgICAgIGVuZGluZ190aW1lOiB2ZXJ0ZXhbJ2VuZC10aW1lJ10sXG4gICAgICAgICAgICAgICAgICAgIGxpbms6IHZlcnRleC5pZCxcbiAgICAgICAgICAgICAgICAgICAgdHlwZTogdmVydGV4LnR5cGVcbiAgICAgICAgICAgICAgICAgIH1cbiAgICAgICAgICAgICAgICBdXG4gICAgICAgICAgICAgIH0pO1xuICAgICAgICAgICAgfVxuICAgICAgICAgIH1cbiAgICAgICAgfSk7XG4gICAgICAgIGNoYXJ0ID0gZDMudGltZWxpbmUoKS5zdGFjaygpLmNsaWNrKGZ1bmN0aW9uKGQsIGksIGRhdHVtKSB7XG4gICAgICAgICAgaWYgKGQubGluaykge1xuICAgICAgICAgICAgcmV0dXJuICRzdGF0ZS5nbyhcInNpbmdsZS1qb2IudGltZWxpbmUudmVydGV4XCIsIHtcbiAgICAgICAgICAgICAgam9iaWQ6IHNjb3BlLmpvYmlkLFxuICAgICAgICAgICAgICB2ZXJ0ZXhJZDogZC5saW5rXG4gICAgICAgICAgICB9KTtcbiAgICAgICAgICB9XG4gICAgICAgIH0pLnRpY2tGb3JtYXQoe1xuICAgICAgICAgIGZvcm1hdDogZDMudGltZS5mb3JtYXQoXCIlTFwiKSxcbiAgICAgICAgICB0aWNrU2l6ZTogMVxuICAgICAgICB9KS5wcmVmaXgoXCJtYWluXCIpLm1hcmdpbih7XG4gICAgICAgICAgbGVmdDogMCxcbiAgICAgICAgICByaWdodDogMCxcbiAgICAgICAgICB0b3A6IDAsXG4gICAgICAgICAgYm90dG9tOiAwXG4gICAgICAgIH0pLml0ZW1IZWlnaHQoMzApLnNob3dCb3JkZXJMaW5lKCkuc2hvd0hvdXJUaW1lbGluZSgpO1xuICAgICAgICByZXR1cm4gc3ZnID0gZDMuc2VsZWN0KHN2Z0VsKS5kYXR1bSh0ZXN0RGF0YSkuY2FsbChjaGFydCk7XG4gICAgICB9O1xuICAgICAgc2NvcGUuJHdhdGNoKGF0dHJzLnZlcnRpY2VzLCBmdW5jdGlvbihkYXRhKSB7XG4gICAgICAgIGlmIChkYXRhKSB7XG4gICAgICAgICAgcmV0dXJuIGFuYWx5emVUaW1lKGRhdGEpO1xuICAgICAgICB9XG4gICAgICB9KTtcbiAgICB9XG4gIH07XG59KS5kaXJlY3RpdmUoJ2pvYlBsYW4nLCBmdW5jdGlvbigkdGltZW91dCkge1xuICByZXR1cm4ge1xuICAgIHRlbXBsYXRlOiBcIjxzdmcgY2xhc3M9J2dyYXBoJyB3aWR0aD0nNTAwJyBoZWlnaHQ9JzQwMCc+PGcgLz48L3N2Zz4gPHN2ZyBjbGFzcz0ndG1wJyB3aWR0aD0nMScgaGVpZ2h0PScxJz48ZyAvPjwvc3ZnPiA8ZGl2IGNsYXNzPSdidG4tZ3JvdXAgem9vbS1idXR0b25zJz4gPGEgY2xhc3M9J2J0biBidG4tZGVmYXVsdCB6b29tLWluJyBuZy1jbGljaz0nem9vbUluKCknPjxpIGNsYXNzPSdmYSBmYS1wbHVzJyAvPjwvYT4gPGEgY2xhc3M9J2J0biBidG4tZGVmYXVsdCB6b29tLW91dCcgbmctY2xpY2s9J3pvb21PdXQoKSc+PGkgY2xhc3M9J2ZhIGZhLW1pbnVzJyAvPjwvYT4gPC9kaXY+XCIsXG4gICAgc2NvcGU6IHtcbiAgICAgIHBsYW46ICc9JyxcbiAgICAgIHNldE5vZGU6ICcmJ1xuICAgIH0sXG4gICAgbGluazogZnVuY3Rpb24oc2NvcGUsIGVsZW0sIGF0dHJzKSB7XG4gICAgICB2YXIgY29udGFpbmVyVywgY3JlYXRlRWRnZSwgY3JlYXRlTGFiZWxFZGdlLCBjcmVhdGVMYWJlbE5vZGUsIGNyZWF0ZU5vZGUsIGQzbWFpblN2ZywgZDNtYWluU3ZnRywgZDN0bXBTdmcsIGRyYXdHcmFwaCwgZXh0ZW5kTGFiZWxOb2RlRm9ySXRlcmF0aW9uLCBnLCBnZXROb2RlVHlwZSwgaXNTcGVjaWFsSXRlcmF0aW9uTm9kZSwgam9iaWQsIGxvYWRKc29uVG9EYWdyZSwgbWFpbkcsIG1haW5TdmdFbGVtZW50LCBtYWluVG1wRWxlbWVudCwgbWFpblpvb20sIHNlYXJjaEZvck5vZGUsIHNob3J0ZW5TdHJpbmcsIHN1YmdyYXBocztcbiAgICAgIGcgPSBudWxsO1xuICAgICAgbWFpblpvb20gPSBkMy5iZWhhdmlvci56b29tKCk7XG4gICAgICBzdWJncmFwaHMgPSBbXTtcbiAgICAgIGpvYmlkID0gYXR0cnMuam9iaWQ7XG4gICAgICBtYWluU3ZnRWxlbWVudCA9IGVsZW0uY2hpbGRyZW4oKVswXTtcbiAgICAgIG1haW5HID0gZWxlbS5jaGlsZHJlbigpLmNoaWxkcmVuKClbMF07XG4gICAgICBtYWluVG1wRWxlbWVudCA9IGVsZW0uY2hpbGRyZW4oKVsxXTtcbiAgICAgIGQzbWFpblN2ZyA9IGQzLnNlbGVjdChtYWluU3ZnRWxlbWVudCk7XG4gICAgICBkM21haW5TdmdHID0gZDMuc2VsZWN0KG1haW5HKTtcbiAgICAgIGQzdG1wU3ZnID0gZDMuc2VsZWN0KG1haW5UbXBFbGVtZW50KTtcbiAgICAgIGNvbnRhaW5lclcgPSBlbGVtLndpZHRoKCk7XG4gICAgICBhbmd1bGFyLmVsZW1lbnQoZWxlbS5jaGlsZHJlbigpWzBdKS53aWR0aChjb250YWluZXJXKTtcbiAgICAgIHNjb3BlLnpvb21JbiA9IGZ1bmN0aW9uKCkge1xuICAgICAgICB2YXIgdHJhbnNsYXRlLCB2MSwgdjI7XG4gICAgICAgIGlmIChtYWluWm9vbS5zY2FsZSgpIDwgMi45OSkge1xuICAgICAgICAgIHRyYW5zbGF0ZSA9IG1haW5ab29tLnRyYW5zbGF0ZSgpO1xuICAgICAgICAgIHYxID0gdHJhbnNsYXRlWzBdICogKG1haW5ab29tLnNjYWxlKCkgKyAwLjEgLyAobWFpblpvb20uc2NhbGUoKSkpO1xuICAgICAgICAgIHYyID0gdHJhbnNsYXRlWzFdICogKG1haW5ab29tLnNjYWxlKCkgKyAwLjEgLyAobWFpblpvb20uc2NhbGUoKSkpO1xuICAgICAgICAgIG1haW5ab29tLnNjYWxlKG1haW5ab29tLnNjYWxlKCkgKyAwLjEpO1xuICAgICAgICAgIG1haW5ab29tLnRyYW5zbGF0ZShbdjEsIHYyXSk7XG4gICAgICAgICAgcmV0dXJuIGQzbWFpblN2Z0cuYXR0cihcInRyYW5zZm9ybVwiLCBcInRyYW5zbGF0ZShcIiArIHYxICsgXCIsXCIgKyB2MiArIFwiKSBzY2FsZShcIiArIG1haW5ab29tLnNjYWxlKCkgKyBcIilcIik7XG4gICAgICAgIH1cbiAgICAgIH07XG4gICAgICBzY29wZS56b29tT3V0ID0gZnVuY3Rpb24oKSB7XG4gICAgICAgIHZhciB0cmFuc2xhdGUsIHYxLCB2MjtcbiAgICAgICAgaWYgKG1haW5ab29tLnNjYWxlKCkgPiAwLjMxKSB7XG4gICAgICAgICAgbWFpblpvb20uc2NhbGUobWFpblpvb20uc2NhbGUoKSAtIDAuMSk7XG4gICAgICAgICAgdHJhbnNsYXRlID0gbWFpblpvb20udHJhbnNsYXRlKCk7XG4gICAgICAgICAgdjEgPSB0cmFuc2xhdGVbMF0gKiAobWFpblpvb20uc2NhbGUoKSAtIDAuMSAvIChtYWluWm9vbS5zY2FsZSgpKSk7XG4gICAgICAgICAgdjIgPSB0cmFuc2xhdGVbMV0gKiAobWFpblpvb20uc2NhbGUoKSAtIDAuMSAvIChtYWluWm9vbS5zY2FsZSgpKSk7XG4gICAgICAgICAgbWFpblpvb20udHJhbnNsYXRlKFt2MSwgdjJdKTtcbiAgICAgICAgICByZXR1cm4gZDNtYWluU3ZnRy5hdHRyKFwidHJhbnNmb3JtXCIsIFwidHJhbnNsYXRlKFwiICsgdjEgKyBcIixcIiArIHYyICsgXCIpIHNjYWxlKFwiICsgbWFpblpvb20uc2NhbGUoKSArIFwiKVwiKTtcbiAgICAgICAgfVxuICAgICAgfTtcbiAgICAgIGNyZWF0ZUxhYmVsRWRnZSA9IGZ1bmN0aW9uKGVsKSB7XG4gICAgICAgIHZhciBsYWJlbFZhbHVlO1xuICAgICAgICBsYWJlbFZhbHVlID0gXCJcIjtcbiAgICAgICAgaWYgKChlbC5zaGlwX3N0cmF0ZWd5ICE9IG51bGwpIHx8IChlbC5sb2NhbF9zdHJhdGVneSAhPSBudWxsKSkge1xuICAgICAgICAgIGxhYmVsVmFsdWUgKz0gXCI8ZGl2IGNsYXNzPSdlZGdlLWxhYmVsJz5cIjtcbiAgICAgICAgICBpZiAoZWwuc2hpcF9zdHJhdGVneSAhPSBudWxsKSB7XG4gICAgICAgICAgICBsYWJlbFZhbHVlICs9IGVsLnNoaXBfc3RyYXRlZ3k7XG4gICAgICAgICAgfVxuICAgICAgICAgIGlmIChlbC50ZW1wX21vZGUgIT09IHVuZGVmaW5lZCkge1xuICAgICAgICAgICAgbGFiZWxWYWx1ZSArPSBcIiAoXCIgKyBlbC50ZW1wX21vZGUgKyBcIilcIjtcbiAgICAgICAgICB9XG4gICAgICAgICAgaWYgKGVsLmxvY2FsX3N0cmF0ZWd5ICE9PSB1bmRlZmluZWQpIHtcbiAgICAgICAgICAgIGxhYmVsVmFsdWUgKz0gXCIsPGJyPlwiICsgZWwubG9jYWxfc3RyYXRlZ3k7XG4gICAgICAgICAgfVxuICAgICAgICAgIGxhYmVsVmFsdWUgKz0gXCI8L2Rpdj5cIjtcbiAgICAgICAgfVxuICAgICAgICByZXR1cm4gbGFiZWxWYWx1ZTtcbiAgICAgIH07XG4gICAgICBpc1NwZWNpYWxJdGVyYXRpb25Ob2RlID0gZnVuY3Rpb24oaW5mbykge1xuICAgICAgICByZXR1cm4gaW5mbyA9PT0gXCJwYXJ0aWFsU29sdXRpb25cIiB8fCBpbmZvID09PSBcIm5leHRQYXJ0aWFsU29sdXRpb25cIiB8fCBpbmZvID09PSBcIndvcmtzZXRcIiB8fCBpbmZvID09PSBcIm5leHRXb3Jrc2V0XCIgfHwgaW5mbyA9PT0gXCJzb2x1dGlvblNldFwiIHx8IGluZm8gPT09IFwic29sdXRpb25EZWx0YVwiO1xuICAgICAgfTtcbiAgICAgIGdldE5vZGVUeXBlID0gZnVuY3Rpb24oZWwsIGluZm8pIHtcbiAgICAgICAgaWYgKGluZm8gPT09IFwibWlycm9yXCIpIHtcbiAgICAgICAgICByZXR1cm4gJ25vZGUtbWlycm9yJztcbiAgICAgICAgfSBlbHNlIGlmIChpc1NwZWNpYWxJdGVyYXRpb25Ob2RlKGluZm8pKSB7XG4gICAgICAgICAgcmV0dXJuICdub2RlLWl0ZXJhdGlvbic7XG4gICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgcmV0dXJuICdub2RlLW5vcm1hbCc7XG4gICAgICAgIH1cbiAgICAgIH07XG4gICAgICBjcmVhdGVMYWJlbE5vZGUgPSBmdW5jdGlvbihlbCwgaW5mbywgbWF4VywgbWF4SCkge1xuICAgICAgICB2YXIgbGFiZWxWYWx1ZSwgc3RlcE5hbWU7XG4gICAgICAgIGxhYmVsVmFsdWUgPSBcIjxkaXYgaHJlZj0nIy9qb2JzL1wiICsgam9iaWQgKyBcIi92ZXJ0ZXgvXCIgKyBlbC5pZCArIFwiJyBjbGFzcz0nbm9kZS1sYWJlbCBcIiArIGdldE5vZGVUeXBlKGVsLCBpbmZvKSArIFwiJz5cIjtcbiAgICAgICAgaWYgKGluZm8gPT09IFwibWlycm9yXCIpIHtcbiAgICAgICAgICBsYWJlbFZhbHVlICs9IFwiPGgzIGNsYXNzPSdub2RlLW5hbWUnPk1pcnJvciBvZiBcIiArIGVsLm9wZXJhdG9yICsgXCI8L2gzPlwiO1xuICAgICAgICB9IGVsc2Uge1xuICAgICAgICAgIGxhYmVsVmFsdWUgKz0gXCI8aDMgY2xhc3M9J25vZGUtbmFtZSc+XCIgKyBlbC5vcGVyYXRvciArIFwiPC9oMz5cIjtcbiAgICAgICAgfVxuICAgICAgICBpZiAoZWwuZGVzY3JpcHRpb24gPT09IFwiXCIpIHtcbiAgICAgICAgICBsYWJlbFZhbHVlICs9IFwiXCI7XG4gICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgc3RlcE5hbWUgPSBlbC5kZXNjcmlwdGlvbjtcbiAgICAgICAgICBzdGVwTmFtZSA9IHNob3J0ZW5TdHJpbmcoc3RlcE5hbWUpO1xuICAgICAgICAgIGxhYmVsVmFsdWUgKz0gXCI8aDQgY2xhc3M9J3N0ZXAtbmFtZSc+XCIgKyBzdGVwTmFtZSArIFwiPC9oND5cIjtcbiAgICAgICAgfVxuICAgICAgICBpZiAoZWwuc3RlcF9mdW5jdGlvbiAhPSBudWxsKSB7XG4gICAgICAgICAgbGFiZWxWYWx1ZSArPSBleHRlbmRMYWJlbE5vZGVGb3JJdGVyYXRpb24oZWwuaWQsIG1heFcsIG1heEgpO1xuICAgICAgICB9IGVsc2Uge1xuICAgICAgICAgIGlmIChpc1NwZWNpYWxJdGVyYXRpb25Ob2RlKGluZm8pKSB7XG4gICAgICAgICAgICBsYWJlbFZhbHVlICs9IFwiPGg1PlwiICsgaW5mbyArIFwiIE5vZGU8L2g1PlwiO1xuICAgICAgICAgIH1cbiAgICAgICAgICBpZiAoZWwucGFyYWxsZWxpc20gIT09IFwiXCIpIHtcbiAgICAgICAgICAgIGxhYmVsVmFsdWUgKz0gXCI8aDU+UGFyYWxsZWxpc206IFwiICsgZWwucGFyYWxsZWxpc20gKyBcIjwvaDU+XCI7XG4gICAgICAgICAgfVxuICAgICAgICAgIGlmIChlbC5vcGVyYXRvciAhPT0gdW5kZWZpbmVkKSB7XG4gICAgICAgICAgICBsYWJlbFZhbHVlICs9IFwiPGg1Pk9wZXJhdGlvbjogXCIgKyBzaG9ydGVuU3RyaW5nKGVsLm9wZXJhdG9yX3N0cmF0ZWd5KSArIFwiPC9oNT5cIjtcbiAgICAgICAgICB9XG4gICAgICAgIH1cbiAgICAgICAgbGFiZWxWYWx1ZSArPSBcIjwvZGl2PlwiO1xuICAgICAgICByZXR1cm4gbGFiZWxWYWx1ZTtcbiAgICAgIH07XG4gICAgICBleHRlbmRMYWJlbE5vZGVGb3JJdGVyYXRpb24gPSBmdW5jdGlvbihpZCwgbWF4VywgbWF4SCkge1xuICAgICAgICB2YXIgbGFiZWxWYWx1ZSwgc3ZnSUQ7XG4gICAgICAgIHN2Z0lEID0gXCJzdmctXCIgKyBpZDtcbiAgICAgICAgbGFiZWxWYWx1ZSA9IFwiPHN2ZyBjbGFzcz0nXCIgKyBzdmdJRCArIFwiJyB3aWR0aD1cIiArIG1heFcgKyBcIiBoZWlnaHQ9XCIgKyBtYXhIICsgXCI+PGcgLz48L3N2Zz5cIjtcbiAgICAgICAgcmV0dXJuIGxhYmVsVmFsdWU7XG4gICAgICB9O1xuICAgICAgc2hvcnRlblN0cmluZyA9IGZ1bmN0aW9uKHMpIHtcbiAgICAgICAgdmFyIHNicjtcbiAgICAgICAgaWYgKHMuY2hhckF0KDApID09PSBcIjxcIikge1xuICAgICAgICAgIHMgPSBzLnJlcGxhY2UoXCI8XCIsIFwiJmx0O1wiKTtcbiAgICAgICAgICBzID0gcy5yZXBsYWNlKFwiPlwiLCBcIiZndDtcIik7XG4gICAgICAgIH1cbiAgICAgICAgc2JyID0gXCJcIjtcbiAgICAgICAgd2hpbGUgKHMubGVuZ3RoID4gMzApIHtcbiAgICAgICAgICBzYnIgPSBzYnIgKyBzLnN1YnN0cmluZygwLCAzMCkgKyBcIjxicj5cIjtcbiAgICAgICAgICBzID0gcy5zdWJzdHJpbmcoMzAsIHMubGVuZ3RoKTtcbiAgICAgICAgfVxuICAgICAgICBzYnIgPSBzYnIgKyBzO1xuICAgICAgICByZXR1cm4gc2JyO1xuICAgICAgfTtcbiAgICAgIGNyZWF0ZU5vZGUgPSBmdW5jdGlvbihnLCBkYXRhLCBlbCwgaXNQYXJlbnQsIG1heFcsIG1heEgpIHtcbiAgICAgICAgaWYgKGlzUGFyZW50ID09IG51bGwpIHtcbiAgICAgICAgICBpc1BhcmVudCA9IGZhbHNlO1xuICAgICAgICB9XG4gICAgICAgIGlmIChlbC5pZCA9PT0gZGF0YS5wYXJ0aWFsX3NvbHV0aW9uKSB7XG4gICAgICAgICAgcmV0dXJuIGcuc2V0Tm9kZShlbC5pZCwge1xuICAgICAgICAgICAgbGFiZWw6IGNyZWF0ZUxhYmVsTm9kZShlbCwgXCJwYXJ0aWFsU29sdXRpb25cIiwgbWF4VywgbWF4SCksXG4gICAgICAgICAgICBsYWJlbFR5cGU6ICdodG1sJyxcbiAgICAgICAgICAgIFwiY2xhc3NcIjogZ2V0Tm9kZVR5cGUoZWwsIFwicGFydGlhbFNvbHV0aW9uXCIpXG4gICAgICAgICAgfSk7XG4gICAgICAgIH0gZWxzZSBpZiAoZWwuaWQgPT09IGRhdGEubmV4dF9wYXJ0aWFsX3NvbHV0aW9uKSB7XG4gICAgICAgICAgcmV0dXJuIGcuc2V0Tm9kZShlbC5pZCwge1xuICAgICAgICAgICAgbGFiZWw6IGNyZWF0ZUxhYmVsTm9kZShlbCwgXCJuZXh0UGFydGlhbFNvbHV0aW9uXCIsIG1heFcsIG1heEgpLFxuICAgICAgICAgICAgbGFiZWxUeXBlOiAnaHRtbCcsXG4gICAgICAgICAgICBcImNsYXNzXCI6IGdldE5vZGVUeXBlKGVsLCBcIm5leHRQYXJ0aWFsU29sdXRpb25cIilcbiAgICAgICAgICB9KTtcbiAgICAgICAgfSBlbHNlIGlmIChlbC5pZCA9PT0gZGF0YS53b3Jrc2V0KSB7XG4gICAgICAgICAgcmV0dXJuIGcuc2V0Tm9kZShlbC5pZCwge1xuICAgICAgICAgICAgbGFiZWw6IGNyZWF0ZUxhYmVsTm9kZShlbCwgXCJ3b3Jrc2V0XCIsIG1heFcsIG1heEgpLFxuICAgICAgICAgICAgbGFiZWxUeXBlOiAnaHRtbCcsXG4gICAgICAgICAgICBcImNsYXNzXCI6IGdldE5vZGVUeXBlKGVsLCBcIndvcmtzZXRcIilcbiAgICAgICAgICB9KTtcbiAgICAgICAgfSBlbHNlIGlmIChlbC5pZCA9PT0gZGF0YS5uZXh0X3dvcmtzZXQpIHtcbiAgICAgICAgICByZXR1cm4gZy5zZXROb2RlKGVsLmlkLCB7XG4gICAgICAgICAgICBsYWJlbDogY3JlYXRlTGFiZWxOb2RlKGVsLCBcIm5leHRXb3Jrc2V0XCIsIG1heFcsIG1heEgpLFxuICAgICAgICAgICAgbGFiZWxUeXBlOiAnaHRtbCcsXG4gICAgICAgICAgICBcImNsYXNzXCI6IGdldE5vZGVUeXBlKGVsLCBcIm5leHRXb3Jrc2V0XCIpXG4gICAgICAgICAgfSk7XG4gICAgICAgIH0gZWxzZSBpZiAoZWwuaWQgPT09IGRhdGEuc29sdXRpb25fc2V0KSB7XG4gICAgICAgICAgcmV0dXJuIGcuc2V0Tm9kZShlbC5pZCwge1xuICAgICAgICAgICAgbGFiZWw6IGNyZWF0ZUxhYmVsTm9kZShlbCwgXCJzb2x1dGlvblNldFwiLCBtYXhXLCBtYXhIKSxcbiAgICAgICAgICAgIGxhYmVsVHlwZTogJ2h0bWwnLFxuICAgICAgICAgICAgXCJjbGFzc1wiOiBnZXROb2RlVHlwZShlbCwgXCJzb2x1dGlvblNldFwiKVxuICAgICAgICAgIH0pO1xuICAgICAgICB9IGVsc2UgaWYgKGVsLmlkID09PSBkYXRhLnNvbHV0aW9uX2RlbHRhKSB7XG4gICAgICAgICAgcmV0dXJuIGcuc2V0Tm9kZShlbC5pZCwge1xuICAgICAgICAgICAgbGFiZWw6IGNyZWF0ZUxhYmVsTm9kZShlbCwgXCJzb2x1dGlvbkRlbHRhXCIsIG1heFcsIG1heEgpLFxuICAgICAgICAgICAgbGFiZWxUeXBlOiAnaHRtbCcsXG4gICAgICAgICAgICBcImNsYXNzXCI6IGdldE5vZGVUeXBlKGVsLCBcInNvbHV0aW9uRGVsdGFcIilcbiAgICAgICAgICB9KTtcbiAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICByZXR1cm4gZy5zZXROb2RlKGVsLmlkLCB7XG4gICAgICAgICAgICBsYWJlbDogY3JlYXRlTGFiZWxOb2RlKGVsLCBcIlwiLCBtYXhXLCBtYXhIKSxcbiAgICAgICAgICAgIGxhYmVsVHlwZTogJ2h0bWwnLFxuICAgICAgICAgICAgXCJjbGFzc1wiOiBnZXROb2RlVHlwZShlbCwgXCJcIilcbiAgICAgICAgICB9KTtcbiAgICAgICAgfVxuICAgICAgfTtcbiAgICAgIGNyZWF0ZUVkZ2UgPSBmdW5jdGlvbihnLCBkYXRhLCBlbCwgZXhpc3RpbmdOb2RlcywgcHJlZCkge1xuICAgICAgICB2YXIgbWlzc2luZ05vZGU7XG4gICAgICAgIGlmIChleGlzdGluZ05vZGVzLmluZGV4T2YocHJlZC5pZCkgIT09IC0xKSB7XG4gICAgICAgICAgcmV0dXJuIGcuc2V0RWRnZShwcmVkLmlkLCBlbC5pZCwge1xuICAgICAgICAgICAgbGFiZWw6IGNyZWF0ZUxhYmVsRWRnZShwcmVkKSxcbiAgICAgICAgICAgIGxhYmVsVHlwZTogJ2h0bWwnLFxuICAgICAgICAgICAgYXJyb3doZWFkOiAnbm9ybWFsJ1xuICAgICAgICAgIH0pO1xuICAgICAgICB9IGVsc2Uge1xuICAgICAgICAgIG1pc3NpbmdOb2RlID0gc2VhcmNoRm9yTm9kZShkYXRhLCBwcmVkLmlkKTtcbiAgICAgICAgICBpZiAoISFtaXNzaW5nTm9kZSkge1xuICAgICAgICAgICAgcmV0dXJuIGcuc2V0RWRnZShtaXNzaW5nTm9kZS5pZCwgZWwuaWQsIHtcbiAgICAgICAgICAgICAgbGFiZWw6IGNyZWF0ZUxhYmVsRWRnZShtaXNzaW5nTm9kZSksXG4gICAgICAgICAgICAgIGxhYmVsVHlwZTogJ2h0bWwnXG4gICAgICAgICAgICB9KTtcbiAgICAgICAgICB9XG4gICAgICAgIH1cbiAgICAgIH07XG4gICAgICBsb2FkSnNvblRvRGFncmUgPSBmdW5jdGlvbihnLCBkYXRhKSB7XG4gICAgICAgIHZhciBlbCwgZXhpc3RpbmdOb2RlcywgaXNQYXJlbnQsIGssIGwsIGxlbiwgbGVuMSwgbWF4SCwgbWF4VywgcHJlZCwgciwgcmVmLCBzZywgdG9JdGVyYXRlO1xuICAgICAgICBleGlzdGluZ05vZGVzID0gW107XG4gICAgICAgIGlmIChkYXRhLm5vZGVzICE9IG51bGwpIHtcbiAgICAgICAgICB0b0l0ZXJhdGUgPSBkYXRhLm5vZGVzO1xuICAgICAgICB9IGVsc2Uge1xuICAgICAgICAgIHRvSXRlcmF0ZSA9IGRhdGEuc3RlcF9mdW5jdGlvbjtcbiAgICAgICAgICBpc1BhcmVudCA9IHRydWU7XG4gICAgICAgIH1cbiAgICAgICAgZm9yIChrID0gMCwgbGVuID0gdG9JdGVyYXRlLmxlbmd0aDsgayA8IGxlbjsgaysrKSB7XG4gICAgICAgICAgZWwgPSB0b0l0ZXJhdGVba107XG4gICAgICAgICAgbWF4VyA9IDA7XG4gICAgICAgICAgbWF4SCA9IDA7XG4gICAgICAgICAgaWYgKGVsLnN0ZXBfZnVuY3Rpb24pIHtcbiAgICAgICAgICAgIHNnID0gbmV3IGRhZ3JlRDMuZ3JhcGhsaWIuR3JhcGgoe1xuICAgICAgICAgICAgICBtdWx0aWdyYXBoOiB0cnVlLFxuICAgICAgICAgICAgICBjb21wb3VuZDogdHJ1ZVxuICAgICAgICAgICAgfSkuc2V0R3JhcGgoe1xuICAgICAgICAgICAgICBub2Rlc2VwOiAyMCxcbiAgICAgICAgICAgICAgZWRnZXNlcDogMCxcbiAgICAgICAgICAgICAgcmFua3NlcDogMjAsXG4gICAgICAgICAgICAgIHJhbmtkaXI6IFwiTFJcIixcbiAgICAgICAgICAgICAgbWFyZ2lueDogMTAsXG4gICAgICAgICAgICAgIG1hcmdpbnk6IDEwXG4gICAgICAgICAgICB9KTtcbiAgICAgICAgICAgIHN1YmdyYXBoc1tlbC5pZF0gPSBzZztcbiAgICAgICAgICAgIGxvYWRKc29uVG9EYWdyZShzZywgZWwpO1xuICAgICAgICAgICAgciA9IG5ldyBkYWdyZUQzLnJlbmRlcigpO1xuICAgICAgICAgICAgZDN0bXBTdmcuc2VsZWN0KCdnJykuY2FsbChyLCBzZyk7XG4gICAgICAgICAgICBtYXhXID0gc2cuZ3JhcGgoKS53aWR0aDtcbiAgICAgICAgICAgIG1heEggPSBzZy5ncmFwaCgpLmhlaWdodDtcbiAgICAgICAgICAgIGFuZ3VsYXIuZWxlbWVudChtYWluVG1wRWxlbWVudCkuZW1wdHkoKTtcbiAgICAgICAgICB9XG4gICAgICAgICAgY3JlYXRlTm9kZShnLCBkYXRhLCBlbCwgaXNQYXJlbnQsIG1heFcsIG1heEgpO1xuICAgICAgICAgIGV4aXN0aW5nTm9kZXMucHVzaChlbC5pZCk7XG4gICAgICAgICAgaWYgKGVsLmlucHV0cyAhPSBudWxsKSB7XG4gICAgICAgICAgICByZWYgPSBlbC5pbnB1dHM7XG4gICAgICAgICAgICBmb3IgKGwgPSAwLCBsZW4xID0gcmVmLmxlbmd0aDsgbCA8IGxlbjE7IGwrKykge1xuICAgICAgICAgICAgICBwcmVkID0gcmVmW2xdO1xuICAgICAgICAgICAgICBjcmVhdGVFZGdlKGcsIGRhdGEsIGVsLCBleGlzdGluZ05vZGVzLCBwcmVkKTtcbiAgICAgICAgICAgIH1cbiAgICAgICAgICB9XG4gICAgICAgIH1cbiAgICAgICAgcmV0dXJuIGc7XG4gICAgICB9O1xuICAgICAgc2VhcmNoRm9yTm9kZSA9IGZ1bmN0aW9uKGRhdGEsIG5vZGVJRCkge1xuICAgICAgICB2YXIgZWwsIGksIGo7XG4gICAgICAgIGZvciAoaSBpbiBkYXRhLm5vZGVzKSB7XG4gICAgICAgICAgZWwgPSBkYXRhLm5vZGVzW2ldO1xuICAgICAgICAgIGlmIChlbC5pZCA9PT0gbm9kZUlEKSB7XG4gICAgICAgICAgICByZXR1cm4gZWw7XG4gICAgICAgICAgfVxuICAgICAgICAgIGlmIChlbC5zdGVwX2Z1bmN0aW9uICE9IG51bGwpIHtcbiAgICAgICAgICAgIGZvciAoaiBpbiBlbC5zdGVwX2Z1bmN0aW9uKSB7XG4gICAgICAgICAgICAgIGlmIChlbC5zdGVwX2Z1bmN0aW9uW2pdLmlkID09PSBub2RlSUQpIHtcbiAgICAgICAgICAgICAgICByZXR1cm4gZWwuc3RlcF9mdW5jdGlvbltqXTtcbiAgICAgICAgICAgICAgfVxuICAgICAgICAgICAgfVxuICAgICAgICAgIH1cbiAgICAgICAgfVxuICAgICAgfTtcbiAgICAgIGRyYXdHcmFwaCA9IGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgICAgdmFyIGksIG5ld1NjYWxlLCByZW5kZXJlciwgc2csIHhDZW50ZXJPZmZzZXQsIHlDZW50ZXJPZmZzZXQ7XG4gICAgICAgIGcgPSBuZXcgZGFncmVEMy5ncmFwaGxpYi5HcmFwaCh7XG4gICAgICAgICAgbXVsdGlncmFwaDogdHJ1ZSxcbiAgICAgICAgICBjb21wb3VuZDogdHJ1ZVxuICAgICAgICB9KS5zZXRHcmFwaCh7XG4gICAgICAgICAgbm9kZXNlcDogNzAsXG4gICAgICAgICAgZWRnZXNlcDogMCxcbiAgICAgICAgICByYW5rc2VwOiA1MCxcbiAgICAgICAgICByYW5rZGlyOiBcIkxSXCIsXG4gICAgICAgICAgbWFyZ2lueDogNDAsXG4gICAgICAgICAgbWFyZ2lueTogNDBcbiAgICAgICAgfSk7XG4gICAgICAgIGxvYWRKc29uVG9EYWdyZShnLCBkYXRhKTtcbiAgICAgICAgcmVuZGVyZXIgPSBuZXcgZGFncmVEMy5yZW5kZXIoKTtcbiAgICAgICAgZDNtYWluU3ZnRy5jYWxsKHJlbmRlcmVyLCBnKTtcbiAgICAgICAgZm9yIChpIGluIHN1YmdyYXBocykge1xuICAgICAgICAgIHNnID0gc3ViZ3JhcGhzW2ldO1xuICAgICAgICAgIGQzbWFpblN2Zy5zZWxlY3QoJ3N2Zy5zdmctJyArIGkgKyAnIGcnKS5jYWxsKHJlbmRlcmVyLCBzZyk7XG4gICAgICAgIH1cbiAgICAgICAgbmV3U2NhbGUgPSAwLjU7XG4gICAgICAgIHhDZW50ZXJPZmZzZXQgPSBNYXRoLmZsb29yKChhbmd1bGFyLmVsZW1lbnQobWFpblN2Z0VsZW1lbnQpLndpZHRoKCkgLSBnLmdyYXBoKCkud2lkdGggKiBuZXdTY2FsZSkgLyAyKTtcbiAgICAgICAgeUNlbnRlck9mZnNldCA9IE1hdGguZmxvb3IoKGFuZ3VsYXIuZWxlbWVudChtYWluU3ZnRWxlbWVudCkuaGVpZ2h0KCkgLSBnLmdyYXBoKCkuaGVpZ2h0ICogbmV3U2NhbGUpIC8gMik7XG4gICAgICAgIG1haW5ab29tLnNjYWxlKG5ld1NjYWxlKS50cmFuc2xhdGUoW3hDZW50ZXJPZmZzZXQsIHlDZW50ZXJPZmZzZXRdKTtcbiAgICAgICAgZDNtYWluU3ZnRy5hdHRyKFwidHJhbnNmb3JtXCIsIFwidHJhbnNsYXRlKFwiICsgeENlbnRlck9mZnNldCArIFwiLCBcIiArIHlDZW50ZXJPZmZzZXQgKyBcIikgc2NhbGUoXCIgKyBtYWluWm9vbS5zY2FsZSgpICsgXCIpXCIpO1xuICAgICAgICBtYWluWm9vbS5vbihcInpvb21cIiwgZnVuY3Rpb24oKSB7XG4gICAgICAgICAgdmFyIGV2O1xuICAgICAgICAgIGV2ID0gZDMuZXZlbnQ7XG4gICAgICAgICAgcmV0dXJuIGQzbWFpblN2Z0cuYXR0cihcInRyYW5zZm9ybVwiLCBcInRyYW5zbGF0ZShcIiArIGV2LnRyYW5zbGF0ZSArIFwiKSBzY2FsZShcIiArIGV2LnNjYWxlICsgXCIpXCIpO1xuICAgICAgICB9KTtcbiAgICAgICAgbWFpblpvb20oZDNtYWluU3ZnKTtcbiAgICAgICAgcmV0dXJuIGQzbWFpblN2Z0cuc2VsZWN0QWxsKCcubm9kZScpLm9uKCdjbGljaycsIGZ1bmN0aW9uKGQpIHtcbiAgICAgICAgICByZXR1cm4gc2NvcGUuc2V0Tm9kZSh7XG4gICAgICAgICAgICBub2RlaWQ6IGRcbiAgICAgICAgICB9KTtcbiAgICAgICAgfSk7XG4gICAgICB9O1xuICAgICAgc2NvcGUuJHdhdGNoKGF0dHJzLnBsYW4sIGZ1bmN0aW9uKG5ld1BsYW4pIHtcbiAgICAgICAgaWYgKG5ld1BsYW4pIHtcbiAgICAgICAgICByZXR1cm4gZHJhd0dyYXBoKG5ld1BsYW4pO1xuICAgICAgICB9XG4gICAgICB9KTtcbiAgICB9XG4gIH07XG59KTtcbiIsIiNcbiMgTGljZW5zZWQgdG8gdGhlIEFwYWNoZSBTb2Z0d2FyZSBGb3VuZGF0aW9uIChBU0YpIHVuZGVyIG9uZVxuIyBvciBtb3JlIGNvbnRyaWJ1dG9yIGxpY2Vuc2UgYWdyZWVtZW50cy4gIFNlZSB0aGUgTk9USUNFIGZpbGVcbiMgZGlzdHJpYnV0ZWQgd2l0aCB0aGlzIHdvcmsgZm9yIGFkZGl0aW9uYWwgaW5mb3JtYXRpb25cbiMgcmVnYXJkaW5nIGNvcHlyaWdodCBvd25lcnNoaXAuICBUaGUgQVNGIGxpY2Vuc2VzIHRoaXMgZmlsZVxuIyB0byB5b3UgdW5kZXIgdGhlIEFwYWNoZSBMaWNlbnNlLCBWZXJzaW9uIDIuMCAodGhlXG4jIFwiTGljZW5zZVwiKTsgeW91IG1heSBub3QgdXNlIHRoaXMgZmlsZSBleGNlcHQgaW4gY29tcGxpYW5jZVxuIyB3aXRoIHRoZSBMaWNlbnNlLiAgWW91IG1heSBvYnRhaW4gYSBjb3B5IG9mIHRoZSBMaWNlbnNlIGF0XG4jXG4jICAgICBodHRwOi8vd3d3LmFwYWNoZS5vcmcvbGljZW5zZXMvTElDRU5TRS0yLjBcbiNcbiMgVW5sZXNzIHJlcXVpcmVkIGJ5IGFwcGxpY2FibGUgbGF3IG9yIGFncmVlZCB0byBpbiB3cml0aW5nLCBzb2Z0d2FyZVxuIyBkaXN0cmlidXRlZCB1bmRlciB0aGUgTGljZW5zZSBpcyBkaXN0cmlidXRlZCBvbiBhbiBcIkFTIElTXCIgQkFTSVMsXG4jIFdJVEhPVVQgV0FSUkFOVElFUyBPUiBDT05ESVRJT05TIE9GIEFOWSBLSU5ELCBlaXRoZXIgZXhwcmVzcyBvciBpbXBsaWVkLlxuIyBTZWUgdGhlIExpY2Vuc2UgZm9yIHRoZSBzcGVjaWZpYyBsYW5ndWFnZSBnb3Zlcm5pbmcgcGVybWlzc2lvbnMgYW5kXG4jIGxpbWl0YXRpb25zIHVuZGVyIHRoZSBMaWNlbnNlLlxuI1xuXG5hbmd1bGFyLm1vZHVsZSgnZmxpbmtBcHAnKVxuXG4uc2VydmljZSAnSm9ic1NlcnZpY2UnLCAoJGh0dHAsIGZsaW5rQ29uZmlnLCAkbG9nLCBhbU1vbWVudCwgJHEsICR0aW1lb3V0KSAtPlxuICBjdXJyZW50Sm9iID0gbnVsbFxuICBjdXJyZW50UGxhbiA9IG51bGxcblxuICBkZWZlcnJlZHMgPSB7fVxuICBqb2JzID0ge1xuICAgIHJ1bm5pbmc6IFtdXG4gICAgZmluaXNoZWQ6IFtdXG4gICAgY2FuY2VsbGVkOiBbXVxuICAgIGZhaWxlZDogW11cbiAgfVxuXG4gIGpvYk9ic2VydmVycyA9IFtdXG5cbiAgbm90aWZ5T2JzZXJ2ZXJzID0gLT5cbiAgICBhbmd1bGFyLmZvckVhY2ggam9iT2JzZXJ2ZXJzLCAoY2FsbGJhY2spIC0+XG4gICAgICBjYWxsYmFjaygpXG5cbiAgQHJlZ2lzdGVyT2JzZXJ2ZXIgPSAoY2FsbGJhY2spIC0+XG4gICAgam9iT2JzZXJ2ZXJzLnB1c2goY2FsbGJhY2spXG5cbiAgQHVuUmVnaXN0ZXJPYnNlcnZlciA9IChjYWxsYmFjaykgLT5cbiAgICBpbmRleCA9IGpvYk9ic2VydmVycy5pbmRleE9mKGNhbGxiYWNrKVxuICAgIGpvYk9ic2VydmVycy5zcGxpY2UoaW5kZXgsIDEpXG5cbiAgQHN0YXRlTGlzdCA9IC0+XG4gICAgWyBcbiAgICAgICMgJ0NSRUFURUQnXG4gICAgICAnU0NIRURVTEVEJ1xuICAgICAgJ0RFUExPWUlORydcbiAgICAgICdSVU5OSU5HJ1xuICAgICAgJ0ZJTklTSEVEJ1xuICAgICAgJ0ZBSUxFRCdcbiAgICAgICdDQU5DRUxJTkcnXG4gICAgICAnQ0FOQ0VMRUQnXG4gICAgXVxuXG4gIEB0cmFuc2xhdGVMYWJlbFN0YXRlID0gKHN0YXRlKSAtPlxuICAgIHN3aXRjaCBzdGF0ZS50b0xvd2VyQ2FzZSgpXG4gICAgICB3aGVuICdmaW5pc2hlZCcgdGhlbiAnc3VjY2VzcydcbiAgICAgIHdoZW4gJ2ZhaWxlZCcgdGhlbiAnZGFuZ2VyJ1xuICAgICAgd2hlbiAnc2NoZWR1bGVkJyB0aGVuICdkZWZhdWx0J1xuICAgICAgd2hlbiAnZGVwbG95aW5nJyB0aGVuICdpbmZvJ1xuICAgICAgd2hlbiAncnVubmluZycgdGhlbiAncHJpbWFyeSdcbiAgICAgIHdoZW4gJ2NhbmNlbGluZycgdGhlbiAnd2FybmluZydcbiAgICAgIHdoZW4gJ3BlbmRpbmcnIHRoZW4gJ2luZm8nXG4gICAgICB3aGVuICd0b3RhbCcgdGhlbiAnYmxhY2snXG4gICAgICBlbHNlICdkZWZhdWx0J1xuXG4gIEBzZXRFbmRUaW1lcyA9IChsaXN0KSAtPlxuICAgIGFuZ3VsYXIuZm9yRWFjaCBsaXN0LCAoaXRlbSwgam9iS2V5KSAtPlxuICAgICAgdW5sZXNzIGl0ZW1bJ2VuZC10aW1lJ10gPiAtMVxuICAgICAgICBpdGVtWydlbmQtdGltZSddID0gaXRlbVsnc3RhcnQtdGltZSddICsgaXRlbVsnZHVyYXRpb24nXVxuXG4gIEBwcm9jZXNzVmVydGljZXMgPSAoZGF0YSkgLT5cbiAgICBhbmd1bGFyLmZvckVhY2ggZGF0YS52ZXJ0aWNlcywgKHZlcnRleCwgaSkgLT5cbiAgICAgIHZlcnRleC50eXBlID0gJ3JlZ3VsYXInXG5cbiAgICBkYXRhLnZlcnRpY2VzLnVuc2hpZnQoe1xuICAgICAgbmFtZTogJ1NjaGVkdWxlZCdcbiAgICAgICdzdGFydC10aW1lJzogZGF0YS50aW1lc3RhbXBzWydDUkVBVEVEJ11cbiAgICAgICdlbmQtdGltZSc6IGRhdGEudGltZXN0YW1wc1snQ1JFQVRFRCddICsgMVxuICAgICAgdHlwZTogJ3NjaGVkdWxlZCdcbiAgICB9KVxuXG4gIEBsaXN0Sm9icyA9IC0+XG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpXG5cbiAgICAkaHR0cC5nZXQgXCJqb2JvdmVydmlld1wiXG4gICAgLnN1Y2Nlc3MgKGRhdGEsIHN0YXR1cywgaGVhZGVycywgY29uZmlnKSA9PlxuICAgICAgYW5ndWxhci5mb3JFYWNoIGRhdGEsIChsaXN0LCBsaXN0S2V5KSA9PlxuICAgICAgICBzd2l0Y2ggbGlzdEtleVxuICAgICAgICAgIHdoZW4gJ3J1bm5pbmcnIHRoZW4gam9icy5ydW5uaW5nID0gQHNldEVuZFRpbWVzKGxpc3QpXG4gICAgICAgICAgd2hlbiAnZmluaXNoZWQnIHRoZW4gam9icy5maW5pc2hlZCA9IEBzZXRFbmRUaW1lcyhsaXN0KVxuICAgICAgICAgIHdoZW4gJ2NhbmNlbGxlZCcgdGhlbiBqb2JzLmNhbmNlbGxlZCA9IEBzZXRFbmRUaW1lcyhsaXN0KVxuICAgICAgICAgIHdoZW4gJ2ZhaWxlZCcgdGhlbiBqb2JzLmZhaWxlZCA9IEBzZXRFbmRUaW1lcyhsaXN0KVxuXG4gICAgICBkZWZlcnJlZC5yZXNvbHZlKGpvYnMpXG4gICAgICBub3RpZnlPYnNlcnZlcnMoKVxuXG4gICAgZGVmZXJyZWQucHJvbWlzZVxuXG4gIEBnZXRKb2JzID0gKHR5cGUpIC0+XG4gICAgam9ic1t0eXBlXVxuXG4gIEBnZXRBbGxKb2JzID0gLT5cbiAgICBqb2JzXG5cbiAgQGxvYWRKb2IgPSAoam9iaWQpIC0+XG4gICAgY3VycmVudEpvYiA9IG51bGxcbiAgICBkZWZlcnJlZHMuam9iID0gJHEuZGVmZXIoKVxuXG4gICAgJGh0dHAuZ2V0IFwiam9icy9cIiArIGpvYmlkXG4gICAgLnN1Y2Nlc3MgKGRhdGEsIHN0YXR1cywgaGVhZGVycywgY29uZmlnKSA9PlxuICAgICAgQHNldEVuZFRpbWVzKGRhdGEudmVydGljZXMpXG4gICAgICBAcHJvY2Vzc1ZlcnRpY2VzKGRhdGEpXG5cbiAgICAgICRodHRwLmdldCBcImpvYnMvXCIgKyBqb2JpZCArIFwiL2NvbmZpZ1wiXG4gICAgICAuc3VjY2VzcyAoam9iQ29uZmlnKSAtPlxuICAgICAgICBkYXRhID0gYW5ndWxhci5leHRlbmQoZGF0YSwgam9iQ29uZmlnKVxuXG4gICAgICAgIGN1cnJlbnRKb2IgPSBkYXRhXG5cbiAgICAgICAgZGVmZXJyZWRzLmpvYi5yZXNvbHZlKGN1cnJlbnRKb2IpXG5cbiAgICBkZWZlcnJlZHMuam9iLnByb21pc2VcblxuICBAZ2V0Tm9kZSA9IChub2RlaWQpIC0+XG4gICAgc2Vla05vZGUgPSAobm9kZWlkLCBkYXRhKSAtPlxuICAgICAgZm9yIG5vZGUgaW4gZGF0YVxuICAgICAgICByZXR1cm4gbm9kZSBpZiBub2RlLmlkIGlzIG5vZGVpZFxuICAgICAgICBzdWIgPSBzZWVrTm9kZShub2RlaWQsIG5vZGUuc3RlcF9mdW5jdGlvbikgaWYgbm9kZS5zdGVwX2Z1bmN0aW9uXG4gICAgICAgIHJldHVybiBzdWIgaWYgc3ViXG5cbiAgICAgIG51bGxcblxuICAgIGRlZmVycmVkID0gJHEuZGVmZXIoKVxuXG4gICAgZGVmZXJyZWRzLmpvYi5wcm9taXNlLnRoZW4gKGRhdGEpID0+XG4gICAgICBmb3VuZE5vZGUgPSBzZWVrTm9kZShub2RlaWQsIGN1cnJlbnRKb2IucGxhbi5ub2RlcylcblxuICAgICAgZm91bmROb2RlLnZlcnRleCA9IEBzZWVrVmVydGV4KG5vZGVpZClcblxuICAgICAgZGVmZXJyZWQucmVzb2x2ZShmb3VuZE5vZGUpXG5cbiAgICBkZWZlcnJlZC5wcm9taXNlXG5cbiAgQHNlZWtWZXJ0ZXggPSAobm9kZWlkKSAtPlxuICAgIGZvciB2ZXJ0ZXggaW4gY3VycmVudEpvYi52ZXJ0aWNlc1xuICAgICAgcmV0dXJuIHZlcnRleCBpZiB2ZXJ0ZXguaWQgaXMgbm9kZWlkXG5cbiAgICByZXR1cm4gbnVsbFxuXG4gIEBnZXRWZXJ0ZXggPSAodmVydGV4aWQpIC0+XG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpXG5cbiAgICBkZWZlcnJlZHMuam9iLnByb21pc2UudGhlbiAoZGF0YSkgPT5cbiAgICAgIHZlcnRleCA9IEBzZWVrVmVydGV4KHZlcnRleGlkKVxuXG4gICAgICAkaHR0cC5nZXQgXCJqb2JzL1wiICsgY3VycmVudEpvYi5qaWQgKyBcIi92ZXJ0aWNlcy9cIiArIHZlcnRleGlkICsgXCIvc3VidGFza3RpbWVzXCJcbiAgICAgIC5zdWNjZXNzIChkYXRhKSA9PlxuICAgICAgICAjIFRPRE86IGNoYW5nZSB0byBzdWJ0YXNrdGltZXNcbiAgICAgICAgdmVydGV4LnN1YnRhc2tzID0gZGF0YS5zdWJ0YXNrc1xuXG4gICAgICAgIGRlZmVycmVkLnJlc29sdmUodmVydGV4KVxuXG4gICAgZGVmZXJyZWQucHJvbWlzZVxuXG4gIEBnZXRTdWJ0YXNrcyA9ICh2ZXJ0ZXhpZCkgLT5cbiAgICBkZWZlcnJlZCA9ICRxLmRlZmVyKClcblxuICAgIGRlZmVycmVkcy5qb2IucHJvbWlzZS50aGVuIChkYXRhKSA9PlxuICAgICAgIyB2ZXJ0ZXggPSBAc2Vla1ZlcnRleCh2ZXJ0ZXhpZClcblxuICAgICAgJGh0dHAuZ2V0IFwiam9icy9cIiArIGN1cnJlbnRKb2IuamlkICsgXCIvdmVydGljZXMvXCIgKyB2ZXJ0ZXhpZFxuICAgICAgLnN1Y2Nlc3MgKGRhdGEpIC0+XG4gICAgICAgIHN1YnRhc2tzID0gZGF0YS5zdWJ0YXNrc1xuXG4gICAgICAgIGRlZmVycmVkLnJlc29sdmUoc3VidGFza3MpXG5cbiAgICBkZWZlcnJlZC5wcm9taXNlXG5cbiAgQGdldEFjY3VtdWxhdG9ycyA9ICh2ZXJ0ZXhpZCkgLT5cbiAgICBkZWZlcnJlZCA9ICRxLmRlZmVyKClcblxuICAgIGRlZmVycmVkcy5qb2IucHJvbWlzZS50aGVuIChkYXRhKSA9PlxuICAgICAgIyB2ZXJ0ZXggPSBAc2Vla1ZlcnRleCh2ZXJ0ZXhpZClcblxuICAgICAgJGh0dHAuZ2V0IFwiam9icy9cIiArIGN1cnJlbnRKb2IuamlkICsgXCIvdmVydGljZXMvXCIgKyB2ZXJ0ZXhpZCArIFwiL2FjY3VtdWxhdG9yc1wiXG4gICAgICAuc3VjY2VzcyAoZGF0YSkgLT5cbiAgICAgICAgYWNjdW11bGF0b3JzID0gZGF0YVsndXNlci1hY2N1bXVsYXRvcnMnXVxuXG4gICAgICAgICRodHRwLmdldCBcImpvYnMvXCIgKyBjdXJyZW50Sm9iLmppZCArIFwiL3ZlcnRpY2VzL1wiICsgdmVydGV4aWQgKyBcIi9zdWJ0YXNrcy9hY2N1bXVsYXRvcnNcIlxuICAgICAgICAuc3VjY2VzcyAoZGF0YSkgLT5cbiAgICAgICAgICBzdWJ0YXNrQWNjdW11bGF0b3JzID0gZGF0YS5zdWJ0YXNrc1xuXG4gICAgICAgICAgZGVmZXJyZWQucmVzb2x2ZSh7IG1haW46IGFjY3VtdWxhdG9ycywgc3VidGFza3M6IHN1YnRhc2tBY2N1bXVsYXRvcnMgfSlcblxuICAgIGRlZmVycmVkLnByb21pc2VcblxuICBAbG9hZEV4Y2VwdGlvbnMgPSAtPlxuICAgIGRlZmVycmVkID0gJHEuZGVmZXIoKVxuXG4gICAgZGVmZXJyZWRzLmpvYi5wcm9taXNlLnRoZW4gKGRhdGEpID0+XG5cbiAgICAgICRodHRwLmdldCBcImpvYnMvXCIgKyBjdXJyZW50Sm9iLmppZCArIFwiL2V4Y2VwdGlvbnNcIlxuICAgICAgLnN1Y2Nlc3MgKGV4Y2VwdGlvbnMpIC0+XG4gICAgICAgIGN1cnJlbnRKb2IuZXhjZXB0aW9ucyA9IGV4Y2VwdGlvbnNcblxuICAgICAgICBkZWZlcnJlZC5yZXNvbHZlKGV4Y2VwdGlvbnMpXG5cbiAgICBkZWZlcnJlZC5wcm9taXNlXG5cbiAgQGNhbmNlbEpvYiA9IChqb2JpZCkgLT5cbiAgICAjIHVzZXMgdGhlIG5vbiBSRVNULWNvbXBsaWFudCBHRVQgeWFybi1jYW5jZWwgaGFuZGxlciB3aGljaCBpcyBhdmFpbGFibGUgaW4gYWRkaXRpb24gdG8gdGhlXG4gICAgIyBwcm9wZXIgXCJERUxFVEUgam9icy88am9iaWQ+L1wiXG4gICAgJGh0dHAuZ2V0IFwiam9icy9cIiArIGpvYmlkICsgXCIveWFybi1jYW5jZWxcIlxuXG4gIEBcbiIsImFuZ3VsYXIubW9kdWxlKCdmbGlua0FwcCcpLnNlcnZpY2UoJ0pvYnNTZXJ2aWNlJywgZnVuY3Rpb24oJGh0dHAsIGZsaW5rQ29uZmlnLCAkbG9nLCBhbU1vbWVudCwgJHEsICR0aW1lb3V0KSB7XG4gIHZhciBjdXJyZW50Sm9iLCBjdXJyZW50UGxhbiwgZGVmZXJyZWRzLCBqb2JPYnNlcnZlcnMsIGpvYnMsIG5vdGlmeU9ic2VydmVycztcbiAgY3VycmVudEpvYiA9IG51bGw7XG4gIGN1cnJlbnRQbGFuID0gbnVsbDtcbiAgZGVmZXJyZWRzID0ge307XG4gIGpvYnMgPSB7XG4gICAgcnVubmluZzogW10sXG4gICAgZmluaXNoZWQ6IFtdLFxuICAgIGNhbmNlbGxlZDogW10sXG4gICAgZmFpbGVkOiBbXVxuICB9O1xuICBqb2JPYnNlcnZlcnMgPSBbXTtcbiAgbm90aWZ5T2JzZXJ2ZXJzID0gZnVuY3Rpb24oKSB7XG4gICAgcmV0dXJuIGFuZ3VsYXIuZm9yRWFjaChqb2JPYnNlcnZlcnMsIGZ1bmN0aW9uKGNhbGxiYWNrKSB7XG4gICAgICByZXR1cm4gY2FsbGJhY2soKTtcbiAgICB9KTtcbiAgfTtcbiAgdGhpcy5yZWdpc3Rlck9ic2VydmVyID0gZnVuY3Rpb24oY2FsbGJhY2spIHtcbiAgICByZXR1cm4gam9iT2JzZXJ2ZXJzLnB1c2goY2FsbGJhY2spO1xuICB9O1xuICB0aGlzLnVuUmVnaXN0ZXJPYnNlcnZlciA9IGZ1bmN0aW9uKGNhbGxiYWNrKSB7XG4gICAgdmFyIGluZGV4O1xuICAgIGluZGV4ID0gam9iT2JzZXJ2ZXJzLmluZGV4T2YoY2FsbGJhY2spO1xuICAgIHJldHVybiBqb2JPYnNlcnZlcnMuc3BsaWNlKGluZGV4LCAxKTtcbiAgfTtcbiAgdGhpcy5zdGF0ZUxpc3QgPSBmdW5jdGlvbigpIHtcbiAgICByZXR1cm4gWydTQ0hFRFVMRUQnLCAnREVQTE9ZSU5HJywgJ1JVTk5JTkcnLCAnRklOSVNIRUQnLCAnRkFJTEVEJywgJ0NBTkNFTElORycsICdDQU5DRUxFRCddO1xuICB9O1xuICB0aGlzLnRyYW5zbGF0ZUxhYmVsU3RhdGUgPSBmdW5jdGlvbihzdGF0ZSkge1xuICAgIHN3aXRjaCAoc3RhdGUudG9Mb3dlckNhc2UoKSkge1xuICAgICAgY2FzZSAnZmluaXNoZWQnOlxuICAgICAgICByZXR1cm4gJ3N1Y2Nlc3MnO1xuICAgICAgY2FzZSAnZmFpbGVkJzpcbiAgICAgICAgcmV0dXJuICdkYW5nZXInO1xuICAgICAgY2FzZSAnc2NoZWR1bGVkJzpcbiAgICAgICAgcmV0dXJuICdkZWZhdWx0JztcbiAgICAgIGNhc2UgJ2RlcGxveWluZyc6XG4gICAgICAgIHJldHVybiAnaW5mbyc7XG4gICAgICBjYXNlICdydW5uaW5nJzpcbiAgICAgICAgcmV0dXJuICdwcmltYXJ5JztcbiAgICAgIGNhc2UgJ2NhbmNlbGluZyc6XG4gICAgICAgIHJldHVybiAnd2FybmluZyc7XG4gICAgICBjYXNlICdwZW5kaW5nJzpcbiAgICAgICAgcmV0dXJuICdpbmZvJztcbiAgICAgIGNhc2UgJ3RvdGFsJzpcbiAgICAgICAgcmV0dXJuICdibGFjayc7XG4gICAgICBkZWZhdWx0OlxuICAgICAgICByZXR1cm4gJ2RlZmF1bHQnO1xuICAgIH1cbiAgfTtcbiAgdGhpcy5zZXRFbmRUaW1lcyA9IGZ1bmN0aW9uKGxpc3QpIHtcbiAgICByZXR1cm4gYW5ndWxhci5mb3JFYWNoKGxpc3QsIGZ1bmN0aW9uKGl0ZW0sIGpvYktleSkge1xuICAgICAgaWYgKCEoaXRlbVsnZW5kLXRpbWUnXSA+IC0xKSkge1xuICAgICAgICByZXR1cm4gaXRlbVsnZW5kLXRpbWUnXSA9IGl0ZW1bJ3N0YXJ0LXRpbWUnXSArIGl0ZW1bJ2R1cmF0aW9uJ107XG4gICAgICB9XG4gICAgfSk7XG4gIH07XG4gIHRoaXMucHJvY2Vzc1ZlcnRpY2VzID0gZnVuY3Rpb24oZGF0YSkge1xuICAgIGFuZ3VsYXIuZm9yRWFjaChkYXRhLnZlcnRpY2VzLCBmdW5jdGlvbih2ZXJ0ZXgsIGkpIHtcbiAgICAgIHJldHVybiB2ZXJ0ZXgudHlwZSA9ICdyZWd1bGFyJztcbiAgICB9KTtcbiAgICByZXR1cm4gZGF0YS52ZXJ0aWNlcy51bnNoaWZ0KHtcbiAgICAgIG5hbWU6ICdTY2hlZHVsZWQnLFxuICAgICAgJ3N0YXJ0LXRpbWUnOiBkYXRhLnRpbWVzdGFtcHNbJ0NSRUFURUQnXSxcbiAgICAgICdlbmQtdGltZSc6IGRhdGEudGltZXN0YW1wc1snQ1JFQVRFRCddICsgMSxcbiAgICAgIHR5cGU6ICdzY2hlZHVsZWQnXG4gICAgfSk7XG4gIH07XG4gIHRoaXMubGlzdEpvYnMgPSBmdW5jdGlvbigpIHtcbiAgICB2YXIgZGVmZXJyZWQ7XG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpO1xuICAgICRodHRwLmdldChcImpvYm92ZXJ2aWV3XCIpLnN1Y2Nlc3MoKGZ1bmN0aW9uKF90aGlzKSB7XG4gICAgICByZXR1cm4gZnVuY3Rpb24oZGF0YSwgc3RhdHVzLCBoZWFkZXJzLCBjb25maWcpIHtcbiAgICAgICAgYW5ndWxhci5mb3JFYWNoKGRhdGEsIGZ1bmN0aW9uKGxpc3QsIGxpc3RLZXkpIHtcbiAgICAgICAgICBzd2l0Y2ggKGxpc3RLZXkpIHtcbiAgICAgICAgICAgIGNhc2UgJ3J1bm5pbmcnOlxuICAgICAgICAgICAgICByZXR1cm4gam9icy5ydW5uaW5nID0gX3RoaXMuc2V0RW5kVGltZXMobGlzdCk7XG4gICAgICAgICAgICBjYXNlICdmaW5pc2hlZCc6XG4gICAgICAgICAgICAgIHJldHVybiBqb2JzLmZpbmlzaGVkID0gX3RoaXMuc2V0RW5kVGltZXMobGlzdCk7XG4gICAgICAgICAgICBjYXNlICdjYW5jZWxsZWQnOlxuICAgICAgICAgICAgICByZXR1cm4gam9icy5jYW5jZWxsZWQgPSBfdGhpcy5zZXRFbmRUaW1lcyhsaXN0KTtcbiAgICAgICAgICAgIGNhc2UgJ2ZhaWxlZCc6XG4gICAgICAgICAgICAgIHJldHVybiBqb2JzLmZhaWxlZCA9IF90aGlzLnNldEVuZFRpbWVzKGxpc3QpO1xuICAgICAgICAgIH1cbiAgICAgICAgfSk7XG4gICAgICAgIGRlZmVycmVkLnJlc29sdmUoam9icyk7XG4gICAgICAgIHJldHVybiBub3RpZnlPYnNlcnZlcnMoKTtcbiAgICAgIH07XG4gICAgfSkodGhpcykpO1xuICAgIHJldHVybiBkZWZlcnJlZC5wcm9taXNlO1xuICB9O1xuICB0aGlzLmdldEpvYnMgPSBmdW5jdGlvbih0eXBlKSB7XG4gICAgcmV0dXJuIGpvYnNbdHlwZV07XG4gIH07XG4gIHRoaXMuZ2V0QWxsSm9icyA9IGZ1bmN0aW9uKCkge1xuICAgIHJldHVybiBqb2JzO1xuICB9O1xuICB0aGlzLmxvYWRKb2IgPSBmdW5jdGlvbihqb2JpZCkge1xuICAgIGN1cnJlbnRKb2IgPSBudWxsO1xuICAgIGRlZmVycmVkcy5qb2IgPSAkcS5kZWZlcigpO1xuICAgICRodHRwLmdldChcImpvYnMvXCIgKyBqb2JpZCkuc3VjY2VzcygoZnVuY3Rpb24oX3RoaXMpIHtcbiAgICAgIHJldHVybiBmdW5jdGlvbihkYXRhLCBzdGF0dXMsIGhlYWRlcnMsIGNvbmZpZykge1xuICAgICAgICBfdGhpcy5zZXRFbmRUaW1lcyhkYXRhLnZlcnRpY2VzKTtcbiAgICAgICAgX3RoaXMucHJvY2Vzc1ZlcnRpY2VzKGRhdGEpO1xuICAgICAgICByZXR1cm4gJGh0dHAuZ2V0KFwiam9icy9cIiArIGpvYmlkICsgXCIvY29uZmlnXCIpLnN1Y2Nlc3MoZnVuY3Rpb24oam9iQ29uZmlnKSB7XG4gICAgICAgICAgZGF0YSA9IGFuZ3VsYXIuZXh0ZW5kKGRhdGEsIGpvYkNvbmZpZyk7XG4gICAgICAgICAgY3VycmVudEpvYiA9IGRhdGE7XG4gICAgICAgICAgcmV0dXJuIGRlZmVycmVkcy5qb2IucmVzb2x2ZShjdXJyZW50Sm9iKTtcbiAgICAgICAgfSk7XG4gICAgICB9O1xuICAgIH0pKHRoaXMpKTtcbiAgICByZXR1cm4gZGVmZXJyZWRzLmpvYi5wcm9taXNlO1xuICB9O1xuICB0aGlzLmdldE5vZGUgPSBmdW5jdGlvbihub2RlaWQpIHtcbiAgICB2YXIgZGVmZXJyZWQsIHNlZWtOb2RlO1xuICAgIHNlZWtOb2RlID0gZnVuY3Rpb24obm9kZWlkLCBkYXRhKSB7XG4gICAgICB2YXIgaiwgbGVuLCBub2RlLCBzdWI7XG4gICAgICBmb3IgKGogPSAwLCBsZW4gPSBkYXRhLmxlbmd0aDsgaiA8IGxlbjsgaisrKSB7XG4gICAgICAgIG5vZGUgPSBkYXRhW2pdO1xuICAgICAgICBpZiAobm9kZS5pZCA9PT0gbm9kZWlkKSB7XG4gICAgICAgICAgcmV0dXJuIG5vZGU7XG4gICAgICAgIH1cbiAgICAgICAgaWYgKG5vZGUuc3RlcF9mdW5jdGlvbikge1xuICAgICAgICAgIHN1YiA9IHNlZWtOb2RlKG5vZGVpZCwgbm9kZS5zdGVwX2Z1bmN0aW9uKTtcbiAgICAgICAgfVxuICAgICAgICBpZiAoc3ViKSB7XG4gICAgICAgICAgcmV0dXJuIHN1YjtcbiAgICAgICAgfVxuICAgICAgfVxuICAgICAgcmV0dXJuIG51bGw7XG4gICAgfTtcbiAgICBkZWZlcnJlZCA9ICRxLmRlZmVyKCk7XG4gICAgZGVmZXJyZWRzLmpvYi5wcm9taXNlLnRoZW4oKGZ1bmN0aW9uKF90aGlzKSB7XG4gICAgICByZXR1cm4gZnVuY3Rpb24oZGF0YSkge1xuICAgICAgICB2YXIgZm91bmROb2RlO1xuICAgICAgICBmb3VuZE5vZGUgPSBzZWVrTm9kZShub2RlaWQsIGN1cnJlbnRKb2IucGxhbi5ub2Rlcyk7XG4gICAgICAgIGZvdW5kTm9kZS52ZXJ0ZXggPSBfdGhpcy5zZWVrVmVydGV4KG5vZGVpZCk7XG4gICAgICAgIHJldHVybiBkZWZlcnJlZC5yZXNvbHZlKGZvdW5kTm9kZSk7XG4gICAgICB9O1xuICAgIH0pKHRoaXMpKTtcbiAgICByZXR1cm4gZGVmZXJyZWQucHJvbWlzZTtcbiAgfTtcbiAgdGhpcy5zZWVrVmVydGV4ID0gZnVuY3Rpb24obm9kZWlkKSB7XG4gICAgdmFyIGosIGxlbiwgcmVmLCB2ZXJ0ZXg7XG4gICAgcmVmID0gY3VycmVudEpvYi52ZXJ0aWNlcztcbiAgICBmb3IgKGogPSAwLCBsZW4gPSByZWYubGVuZ3RoOyBqIDwgbGVuOyBqKyspIHtcbiAgICAgIHZlcnRleCA9IHJlZltqXTtcbiAgICAgIGlmICh2ZXJ0ZXguaWQgPT09IG5vZGVpZCkge1xuICAgICAgICByZXR1cm4gdmVydGV4O1xuICAgICAgfVxuICAgIH1cbiAgICByZXR1cm4gbnVsbDtcbiAgfTtcbiAgdGhpcy5nZXRWZXJ0ZXggPSBmdW5jdGlvbih2ZXJ0ZXhpZCkge1xuICAgIHZhciBkZWZlcnJlZDtcbiAgICBkZWZlcnJlZCA9ICRxLmRlZmVyKCk7XG4gICAgZGVmZXJyZWRzLmpvYi5wcm9taXNlLnRoZW4oKGZ1bmN0aW9uKF90aGlzKSB7XG4gICAgICByZXR1cm4gZnVuY3Rpb24oZGF0YSkge1xuICAgICAgICB2YXIgdmVydGV4O1xuICAgICAgICB2ZXJ0ZXggPSBfdGhpcy5zZWVrVmVydGV4KHZlcnRleGlkKTtcbiAgICAgICAgcmV0dXJuICRodHRwLmdldChcImpvYnMvXCIgKyBjdXJyZW50Sm9iLmppZCArIFwiL3ZlcnRpY2VzL1wiICsgdmVydGV4aWQgKyBcIi9zdWJ0YXNrdGltZXNcIikuc3VjY2VzcyhmdW5jdGlvbihkYXRhKSB7XG4gICAgICAgICAgdmVydGV4LnN1YnRhc2tzID0gZGF0YS5zdWJ0YXNrcztcbiAgICAgICAgICByZXR1cm4gZGVmZXJyZWQucmVzb2x2ZSh2ZXJ0ZXgpO1xuICAgICAgICB9KTtcbiAgICAgIH07XG4gICAgfSkodGhpcykpO1xuICAgIHJldHVybiBkZWZlcnJlZC5wcm9taXNlO1xuICB9O1xuICB0aGlzLmdldFN1YnRhc2tzID0gZnVuY3Rpb24odmVydGV4aWQpIHtcbiAgICB2YXIgZGVmZXJyZWQ7XG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpO1xuICAgIGRlZmVycmVkcy5qb2IucHJvbWlzZS50aGVuKChmdW5jdGlvbihfdGhpcykge1xuICAgICAgcmV0dXJuIGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgICAgcmV0dXJuICRodHRwLmdldChcImpvYnMvXCIgKyBjdXJyZW50Sm9iLmppZCArIFwiL3ZlcnRpY2VzL1wiICsgdmVydGV4aWQpLnN1Y2Nlc3MoZnVuY3Rpb24oZGF0YSkge1xuICAgICAgICAgIHZhciBzdWJ0YXNrcztcbiAgICAgICAgICBzdWJ0YXNrcyA9IGRhdGEuc3VidGFza3M7XG4gICAgICAgICAgcmV0dXJuIGRlZmVycmVkLnJlc29sdmUoc3VidGFza3MpO1xuICAgICAgICB9KTtcbiAgICAgIH07XG4gICAgfSkodGhpcykpO1xuICAgIHJldHVybiBkZWZlcnJlZC5wcm9taXNlO1xuICB9O1xuICB0aGlzLmdldEFjY3VtdWxhdG9ycyA9IGZ1bmN0aW9uKHZlcnRleGlkKSB7XG4gICAgdmFyIGRlZmVycmVkO1xuICAgIGRlZmVycmVkID0gJHEuZGVmZXIoKTtcbiAgICBkZWZlcnJlZHMuam9iLnByb21pc2UudGhlbigoZnVuY3Rpb24oX3RoaXMpIHtcbiAgICAgIHJldHVybiBmdW5jdGlvbihkYXRhKSB7XG4gICAgICAgIHJldHVybiAkaHR0cC5nZXQoXCJqb2JzL1wiICsgY3VycmVudEpvYi5qaWQgKyBcIi92ZXJ0aWNlcy9cIiArIHZlcnRleGlkICsgXCIvYWNjdW11bGF0b3JzXCIpLnN1Y2Nlc3MoZnVuY3Rpb24oZGF0YSkge1xuICAgICAgICAgIHZhciBhY2N1bXVsYXRvcnM7XG4gICAgICAgICAgYWNjdW11bGF0b3JzID0gZGF0YVsndXNlci1hY2N1bXVsYXRvcnMnXTtcbiAgICAgICAgICByZXR1cm4gJGh0dHAuZ2V0KFwiam9icy9cIiArIGN1cnJlbnRKb2IuamlkICsgXCIvdmVydGljZXMvXCIgKyB2ZXJ0ZXhpZCArIFwiL3N1YnRhc2tzL2FjY3VtdWxhdG9yc1wiKS5zdWNjZXNzKGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgICAgICAgIHZhciBzdWJ0YXNrQWNjdW11bGF0b3JzO1xuICAgICAgICAgICAgc3VidGFza0FjY3VtdWxhdG9ycyA9IGRhdGEuc3VidGFza3M7XG4gICAgICAgICAgICByZXR1cm4gZGVmZXJyZWQucmVzb2x2ZSh7XG4gICAgICAgICAgICAgIG1haW46IGFjY3VtdWxhdG9ycyxcbiAgICAgICAgICAgICAgc3VidGFza3M6IHN1YnRhc2tBY2N1bXVsYXRvcnNcbiAgICAgICAgICAgIH0pO1xuICAgICAgICAgIH0pO1xuICAgICAgICB9KTtcbiAgICAgIH07XG4gICAgfSkodGhpcykpO1xuICAgIHJldHVybiBkZWZlcnJlZC5wcm9taXNlO1xuICB9O1xuICB0aGlzLmxvYWRFeGNlcHRpb25zID0gZnVuY3Rpb24oKSB7XG4gICAgdmFyIGRlZmVycmVkO1xuICAgIGRlZmVycmVkID0gJHEuZGVmZXIoKTtcbiAgICBkZWZlcnJlZHMuam9iLnByb21pc2UudGhlbigoZnVuY3Rpb24oX3RoaXMpIHtcbiAgICAgIHJldHVybiBmdW5jdGlvbihkYXRhKSB7XG4gICAgICAgIHJldHVybiAkaHR0cC5nZXQoXCJqb2JzL1wiICsgY3VycmVudEpvYi5qaWQgKyBcIi9leGNlcHRpb25zXCIpLnN1Y2Nlc3MoZnVuY3Rpb24oZXhjZXB0aW9ucykge1xuICAgICAgICAgIGN1cnJlbnRKb2IuZXhjZXB0aW9ucyA9IGV4Y2VwdGlvbnM7XG4gICAgICAgICAgcmV0dXJuIGRlZmVycmVkLnJlc29sdmUoZXhjZXB0aW9ucyk7XG4gICAgICAgIH0pO1xuICAgICAgfTtcbiAgICB9KSh0aGlzKSk7XG4gICAgcmV0dXJuIGRlZmVycmVkLnByb21pc2U7XG4gIH07XG4gIHRoaXMuY2FuY2VsSm9iID0gZnVuY3Rpb24oam9iaWQpIHtcbiAgICByZXR1cm4gJGh0dHAuZ2V0KFwiam9icy9cIiArIGpvYmlkICsgXCIveWFybi1jYW5jZWxcIik7XG4gIH07XG4gIHJldHVybiB0aGlzO1xufSk7XG4iLCIjXG4jIExpY2Vuc2VkIHRvIHRoZSBBcGFjaGUgU29mdHdhcmUgRm91bmRhdGlvbiAoQVNGKSB1bmRlciBvbmVcbiMgb3IgbW9yZSBjb250cmlidXRvciBsaWNlbnNlIGFncmVlbWVudHMuICBTZWUgdGhlIE5PVElDRSBmaWxlXG4jIGRpc3RyaWJ1dGVkIHdpdGggdGhpcyB3b3JrIGZvciBhZGRpdGlvbmFsIGluZm9ybWF0aW9uXG4jIHJlZ2FyZGluZyBjb3B5cmlnaHQgb3duZXJzaGlwLiAgVGhlIEFTRiBsaWNlbnNlcyB0aGlzIGZpbGVcbiMgdG8geW91IHVuZGVyIHRoZSBBcGFjaGUgTGljZW5zZSwgVmVyc2lvbiAyLjAgKHRoZVxuIyBcIkxpY2Vuc2VcIik7IHlvdSBtYXkgbm90IHVzZSB0aGlzIGZpbGUgZXhjZXB0IGluIGNvbXBsaWFuY2VcbiMgd2l0aCB0aGUgTGljZW5zZS4gIFlvdSBtYXkgb2J0YWluIGEgY29weSBvZiB0aGUgTGljZW5zZSBhdFxuI1xuIyAgICAgaHR0cDovL3d3dy5hcGFjaGUub3JnL2xpY2Vuc2VzL0xJQ0VOU0UtMi4wXG4jXG4jIFVubGVzcyByZXF1aXJlZCBieSBhcHBsaWNhYmxlIGxhdyBvciBhZ3JlZWQgdG8gaW4gd3JpdGluZywgc29mdHdhcmVcbiMgZGlzdHJpYnV0ZWQgdW5kZXIgdGhlIExpY2Vuc2UgaXMgZGlzdHJpYnV0ZWQgb24gYW4gXCJBUyBJU1wiIEJBU0lTLFxuIyBXSVRIT1VUIFdBUlJBTlRJRVMgT1IgQ09ORElUSU9OUyBPRiBBTlkgS0lORCwgZWl0aGVyIGV4cHJlc3Mgb3IgaW1wbGllZC5cbiMgU2VlIHRoZSBMaWNlbnNlIGZvciB0aGUgc3BlY2lmaWMgbGFuZ3VhZ2UgZ292ZXJuaW5nIHBlcm1pc3Npb25zIGFuZFxuIyBsaW1pdGF0aW9ucyB1bmRlciB0aGUgTGljZW5zZS5cbiNcblxuYW5ndWxhci5tb2R1bGUoJ2ZsaW5rQXBwJylcblxuLmNvbnRyb2xsZXIgJ0FsbFRhc2tNYW5hZ2Vyc0NvbnRyb2xsZXInLCAoJHNjb3BlLCBUYXNrTWFuYWdlcnNTZXJ2aWNlLCAkaW50ZXJ2YWwsIGZsaW5rQ29uZmlnKSAtPlxuICBUYXNrTWFuYWdlcnNTZXJ2aWNlLmxvYWRNYW5hZ2VycygpLnRoZW4gKGRhdGEpIC0+XG4gICAgJHNjb3BlLm1hbmFnZXJzID0gZGF0YVxuXG4gIHJlZnJlc2ggPSAkaW50ZXJ2YWwgLT5cbiAgICBUYXNrTWFuYWdlcnNTZXJ2aWNlLmxvYWRNYW5hZ2VycygpLnRoZW4gKGRhdGEpIC0+XG4gICAgICAkc2NvcGUubWFuYWdlcnMgPSBkYXRhXG4gICwgZmxpbmtDb25maWdbXCJyZWZyZXNoLWludGVydmFsXCJdXG5cbiAgJHNjb3BlLiRvbiAnJGRlc3Ryb3knLCAtPlxuICAgICRpbnRlcnZhbC5jYW5jZWwocmVmcmVzaClcblxuLmNvbnRyb2xsZXIgJ1NpbmdsZVRhc2tNYW5hZ2VyQ29udHJvbGxlcicsICgkc2NvcGUsICRzdGF0ZVBhcmFtcywgU2luZ2xlVGFza01hbmFnZXJTZXJ2aWNlLCAkaW50ZXJ2YWwsIGZsaW5rQ29uZmlnKSAtPlxuICAkc2NvcGUubWV0cmljcyA9IHt9XG4gIFNpbmdsZVRhc2tNYW5hZ2VyU2VydmljZS5sb2FkTWV0cmljcygkc3RhdGVQYXJhbXMudGFza21hbmFnZXJpZCkudGhlbiAoZGF0YSkgLT5cbiAgICAgICRzY29wZS5tZXRyaWNzID0gZGF0YVswXVxuXG4gICAgcmVmcmVzaCA9ICRpbnRlcnZhbCAtPlxuICAgICAgU2luZ2xlVGFza01hbmFnZXJTZXJ2aWNlLmxvYWRNZXRyaWNzKCRzdGF0ZVBhcmFtcy50YXNrbWFuYWdlcmlkKS50aGVuIChkYXRhKSAtPlxuICAgICAgICAkc2NvcGUubWV0cmljcyA9IGRhdGFbMF1cbiAgICAsIGZsaW5rQ29uZmlnW1wicmVmcmVzaC1pbnRlcnZhbFwiXVxuXG4gICAgJHNjb3BlLiRvbiAnJGRlc3Ryb3knLCAtPlxuICAgICAgJGludGVydmFsLmNhbmNlbChyZWZyZXNoKVxuXG4iLCJhbmd1bGFyLm1vZHVsZSgnZmxpbmtBcHAnKS5jb250cm9sbGVyKCdBbGxUYXNrTWFuYWdlcnNDb250cm9sbGVyJywgZnVuY3Rpb24oJHNjb3BlLCBUYXNrTWFuYWdlcnNTZXJ2aWNlLCAkaW50ZXJ2YWwsIGZsaW5rQ29uZmlnKSB7XG4gIHZhciByZWZyZXNoO1xuICBUYXNrTWFuYWdlcnNTZXJ2aWNlLmxvYWRNYW5hZ2VycygpLnRoZW4oZnVuY3Rpb24oZGF0YSkge1xuICAgIHJldHVybiAkc2NvcGUubWFuYWdlcnMgPSBkYXRhO1xuICB9KTtcbiAgcmVmcmVzaCA9ICRpbnRlcnZhbChmdW5jdGlvbigpIHtcbiAgICByZXR1cm4gVGFza01hbmFnZXJzU2VydmljZS5sb2FkTWFuYWdlcnMoKS50aGVuKGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgIHJldHVybiAkc2NvcGUubWFuYWdlcnMgPSBkYXRhO1xuICAgIH0pO1xuICB9LCBmbGlua0NvbmZpZ1tcInJlZnJlc2gtaW50ZXJ2YWxcIl0pO1xuICByZXR1cm4gJHNjb3BlLiRvbignJGRlc3Ryb3knLCBmdW5jdGlvbigpIHtcbiAgICByZXR1cm4gJGludGVydmFsLmNhbmNlbChyZWZyZXNoKTtcbiAgfSk7XG59KS5jb250cm9sbGVyKCdTaW5nbGVUYXNrTWFuYWdlckNvbnRyb2xsZXInLCBmdW5jdGlvbigkc2NvcGUsICRzdGF0ZVBhcmFtcywgU2luZ2xlVGFza01hbmFnZXJTZXJ2aWNlLCAkaW50ZXJ2YWwsIGZsaW5rQ29uZmlnKSB7XG4gIHZhciByZWZyZXNoO1xuICAkc2NvcGUubWV0cmljcyA9IHt9O1xuICBTaW5nbGVUYXNrTWFuYWdlclNlcnZpY2UubG9hZE1ldHJpY3MoJHN0YXRlUGFyYW1zLnRhc2ttYW5hZ2VyaWQpLnRoZW4oZnVuY3Rpb24oZGF0YSkge1xuICAgIHJldHVybiAkc2NvcGUubWV0cmljcyA9IGRhdGFbMF07XG4gIH0pO1xuICByZWZyZXNoID0gJGludGVydmFsKGZ1bmN0aW9uKCkge1xuICAgIHJldHVybiBTaW5nbGVUYXNrTWFuYWdlclNlcnZpY2UubG9hZE1ldHJpY3MoJHN0YXRlUGFyYW1zLnRhc2ttYW5hZ2VyaWQpLnRoZW4oZnVuY3Rpb24oZGF0YSkge1xuICAgICAgcmV0dXJuICRzY29wZS5tZXRyaWNzID0gZGF0YVswXTtcbiAgICB9KTtcbiAgfSwgZmxpbmtDb25maWdbXCJyZWZyZXNoLWludGVydmFsXCJdKTtcbiAgcmV0dXJuICRzY29wZS4kb24oJyRkZXN0cm95JywgZnVuY3Rpb24oKSB7XG4gICAgcmV0dXJuICRpbnRlcnZhbC5jYW5jZWwocmVmcmVzaCk7XG4gIH0pO1xufSk7XG4iLCIjXG4jIExpY2Vuc2VkIHRvIHRoZSBBcGFjaGUgU29mdHdhcmUgRm91bmRhdGlvbiAoQVNGKSB1bmRlciBvbmVcbiMgb3IgbW9yZSBjb250cmlidXRvciBsaWNlbnNlIGFncmVlbWVudHMuICBTZWUgdGhlIE5PVElDRSBmaWxlXG4jIGRpc3RyaWJ1dGVkIHdpdGggdGhpcyB3b3JrIGZvciBhZGRpdGlvbmFsIGluZm9ybWF0aW9uXG4jIHJlZ2FyZGluZyBjb3B5cmlnaHQgb3duZXJzaGlwLiAgVGhlIEFTRiBsaWNlbnNlcyB0aGlzIGZpbGVcbiMgdG8geW91IHVuZGVyIHRoZSBBcGFjaGUgTGljZW5zZSwgVmVyc2lvbiAyLjAgKHRoZVxuIyBcIkxpY2Vuc2VcIik7IHlvdSBtYXkgbm90IHVzZSB0aGlzIGZpbGUgZXhjZXB0IGluIGNvbXBsaWFuY2VcbiMgd2l0aCB0aGUgTGljZW5zZS4gIFlvdSBtYXkgb2J0YWluIGEgY29weSBvZiB0aGUgTGljZW5zZSBhdFxuI1xuIyAgICAgaHR0cDovL3d3dy5hcGFjaGUub3JnL2xpY2Vuc2VzL0xJQ0VOU0UtMi4wXG4jXG4jIFVubGVzcyByZXF1aXJlZCBieSBhcHBsaWNhYmxlIGxhdyBvciBhZ3JlZWQgdG8gaW4gd3JpdGluZywgc29mdHdhcmVcbiMgZGlzdHJpYnV0ZWQgdW5kZXIgdGhlIExpY2Vuc2UgaXMgZGlzdHJpYnV0ZWQgb24gYW4gXCJBUyBJU1wiIEJBU0lTLFxuIyBXSVRIT1VUIFdBUlJBTlRJRVMgT1IgQ09ORElUSU9OUyBPRiBBTlkgS0lORCwgZWl0aGVyIGV4cHJlc3Mgb3IgaW1wbGllZC5cbiMgU2VlIHRoZSBMaWNlbnNlIGZvciB0aGUgc3BlY2lmaWMgbGFuZ3VhZ2UgZ292ZXJuaW5nIHBlcm1pc3Npb25zIGFuZFxuIyBsaW1pdGF0aW9ucyB1bmRlciB0aGUgTGljZW5zZS5cbiNcblxuYW5ndWxhci5tb2R1bGUoJ2ZsaW5rQXBwJylcblxuLnNlcnZpY2UgJ1Rhc2tNYW5hZ2Vyc1NlcnZpY2UnLCAoJGh0dHAsIGZsaW5rQ29uZmlnLCAkcSkgLT5cbiAgQGxvYWRNYW5hZ2VycyA9ICgpIC0+XG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpXG5cbiAgICAkaHR0cC5nZXQoXCJ0YXNrbWFuYWdlcnNcIilcbiAgICAuc3VjY2VzcyAoZGF0YSwgc3RhdHVzLCBoZWFkZXJzLCBjb25maWcpIC0+XG4gICAgICBkZWZlcnJlZC5yZXNvbHZlKGRhdGFbJ3Rhc2ttYW5hZ2VycyddKVxuXG4gICAgZGVmZXJyZWQucHJvbWlzZVxuXG4gIEBcblxuLnNlcnZpY2UgJ1NpbmdsZVRhc2tNYW5hZ2VyU2VydmljZScsICgkaHR0cCwgZmxpbmtDb25maWcsICRxKSAtPlxuICBAbG9hZE1ldHJpY3MgPSAodGFza21hbmFnZXJpZCkgLT5cbiAgICBkZWZlcnJlZCA9ICRxLmRlZmVyKClcblxuICAgICRodHRwLmdldChcInRhc2ttYW5hZ2Vycy9cIiArIHRhc2ttYW5hZ2VyaWQpXG4gICAgLnN1Y2Nlc3MgKGRhdGEsIHN0YXR1cywgaGVhZGVycywgY29uZmlnKSAtPlxuICAgICAgZGVmZXJyZWQucmVzb2x2ZShkYXRhWyd0YXNrbWFuYWdlcnMnXSlcblxuICAgIGRlZmVycmVkLnByb21pc2VcblxuICBAXG5cbiIsImFuZ3VsYXIubW9kdWxlKCdmbGlua0FwcCcpLnNlcnZpY2UoJ1Rhc2tNYW5hZ2Vyc1NlcnZpY2UnLCBmdW5jdGlvbigkaHR0cCwgZmxpbmtDb25maWcsICRxKSB7XG4gIHRoaXMubG9hZE1hbmFnZXJzID0gZnVuY3Rpb24oKSB7XG4gICAgdmFyIGRlZmVycmVkO1xuICAgIGRlZmVycmVkID0gJHEuZGVmZXIoKTtcbiAgICAkaHR0cC5nZXQoXCJ0YXNrbWFuYWdlcnNcIikuc3VjY2VzcyhmdW5jdGlvbihkYXRhLCBzdGF0dXMsIGhlYWRlcnMsIGNvbmZpZykge1xuICAgICAgcmV0dXJuIGRlZmVycmVkLnJlc29sdmUoZGF0YVsndGFza21hbmFnZXJzJ10pO1xuICAgIH0pO1xuICAgIHJldHVybiBkZWZlcnJlZC5wcm9taXNlO1xuICB9O1xuICByZXR1cm4gdGhpcztcbn0pLnNlcnZpY2UoJ1NpbmdsZVRhc2tNYW5hZ2VyU2VydmljZScsIGZ1bmN0aW9uKCRodHRwLCBmbGlua0NvbmZpZywgJHEpIHtcbiAgdGhpcy5sb2FkTWV0cmljcyA9IGZ1bmN0aW9uKHRhc2ttYW5hZ2VyaWQpIHtcbiAgICB2YXIgZGVmZXJyZWQ7XG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpO1xuICAgICRodHRwLmdldChcInRhc2ttYW5hZ2Vycy9cIiArIHRhc2ttYW5hZ2VyaWQpLnN1Y2Nlc3MoZnVuY3Rpb24oZGF0YSwgc3RhdHVzLCBoZWFkZXJzLCBjb25maWcpIHtcbiAgICAgIHJldHVybiBkZWZlcnJlZC5yZXNvbHZlKGRhdGFbJ3Rhc2ttYW5hZ2VycyddKTtcbiAgICB9KTtcbiAgICByZXR1cm4gZGVmZXJyZWQucHJvbWlzZTtcbiAgfTtcbiAgcmV0dXJuIHRoaXM7XG59KTtcbiJdLCJzb3VyY2VSb290IjoiL3NvdXJjZS8ifQ==
