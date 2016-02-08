angular.module('flinkApp', ['ui.router', 'angularMoment']).run(["$rootScope", function($rootScope) {
  $rootScope.sidebarVisible = false;
  return $rootScope.showSidebar = function() {
    $rootScope.sidebarVisible = !$rootScope.sidebarVisible;
    return $rootScope.sidebarClass = 'force-show';
  };
}]).value('flinkConfig', {
  jobServer: '',
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
  }).state("single-job.plan.checkpoints", {
    url: "/checkpoints",
    views: {
      'node-details': {
        templateUrl: "partials/jobs/job.plan.node-list.checkpoints.html",
        controller: 'JobPlanCheckpointsController'
      }
    }
  }).state("single-job.plan.backpressure", {
    url: "/backpressure",
    views: {
      'node-details': {
        templateUrl: "partials/jobs/job.plan.node-list.backpressure.html",
        controller: 'JobPlanBackPressureController'
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
  }).state("submit", {
    url: "/submit",
    views: {
      main: {
        templateUrl: "partials/submit.html",
        controller: "JobSubmitController"
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
}]).directive('bpLabel', ["JobsService", function(JobsService) {
  return {
    transclude: true,
    replace: true,
    scope: {
      getBackPressureLabelClass: "&",
      status: "@"
    },
    template: "<span title='{{status}}' ng-class='getBackPressureLabelClass()'><ng-transclude></ng-transclude></span>",
    link: function(scope, element, attrs) {
      return scope.getBackPressureLabelClass = function() {
        return 'label label-' + JobsService.translateBackPressureLabelState(attrs.status);
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
}).filter("humanizeBytes", function() {
  return function(bytes) {
    var converter, units;
    units = ["B", "KB", "MB", "GB", "TB", "PB", "EB"];
    converter = function(value, power) {
      var base;
      base = Math.pow(1024, power);
      if (value < base) {
        return (value / base).toFixed(2) + " " + units[power];
      } else if (value < base * 1000) {
        return (value / base).toPrecision(3) + " " + units[power];
      } else {
        return converter(value, power + 1);
      }
    };
    if (typeof bytes === "undefined" || bytes === null) {
      return "";
    }
    if (bytes < 1000) {
      return bytes + " B";
    } else {
      return converter(bytes, 1);
    }
  };
}).filter("toUpperCase", function() {
  return function(text) {
    return text.toUpperCase();
  };
});

angular.module('flinkApp').service('MainService', ["$http", "flinkConfig", "$q", function($http, flinkConfig, $q) {
  this.loadConfig = function() {
    var deferred;
    deferred = $q.defer();
    $http.get(flinkConfig.jobServer + "config").success(function(data, status, headers, config) {
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
    $http.get(flinkConfig.jobServer + "jobmanager/config").success(function(data, status, headers, config) {
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
    $http.get(flinkConfig.jobServer + "jobmanager/log").success(function(data, status, headers, config) {
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
    $http.get(flinkConfig.jobServer + "jobmanager/stdout").success(function(data, status, headers, config) {
      stdout = data;
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
  $scope.jobCheckpointStats = null;
  $scope.showHistory = false;
  $scope.backPressureOperatorStats = {};
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
    $scope.jobCheckpointStats = null;
    $scope.backPressureOperatorStats = null;
    return $interval.cancel(refresher);
  });
  $scope.cancelJob = function(cancelEvent) {
    angular.element(cancelEvent.currentTarget).removeClass("btn").removeClass("btn-default").html('Cancelling...');
    return JobsService.cancelJob($stateParams.jobid).then(function(data) {
      return {};
    });
  };
  return $scope.toggleHistory = function() {
    return $scope.showHistory = !$scope.showHistory;
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
      $scope.operatorCheckpointStats = null;
      return $scope.$broadcast('reload');
    } else {
      $scope.nodeid = null;
      $scope.nodeUnfolded = false;
      $scope.vertex = null;
      $scope.subtasks = null;
      $scope.accumulators = null;
      return $scope.operatorCheckpointStats = null;
    }
  };
  $scope.deactivateNode = function() {
    $scope.nodeid = null;
    $scope.nodeUnfolded = false;
    $scope.vertex = null;
    $scope.subtasks = null;
    $scope.accumulators = null;
    return $scope.operatorCheckpointStats = null;
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
}]).controller('JobPlanCheckpointsController', ["$scope", "JobsService", function($scope, JobsService) {
  console.log('JobPlanCheckpointsController');
  JobsService.getJobCheckpointStats($scope.jobid).then(function(data) {
    return $scope.jobCheckpointStats = data;
  });
  if ($scope.nodeid && (!$scope.vertex || !$scope.vertex.operatorCheckpointStats)) {
    JobsService.getOperatorCheckpointStats($scope.nodeid).then(function(data) {
      $scope.operatorCheckpointStats = data.operatorStats;
      return $scope.subtasksCheckpointStats = data.subtasksStats;
    });
  }
  return $scope.$on('reload', function(event) {
    console.log('JobPlanCheckpointsController');
    JobsService.getJobCheckpointStats($scope.jobid).then(function(data) {
      return $scope.jobCheckpointStats = data;
    });
    if ($scope.nodeid) {
      return JobsService.getOperatorCheckpointStats($scope.nodeid).then(function(data) {
        $scope.operatorCheckpointStats = data.operatorStats;
        return $scope.subtasksCheckpointStats = data.subtasksStats;
      });
    }
  });
}]).controller('JobPlanBackPressureController', ["$scope", "JobsService", function($scope, JobsService) {
  console.log('JobPlanBackPressureController');
  $scope.now = Date.now();
  if ($scope.nodeid) {
    JobsService.getOperatorBackPressure($scope.nodeid).then(function(data) {
      return $scope.backPressureOperatorStats[$scope.nodeid] = data;
    });
  }
  return $scope.$on('reload', function(event) {
    console.log('JobPlanBackPressureController (relaod)');
    $scope.now = Date.now();
    if ($scope.nodeid) {
      return JobsService.getOperatorBackPressure($scope.nodeid).then(function(data) {
        return $scope.backPressureOperatorStats[$scope.nodeid] = data;
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
    $http.get(flinkConfig.jobServer + "joboverview").success((function(_this) {
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
    $http.get(flinkConfig.jobServer + "jobs/" + jobid).success((function(_this) {
      return function(data, status, headers, config) {
        _this.setEndTimes(data.vertices);
        _this.processVertices(data);
        return $http.get(flinkConfig.jobServer + "jobs/" + jobid + "/config").success(function(jobConfig) {
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
        return $http.get(flinkConfig.jobServer + "jobs/" + currentJob.jid + "/vertices/" + vertexid + "/subtasktimes").success(function(data) {
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
        return $http.get(flinkConfig.jobServer + "jobs/" + currentJob.jid + "/vertices/" + vertexid).success(function(data) {
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
        return $http.get(flinkConfig.jobServer + "jobs/" + currentJob.jid + "/vertices/" + vertexid + "/accumulators").success(function(data) {
          var accumulators;
          accumulators = data['user-accumulators'];
          return $http.get(flinkConfig.jobServer + "jobs/" + currentJob.jid + "/vertices/" + vertexid + "/subtasks/accumulators").success(function(data) {
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
  this.getJobCheckpointStats = function(jobid) {
    var deferred;
    deferred = $q.defer();
    $http.get(flinkConfig.jobServer + "jobs/" + jobid + "/checkpoints").success((function(_this) {
      return function(data, status, headers, config) {
        if (angular.equals({}, data)) {
          return deferred.resolve(deferred.resolve(null));
        } else {
          return deferred.resolve(data);
        }
      };
    })(this));
    return deferred.promise;
  };
  this.getOperatorCheckpointStats = function(vertexid) {
    var deferred;
    deferred = $q.defer();
    deferreds.job.promise.then((function(_this) {
      return function(data) {
        return $http.get(flinkConfig.jobServer + "jobs/" + currentJob.jid + "/vertices/" + vertexid + "/checkpoints").success(function(data) {
          var operatorStats, subtaskStats;
          if (angular.equals({}, data)) {
            return deferred.resolve({
              operatorStats: null,
              subtasksStats: null
            });
          } else {
            operatorStats = {
              id: data['id'],
              timestamp: data['timestamp'],
              duration: data['duration'],
              size: data['size']
            };
            if (angular.equals({}, data['subtasks'])) {
              return deferred.resolve({
                operatorStats: operatorStats,
                subtasksStats: null
              });
            } else {
              subtaskStats = data['subtasks'];
              return deferred.resolve({
                operatorStats: operatorStats,
                subtasksStats: subtaskStats
              });
            }
          }
        });
      };
    })(this));
    return deferred.promise;
  };
  this.getOperatorBackPressure = function(vertexid) {
    var deferred;
    deferred = $q.defer();
    $http.get(flinkConfig.jobServer + "jobs/" + currentJob.jid + "/vertices/" + vertexid + "/backpressure").success((function(_this) {
      return function(data) {
        return deferred.resolve(data);
      };
    })(this));
    return deferred.promise;
  };
  this.translateBackPressureLabelState = function(state) {
    switch (state.toLowerCase()) {
      case 'in-progress':
        return 'danger';
      case 'ok':
        return 'success';
      case 'low':
        return 'warning';
      case 'high':
        return 'danger';
      default:
        return 'default';
    }
  };
  this.loadExceptions = function() {
    var deferred;
    deferred = $q.defer();
    deferreds.job.promise.then((function(_this) {
      return function(data) {
        return $http.get(flinkConfig.jobServer + "jobs/" + currentJob.jid + "/exceptions").success(function(exceptions) {
          currentJob.exceptions = exceptions;
          return deferred.resolve(exceptions);
        });
      };
    })(this));
    return deferred.promise;
  };
  this.cancelJob = function(jobid) {
    return $http.get(flinkConfig.jobServer + "jobs/" + jobid + "/yarn-cancel");
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
    $http.get(flinkConfig.jobServer + "overview").success(function(data, status, headers, config) {
      overview = data;
      return deferred.resolve(data);
    });
    return deferred.promise;
  };
  return this;
}]);

angular.module('flinkApp').controller('JobSubmitController', ["$scope", "JobSubmitService", "$interval", "flinkConfig", "$state", "$location", function($scope, JobSubmitService, $interval, flinkConfig, $state, $location) {
  var refresh;
  $scope.yarn = $location.absUrl().indexOf("/proxy/application_") !== -1;
  $scope.loadList = function() {
    return JobSubmitService.loadJarList().then(function(data) {
      $scope.address = data.address;
      $scope.noaccess = data.error;
      return $scope.jars = data.files;
    });
  };
  $scope.defaultState = function() {
    $scope.plan = null;
    $scope.error = null;
    return $scope.state = {
      selected: null,
      parallelism: "",
      'entry-class': "",
      'program-args': "",
      'plan-button': "Show Plan",
      'submit-button': "Submit",
      'action-time': 0
    };
  };
  $scope.defaultState();
  $scope.uploader = {};
  $scope.loadList();
  refresh = $interval(function() {
    return $scope.loadList();
  }, flinkConfig["refresh-interval"]);
  $scope.$on('$destroy', function() {
    return $interval.cancel(refresh);
  });
  $scope.selectJar = function(id) {
    if ($scope.state.selected === id) {
      return $scope.defaultState();
    } else {
      $scope.defaultState();
      return $scope.state.selected = id;
    }
  };
  $scope.deleteJar = function(event, id) {
    if ($scope.state.selected === id) {
      $scope.defaultState();
    }
    angular.element(event.currentTarget).removeClass("fa-remove").addClass("fa-spin fa-spinner");
    return JobSubmitService.deleteJar(id).then(function(data) {
      angular.element(event.currentTarget).removeClass("fa-spin fa-spinner").addClass("fa-remove");
      if (data.error != null) {
        return alert(data.error);
      }
    });
  };
  $scope.loadEntryClass = function(name) {
    return $scope.state['entry-class'] = name;
  };
  $scope.getPlan = function() {
    var action;
    if ($scope.state['plan-button'] === "Show Plan") {
      action = new Date().getTime();
      $scope.state['action-time'] = action;
      $scope.state['submit-button'] = "Submit";
      $scope.state['plan-button'] = "Getting Plan";
      $scope.error = null;
      $scope.plan = null;
      return JobSubmitService.getPlan($scope.state.selected, {
        'entry-class': $scope.state['entry-class'],
        parallelism: $scope.state.parallelism,
        'program-args': $scope.state['program-args']
      }).then(function(data) {
        if (action === $scope.state['action-time']) {
          $scope.state['plan-button'] = "Show Plan";
          $scope.error = data.error;
          return $scope.plan = data.plan;
        }
      });
    }
  };
  $scope.runJob = function() {
    var action;
    if ($scope.state['submit-button'] === "Submit") {
      action = new Date().getTime();
      $scope.state['action-time'] = action;
      $scope.state['submit-button'] = "Submitting";
      $scope.state['plan-button'] = "Show Plan";
      $scope.error = null;
      return JobSubmitService.runJob($scope.state.selected, {
        'entry-class': $scope.state['entry-class'],
        parallelism: $scope.state.parallelism,
        'program-args': $scope.state['program-args']
      }).then(function(data) {
        if (action === $scope.state['action-time']) {
          $scope.state['submit-button'] = "Submit";
          $scope.error = data.error;
          if (data.jobid != null) {
            return $state.go("single-job.plan.overview", {
              jobid: data.jobid
            });
          }
        }
      });
    }
  };
  $scope.nodeid = null;
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
  $scope.clearFiles = function() {
    return $scope.uploader = {};
  };
  $scope.uploadFiles = function(files) {
    $scope.uploader = {};
    if (files.length === 1) {
      $scope.uploader['file'] = files[0];
      return $scope.uploader['upload'] = true;
    } else {
      return $scope.uploader['error'] = "Did ya forget to select a file?";
    }
  };
  return $scope.startUpload = function() {
    var formdata, xhr;
    if ($scope.uploader['file'] != null) {
      formdata = new FormData();
      formdata.append("jarfile", $scope.uploader['file']);
      $scope.uploader['upload'] = false;
      $scope.uploader['success'] = "Initializing upload...";
      xhr = new XMLHttpRequest();
      xhr.upload.onprogress = function(event) {
        $scope.uploader['success'] = null;
        return $scope.uploader['progress'] = parseInt(100 * event.loaded / event.total);
      };
      xhr.upload.onerror = function(event) {
        $scope.uploader['progress'] = null;
        return $scope.uploader['error'] = "An error occurred while uploading your file";
      };
      xhr.upload.onload = function(event) {
        $scope.uploader['progress'] = null;
        return $scope.uploader['success'] = "Saving...";
      };
      xhr.onreadystatechange = function() {
        var response;
        if (xhr.readyState === 4) {
          response = JSON.parse(xhr.responseText);
          if (response.error != null) {
            $scope.uploader['error'] = response.error;
            return $scope.uploader['success'] = null;
          } else {
            return $scope.uploader['success'] = "Uploaded!";
          }
        }
      };
      xhr.open("POST", "/jars/upload");
      return xhr.send(formdata);
    } else {
      return console.log("Unexpected Error. This should not happen");
    }
  };
}]).filter('getJarSelectClass', function() {
  return function(selected, actual) {
    if (selected === actual) {
      return "fa-check-square";
    } else {
      return "fa-square-o";
    }
  };
});

angular.module('flinkApp').service('JobSubmitService', ["$http", "flinkConfig", "$q", function($http, flinkConfig, $q) {
  this.loadJarList = function() {
    var deferred;
    deferred = $q.defer();
    $http.get("jars/").success(function(data, status, headers, config) {
      return deferred.resolve(data);
    });
    return deferred.promise;
  };
  this.deleteJar = function(id) {
    var deferred;
    deferred = $q.defer();
    $http["delete"]("jars/" + id).success(function(data, status, headers, config) {
      return deferred.resolve(data);
    });
    return deferred.promise;
  };
  this.getPlan = function(id, args) {
    var deferred;
    deferred = $q.defer();
    $http.get("jars/" + id + "/plan", {
      params: args
    }).success(function(data, status, headers, config) {
      return deferred.resolve(data);
    });
    return deferred.promise;
  };
  this.runJob = function(id, args) {
    var deferred;
    deferred = $q.defer();
    $http.post("jars/" + id + "/run", {}, {
      params: args
    }).success(function(data, status, headers, config) {
      return deferred.resolve(data);
    });
    return deferred.promise;
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
    $http.get(flinkConfig.jobServer + "taskmanagers").success(function(data, status, headers, config) {
      return deferred.resolve(data['taskmanagers']);
    });
    return deferred.promise;
  };
  return this;
}]).service('SingleTaskManagerService', ["$http", "flinkConfig", "$q", function($http, flinkConfig, $q) {
  this.loadMetrics = function(taskmanagerid) {
    var deferred;
    deferred = $q.defer();
    $http.get(flinkConfig.jobServer + "taskmanagers/" + taskmanagerid).success(function(data, status, headers, config) {
      return deferred.resolve(data['taskmanagers']);
    });
    return deferred.promise;
  };
  return this;
}]);

//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJzb3VyY2VzIjpbImluZGV4LmNvZmZlZSIsImluZGV4LmpzIiwiY29tbW9uL2RpcmVjdGl2ZXMuY29mZmVlIiwiY29tbW9uL2RpcmVjdGl2ZXMuanMiLCJjb21tb24vZmlsdGVycy5jb2ZmZWUiLCJjb21tb24vZmlsdGVycy5qcyIsImNvbW1vbi9zZXJ2aWNlcy5jb2ZmZWUiLCJjb21tb24vc2VydmljZXMuanMiLCJtb2R1bGVzL2pvYm1hbmFnZXIvam9ibWFuYWdlci5jdHJsLmNvZmZlZSIsIm1vZHVsZXMvam9ibWFuYWdlci9qb2JtYW5hZ2VyLmN0cmwuanMiLCJtb2R1bGVzL2pvYm1hbmFnZXIvam9ibWFuYWdlci5zdmMuY29mZmVlIiwibW9kdWxlcy9qb2JtYW5hZ2VyL2pvYm1hbmFnZXIuc3ZjLmpzIiwibW9kdWxlcy9qb2JzL2pvYnMuY3RybC5jb2ZmZWUiLCJtb2R1bGVzL2pvYnMvam9icy5jdHJsLmpzIiwibW9kdWxlcy9qb2JzL2pvYnMuZGlyLmNvZmZlZSIsIm1vZHVsZXMvam9icy9qb2JzLmRpci5qcyIsIm1vZHVsZXMvam9icy9qb2JzLnN2Yy5jb2ZmZWUiLCJtb2R1bGVzL2pvYnMvam9icy5zdmMuanMiLCJtb2R1bGVzL292ZXJ2aWV3L292ZXJ2aWV3LmN0cmwuY29mZmVlIiwibW9kdWxlcy9vdmVydmlldy9vdmVydmlldy5jdHJsLmpzIiwibW9kdWxlcy9vdmVydmlldy9vdmVydmlldy5zdmMuY29mZmVlIiwibW9kdWxlcy9vdmVydmlldy9vdmVydmlldy5zdmMuanMiLCJtb2R1bGVzL3N1Ym1pdC9zdWJtaXQuY3RybC5jb2ZmZWUiLCJtb2R1bGVzL3N1Ym1pdC9zdWJtaXQuY3RybC5qcyIsIm1vZHVsZXMvc3VibWl0L3N1Ym1pdC5zdmMuY29mZmVlIiwibW9kdWxlcy9zdWJtaXQvc3VibWl0LnN2Yy5qcyIsIm1vZHVsZXMvdGFza21hbmFnZXIvdGFza21hbmFnZXIuY3RybC5jb2ZmZWUiLCJtb2R1bGVzL3Rhc2ttYW5hZ2VyL3Rhc2ttYW5hZ2VyLmN0cmwuanMiLCJtb2R1bGVzL3Rhc2ttYW5hZ2VyL3Rhc2ttYW5hZ2VyLnN2Yy5jb2ZmZWUiLCJtb2R1bGVzL3Rhc2ttYW5hZ2VyL3Rhc2ttYW5hZ2VyLnN2Yy5qcyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiQUFrQkEsUUFBUSxPQUFPLFlBQVksQ0FBQyxhQUFhLGtCQUl4QyxtQkFBSSxTQUFDLFlBQUQ7RUFDSCxXQUFXLGlCQUFpQjtFQ3JCNUIsT0RzQkEsV0FBVyxjQUFjLFdBQUE7SUFDdkIsV0FBVyxpQkFBaUIsQ0FBQyxXQUFXO0lDckJ4QyxPRHNCQSxXQUFXLGVBQWU7O0lBSTdCLE1BQU0sZUFBZTtFQUNwQixXQUFXO0VBRVgsb0JBQW9CO0dBS3JCLCtEQUFJLFNBQUMsYUFBYSxhQUFhLGFBQWEsV0FBeEM7RUM1QkgsT0Q2QkEsWUFBWSxhQUFhLEtBQUssU0FBQyxRQUFEO0lBQzVCLFFBQVEsT0FBTyxhQUFhO0lBRTVCLFlBQVk7SUM3QlosT0QrQkEsVUFBVSxXQUFBO01DOUJSLE9EK0JBLFlBQVk7T0FDWixZQUFZOztJQUtqQixpQ0FBTyxTQUFDLHVCQUFEO0VDakNOLE9Ea0NBLHNCQUFzQjtJQUl2QixnREFBTyxTQUFDLGdCQUFnQixvQkFBakI7RUFDTixlQUFlLE1BQU0sWUFDbkI7SUFBQSxLQUFLO0lBQ0wsT0FDRTtNQUFBLE1BQ0U7UUFBQSxhQUFhO1FBQ2IsWUFBWTs7O0tBRWpCLE1BQU0sZ0JBQ0w7SUFBQSxLQUFLO0lBQ0wsT0FDRTtNQUFBLE1BQ0U7UUFBQSxhQUFhO1FBQ2IsWUFBWTs7O0tBRWpCLE1BQU0sa0JBQ0w7SUFBQSxLQUFLO0lBQ0wsT0FDRTtNQUFBLE1BQ0U7UUFBQSxhQUFhO1FBQ2IsWUFBWTs7O0tBRWpCLE1BQU0sY0FDTDtJQUFBLEtBQUs7SUFDTCxVQUFVO0lBQ1YsT0FDRTtNQUFBLE1BQ0U7UUFBQSxhQUFhO1FBQ2IsWUFBWTs7O0tBRWpCLE1BQU0sbUJBQ0w7SUFBQSxLQUFLO0lBQ0wsVUFBVTtJQUNWLE9BQ0U7TUFBQSxTQUNFO1FBQUEsYUFBYTtRQUNiLFlBQVk7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ZSI6ImluZGV4LmpzIiwic291cmNlc0NvbnRlbnQiOlsiI1xuIyBMaWNlbnNlZCB0byB0aGUgQXBhY2hlIFNvZnR3YXJlIEZvdW5kYXRpb24gKEFTRikgdW5kZXIgb25lXG4jIG9yIG1vcmUgY29udHJpYnV0b3IgbGljZW5zZSBhZ3JlZW1lbnRzLiAgU2VlIHRoZSBOT1RJQ0UgZmlsZVxuIyBkaXN0cmlidXRlZCB3aXRoIHRoaXMgd29yayBmb3IgYWRkaXRpb25hbCBpbmZvcm1hdGlvblxuIyByZWdhcmRpbmcgY29weXJpZ2h0IG93bmVyc2hpcC4gIFRoZSBBU0YgbGljZW5zZXMgdGhpcyBmaWxlXG4jIHRvIHlvdSB1bmRlciB0aGUgQXBhY2hlIExpY2Vuc2UsIFZlcnNpb24gMi4wICh0aGVcbiMgXCJMaWNlbnNlXCIpOyB5b3UgbWF5IG5vdCB1c2UgdGhpcyBmaWxlIGV4Y2VwdCBpbiBjb21wbGlhbmNlXG4jIHdpdGggdGhlIExpY2Vuc2UuICBZb3UgbWF5IG9idGFpbiBhIGNvcHkgb2YgdGhlIExpY2Vuc2UgYXRcbiNcbiMgICAgIGh0dHA6Ly93d3cuYXBhY2hlLm9yZy9saWNlbnNlcy9MSUNFTlNFLTIuMFxuI1xuIyBVbmxlc3MgcmVxdWlyZWQgYnkgYXBwbGljYWJsZSBsYXcgb3IgYWdyZWVkIHRvIGluIHdyaXRpbmcsIHNvZnR3YXJlXG4jIGRpc3RyaWJ1dGVkIHVuZGVyIHRoZSBMaWNlbnNlIGlzIGRpc3RyaWJ1dGVkIG9uIGFuIFwiQVMgSVNcIiBCQVNJUyxcbiMgV0lUSE9VVCBXQVJSQU5USUVTIE9SIENPTkRJVElPTlMgT0YgQU5ZIEtJTkQsIGVpdGhlciBleHByZXNzIG9yIGltcGxpZWQuXG4jIFNlZSB0aGUgTGljZW5zZSBmb3IgdGhlIHNwZWNpZmljIGxhbmd1YWdlIGdvdmVybmluZyBwZXJtaXNzaW9ucyBhbmRcbiMgbGltaXRhdGlvbnMgdW5kZXIgdGhlIExpY2Vuc2UuXG4jXG5cbmFuZ3VsYXIubW9kdWxlKCdmbGlua0FwcCcsIFsndWkucm91dGVyJywgJ2FuZ3VsYXJNb21lbnQnXSlcblxuIyAtLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLVxuXG4ucnVuICgkcm9vdFNjb3BlKSAtPlxuICAkcm9vdFNjb3BlLnNpZGViYXJWaXNpYmxlID0gZmFsc2VcbiAgJHJvb3RTY29wZS5zaG93U2lkZWJhciA9IC0+XG4gICAgJHJvb3RTY29wZS5zaWRlYmFyVmlzaWJsZSA9ICEkcm9vdFNjb3BlLnNpZGViYXJWaXNpYmxlXG4gICAgJHJvb3RTY29wZS5zaWRlYmFyQ2xhc3MgPSAnZm9yY2Utc2hvdydcblxuIyAtLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLVxuXG4udmFsdWUgJ2ZsaW5rQ29uZmlnJywge1xuICBqb2JTZXJ2ZXI6ICcnXG4jIGpvYlNlcnZlcjogJ2h0dHA6Ly9sb2NhbGhvc3Q6ODA4MS8nXG4gIFwicmVmcmVzaC1pbnRlcnZhbFwiOiAxMDAwMFxufVxuXG4jIC0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tXG5cbi5ydW4gKEpvYnNTZXJ2aWNlLCBNYWluU2VydmljZSwgZmxpbmtDb25maWcsICRpbnRlcnZhbCkgLT5cbiAgTWFpblNlcnZpY2UubG9hZENvbmZpZygpLnRoZW4gKGNvbmZpZykgLT5cbiAgICBhbmd1bGFyLmV4dGVuZCBmbGlua0NvbmZpZywgY29uZmlnXG5cbiAgICBKb2JzU2VydmljZS5saXN0Sm9icygpXG5cbiAgICAkaW50ZXJ2YWwgLT5cbiAgICAgIEpvYnNTZXJ2aWNlLmxpc3RKb2JzKClcbiAgICAsIGZsaW5rQ29uZmlnW1wicmVmcmVzaC1pbnRlcnZhbFwiXVxuXG5cbiMgLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS1cblxuLmNvbmZpZyAoJHVpVmlld1Njcm9sbFByb3ZpZGVyKSAtPlxuICAkdWlWaWV3U2Nyb2xsUHJvdmlkZXIudXNlQW5jaG9yU2Nyb2xsKClcblxuIyAtLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLVxuXG4uY29uZmlnICgkc3RhdGVQcm92aWRlciwgJHVybFJvdXRlclByb3ZpZGVyKSAtPlxuICAkc3RhdGVQcm92aWRlci5zdGF0ZSBcIm92ZXJ2aWV3XCIsXG4gICAgdXJsOiBcIi9vdmVydmlld1wiXG4gICAgdmlld3M6XG4gICAgICBtYWluOlxuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9vdmVydmlldy5odG1sXCJcbiAgICAgICAgY29udHJvbGxlcjogJ092ZXJ2aWV3Q29udHJvbGxlcidcblxuICAuc3RhdGUgXCJydW5uaW5nLWpvYnNcIixcbiAgICB1cmw6IFwiL3J1bm5pbmctam9ic1wiXG4gICAgdmlld3M6XG4gICAgICBtYWluOlxuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JzL3J1bm5pbmctam9icy5odG1sXCJcbiAgICAgICAgY29udHJvbGxlcjogJ1J1bm5pbmdKb2JzQ29udHJvbGxlcidcblxuICAuc3RhdGUgXCJjb21wbGV0ZWQtam9ic1wiLFxuICAgIHVybDogXCIvY29tcGxldGVkLWpvYnNcIlxuICAgIHZpZXdzOlxuICAgICAgbWFpbjpcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvam9icy9jb21wbGV0ZWQtam9icy5odG1sXCJcbiAgICAgICAgY29udHJvbGxlcjogJ0NvbXBsZXRlZEpvYnNDb250cm9sbGVyJ1xuXG4gIC5zdGF0ZSBcInNpbmdsZS1qb2JcIixcbiAgICB1cmw6IFwiL2pvYnMve2pvYmlkfVwiXG4gICAgYWJzdHJhY3Q6IHRydWVcbiAgICB2aWV3czpcbiAgICAgIG1haW46XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYnMvam9iLmh0bWxcIlxuICAgICAgICBjb250cm9sbGVyOiAnU2luZ2xlSm9iQ29udHJvbGxlcidcblxuICAuc3RhdGUgXCJzaW5nbGUtam9iLnBsYW5cIixcbiAgICB1cmw6IFwiXCJcbiAgICBhYnN0cmFjdDogdHJ1ZVxuICAgIHZpZXdzOlxuICAgICAgZGV0YWlsczpcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvam9icy9qb2IucGxhbi5odG1sXCJcbiAgICAgICAgY29udHJvbGxlcjogJ0pvYlBsYW5Db250cm9sbGVyJ1xuXG4gIC5zdGF0ZSBcInNpbmdsZS1qb2IucGxhbi5vdmVydmlld1wiLFxuICAgIHVybDogXCJcIlxuICAgIHZpZXdzOlxuICAgICAgJ25vZGUtZGV0YWlscyc6XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYnMvam9iLnBsYW4ubm9kZS1saXN0Lm92ZXJ2aWV3Lmh0bWxcIlxuICAgICAgICBjb250cm9sbGVyOiAnSm9iUGxhbk92ZXJ2aWV3Q29udHJvbGxlcidcblxuICAuc3RhdGUgXCJzaW5nbGUtam9iLnBsYW4uYWNjdW11bGF0b3JzXCIsXG4gICAgdXJsOiBcIi9hY2N1bXVsYXRvcnNcIlxuICAgIHZpZXdzOlxuICAgICAgJ25vZGUtZGV0YWlscyc6XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYnMvam9iLnBsYW4ubm9kZS1saXN0LmFjY3VtdWxhdG9ycy5odG1sXCJcbiAgICAgICAgY29udHJvbGxlcjogJ0pvYlBsYW5BY2N1bXVsYXRvcnNDb250cm9sbGVyJ1xuXG4gIC5zdGF0ZSBcInNpbmdsZS1qb2IucGxhbi5jaGVja3BvaW50c1wiLFxuICAgIHVybDogXCIvY2hlY2twb2ludHNcIlxuICAgIHZpZXdzOlxuICAgICAgJ25vZGUtZGV0YWlscyc6XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYnMvam9iLnBsYW4ubm9kZS1saXN0LmNoZWNrcG9pbnRzLmh0bWxcIlxuICAgICAgICBjb250cm9sbGVyOiAnSm9iUGxhbkNoZWNrcG9pbnRzQ29udHJvbGxlcidcblxuICAuc3RhdGUgXCJzaW5nbGUtam9iLnBsYW4uYmFja3ByZXNzdXJlXCIsXG4gICAgdXJsOiBcIi9iYWNrcHJlc3N1cmVcIlxuICAgIHZpZXdzOlxuICAgICAgJ25vZGUtZGV0YWlscyc6XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYnMvam9iLnBsYW4ubm9kZS1saXN0LmJhY2twcmVzc3VyZS5odG1sXCJcbiAgICAgICAgY29udHJvbGxlcjogJ0pvYlBsYW5CYWNrUHJlc3N1cmVDb250cm9sbGVyJ1xuXG4gIC5zdGF0ZSBcInNpbmdsZS1qb2IudGltZWxpbmVcIixcbiAgICB1cmw6IFwiL3RpbWVsaW5lXCJcbiAgICB2aWV3czpcbiAgICAgIGRldGFpbHM6XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYnMvam9iLnRpbWVsaW5lLmh0bWxcIlxuXG4gIC5zdGF0ZSBcInNpbmdsZS1qb2IudGltZWxpbmUudmVydGV4XCIsXG4gICAgdXJsOiBcIi97dmVydGV4SWR9XCJcbiAgICB2aWV3czpcbiAgICAgIHZlcnRleDpcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvam9icy9qb2IudGltZWxpbmUudmVydGV4Lmh0bWxcIlxuICAgICAgICBjb250cm9sbGVyOiAnSm9iVGltZWxpbmVWZXJ0ZXhDb250cm9sbGVyJ1xuXG4gIC5zdGF0ZSBcInNpbmdsZS1qb2IuZXhjZXB0aW9uc1wiLFxuICAgIHVybDogXCIvZXhjZXB0aW9uc1wiXG4gICAgdmlld3M6XG4gICAgICBkZXRhaWxzOlxuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JzL2pvYi5leGNlcHRpb25zLmh0bWxcIlxuICAgICAgICBjb250cm9sbGVyOiAnSm9iRXhjZXB0aW9uc0NvbnRyb2xsZXInXG5cbiAgLnN0YXRlIFwic2luZ2xlLWpvYi5wcm9wZXJ0aWVzXCIsXG4gICAgdXJsOiBcIi9wcm9wZXJ0aWVzXCJcbiAgICB2aWV3czpcbiAgICAgIGRldGFpbHM6XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYnMvam9iLnByb3BlcnRpZXMuaHRtbFwiXG4gICAgICAgIGNvbnRyb2xsZXI6ICdKb2JQcm9wZXJ0aWVzQ29udHJvbGxlcidcblxuICAuc3RhdGUgXCJzaW5nbGUtam9iLmNvbmZpZ1wiLFxuICAgIHVybDogXCIvY29uZmlnXCJcbiAgICB2aWV3czpcbiAgICAgIGRldGFpbHM6XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYnMvam9iLmNvbmZpZy5odG1sXCJcblxuICAuc3RhdGUgXCJhbGwtbWFuYWdlclwiLFxuICAgIHVybDogXCIvdGFza21hbmFnZXJzXCJcbiAgICB2aWV3czpcbiAgICAgIG1haW46XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL3Rhc2ttYW5hZ2VyL2luZGV4Lmh0bWxcIlxuICAgICAgICBjb250cm9sbGVyOiAnQWxsVGFza01hbmFnZXJzQ29udHJvbGxlcidcblxuICAuc3RhdGUgXCJzaW5nbGUtbWFuYWdlclwiLFxuICAgICAgdXJsOiBcIi90YXNrbWFuYWdlci97dGFza21hbmFnZXJpZH1cIlxuICAgICAgdmlld3M6XG4gICAgICAgIG1haW46XG4gICAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvdGFza21hbmFnZXIvdGFza21hbmFnZXIuaHRtbFwiXG4gICAgICAgICAgY29udHJvbGxlcjogJ1NpbmdsZVRhc2tNYW5hZ2VyQ29udHJvbGxlcidcblxuICAuc3RhdGUgXCJzaW5nbGUtbWFuYWdlci5tZXRyaWNzXCIsXG4gICAgdXJsOiBcIi9tZXRyaWNzXCJcbiAgICB2aWV3czpcbiAgICAgIGRldGFpbHM6XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL3Rhc2ttYW5hZ2VyL3Rhc2ttYW5hZ2VyLm1ldHJpY3MuaHRtbFwiXG5cbiAgLnN0YXRlIFwiam9ibWFuYWdlclwiLFxuICAgICAgdXJsOiBcIi9qb2JtYW5hZ2VyXCJcbiAgICAgIHZpZXdzOlxuICAgICAgICBtYWluOlxuICAgICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYm1hbmFnZXIvaW5kZXguaHRtbFwiXG5cbiAgLnN0YXRlIFwiam9ibWFuYWdlci5jb25maWdcIixcbiAgICB1cmw6IFwiL2NvbmZpZ1wiXG4gICAgdmlld3M6XG4gICAgICBkZXRhaWxzOlxuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JtYW5hZ2VyL2NvbmZpZy5odG1sXCJcbiAgICAgICAgY29udHJvbGxlcjogJ0pvYk1hbmFnZXJDb25maWdDb250cm9sbGVyJ1xuXG4gIC5zdGF0ZSBcImpvYm1hbmFnZXIuc3Rkb3V0XCIsXG4gICAgdXJsOiBcIi9zdGRvdXRcIlxuICAgIHZpZXdzOlxuICAgICAgZGV0YWlsczpcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvam9ibWFuYWdlci9zdGRvdXQuaHRtbFwiXG4gICAgICAgIGNvbnRyb2xsZXI6ICdKb2JNYW5hZ2VyU3Rkb3V0Q29udHJvbGxlcidcblxuICAuc3RhdGUgXCJqb2JtYW5hZ2VyLmxvZ1wiLFxuICAgIHVybDogXCIvbG9nXCJcbiAgICB2aWV3czpcbiAgICAgIGRldGFpbHM6XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYm1hbmFnZXIvbG9nLmh0bWxcIlxuICAgICAgICBjb250cm9sbGVyOiAnSm9iTWFuYWdlckxvZ3NDb250cm9sbGVyJ1xuXG4gIC5zdGF0ZSBcInN1Ym1pdFwiLFxuICAgICAgdXJsOiBcIi9zdWJtaXRcIlxuICAgICAgdmlld3M6XG4gICAgICAgIG1haW46XG4gICAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvc3VibWl0Lmh0bWxcIlxuICAgICAgICAgIGNvbnRyb2xsZXI6IFwiSm9iU3VibWl0Q29udHJvbGxlclwiXG5cbiAgJHVybFJvdXRlclByb3ZpZGVyLm90aGVyd2lzZSBcIi9vdmVydmlld1wiXG4iLCJhbmd1bGFyLm1vZHVsZSgnZmxpbmtBcHAnLCBbJ3VpLnJvdXRlcicsICdhbmd1bGFyTW9tZW50J10pLnJ1bihmdW5jdGlvbigkcm9vdFNjb3BlKSB7XG4gICRyb290U2NvcGUuc2lkZWJhclZpc2libGUgPSBmYWxzZTtcbiAgcmV0dXJuICRyb290U2NvcGUuc2hvd1NpZGViYXIgPSBmdW5jdGlvbigpIHtcbiAgICAkcm9vdFNjb3BlLnNpZGViYXJWaXNpYmxlID0gISRyb290U2NvcGUuc2lkZWJhclZpc2libGU7XG4gICAgcmV0dXJuICRyb290U2NvcGUuc2lkZWJhckNsYXNzID0gJ2ZvcmNlLXNob3cnO1xuICB9O1xufSkudmFsdWUoJ2ZsaW5rQ29uZmlnJywge1xuICBqb2JTZXJ2ZXI6ICcnLFxuICBcInJlZnJlc2gtaW50ZXJ2YWxcIjogMTAwMDBcbn0pLnJ1bihmdW5jdGlvbihKb2JzU2VydmljZSwgTWFpblNlcnZpY2UsIGZsaW5rQ29uZmlnLCAkaW50ZXJ2YWwpIHtcbiAgcmV0dXJuIE1haW5TZXJ2aWNlLmxvYWRDb25maWcoKS50aGVuKGZ1bmN0aW9uKGNvbmZpZykge1xuICAgIGFuZ3VsYXIuZXh0ZW5kKGZsaW5rQ29uZmlnLCBjb25maWcpO1xuICAgIEpvYnNTZXJ2aWNlLmxpc3RKb2JzKCk7XG4gICAgcmV0dXJuICRpbnRlcnZhbChmdW5jdGlvbigpIHtcbiAgICAgIHJldHVybiBKb2JzU2VydmljZS5saXN0Sm9icygpO1xuICAgIH0sIGZsaW5rQ29uZmlnW1wicmVmcmVzaC1pbnRlcnZhbFwiXSk7XG4gIH0pO1xufSkuY29uZmlnKGZ1bmN0aW9uKCR1aVZpZXdTY3JvbGxQcm92aWRlcikge1xuICByZXR1cm4gJHVpVmlld1Njcm9sbFByb3ZpZGVyLnVzZUFuY2hvclNjcm9sbCgpO1xufSkuY29uZmlnKGZ1bmN0aW9uKCRzdGF0ZVByb3ZpZGVyLCAkdXJsUm91dGVyUHJvdmlkZXIpIHtcbiAgJHN0YXRlUHJvdmlkZXIuc3RhdGUoXCJvdmVydmlld1wiLCB7XG4gICAgdXJsOiBcIi9vdmVydmlld1wiLFxuICAgIHZpZXdzOiB7XG4gICAgICBtYWluOiB7XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL292ZXJ2aWV3Lmh0bWxcIixcbiAgICAgICAgY29udHJvbGxlcjogJ092ZXJ2aWV3Q29udHJvbGxlcidcbiAgICAgIH1cbiAgICB9XG4gIH0pLnN0YXRlKFwicnVubmluZy1qb2JzXCIsIHtcbiAgICB1cmw6IFwiL3J1bm5pbmctam9ic1wiLFxuICAgIHZpZXdzOiB7XG4gICAgICBtYWluOiB7XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYnMvcnVubmluZy1qb2JzLmh0bWxcIixcbiAgICAgICAgY29udHJvbGxlcjogJ1J1bm5pbmdKb2JzQ29udHJvbGxlcidcbiAgICAgIH1cbiAgICB9XG4gIH0pLnN0YXRlKFwiY29tcGxldGVkLWpvYnNcIiwge1xuICAgIHVybDogXCIvY29tcGxldGVkLWpvYnNcIixcbiAgICB2aWV3czoge1xuICAgICAgbWFpbjoge1xuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JzL2NvbXBsZXRlZC1qb2JzLmh0bWxcIixcbiAgICAgICAgY29udHJvbGxlcjogJ0NvbXBsZXRlZEpvYnNDb250cm9sbGVyJ1xuICAgICAgfVxuICAgIH1cbiAgfSkuc3RhdGUoXCJzaW5nbGUtam9iXCIsIHtcbiAgICB1cmw6IFwiL2pvYnMve2pvYmlkfVwiLFxuICAgIGFic3RyYWN0OiB0cnVlLFxuICAgIHZpZXdzOiB7XG4gICAgICBtYWluOiB7XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYnMvam9iLmh0bWxcIixcbiAgICAgICAgY29udHJvbGxlcjogJ1NpbmdsZUpvYkNvbnRyb2xsZXInXG4gICAgICB9XG4gICAgfVxuICB9KS5zdGF0ZShcInNpbmdsZS1qb2IucGxhblwiLCB7XG4gICAgdXJsOiBcIlwiLFxuICAgIGFic3RyYWN0OiB0cnVlLFxuICAgIHZpZXdzOiB7XG4gICAgICBkZXRhaWxzOiB7XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYnMvam9iLnBsYW4uaHRtbFwiLFxuICAgICAgICBjb250cm9sbGVyOiAnSm9iUGxhbkNvbnRyb2xsZXInXG4gICAgICB9XG4gICAgfVxuICB9KS5zdGF0ZShcInNpbmdsZS1qb2IucGxhbi5vdmVydmlld1wiLCB7XG4gICAgdXJsOiBcIlwiLFxuICAgIHZpZXdzOiB7XG4gICAgICAnbm9kZS1kZXRhaWxzJzoge1xuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JzL2pvYi5wbGFuLm5vZGUtbGlzdC5vdmVydmlldy5odG1sXCIsXG4gICAgICAgIGNvbnRyb2xsZXI6ICdKb2JQbGFuT3ZlcnZpZXdDb250cm9sbGVyJ1xuICAgICAgfVxuICAgIH1cbiAgfSkuc3RhdGUoXCJzaW5nbGUtam9iLnBsYW4uYWNjdW11bGF0b3JzXCIsIHtcbiAgICB1cmw6IFwiL2FjY3VtdWxhdG9yc1wiLFxuICAgIHZpZXdzOiB7XG4gICAgICAnbm9kZS1kZXRhaWxzJzoge1xuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JzL2pvYi5wbGFuLm5vZGUtbGlzdC5hY2N1bXVsYXRvcnMuaHRtbFwiLFxuICAgICAgICBjb250cm9sbGVyOiAnSm9iUGxhbkFjY3VtdWxhdG9yc0NvbnRyb2xsZXInXG4gICAgICB9XG4gICAgfVxuICB9KS5zdGF0ZShcInNpbmdsZS1qb2IucGxhbi5jaGVja3BvaW50c1wiLCB7XG4gICAgdXJsOiBcIi9jaGVja3BvaW50c1wiLFxuICAgIHZpZXdzOiB7XG4gICAgICAnbm9kZS1kZXRhaWxzJzoge1xuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JzL2pvYi5wbGFuLm5vZGUtbGlzdC5jaGVja3BvaW50cy5odG1sXCIsXG4gICAgICAgIGNvbnRyb2xsZXI6ICdKb2JQbGFuQ2hlY2twb2ludHNDb250cm9sbGVyJ1xuICAgICAgfVxuICAgIH1cbiAgfSkuc3RhdGUoXCJzaW5nbGUtam9iLnBsYW4uYmFja3ByZXNzdXJlXCIsIHtcbiAgICB1cmw6IFwiL2JhY2twcmVzc3VyZVwiLFxuICAgIHZpZXdzOiB7XG4gICAgICAnbm9kZS1kZXRhaWxzJzoge1xuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JzL2pvYi5wbGFuLm5vZGUtbGlzdC5iYWNrcHJlc3N1cmUuaHRtbFwiLFxuICAgICAgICBjb250cm9sbGVyOiAnSm9iUGxhbkJhY2tQcmVzc3VyZUNvbnRyb2xsZXInXG4gICAgICB9XG4gICAgfVxuICB9KS5zdGF0ZShcInNpbmdsZS1qb2IudGltZWxpbmVcIiwge1xuICAgIHVybDogXCIvdGltZWxpbmVcIixcbiAgICB2aWV3czoge1xuICAgICAgZGV0YWlsczoge1xuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JzL2pvYi50aW1lbGluZS5odG1sXCJcbiAgICAgIH1cbiAgICB9XG4gIH0pLnN0YXRlKFwic2luZ2xlLWpvYi50aW1lbGluZS52ZXJ0ZXhcIiwge1xuICAgIHVybDogXCIve3ZlcnRleElkfVwiLFxuICAgIHZpZXdzOiB7XG4gICAgICB2ZXJ0ZXg6IHtcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvam9icy9qb2IudGltZWxpbmUudmVydGV4Lmh0bWxcIixcbiAgICAgICAgY29udHJvbGxlcjogJ0pvYlRpbWVsaW5lVmVydGV4Q29udHJvbGxlcidcbiAgICAgIH1cbiAgICB9XG4gIH0pLnN0YXRlKFwic2luZ2xlLWpvYi5leGNlcHRpb25zXCIsIHtcbiAgICB1cmw6IFwiL2V4Y2VwdGlvbnNcIixcbiAgICB2aWV3czoge1xuICAgICAgZGV0YWlsczoge1xuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JzL2pvYi5leGNlcHRpb25zLmh0bWxcIixcbiAgICAgICAgY29udHJvbGxlcjogJ0pvYkV4Y2VwdGlvbnNDb250cm9sbGVyJ1xuICAgICAgfVxuICAgIH1cbiAgfSkuc3RhdGUoXCJzaW5nbGUtam9iLnByb3BlcnRpZXNcIiwge1xuICAgIHVybDogXCIvcHJvcGVydGllc1wiLFxuICAgIHZpZXdzOiB7XG4gICAgICBkZXRhaWxzOiB7XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYnMvam9iLnByb3BlcnRpZXMuaHRtbFwiLFxuICAgICAgICBjb250cm9sbGVyOiAnSm9iUHJvcGVydGllc0NvbnRyb2xsZXInXG4gICAgICB9XG4gICAgfVxuICB9KS5zdGF0ZShcInNpbmdsZS1qb2IuY29uZmlnXCIsIHtcbiAgICB1cmw6IFwiL2NvbmZpZ1wiLFxuICAgIHZpZXdzOiB7XG4gICAgICBkZXRhaWxzOiB7XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYnMvam9iLmNvbmZpZy5odG1sXCJcbiAgICAgIH1cbiAgICB9XG4gIH0pLnN0YXRlKFwiYWxsLW1hbmFnZXJcIiwge1xuICAgIHVybDogXCIvdGFza21hbmFnZXJzXCIsXG4gICAgdmlld3M6IHtcbiAgICAgIG1haW46IHtcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvdGFza21hbmFnZXIvaW5kZXguaHRtbFwiLFxuICAgICAgICBjb250cm9sbGVyOiAnQWxsVGFza01hbmFnZXJzQ29udHJvbGxlcidcbiAgICAgIH1cbiAgICB9XG4gIH0pLnN0YXRlKFwic2luZ2xlLW1hbmFnZXJcIiwge1xuICAgIHVybDogXCIvdGFza21hbmFnZXIve3Rhc2ttYW5hZ2VyaWR9XCIsXG4gICAgdmlld3M6IHtcbiAgICAgIG1haW46IHtcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvdGFza21hbmFnZXIvdGFza21hbmFnZXIuaHRtbFwiLFxuICAgICAgICBjb250cm9sbGVyOiAnU2luZ2xlVGFza01hbmFnZXJDb250cm9sbGVyJ1xuICAgICAgfVxuICAgIH1cbiAgfSkuc3RhdGUoXCJzaW5nbGUtbWFuYWdlci5tZXRyaWNzXCIsIHtcbiAgICB1cmw6IFwiL21ldHJpY3NcIixcbiAgICB2aWV3czoge1xuICAgICAgZGV0YWlsczoge1xuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy90YXNrbWFuYWdlci90YXNrbWFuYWdlci5tZXRyaWNzLmh0bWxcIlxuICAgICAgfVxuICAgIH1cbiAgfSkuc3RhdGUoXCJqb2JtYW5hZ2VyXCIsIHtcbiAgICB1cmw6IFwiL2pvYm1hbmFnZXJcIixcbiAgICB2aWV3czoge1xuICAgICAgbWFpbjoge1xuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JtYW5hZ2VyL2luZGV4Lmh0bWxcIlxuICAgICAgfVxuICAgIH1cbiAgfSkuc3RhdGUoXCJqb2JtYW5hZ2VyLmNvbmZpZ1wiLCB7XG4gICAgdXJsOiBcIi9jb25maWdcIixcbiAgICB2aWV3czoge1xuICAgICAgZGV0YWlsczoge1xuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JtYW5hZ2VyL2NvbmZpZy5odG1sXCIsXG4gICAgICAgIGNvbnRyb2xsZXI6ICdKb2JNYW5hZ2VyQ29uZmlnQ29udHJvbGxlcidcbiAgICAgIH1cbiAgICB9XG4gIH0pLnN0YXRlKFwiam9ibWFuYWdlci5zdGRvdXRcIiwge1xuICAgIHVybDogXCIvc3Rkb3V0XCIsXG4gICAgdmlld3M6IHtcbiAgICAgIGRldGFpbHM6IHtcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvam9ibWFuYWdlci9zdGRvdXQuaHRtbFwiLFxuICAgICAgICBjb250cm9sbGVyOiAnSm9iTWFuYWdlclN0ZG91dENvbnRyb2xsZXInXG4gICAgICB9XG4gICAgfVxuICB9KS5zdGF0ZShcImpvYm1hbmFnZXIubG9nXCIsIHtcbiAgICB1cmw6IFwiL2xvZ1wiLFxuICAgIHZpZXdzOiB7XG4gICAgICBkZXRhaWxzOiB7XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYm1hbmFnZXIvbG9nLmh0bWxcIixcbiAgICAgICAgY29udHJvbGxlcjogJ0pvYk1hbmFnZXJMb2dzQ29udHJvbGxlcidcbiAgICAgIH1cbiAgICB9XG4gIH0pLnN0YXRlKFwic3VibWl0XCIsIHtcbiAgICB1cmw6IFwiL3N1Ym1pdFwiLFxuICAgIHZpZXdzOiB7XG4gICAgICBtYWluOiB7XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL3N1Ym1pdC5odG1sXCIsXG4gICAgICAgIGNvbnRyb2xsZXI6IFwiSm9iU3VibWl0Q29udHJvbGxlclwiXG4gICAgICB9XG4gICAgfVxuICB9KTtcbiAgcmV0dXJuICR1cmxSb3V0ZXJQcm92aWRlci5vdGhlcndpc2UoXCIvb3ZlcnZpZXdcIik7XG59KTtcbiIsIiNcbiMgTGljZW5zZWQgdG8gdGhlIEFwYWNoZSBTb2Z0d2FyZSBGb3VuZGF0aW9uIChBU0YpIHVuZGVyIG9uZVxuIyBvciBtb3JlIGNvbnRyaWJ1dG9yIGxpY2Vuc2UgYWdyZWVtZW50cy4gIFNlZSB0aGUgTk9USUNFIGZpbGVcbiMgZGlzdHJpYnV0ZWQgd2l0aCB0aGlzIHdvcmsgZm9yIGFkZGl0aW9uYWwgaW5mb3JtYXRpb25cbiMgcmVnYXJkaW5nIGNvcHlyaWdodCBvd25lcnNoaXAuICBUaGUgQVNGIGxpY2Vuc2VzIHRoaXMgZmlsZVxuIyB0byB5b3UgdW5kZXIgdGhlIEFwYWNoZSBMaWNlbnNlLCBWZXJzaW9uIDIuMCAodGhlXG4jIFwiTGljZW5zZVwiKTsgeW91IG1heSBub3QgdXNlIHRoaXMgZmlsZSBleGNlcHQgaW4gY29tcGxpYW5jZVxuIyB3aXRoIHRoZSBMaWNlbnNlLiAgWW91IG1heSBvYnRhaW4gYSBjb3B5IG9mIHRoZSBMaWNlbnNlIGF0XG4jXG4jICAgICBodHRwOi8vd3d3LmFwYWNoZS5vcmcvbGljZW5zZXMvTElDRU5TRS0yLjBcbiNcbiMgVW5sZXNzIHJlcXVpcmVkIGJ5IGFwcGxpY2FibGUgbGF3IG9yIGFncmVlZCB0byBpbiB3cml0aW5nLCBzb2Z0d2FyZVxuIyBkaXN0cmlidXRlZCB1bmRlciB0aGUgTGljZW5zZSBpcyBkaXN0cmlidXRlZCBvbiBhbiBcIkFTIElTXCIgQkFTSVMsXG4jIFdJVEhPVVQgV0FSUkFOVElFUyBPUiBDT05ESVRJT05TIE9GIEFOWSBLSU5ELCBlaXRoZXIgZXhwcmVzcyBvciBpbXBsaWVkLlxuIyBTZWUgdGhlIExpY2Vuc2UgZm9yIHRoZSBzcGVjaWZpYyBsYW5ndWFnZSBnb3Zlcm5pbmcgcGVybWlzc2lvbnMgYW5kXG4jIGxpbWl0YXRpb25zIHVuZGVyIHRoZSBMaWNlbnNlLlxuI1xuXG5hbmd1bGFyLm1vZHVsZSgnZmxpbmtBcHAnKVxuXG4jIC0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS1cblxuLmRpcmVjdGl2ZSAnYnNMYWJlbCcsIChKb2JzU2VydmljZSkgLT5cbiAgdHJhbnNjbHVkZTogdHJ1ZVxuICByZXBsYWNlOiB0cnVlXG4gIHNjb3BlOiBcbiAgICBnZXRMYWJlbENsYXNzOiBcIiZcIlxuICAgIHN0YXR1czogXCJAXCJcblxuICB0ZW1wbGF0ZTogXCI8c3BhbiB0aXRsZT0ne3tzdGF0dXN9fScgbmctY2xhc3M9J2dldExhYmVsQ2xhc3MoKSc+PG5nLXRyYW5zY2x1ZGU+PC9uZy10cmFuc2NsdWRlPjwvc3Bhbj5cIlxuICBcbiAgbGluazogKHNjb3BlLCBlbGVtZW50LCBhdHRycykgLT5cbiAgICBzY29wZS5nZXRMYWJlbENsYXNzID0gLT5cbiAgICAgICdsYWJlbCBsYWJlbC0nICsgSm9ic1NlcnZpY2UudHJhbnNsYXRlTGFiZWxTdGF0ZShhdHRycy5zdGF0dXMpXG5cbiMgLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLVxuXG4uZGlyZWN0aXZlICdicExhYmVsJywgKEpvYnNTZXJ2aWNlKSAtPlxuICB0cmFuc2NsdWRlOiB0cnVlXG4gIHJlcGxhY2U6IHRydWVcbiAgc2NvcGU6XG4gICAgZ2V0QmFja1ByZXNzdXJlTGFiZWxDbGFzczogXCImXCJcbiAgICBzdGF0dXM6IFwiQFwiXG5cbiAgdGVtcGxhdGU6IFwiPHNwYW4gdGl0bGU9J3t7c3RhdHVzfX0nIG5nLWNsYXNzPSdnZXRCYWNrUHJlc3N1cmVMYWJlbENsYXNzKCknPjxuZy10cmFuc2NsdWRlPjwvbmctdHJhbnNjbHVkZT48L3NwYW4+XCJcblxuICBsaW5rOiAoc2NvcGUsIGVsZW1lbnQsIGF0dHJzKSAtPlxuICAgIHNjb3BlLmdldEJhY2tQcmVzc3VyZUxhYmVsQ2xhc3MgPSAtPlxuICAgICAgJ2xhYmVsIGxhYmVsLScgKyBKb2JzU2VydmljZS50cmFuc2xhdGVCYWNrUHJlc3N1cmVMYWJlbFN0YXRlKGF0dHJzLnN0YXR1cylcblxuIyAtLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tXG5cbi5kaXJlY3RpdmUgJ2luZGljYXRvclByaW1hcnknLCAoSm9ic1NlcnZpY2UpIC0+XG4gIHJlcGxhY2U6IHRydWVcbiAgc2NvcGU6IFxuICAgIGdldExhYmVsQ2xhc3M6IFwiJlwiXG4gICAgc3RhdHVzOiAnQCdcblxuICB0ZW1wbGF0ZTogXCI8aSB0aXRsZT0ne3tzdGF0dXN9fScgbmctY2xhc3M9J2dldExhYmVsQ2xhc3MoKScgLz5cIlxuICBcbiAgbGluazogKHNjb3BlLCBlbGVtZW50LCBhdHRycykgLT5cbiAgICBzY29wZS5nZXRMYWJlbENsYXNzID0gLT5cbiAgICAgICdmYSBmYS1jaXJjbGUgaW5kaWNhdG9yIGluZGljYXRvci0nICsgSm9ic1NlcnZpY2UudHJhbnNsYXRlTGFiZWxTdGF0ZShhdHRycy5zdGF0dXMpXG5cbiMgLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLVxuXG4uZGlyZWN0aXZlICd0YWJsZVByb3BlcnR5JywgLT5cbiAgcmVwbGFjZTogdHJ1ZVxuICBzY29wZTpcbiAgICB2YWx1ZTogJz0nXG5cbiAgdGVtcGxhdGU6IFwiPHRkIHRpdGxlPVxcXCJ7e3ZhbHVlIHx8ICdOb25lJ319XFxcIj57e3ZhbHVlIHx8ICdOb25lJ319PC90ZD5cIlxuIiwiYW5ndWxhci5tb2R1bGUoJ2ZsaW5rQXBwJykuZGlyZWN0aXZlKCdic0xhYmVsJywgZnVuY3Rpb24oSm9ic1NlcnZpY2UpIHtcbiAgcmV0dXJuIHtcbiAgICB0cmFuc2NsdWRlOiB0cnVlLFxuICAgIHJlcGxhY2U6IHRydWUsXG4gICAgc2NvcGU6IHtcbiAgICAgIGdldExhYmVsQ2xhc3M6IFwiJlwiLFxuICAgICAgc3RhdHVzOiBcIkBcIlxuICAgIH0sXG4gICAgdGVtcGxhdGU6IFwiPHNwYW4gdGl0bGU9J3t7c3RhdHVzfX0nIG5nLWNsYXNzPSdnZXRMYWJlbENsYXNzKCknPjxuZy10cmFuc2NsdWRlPjwvbmctdHJhbnNjbHVkZT48L3NwYW4+XCIsXG4gICAgbGluazogZnVuY3Rpb24oc2NvcGUsIGVsZW1lbnQsIGF0dHJzKSB7XG4gICAgICByZXR1cm4gc2NvcGUuZ2V0TGFiZWxDbGFzcyA9IGZ1bmN0aW9uKCkge1xuICAgICAgICByZXR1cm4gJ2xhYmVsIGxhYmVsLScgKyBKb2JzU2VydmljZS50cmFuc2xhdGVMYWJlbFN0YXRlKGF0dHJzLnN0YXR1cyk7XG4gICAgICB9O1xuICAgIH1cbiAgfTtcbn0pLmRpcmVjdGl2ZSgnYnBMYWJlbCcsIGZ1bmN0aW9uKEpvYnNTZXJ2aWNlKSB7XG4gIHJldHVybiB7XG4gICAgdHJhbnNjbHVkZTogdHJ1ZSxcbiAgICByZXBsYWNlOiB0cnVlLFxuICAgIHNjb3BlOiB7XG4gICAgICBnZXRCYWNrUHJlc3N1cmVMYWJlbENsYXNzOiBcIiZcIixcbiAgICAgIHN0YXR1czogXCJAXCJcbiAgICB9LFxuICAgIHRlbXBsYXRlOiBcIjxzcGFuIHRpdGxlPSd7e3N0YXR1c319JyBuZy1jbGFzcz0nZ2V0QmFja1ByZXNzdXJlTGFiZWxDbGFzcygpJz48bmctdHJhbnNjbHVkZT48L25nLXRyYW5zY2x1ZGU+PC9zcGFuPlwiLFxuICAgIGxpbms6IGZ1bmN0aW9uKHNjb3BlLCBlbGVtZW50LCBhdHRycykge1xuICAgICAgcmV0dXJuIHNjb3BlLmdldEJhY2tQcmVzc3VyZUxhYmVsQ2xhc3MgPSBmdW5jdGlvbigpIHtcbiAgICAgICAgcmV0dXJuICdsYWJlbCBsYWJlbC0nICsgSm9ic1NlcnZpY2UudHJhbnNsYXRlQmFja1ByZXNzdXJlTGFiZWxTdGF0ZShhdHRycy5zdGF0dXMpO1xuICAgICAgfTtcbiAgICB9XG4gIH07XG59KS5kaXJlY3RpdmUoJ2luZGljYXRvclByaW1hcnknLCBmdW5jdGlvbihKb2JzU2VydmljZSkge1xuICByZXR1cm4ge1xuICAgIHJlcGxhY2U6IHRydWUsXG4gICAgc2NvcGU6IHtcbiAgICAgIGdldExhYmVsQ2xhc3M6IFwiJlwiLFxuICAgICAgc3RhdHVzOiAnQCdcbiAgICB9LFxuICAgIHRlbXBsYXRlOiBcIjxpIHRpdGxlPSd7e3N0YXR1c319JyBuZy1jbGFzcz0nZ2V0TGFiZWxDbGFzcygpJyAvPlwiLFxuICAgIGxpbms6IGZ1bmN0aW9uKHNjb3BlLCBlbGVtZW50LCBhdHRycykge1xuICAgICAgcmV0dXJuIHNjb3BlLmdldExhYmVsQ2xhc3MgPSBmdW5jdGlvbigpIHtcbiAgICAgICAgcmV0dXJuICdmYSBmYS1jaXJjbGUgaW5kaWNhdG9yIGluZGljYXRvci0nICsgSm9ic1NlcnZpY2UudHJhbnNsYXRlTGFiZWxTdGF0ZShhdHRycy5zdGF0dXMpO1xuICAgICAgfTtcbiAgICB9XG4gIH07XG59KS5kaXJlY3RpdmUoJ3RhYmxlUHJvcGVydHknLCBmdW5jdGlvbigpIHtcbiAgcmV0dXJuIHtcbiAgICByZXBsYWNlOiB0cnVlLFxuICAgIHNjb3BlOiB7XG4gICAgICB2YWx1ZTogJz0nXG4gICAgfSxcbiAgICB0ZW1wbGF0ZTogXCI8dGQgdGl0bGU9XFxcInt7dmFsdWUgfHwgJ05vbmUnfX1cXFwiPnt7dmFsdWUgfHwgJ05vbmUnfX08L3RkPlwiXG4gIH07XG59KTtcbiIsIiNcbiMgTGljZW5zZWQgdG8gdGhlIEFwYWNoZSBTb2Z0d2FyZSBGb3VuZGF0aW9uIChBU0YpIHVuZGVyIG9uZVxuIyBvciBtb3JlIGNvbnRyaWJ1dG9yIGxpY2Vuc2UgYWdyZWVtZW50cy4gIFNlZSB0aGUgTk9USUNFIGZpbGVcbiMgZGlzdHJpYnV0ZWQgd2l0aCB0aGlzIHdvcmsgZm9yIGFkZGl0aW9uYWwgaW5mb3JtYXRpb25cbiMgcmVnYXJkaW5nIGNvcHlyaWdodCBvd25lcnNoaXAuICBUaGUgQVNGIGxpY2Vuc2VzIHRoaXMgZmlsZVxuIyB0byB5b3UgdW5kZXIgdGhlIEFwYWNoZSBMaWNlbnNlLCBWZXJzaW9uIDIuMCAodGhlXG4jIFwiTGljZW5zZVwiKTsgeW91IG1heSBub3QgdXNlIHRoaXMgZmlsZSBleGNlcHQgaW4gY29tcGxpYW5jZVxuIyB3aXRoIHRoZSBMaWNlbnNlLiAgWW91IG1heSBvYnRhaW4gYSBjb3B5IG9mIHRoZSBMaWNlbnNlIGF0XG4jXG4jICAgICBodHRwOi8vd3d3LmFwYWNoZS5vcmcvbGljZW5zZXMvTElDRU5TRS0yLjBcbiNcbiMgVW5sZXNzIHJlcXVpcmVkIGJ5IGFwcGxpY2FibGUgbGF3IG9yIGFncmVlZCB0byBpbiB3cml0aW5nLCBzb2Z0d2FyZVxuIyBkaXN0cmlidXRlZCB1bmRlciB0aGUgTGljZW5zZSBpcyBkaXN0cmlidXRlZCBvbiBhbiBcIkFTIElTXCIgQkFTSVMsXG4jIFdJVEhPVVQgV0FSUkFOVElFUyBPUiBDT05ESVRJT05TIE9GIEFOWSBLSU5ELCBlaXRoZXIgZXhwcmVzcyBvciBpbXBsaWVkLlxuIyBTZWUgdGhlIExpY2Vuc2UgZm9yIHRoZSBzcGVjaWZpYyBsYW5ndWFnZSBnb3Zlcm5pbmcgcGVybWlzc2lvbnMgYW5kXG4jIGxpbWl0YXRpb25zIHVuZGVyIHRoZSBMaWNlbnNlLlxuI1xuXG5hbmd1bGFyLm1vZHVsZSgnZmxpbmtBcHAnKVxuXG4uZmlsdGVyIFwiYW1EdXJhdGlvbkZvcm1hdEV4dGVuZGVkXCIsIChhbmd1bGFyTW9tZW50Q29uZmlnKSAtPlxuICBhbUR1cmF0aW9uRm9ybWF0RXh0ZW5kZWRGaWx0ZXIgPSAodmFsdWUsIGZvcm1hdCwgZHVyYXRpb25Gb3JtYXQpIC0+XG4gICAgcmV0dXJuIFwiXCIgIGlmIHR5cGVvZiB2YWx1ZSBpcyBcInVuZGVmaW5lZFwiIG9yIHZhbHVlIGlzIG51bGxcblxuICAgIG1vbWVudC5kdXJhdGlvbih2YWx1ZSwgZm9ybWF0KS5mb3JtYXQoZHVyYXRpb25Gb3JtYXQsIHsgdHJpbTogZmFsc2UgfSlcblxuICBhbUR1cmF0aW9uRm9ybWF0RXh0ZW5kZWRGaWx0ZXIuJHN0YXRlZnVsID0gYW5ndWxhck1vbWVudENvbmZpZy5zdGF0ZWZ1bEZpbHRlcnNcblxuICBhbUR1cmF0aW9uRm9ybWF0RXh0ZW5kZWRGaWx0ZXJcblxuLmZpbHRlciBcImh1bWFuaXplRHVyYXRpb25cIiwgLT5cbiAgKHZhbHVlLCBzaG9ydCkgLT5cbiAgICByZXR1cm4gXCJcIiBpZiB0eXBlb2YgdmFsdWUgaXMgXCJ1bmRlZmluZWRcIiBvciB2YWx1ZSBpcyBudWxsXG4gICAgbXMgPSB2YWx1ZSAlIDEwMDBcbiAgICB4ID0gTWF0aC5mbG9vcih2YWx1ZSAvIDEwMDApXG4gICAgc2Vjb25kcyA9IHggJSA2MFxuICAgIHggPSBNYXRoLmZsb29yKHggLyA2MClcbiAgICBtaW51dGVzID0geCAlIDYwXG4gICAgeCA9IE1hdGguZmxvb3IoeCAvIDYwKVxuICAgIGhvdXJzID0geCAlIDI0XG4gICAgeCA9IE1hdGguZmxvb3IoeCAvIDI0KVxuICAgIGRheXMgPSB4XG4gICAgaWYgZGF5cyA9PSAwXG4gICAgICBpZiBob3VycyA9PSAwXG4gICAgICAgIGlmIG1pbnV0ZXMgPT0gMFxuICAgICAgICAgIGlmIHNlY29uZHMgPT0gMFxuICAgICAgICAgICAgcmV0dXJuIG1zICsgXCJtc1wiXG4gICAgICAgICAgZWxzZVxuICAgICAgICAgICAgcmV0dXJuIHNlY29uZHMgKyBcInMgXCJcbiAgICAgICAgZWxzZVxuICAgICAgICAgIHJldHVybiBtaW51dGVzICsgXCJtIFwiICsgc2Vjb25kcyArIFwic1wiXG4gICAgICBlbHNlXG4gICAgICAgIGlmIHNob3J0IHRoZW4gcmV0dXJuIGhvdXJzICsgXCJoIFwiICsgbWludXRlcyArIFwibVwiIGVsc2UgcmV0dXJuIGhvdXJzICsgXCJoIFwiICsgbWludXRlcyArIFwibSBcIiArIHNlY29uZHMgKyBcInNcIlxuICAgIGVsc2VcbiAgICAgIGlmIHNob3J0IHRoZW4gcmV0dXJuIGRheXMgKyBcImQgXCIgKyBob3VycyArIFwiaFwiIGVsc2UgcmV0dXJuIGRheXMgKyBcImQgXCIgKyBob3VycyArIFwiaCBcIiArIG1pbnV0ZXMgKyBcIm0gXCIgKyBzZWNvbmRzICsgXCJzXCJcblxuLmZpbHRlciBcImh1bWFuaXplVGV4dFwiLCAtPlxuICAodGV4dCkgLT5cbiAgICAjIFRPRE86IGV4dGVuZC4uLiBhIGxvdFxuICAgIGlmIHRleHQgdGhlbiB0ZXh0LnJlcGxhY2UoLyZndDsvZywgXCI+XCIpLnJlcGxhY2UoLzxiclxcLz4vZyxcIlwiKSBlbHNlICcnXG5cbi5maWx0ZXIgXCJodW1hbml6ZUJ5dGVzXCIsIC0+XG4gIChieXRlcykgLT5cbiAgICB1bml0cyA9IFtcIkJcIiwgXCJLQlwiLCBcIk1CXCIsIFwiR0JcIiwgXCJUQlwiLCBcIlBCXCIsIFwiRUJcIl1cbiAgICBjb252ZXJ0ZXIgPSAodmFsdWUsIHBvd2VyKSAtPlxuICAgICAgYmFzZSA9IE1hdGgucG93KDEwMjQsIHBvd2VyKVxuICAgICAgaWYgdmFsdWUgPCBiYXNlXG4gICAgICAgIHJldHVybiAodmFsdWUgLyBiYXNlKS50b0ZpeGVkKDIpICsgXCIgXCIgKyB1bml0c1twb3dlcl1cbiAgICAgIGVsc2UgaWYgdmFsdWUgPCBiYXNlICogMTAwMFxuICAgICAgICByZXR1cm4gKHZhbHVlIC8gYmFzZSkudG9QcmVjaXNpb24oMykgKyBcIiBcIiArIHVuaXRzW3Bvd2VyXVxuICAgICAgZWxzZVxuICAgICAgICByZXR1cm4gY29udmVydGVyKHZhbHVlLCBwb3dlciArIDEpXG4gICAgcmV0dXJuIFwiXCIgaWYgdHlwZW9mIGJ5dGVzIGlzIFwidW5kZWZpbmVkXCIgb3IgYnl0ZXMgaXMgbnVsbFxuICAgIGlmIGJ5dGVzIDwgMTAwMCB0aGVuIGJ5dGVzICsgXCIgQlwiIGVsc2UgY29udmVydGVyKGJ5dGVzLCAxKVxuXG4uZmlsdGVyIFwidG9VcHBlckNhc2VcIiwgLT5cbiAgKHRleHQpIC0+IHRleHQudG9VcHBlckNhc2UoKVxuIiwiYW5ndWxhci5tb2R1bGUoJ2ZsaW5rQXBwJykuZmlsdGVyKFwiYW1EdXJhdGlvbkZvcm1hdEV4dGVuZGVkXCIsIGZ1bmN0aW9uKGFuZ3VsYXJNb21lbnRDb25maWcpIHtcbiAgdmFyIGFtRHVyYXRpb25Gb3JtYXRFeHRlbmRlZEZpbHRlcjtcbiAgYW1EdXJhdGlvbkZvcm1hdEV4dGVuZGVkRmlsdGVyID0gZnVuY3Rpb24odmFsdWUsIGZvcm1hdCwgZHVyYXRpb25Gb3JtYXQpIHtcbiAgICBpZiAodHlwZW9mIHZhbHVlID09PSBcInVuZGVmaW5lZFwiIHx8IHZhbHVlID09PSBudWxsKSB7XG4gICAgICByZXR1cm4gXCJcIjtcbiAgICB9XG4gICAgcmV0dXJuIG1vbWVudC5kdXJhdGlvbih2YWx1ZSwgZm9ybWF0KS5mb3JtYXQoZHVyYXRpb25Gb3JtYXQsIHtcbiAgICAgIHRyaW06IGZhbHNlXG4gICAgfSk7XG4gIH07XG4gIGFtRHVyYXRpb25Gb3JtYXRFeHRlbmRlZEZpbHRlci4kc3RhdGVmdWwgPSBhbmd1bGFyTW9tZW50Q29uZmlnLnN0YXRlZnVsRmlsdGVycztcbiAgcmV0dXJuIGFtRHVyYXRpb25Gb3JtYXRFeHRlbmRlZEZpbHRlcjtcbn0pLmZpbHRlcihcImh1bWFuaXplRHVyYXRpb25cIiwgZnVuY3Rpb24oKSB7XG4gIHJldHVybiBmdW5jdGlvbih2YWx1ZSwgc2hvcnQpIHtcbiAgICB2YXIgZGF5cywgaG91cnMsIG1pbnV0ZXMsIG1zLCBzZWNvbmRzLCB4O1xuICAgIGlmICh0eXBlb2YgdmFsdWUgPT09IFwidW5kZWZpbmVkXCIgfHwgdmFsdWUgPT09IG51bGwpIHtcbiAgICAgIHJldHVybiBcIlwiO1xuICAgIH1cbiAgICBtcyA9IHZhbHVlICUgMTAwMDtcbiAgICB4ID0gTWF0aC5mbG9vcih2YWx1ZSAvIDEwMDApO1xuICAgIHNlY29uZHMgPSB4ICUgNjA7XG4gICAgeCA9IE1hdGguZmxvb3IoeCAvIDYwKTtcbiAgICBtaW51dGVzID0geCAlIDYwO1xuICAgIHggPSBNYXRoLmZsb29yKHggLyA2MCk7XG4gICAgaG91cnMgPSB4ICUgMjQ7XG4gICAgeCA9IE1hdGguZmxvb3IoeCAvIDI0KTtcbiAgICBkYXlzID0geDtcbiAgICBpZiAoZGF5cyA9PT0gMCkge1xuICAgICAgaWYgKGhvdXJzID09PSAwKSB7XG4gICAgICAgIGlmIChtaW51dGVzID09PSAwKSB7XG4gICAgICAgICAgaWYgKHNlY29uZHMgPT09IDApIHtcbiAgICAgICAgICAgIHJldHVybiBtcyArIFwibXNcIjtcbiAgICAgICAgICB9IGVsc2Uge1xuICAgICAgICAgICAgcmV0dXJuIHNlY29uZHMgKyBcInMgXCI7XG4gICAgICAgICAgfVxuICAgICAgICB9IGVsc2Uge1xuICAgICAgICAgIHJldHVybiBtaW51dGVzICsgXCJtIFwiICsgc2Vjb25kcyArIFwic1wiO1xuICAgICAgICB9XG4gICAgICB9IGVsc2Uge1xuICAgICAgICBpZiAoc2hvcnQpIHtcbiAgICAgICAgICByZXR1cm4gaG91cnMgKyBcImggXCIgKyBtaW51dGVzICsgXCJtXCI7XG4gICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgcmV0dXJuIGhvdXJzICsgXCJoIFwiICsgbWludXRlcyArIFwibSBcIiArIHNlY29uZHMgKyBcInNcIjtcbiAgICAgICAgfVxuICAgICAgfVxuICAgIH0gZWxzZSB7XG4gICAgICBpZiAoc2hvcnQpIHtcbiAgICAgICAgcmV0dXJuIGRheXMgKyBcImQgXCIgKyBob3VycyArIFwiaFwiO1xuICAgICAgfSBlbHNlIHtcbiAgICAgICAgcmV0dXJuIGRheXMgKyBcImQgXCIgKyBob3VycyArIFwiaCBcIiArIG1pbnV0ZXMgKyBcIm0gXCIgKyBzZWNvbmRzICsgXCJzXCI7XG4gICAgICB9XG4gICAgfVxuICB9O1xufSkuZmlsdGVyKFwiaHVtYW5pemVUZXh0XCIsIGZ1bmN0aW9uKCkge1xuICByZXR1cm4gZnVuY3Rpb24odGV4dCkge1xuICAgIGlmICh0ZXh0KSB7XG4gICAgICByZXR1cm4gdGV4dC5yZXBsYWNlKC8mZ3Q7L2csIFwiPlwiKS5yZXBsYWNlKC88YnJcXC8+L2csIFwiXCIpO1xuICAgIH0gZWxzZSB7XG4gICAgICByZXR1cm4gJyc7XG4gICAgfVxuICB9O1xufSkuZmlsdGVyKFwiaHVtYW5pemVCeXRlc1wiLCBmdW5jdGlvbigpIHtcbiAgcmV0dXJuIGZ1bmN0aW9uKGJ5dGVzKSB7XG4gICAgdmFyIGNvbnZlcnRlciwgdW5pdHM7XG4gICAgdW5pdHMgPSBbXCJCXCIsIFwiS0JcIiwgXCJNQlwiLCBcIkdCXCIsIFwiVEJcIiwgXCJQQlwiLCBcIkVCXCJdO1xuICAgIGNvbnZlcnRlciA9IGZ1bmN0aW9uKHZhbHVlLCBwb3dlcikge1xuICAgICAgdmFyIGJhc2U7XG4gICAgICBiYXNlID0gTWF0aC5wb3coMTAyNCwgcG93ZXIpO1xuICAgICAgaWYgKHZhbHVlIDwgYmFzZSkge1xuICAgICAgICByZXR1cm4gKHZhbHVlIC8gYmFzZSkudG9GaXhlZCgyKSArIFwiIFwiICsgdW5pdHNbcG93ZXJdO1xuICAgICAgfSBlbHNlIGlmICh2YWx1ZSA8IGJhc2UgKiAxMDAwKSB7XG4gICAgICAgIHJldHVybiAodmFsdWUgLyBiYXNlKS50b1ByZWNpc2lvbigzKSArIFwiIFwiICsgdW5pdHNbcG93ZXJdO1xuICAgICAgfSBlbHNlIHtcbiAgICAgICAgcmV0dXJuIGNvbnZlcnRlcih2YWx1ZSwgcG93ZXIgKyAxKTtcbiAgICAgIH1cbiAgICB9O1xuICAgIGlmICh0eXBlb2YgYnl0ZXMgPT09IFwidW5kZWZpbmVkXCIgfHwgYnl0ZXMgPT09IG51bGwpIHtcbiAgICAgIHJldHVybiBcIlwiO1xuICAgIH1cbiAgICBpZiAoYnl0ZXMgPCAxMDAwKSB7XG4gICAgICByZXR1cm4gYnl0ZXMgKyBcIiBCXCI7XG4gICAgfSBlbHNlIHtcbiAgICAgIHJldHVybiBjb252ZXJ0ZXIoYnl0ZXMsIDEpO1xuICAgIH1cbiAgfTtcbn0pLmZpbHRlcihcInRvVXBwZXJDYXNlXCIsIGZ1bmN0aW9uKCkge1xuICByZXR1cm4gZnVuY3Rpb24odGV4dCkge1xuICAgIHJldHVybiB0ZXh0LnRvVXBwZXJDYXNlKCk7XG4gIH07XG59KTtcbiIsIiNcbiMgTGljZW5zZWQgdG8gdGhlIEFwYWNoZSBTb2Z0d2FyZSBGb3VuZGF0aW9uIChBU0YpIHVuZGVyIG9uZVxuIyBvciBtb3JlIGNvbnRyaWJ1dG9yIGxpY2Vuc2UgYWdyZWVtZW50cy4gIFNlZSB0aGUgTk9USUNFIGZpbGVcbiMgZGlzdHJpYnV0ZWQgd2l0aCB0aGlzIHdvcmsgZm9yIGFkZGl0aW9uYWwgaW5mb3JtYXRpb25cbiMgcmVnYXJkaW5nIGNvcHlyaWdodCBvd25lcnNoaXAuICBUaGUgQVNGIGxpY2Vuc2VzIHRoaXMgZmlsZVxuIyB0byB5b3UgdW5kZXIgdGhlIEFwYWNoZSBMaWNlbnNlLCBWZXJzaW9uIDIuMCAodGhlXG4jIFwiTGljZW5zZVwiKTsgeW91IG1heSBub3QgdXNlIHRoaXMgZmlsZSBleGNlcHQgaW4gY29tcGxpYW5jZVxuIyB3aXRoIHRoZSBMaWNlbnNlLiAgWW91IG1heSBvYnRhaW4gYSBjb3B5IG9mIHRoZSBMaWNlbnNlIGF0XG4jXG4jICAgICBodHRwOi8vd3d3LmFwYWNoZS5vcmcvbGljZW5zZXMvTElDRU5TRS0yLjBcbiNcbiMgVW5sZXNzIHJlcXVpcmVkIGJ5IGFwcGxpY2FibGUgbGF3IG9yIGFncmVlZCB0byBpbiB3cml0aW5nLCBzb2Z0d2FyZVxuIyBkaXN0cmlidXRlZCB1bmRlciB0aGUgTGljZW5zZSBpcyBkaXN0cmlidXRlZCBvbiBhbiBcIkFTIElTXCIgQkFTSVMsXG4jIFdJVEhPVVQgV0FSUkFOVElFUyBPUiBDT05ESVRJT05TIE9GIEFOWSBLSU5ELCBlaXRoZXIgZXhwcmVzcyBvciBpbXBsaWVkLlxuIyBTZWUgdGhlIExpY2Vuc2UgZm9yIHRoZSBzcGVjaWZpYyBsYW5ndWFnZSBnb3Zlcm5pbmcgcGVybWlzc2lvbnMgYW5kXG4jIGxpbWl0YXRpb25zIHVuZGVyIHRoZSBMaWNlbnNlLlxuI1xuXG5hbmd1bGFyLm1vZHVsZSgnZmxpbmtBcHAnKVxuXG4uc2VydmljZSAnTWFpblNlcnZpY2UnLCAoJGh0dHAsIGZsaW5rQ29uZmlnLCAkcSkgLT5cbiAgQGxvYWRDb25maWcgPSAtPlxuICAgIGRlZmVycmVkID0gJHEuZGVmZXIoKVxuXG4gICAgJGh0dHAuZ2V0IGZsaW5rQ29uZmlnLmpvYlNlcnZlciArIFwiY29uZmlnXCJcbiAgICAuc3VjY2VzcyAoZGF0YSwgc3RhdHVzLCBoZWFkZXJzLCBjb25maWcpIC0+XG4gICAgICBkZWZlcnJlZC5yZXNvbHZlKGRhdGEpXG5cbiAgICBkZWZlcnJlZC5wcm9taXNlXG5cblxuICBAXG4iLCJhbmd1bGFyLm1vZHVsZSgnZmxpbmtBcHAnKS5zZXJ2aWNlKCdNYWluU2VydmljZScsIGZ1bmN0aW9uKCRodHRwLCBmbGlua0NvbmZpZywgJHEpIHtcbiAgdGhpcy5sb2FkQ29uZmlnID0gZnVuY3Rpb24oKSB7XG4gICAgdmFyIGRlZmVycmVkO1xuICAgIGRlZmVycmVkID0gJHEuZGVmZXIoKTtcbiAgICAkaHR0cC5nZXQoZmxpbmtDb25maWcuam9iU2VydmVyICsgXCJjb25maWdcIikuc3VjY2VzcyhmdW5jdGlvbihkYXRhLCBzdGF0dXMsIGhlYWRlcnMsIGNvbmZpZykge1xuICAgICAgcmV0dXJuIGRlZmVycmVkLnJlc29sdmUoZGF0YSk7XG4gICAgfSk7XG4gICAgcmV0dXJuIGRlZmVycmVkLnByb21pc2U7XG4gIH07XG4gIHJldHVybiB0aGlzO1xufSk7XG4iLCIjXG4jIExpY2Vuc2VkIHRvIHRoZSBBcGFjaGUgU29mdHdhcmUgRm91bmRhdGlvbiAoQVNGKSB1bmRlciBvbmVcbiMgb3IgbW9yZSBjb250cmlidXRvciBsaWNlbnNlIGFncmVlbWVudHMuICBTZWUgdGhlIE5PVElDRSBmaWxlXG4jIGRpc3RyaWJ1dGVkIHdpdGggdGhpcyB3b3JrIGZvciBhZGRpdGlvbmFsIGluZm9ybWF0aW9uXG4jIHJlZ2FyZGluZyBjb3B5cmlnaHQgb3duZXJzaGlwLiAgVGhlIEFTRiBsaWNlbnNlcyB0aGlzIGZpbGVcbiMgdG8geW91IHVuZGVyIHRoZSBBcGFjaGUgTGljZW5zZSwgVmVyc2lvbiAyLjAgKHRoZVxuIyBcIkxpY2Vuc2VcIik7IHlvdSBtYXkgbm90IHVzZSB0aGlzIGZpbGUgZXhjZXB0IGluIGNvbXBsaWFuY2VcbiMgd2l0aCB0aGUgTGljZW5zZS4gIFlvdSBtYXkgb2J0YWluIGEgY29weSBvZiB0aGUgTGljZW5zZSBhdFxuI1xuIyAgICAgaHR0cDovL3d3dy5hcGFjaGUub3JnL2xpY2Vuc2VzL0xJQ0VOU0UtMi4wXG4jXG4jIFVubGVzcyByZXF1aXJlZCBieSBhcHBsaWNhYmxlIGxhdyBvciBhZ3JlZWQgdG8gaW4gd3JpdGluZywgc29mdHdhcmVcbiMgZGlzdHJpYnV0ZWQgdW5kZXIgdGhlIExpY2Vuc2UgaXMgZGlzdHJpYnV0ZWQgb24gYW4gXCJBUyBJU1wiIEJBU0lTLFxuIyBXSVRIT1VUIFdBUlJBTlRJRVMgT1IgQ09ORElUSU9OUyBPRiBBTlkgS0lORCwgZWl0aGVyIGV4cHJlc3Mgb3IgaW1wbGllZC5cbiMgU2VlIHRoZSBMaWNlbnNlIGZvciB0aGUgc3BlY2lmaWMgbGFuZ3VhZ2UgZ292ZXJuaW5nIHBlcm1pc3Npb25zIGFuZFxuIyBsaW1pdGF0aW9ucyB1bmRlciB0aGUgTGljZW5zZS5cbiNcblxuYW5ndWxhci5tb2R1bGUoJ2ZsaW5rQXBwJylcblxuLmNvbnRyb2xsZXIgJ0pvYk1hbmFnZXJDb25maWdDb250cm9sbGVyJywgKCRzY29wZSwgSm9iTWFuYWdlckNvbmZpZ1NlcnZpY2UpIC0+XG4gIEpvYk1hbmFnZXJDb25maWdTZXJ2aWNlLmxvYWRDb25maWcoKS50aGVuIChkYXRhKSAtPlxuICAgIGlmICEkc2NvcGUuam9ibWFuYWdlcj9cbiAgICAgICRzY29wZS5qb2JtYW5hZ2VyID0ge31cbiAgICAkc2NvcGUuam9ibWFuYWdlclsnY29uZmlnJ10gPSBkYXRhXG5cbi5jb250cm9sbGVyICdKb2JNYW5hZ2VyTG9nc0NvbnRyb2xsZXInLCAoJHNjb3BlLCBKb2JNYW5hZ2VyTG9nc1NlcnZpY2UpIC0+XG4gIEpvYk1hbmFnZXJMb2dzU2VydmljZS5sb2FkTG9ncygpLnRoZW4gKGRhdGEpIC0+XG4gICAgaWYgISRzY29wZS5qb2JtYW5hZ2VyP1xuICAgICAgJHNjb3BlLmpvYm1hbmFnZXIgPSB7fVxuICAgICRzY29wZS5qb2JtYW5hZ2VyWydsb2cnXSA9IGRhdGFcblxuICAkc2NvcGUucmVsb2FkRGF0YSA9ICgpIC0+XG4gICAgSm9iTWFuYWdlckxvZ3NTZXJ2aWNlLmxvYWRMb2dzKCkudGhlbiAoZGF0YSkgLT5cbiAgICAgICRzY29wZS5qb2JtYW5hZ2VyWydsb2cnXSA9IGRhdGFcblxuLmNvbnRyb2xsZXIgJ0pvYk1hbmFnZXJTdGRvdXRDb250cm9sbGVyJywgKCRzY29wZSwgSm9iTWFuYWdlclN0ZG91dFNlcnZpY2UpIC0+XG4gIEpvYk1hbmFnZXJTdGRvdXRTZXJ2aWNlLmxvYWRTdGRvdXQoKS50aGVuIChkYXRhKSAtPlxuICAgIGlmICEkc2NvcGUuam9ibWFuYWdlcj9cbiAgICAgICRzY29wZS5qb2JtYW5hZ2VyID0ge31cbiAgICAkc2NvcGUuam9ibWFuYWdlclsnc3Rkb3V0J10gPSBkYXRhXG5cbiAgJHNjb3BlLnJlbG9hZERhdGEgPSAoKSAtPlxuICAgIEpvYk1hbmFnZXJTdGRvdXRTZXJ2aWNlLmxvYWRTdGRvdXQoKS50aGVuIChkYXRhKSAtPlxuICAgICAgJHNjb3BlLmpvYm1hbmFnZXJbJ3N0ZG91dCddID0gZGF0YVxuIiwiYW5ndWxhci5tb2R1bGUoJ2ZsaW5rQXBwJykuY29udHJvbGxlcignSm9iTWFuYWdlckNvbmZpZ0NvbnRyb2xsZXInLCBmdW5jdGlvbigkc2NvcGUsIEpvYk1hbmFnZXJDb25maWdTZXJ2aWNlKSB7XG4gIHJldHVybiBKb2JNYW5hZ2VyQ29uZmlnU2VydmljZS5sb2FkQ29uZmlnKCkudGhlbihmdW5jdGlvbihkYXRhKSB7XG4gICAgaWYgKCRzY29wZS5qb2JtYW5hZ2VyID09IG51bGwpIHtcbiAgICAgICRzY29wZS5qb2JtYW5hZ2VyID0ge307XG4gICAgfVxuICAgIHJldHVybiAkc2NvcGUuam9ibWFuYWdlclsnY29uZmlnJ10gPSBkYXRhO1xuICB9KTtcbn0pLmNvbnRyb2xsZXIoJ0pvYk1hbmFnZXJMb2dzQ29udHJvbGxlcicsIGZ1bmN0aW9uKCRzY29wZSwgSm9iTWFuYWdlckxvZ3NTZXJ2aWNlKSB7XG4gIEpvYk1hbmFnZXJMb2dzU2VydmljZS5sb2FkTG9ncygpLnRoZW4oZnVuY3Rpb24oZGF0YSkge1xuICAgIGlmICgkc2NvcGUuam9ibWFuYWdlciA9PSBudWxsKSB7XG4gICAgICAkc2NvcGUuam9ibWFuYWdlciA9IHt9O1xuICAgIH1cbiAgICByZXR1cm4gJHNjb3BlLmpvYm1hbmFnZXJbJ2xvZyddID0gZGF0YTtcbiAgfSk7XG4gIHJldHVybiAkc2NvcGUucmVsb2FkRGF0YSA9IGZ1bmN0aW9uKCkge1xuICAgIHJldHVybiBKb2JNYW5hZ2VyTG9nc1NlcnZpY2UubG9hZExvZ3MoKS50aGVuKGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgIHJldHVybiAkc2NvcGUuam9ibWFuYWdlclsnbG9nJ10gPSBkYXRhO1xuICAgIH0pO1xuICB9O1xufSkuY29udHJvbGxlcignSm9iTWFuYWdlclN0ZG91dENvbnRyb2xsZXInLCBmdW5jdGlvbigkc2NvcGUsIEpvYk1hbmFnZXJTdGRvdXRTZXJ2aWNlKSB7XG4gIEpvYk1hbmFnZXJTdGRvdXRTZXJ2aWNlLmxvYWRTdGRvdXQoKS50aGVuKGZ1bmN0aW9uKGRhdGEpIHtcbiAgICBpZiAoJHNjb3BlLmpvYm1hbmFnZXIgPT0gbnVsbCkge1xuICAgICAgJHNjb3BlLmpvYm1hbmFnZXIgPSB7fTtcbiAgICB9XG4gICAgcmV0dXJuICRzY29wZS5qb2JtYW5hZ2VyWydzdGRvdXQnXSA9IGRhdGE7XG4gIH0pO1xuICByZXR1cm4gJHNjb3BlLnJlbG9hZERhdGEgPSBmdW5jdGlvbigpIHtcbiAgICByZXR1cm4gSm9iTWFuYWdlclN0ZG91dFNlcnZpY2UubG9hZFN0ZG91dCgpLnRoZW4oZnVuY3Rpb24oZGF0YSkge1xuICAgICAgcmV0dXJuICRzY29wZS5qb2JtYW5hZ2VyWydzdGRvdXQnXSA9IGRhdGE7XG4gICAgfSk7XG4gIH07XG59KTtcbiIsIiNcbiMgTGljZW5zZWQgdG8gdGhlIEFwYWNoZSBTb2Z0d2FyZSBGb3VuZGF0aW9uIChBU0YpIHVuZGVyIG9uZVxuIyBvciBtb3JlIGNvbnRyaWJ1dG9yIGxpY2Vuc2UgYWdyZWVtZW50cy4gIFNlZSB0aGUgTk9USUNFIGZpbGVcbiMgZGlzdHJpYnV0ZWQgd2l0aCB0aGlzIHdvcmsgZm9yIGFkZGl0aW9uYWwgaW5mb3JtYXRpb25cbiMgcmVnYXJkaW5nIGNvcHlyaWdodCBvd25lcnNoaXAuICBUaGUgQVNGIGxpY2Vuc2VzIHRoaXMgZmlsZVxuIyB0byB5b3UgdW5kZXIgdGhlIEFwYWNoZSBMaWNlbnNlLCBWZXJzaW9uIDIuMCAodGhlXG4jIFwiTGljZW5zZVwiKTsgeW91IG1heSBub3QgdXNlIHRoaXMgZmlsZSBleGNlcHQgaW4gY29tcGxpYW5jZVxuIyB3aXRoIHRoZSBMaWNlbnNlLiAgWW91IG1heSBvYnRhaW4gYSBjb3B5IG9mIHRoZSBMaWNlbnNlIGF0XG4jXG4jICAgICBodHRwOi8vd3d3LmFwYWNoZS5vcmcvbGljZW5zZXMvTElDRU5TRS0yLjBcbiNcbiMgVW5sZXNzIHJlcXVpcmVkIGJ5IGFwcGxpY2FibGUgbGF3IG9yIGFncmVlZCB0byBpbiB3cml0aW5nLCBzb2Z0d2FyZVxuIyBkaXN0cmlidXRlZCB1bmRlciB0aGUgTGljZW5zZSBpcyBkaXN0cmlidXRlZCBvbiBhbiBcIkFTIElTXCIgQkFTSVMsXG4jIFdJVEhPVVQgV0FSUkFOVElFUyBPUiBDT05ESVRJT05TIE9GIEFOWSBLSU5ELCBlaXRoZXIgZXhwcmVzcyBvciBpbXBsaWVkLlxuIyBTZWUgdGhlIExpY2Vuc2UgZm9yIHRoZSBzcGVjaWZpYyBsYW5ndWFnZSBnb3Zlcm5pbmcgcGVybWlzc2lvbnMgYW5kXG4jIGxpbWl0YXRpb25zIHVuZGVyIHRoZSBMaWNlbnNlLlxuI1xuXG5hbmd1bGFyLm1vZHVsZSgnZmxpbmtBcHAnKVxuXG4uc2VydmljZSAnSm9iTWFuYWdlckNvbmZpZ1NlcnZpY2UnLCAoJGh0dHAsIGZsaW5rQ29uZmlnLCAkcSkgLT5cbiAgY29uZmlnID0ge31cblxuICBAbG9hZENvbmZpZyA9IC0+XG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpXG5cbiAgICAkaHR0cC5nZXQoZmxpbmtDb25maWcuam9iU2VydmVyICsgXCJqb2JtYW5hZ2VyL2NvbmZpZ1wiKVxuICAgIC5zdWNjZXNzIChkYXRhLCBzdGF0dXMsIGhlYWRlcnMsIGNvbmZpZykgLT5cbiAgICAgIGNvbmZpZyA9IGRhdGFcbiAgICAgIGRlZmVycmVkLnJlc29sdmUoZGF0YSlcblxuICAgIGRlZmVycmVkLnByb21pc2VcblxuICBAXG5cbi5zZXJ2aWNlICdKb2JNYW5hZ2VyTG9nc1NlcnZpY2UnLCAoJGh0dHAsIGZsaW5rQ29uZmlnLCAkcSkgLT5cbiAgbG9ncyA9IHt9XG5cbiAgQGxvYWRMb2dzID0gLT5cbiAgICBkZWZlcnJlZCA9ICRxLmRlZmVyKClcblxuICAgICRodHRwLmdldChmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcImpvYm1hbmFnZXIvbG9nXCIpXG4gICAgLnN1Y2Nlc3MgKGRhdGEsIHN0YXR1cywgaGVhZGVycywgY29uZmlnKSAtPlxuICAgICAgbG9ncyA9IGRhdGFcbiAgICAgIGRlZmVycmVkLnJlc29sdmUoZGF0YSlcblxuICAgIGRlZmVycmVkLnByb21pc2VcblxuICBAXG5cbi5zZXJ2aWNlICdKb2JNYW5hZ2VyU3Rkb3V0U2VydmljZScsICgkaHR0cCwgZmxpbmtDb25maWcsICRxKSAtPlxuICBzdGRvdXQgPSB7fVxuXG4gIEBsb2FkU3Rkb3V0ID0gLT5cbiAgICBkZWZlcnJlZCA9ICRxLmRlZmVyKClcblxuICAgICRodHRwLmdldChmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcImpvYm1hbmFnZXIvc3Rkb3V0XCIpXG4gICAgLnN1Y2Nlc3MgKGRhdGEsIHN0YXR1cywgaGVhZGVycywgY29uZmlnKSAtPlxuICAgICAgc3Rkb3V0ID0gZGF0YVxuICAgICAgZGVmZXJyZWQucmVzb2x2ZShkYXRhKVxuXG4gICAgZGVmZXJyZWQucHJvbWlzZVxuXG4gIEBcbiIsImFuZ3VsYXIubW9kdWxlKCdmbGlua0FwcCcpLnNlcnZpY2UoJ0pvYk1hbmFnZXJDb25maWdTZXJ2aWNlJywgZnVuY3Rpb24oJGh0dHAsIGZsaW5rQ29uZmlnLCAkcSkge1xuICB2YXIgY29uZmlnO1xuICBjb25maWcgPSB7fTtcbiAgdGhpcy5sb2FkQ29uZmlnID0gZnVuY3Rpb24oKSB7XG4gICAgdmFyIGRlZmVycmVkO1xuICAgIGRlZmVycmVkID0gJHEuZGVmZXIoKTtcbiAgICAkaHR0cC5nZXQoZmxpbmtDb25maWcuam9iU2VydmVyICsgXCJqb2JtYW5hZ2VyL2NvbmZpZ1wiKS5zdWNjZXNzKGZ1bmN0aW9uKGRhdGEsIHN0YXR1cywgaGVhZGVycywgY29uZmlnKSB7XG4gICAgICBjb25maWcgPSBkYXRhO1xuICAgICAgcmV0dXJuIGRlZmVycmVkLnJlc29sdmUoZGF0YSk7XG4gICAgfSk7XG4gICAgcmV0dXJuIGRlZmVycmVkLnByb21pc2U7XG4gIH07XG4gIHJldHVybiB0aGlzO1xufSkuc2VydmljZSgnSm9iTWFuYWdlckxvZ3NTZXJ2aWNlJywgZnVuY3Rpb24oJGh0dHAsIGZsaW5rQ29uZmlnLCAkcSkge1xuICB2YXIgbG9ncztcbiAgbG9ncyA9IHt9O1xuICB0aGlzLmxvYWRMb2dzID0gZnVuY3Rpb24oKSB7XG4gICAgdmFyIGRlZmVycmVkO1xuICAgIGRlZmVycmVkID0gJHEuZGVmZXIoKTtcbiAgICAkaHR0cC5nZXQoZmxpbmtDb25maWcuam9iU2VydmVyICsgXCJqb2JtYW5hZ2VyL2xvZ1wiKS5zdWNjZXNzKGZ1bmN0aW9uKGRhdGEsIHN0YXR1cywgaGVhZGVycywgY29uZmlnKSB7XG4gICAgICBsb2dzID0gZGF0YTtcbiAgICAgIHJldHVybiBkZWZlcnJlZC5yZXNvbHZlKGRhdGEpO1xuICAgIH0pO1xuICAgIHJldHVybiBkZWZlcnJlZC5wcm9taXNlO1xuICB9O1xuICByZXR1cm4gdGhpcztcbn0pLnNlcnZpY2UoJ0pvYk1hbmFnZXJTdGRvdXRTZXJ2aWNlJywgZnVuY3Rpb24oJGh0dHAsIGZsaW5rQ29uZmlnLCAkcSkge1xuICB2YXIgc3Rkb3V0O1xuICBzdGRvdXQgPSB7fTtcbiAgdGhpcy5sb2FkU3Rkb3V0ID0gZnVuY3Rpb24oKSB7XG4gICAgdmFyIGRlZmVycmVkO1xuICAgIGRlZmVycmVkID0gJHEuZGVmZXIoKTtcbiAgICAkaHR0cC5nZXQoZmxpbmtDb25maWcuam9iU2VydmVyICsgXCJqb2JtYW5hZ2VyL3N0ZG91dFwiKS5zdWNjZXNzKGZ1bmN0aW9uKGRhdGEsIHN0YXR1cywgaGVhZGVycywgY29uZmlnKSB7XG4gICAgICBzdGRvdXQgPSBkYXRhO1xuICAgICAgcmV0dXJuIGRlZmVycmVkLnJlc29sdmUoZGF0YSk7XG4gICAgfSk7XG4gICAgcmV0dXJuIGRlZmVycmVkLnByb21pc2U7XG4gIH07XG4gIHJldHVybiB0aGlzO1xufSk7XG4iLCIjXG4jIExpY2Vuc2VkIHRvIHRoZSBBcGFjaGUgU29mdHdhcmUgRm91bmRhdGlvbiAoQVNGKSB1bmRlciBvbmVcbiMgb3IgbW9yZSBjb250cmlidXRvciBsaWNlbnNlIGFncmVlbWVudHMuICBTZWUgdGhlIE5PVElDRSBmaWxlXG4jIGRpc3RyaWJ1dGVkIHdpdGggdGhpcyB3b3JrIGZvciBhZGRpdGlvbmFsIGluZm9ybWF0aW9uXG4jIHJlZ2FyZGluZyBjb3B5cmlnaHQgb3duZXJzaGlwLiAgVGhlIEFTRiBsaWNlbnNlcyB0aGlzIGZpbGVcbiMgdG8geW91IHVuZGVyIHRoZSBBcGFjaGUgTGljZW5zZSwgVmVyc2lvbiAyLjAgKHRoZVxuIyBcIkxpY2Vuc2VcIik7IHlvdSBtYXkgbm90IHVzZSB0aGlzIGZpbGUgZXhjZXB0IGluIGNvbXBsaWFuY2VcbiMgd2l0aCB0aGUgTGljZW5zZS4gIFlvdSBtYXkgb2J0YWluIGEgY29weSBvZiB0aGUgTGljZW5zZSBhdFxuI1xuIyAgICAgaHR0cDovL3d3dy5hcGFjaGUub3JnL2xpY2Vuc2VzL0xJQ0VOU0UtMi4wXG4jXG4jIFVubGVzcyByZXF1aXJlZCBieSBhcHBsaWNhYmxlIGxhdyBvciBhZ3JlZWQgdG8gaW4gd3JpdGluZywgc29mdHdhcmVcbiMgZGlzdHJpYnV0ZWQgdW5kZXIgdGhlIExpY2Vuc2UgaXMgZGlzdHJpYnV0ZWQgb24gYW4gXCJBUyBJU1wiIEJBU0lTLFxuIyBXSVRIT1VUIFdBUlJBTlRJRVMgT1IgQ09ORElUSU9OUyBPRiBBTlkgS0lORCwgZWl0aGVyIGV4cHJlc3Mgb3IgaW1wbGllZC5cbiMgU2VlIHRoZSBMaWNlbnNlIGZvciB0aGUgc3BlY2lmaWMgbGFuZ3VhZ2UgZ292ZXJuaW5nIHBlcm1pc3Npb25zIGFuZFxuIyBsaW1pdGF0aW9ucyB1bmRlciB0aGUgTGljZW5zZS5cbiNcblxuYW5ndWxhci5tb2R1bGUoJ2ZsaW5rQXBwJylcblxuLmNvbnRyb2xsZXIgJ1J1bm5pbmdKb2JzQ29udHJvbGxlcicsICgkc2NvcGUsICRzdGF0ZSwgJHN0YXRlUGFyYW1zLCBKb2JzU2VydmljZSkgLT5cbiAgJHNjb3BlLmpvYk9ic2VydmVyID0gLT5cbiAgICAkc2NvcGUuam9icyA9IEpvYnNTZXJ2aWNlLmdldEpvYnMoJ3J1bm5pbmcnKVxuXG4gIEpvYnNTZXJ2aWNlLnJlZ2lzdGVyT2JzZXJ2ZXIoJHNjb3BlLmpvYk9ic2VydmVyKVxuICAkc2NvcGUuJG9uICckZGVzdHJveScsIC0+XG4gICAgSm9ic1NlcnZpY2UudW5SZWdpc3Rlck9ic2VydmVyKCRzY29wZS5qb2JPYnNlcnZlcilcblxuICAkc2NvcGUuam9iT2JzZXJ2ZXIoKVxuXG4jIC0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tXG5cbi5jb250cm9sbGVyICdDb21wbGV0ZWRKb2JzQ29udHJvbGxlcicsICgkc2NvcGUsICRzdGF0ZSwgJHN0YXRlUGFyYW1zLCBKb2JzU2VydmljZSkgLT5cbiAgJHNjb3BlLmpvYk9ic2VydmVyID0gLT5cbiAgICAkc2NvcGUuam9icyA9IEpvYnNTZXJ2aWNlLmdldEpvYnMoJ2ZpbmlzaGVkJylcblxuICBKb2JzU2VydmljZS5yZWdpc3Rlck9ic2VydmVyKCRzY29wZS5qb2JPYnNlcnZlcilcbiAgJHNjb3BlLiRvbiAnJGRlc3Ryb3knLCAtPlxuICAgIEpvYnNTZXJ2aWNlLnVuUmVnaXN0ZXJPYnNlcnZlcigkc2NvcGUuam9iT2JzZXJ2ZXIpXG5cbiAgJHNjb3BlLmpvYk9ic2VydmVyKClcblxuIyAtLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLVxuXG4uY29udHJvbGxlciAnU2luZ2xlSm9iQ29udHJvbGxlcicsICgkc2NvcGUsICRzdGF0ZSwgJHN0YXRlUGFyYW1zLCBKb2JzU2VydmljZSwgJHJvb3RTY29wZSwgZmxpbmtDb25maWcsICRpbnRlcnZhbCkgLT5cbiAgY29uc29sZS5sb2cgJ1NpbmdsZUpvYkNvbnRyb2xsZXInXG5cbiAgJHNjb3BlLmpvYmlkID0gJHN0YXRlUGFyYW1zLmpvYmlkXG4gICRzY29wZS5qb2IgPSBudWxsXG4gICRzY29wZS5wbGFuID0gbnVsbFxuICAkc2NvcGUudmVydGljZXMgPSBudWxsXG4gICRzY29wZS5qb2JDaGVja3BvaW50U3RhdHMgPSBudWxsXG4gICRzY29wZS5zaG93SGlzdG9yeSA9IGZhbHNlXG4gICRzY29wZS5iYWNrUHJlc3N1cmVPcGVyYXRvclN0YXRzID0ge31cblxuICBKb2JzU2VydmljZS5sb2FkSm9iKCRzdGF0ZVBhcmFtcy5qb2JpZCkudGhlbiAoZGF0YSkgLT5cbiAgICAkc2NvcGUuam9iID0gZGF0YVxuICAgICRzY29wZS5wbGFuID0gZGF0YS5wbGFuXG4gICAgJHNjb3BlLnZlcnRpY2VzID0gZGF0YS52ZXJ0aWNlc1xuXG4gIHJlZnJlc2hlciA9ICRpbnRlcnZhbCAtPlxuICAgIEpvYnNTZXJ2aWNlLmxvYWRKb2IoJHN0YXRlUGFyYW1zLmpvYmlkKS50aGVuIChkYXRhKSAtPlxuICAgICAgJHNjb3BlLmpvYiA9IGRhdGFcblxuICAgICAgJHNjb3BlLiRicm9hZGNhc3QgJ3JlbG9hZCdcblxuICAsIGZsaW5rQ29uZmlnW1wicmVmcmVzaC1pbnRlcnZhbFwiXVxuXG4gICRzY29wZS4kb24gJyRkZXN0cm95JywgLT5cbiAgICAkc2NvcGUuam9iID0gbnVsbFxuICAgICRzY29wZS5wbGFuID0gbnVsbFxuICAgICRzY29wZS52ZXJ0aWNlcyA9IG51bGxcbiAgICAkc2NvcGUuam9iQ2hlY2twb2ludFN0YXRzID0gbnVsbFxuICAgICRzY29wZS5iYWNrUHJlc3N1cmVPcGVyYXRvclN0YXRzID0gbnVsbFxuXG4gICAgJGludGVydmFsLmNhbmNlbChyZWZyZXNoZXIpXG5cbiAgJHNjb3BlLmNhbmNlbEpvYiA9IChjYW5jZWxFdmVudCkgLT5cbiAgICBhbmd1bGFyLmVsZW1lbnQoY2FuY2VsRXZlbnQuY3VycmVudFRhcmdldCkucmVtb3ZlQ2xhc3MoXCJidG5cIikucmVtb3ZlQ2xhc3MoXCJidG4tZGVmYXVsdFwiKS5odG1sKCdDYW5jZWxsaW5nLi4uJylcbiAgICBKb2JzU2VydmljZS5jYW5jZWxKb2IoJHN0YXRlUGFyYW1zLmpvYmlkKS50aGVuIChkYXRhKSAtPlxuICAgICAge31cblxuICAkc2NvcGUudG9nZ2xlSGlzdG9yeSA9IC0+XG4gICAgJHNjb3BlLnNob3dIaXN0b3J5ID0gISRzY29wZS5zaG93SGlzdG9yeVxuXG4jIC0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tXG5cbi5jb250cm9sbGVyICdKb2JQbGFuQ29udHJvbGxlcicsICgkc2NvcGUsICRzdGF0ZSwgJHN0YXRlUGFyYW1zLCBKb2JzU2VydmljZSkgLT5cbiAgY29uc29sZS5sb2cgJ0pvYlBsYW5Db250cm9sbGVyJ1xuXG4gICRzY29wZS5ub2RlaWQgPSBudWxsXG4gICRzY29wZS5ub2RlVW5mb2xkZWQgPSBmYWxzZVxuICAkc2NvcGUuc3RhdGVMaXN0ID0gSm9ic1NlcnZpY2Uuc3RhdGVMaXN0KClcblxuICAkc2NvcGUuY2hhbmdlTm9kZSA9IChub2RlaWQpIC0+XG4gICAgaWYgbm9kZWlkICE9ICRzY29wZS5ub2RlaWRcbiAgICAgICRzY29wZS5ub2RlaWQgPSBub2RlaWRcbiAgICAgICRzY29wZS52ZXJ0ZXggPSBudWxsXG4gICAgICAkc2NvcGUuc3VidGFza3MgPSBudWxsXG4gICAgICAkc2NvcGUuYWNjdW11bGF0b3JzID0gbnVsbFxuICAgICAgJHNjb3BlLm9wZXJhdG9yQ2hlY2twb2ludFN0YXRzID0gbnVsbFxuXG4gICAgICAkc2NvcGUuJGJyb2FkY2FzdCAncmVsb2FkJ1xuXG4gICAgZWxzZVxuICAgICAgJHNjb3BlLm5vZGVpZCA9IG51bGxcbiAgICAgICRzY29wZS5ub2RlVW5mb2xkZWQgPSBmYWxzZVxuICAgICAgJHNjb3BlLnZlcnRleCA9IG51bGxcbiAgICAgICRzY29wZS5zdWJ0YXNrcyA9IG51bGxcbiAgICAgICRzY29wZS5hY2N1bXVsYXRvcnMgPSBudWxsXG4gICAgICAkc2NvcGUub3BlcmF0b3JDaGVja3BvaW50U3RhdHMgPSBudWxsXG5cbiAgJHNjb3BlLmRlYWN0aXZhdGVOb2RlID0gLT5cbiAgICAkc2NvcGUubm9kZWlkID0gbnVsbFxuICAgICRzY29wZS5ub2RlVW5mb2xkZWQgPSBmYWxzZVxuICAgICRzY29wZS52ZXJ0ZXggPSBudWxsXG4gICAgJHNjb3BlLnN1YnRhc2tzID0gbnVsbFxuICAgICRzY29wZS5hY2N1bXVsYXRvcnMgPSBudWxsXG4gICAgJHNjb3BlLm9wZXJhdG9yQ2hlY2twb2ludFN0YXRzID0gbnVsbFxuXG4gICRzY29wZS50b2dnbGVGb2xkID0gLT5cbiAgICAkc2NvcGUubm9kZVVuZm9sZGVkID0gISRzY29wZS5ub2RlVW5mb2xkZWRcblxuIyAtLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLVxuXG4uY29udHJvbGxlciAnSm9iUGxhbk92ZXJ2aWV3Q29udHJvbGxlcicsICgkc2NvcGUsIEpvYnNTZXJ2aWNlKSAtPlxuICBjb25zb2xlLmxvZyAnSm9iUGxhbk92ZXJ2aWV3Q29udHJvbGxlcidcblxuICBpZiAkc2NvcGUubm9kZWlkIGFuZCAoISRzY29wZS52ZXJ0ZXggb3IgISRzY29wZS52ZXJ0ZXguc3QpXG4gICAgSm9ic1NlcnZpY2UuZ2V0U3VidGFza3MoJHNjb3BlLm5vZGVpZCkudGhlbiAoZGF0YSkgLT5cbiAgICAgICRzY29wZS5zdWJ0YXNrcyA9IGRhdGFcblxuICAkc2NvcGUuJG9uICdyZWxvYWQnLCAoZXZlbnQpIC0+XG4gICAgY29uc29sZS5sb2cgJ0pvYlBsYW5PdmVydmlld0NvbnRyb2xsZXInXG4gICAgaWYgJHNjb3BlLm5vZGVpZFxuICAgICAgSm9ic1NlcnZpY2UuZ2V0U3VidGFza3MoJHNjb3BlLm5vZGVpZCkudGhlbiAoZGF0YSkgLT5cbiAgICAgICAgJHNjb3BlLnN1YnRhc2tzID0gZGF0YVxuXG4jIC0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tXG5cbi5jb250cm9sbGVyICdKb2JQbGFuQWNjdW11bGF0b3JzQ29udHJvbGxlcicsICgkc2NvcGUsIEpvYnNTZXJ2aWNlKSAtPlxuICBjb25zb2xlLmxvZyAnSm9iUGxhbkFjY3VtdWxhdG9yc0NvbnRyb2xsZXInXG5cbiAgaWYgJHNjb3BlLm5vZGVpZCBhbmQgKCEkc2NvcGUudmVydGV4IG9yICEkc2NvcGUudmVydGV4LmFjY3VtdWxhdG9ycylcbiAgICBKb2JzU2VydmljZS5nZXRBY2N1bXVsYXRvcnMoJHNjb3BlLm5vZGVpZCkudGhlbiAoZGF0YSkgLT5cbiAgICAgICRzY29wZS5hY2N1bXVsYXRvcnMgPSBkYXRhLm1haW5cbiAgICAgICRzY29wZS5zdWJ0YXNrQWNjdW11bGF0b3JzID0gZGF0YS5zdWJ0YXNrc1xuXG4gICRzY29wZS4kb24gJ3JlbG9hZCcsIChldmVudCkgLT5cbiAgICBjb25zb2xlLmxvZyAnSm9iUGxhbkFjY3VtdWxhdG9yc0NvbnRyb2xsZXInXG4gICAgaWYgJHNjb3BlLm5vZGVpZFxuICAgICAgSm9ic1NlcnZpY2UuZ2V0QWNjdW11bGF0b3JzKCRzY29wZS5ub2RlaWQpLnRoZW4gKGRhdGEpIC0+XG4gICAgICAgICRzY29wZS5hY2N1bXVsYXRvcnMgPSBkYXRhLm1haW5cbiAgICAgICAgJHNjb3BlLnN1YnRhc2tBY2N1bXVsYXRvcnMgPSBkYXRhLnN1YnRhc2tzXG5cbiMgLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS1cblxuLmNvbnRyb2xsZXIgJ0pvYlBsYW5DaGVja3BvaW50c0NvbnRyb2xsZXInLCAoJHNjb3BlLCBKb2JzU2VydmljZSkgLT5cbiAgY29uc29sZS5sb2cgJ0pvYlBsYW5DaGVja3BvaW50c0NvbnRyb2xsZXInXG5cbiAgIyBHZXQgdGhlIHBlciBqb2Igc3RhdHNcbiAgSm9ic1NlcnZpY2UuZ2V0Sm9iQ2hlY2twb2ludFN0YXRzKCRzY29wZS5qb2JpZCkudGhlbiAoZGF0YSkgLT5cbiAgICAkc2NvcGUuam9iQ2hlY2twb2ludFN0YXRzID0gZGF0YVxuXG4gICMgR2V0IHRoZSBwZXIgb3BlcmF0b3Igc3RhdHNcbiAgaWYgJHNjb3BlLm5vZGVpZCBhbmQgKCEkc2NvcGUudmVydGV4IG9yICEkc2NvcGUudmVydGV4Lm9wZXJhdG9yQ2hlY2twb2ludFN0YXRzKVxuICAgIEpvYnNTZXJ2aWNlLmdldE9wZXJhdG9yQ2hlY2twb2ludFN0YXRzKCRzY29wZS5ub2RlaWQpLnRoZW4gKGRhdGEpIC0+XG4gICAgICAkc2NvcGUub3BlcmF0b3JDaGVja3BvaW50U3RhdHMgPSBkYXRhLm9wZXJhdG9yU3RhdHNcbiAgICAgICRzY29wZS5zdWJ0YXNrc0NoZWNrcG9pbnRTdGF0cyA9IGRhdGEuc3VidGFza3NTdGF0c1xuXG4gICRzY29wZS4kb24gJ3JlbG9hZCcsIChldmVudCkgLT5cbiAgICBjb25zb2xlLmxvZyAnSm9iUGxhbkNoZWNrcG9pbnRzQ29udHJvbGxlcidcblxuICAgIEpvYnNTZXJ2aWNlLmdldEpvYkNoZWNrcG9pbnRTdGF0cygkc2NvcGUuam9iaWQpLnRoZW4gKGRhdGEpIC0+XG4gICAgICAkc2NvcGUuam9iQ2hlY2twb2ludFN0YXRzID0gZGF0YVxuXG4gICAgaWYgJHNjb3BlLm5vZGVpZFxuICAgICAgSm9ic1NlcnZpY2UuZ2V0T3BlcmF0b3JDaGVja3BvaW50U3RhdHMoJHNjb3BlLm5vZGVpZCkudGhlbiAoZGF0YSkgLT5cbiAgICAgICAgJHNjb3BlLm9wZXJhdG9yQ2hlY2twb2ludFN0YXRzID0gZGF0YS5vcGVyYXRvclN0YXRzXG4gICAgICAgICRzY29wZS5zdWJ0YXNrc0NoZWNrcG9pbnRTdGF0cyA9IGRhdGEuc3VidGFza3NTdGF0c1xuXG4jIC0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tXG5cbi5jb250cm9sbGVyICdKb2JQbGFuQmFja1ByZXNzdXJlQ29udHJvbGxlcicsICgkc2NvcGUsIEpvYnNTZXJ2aWNlKSAtPlxuICBjb25zb2xlLmxvZyAnSm9iUGxhbkJhY2tQcmVzc3VyZUNvbnRyb2xsZXInXG4gICRzY29wZS5ub3cgPSBEYXRlLm5vdygpXG5cbiAgaWYgJHNjb3BlLm5vZGVpZFxuICAgIEpvYnNTZXJ2aWNlLmdldE9wZXJhdG9yQmFja1ByZXNzdXJlKCRzY29wZS5ub2RlaWQpLnRoZW4gKGRhdGEpIC0+XG4gICAgICAkc2NvcGUuYmFja1ByZXNzdXJlT3BlcmF0b3JTdGF0c1skc2NvcGUubm9kZWlkXSA9IGRhdGFcblxuICAkc2NvcGUuJG9uICdyZWxvYWQnLCAoZXZlbnQpIC0+XG4gICAgY29uc29sZS5sb2cgJ0pvYlBsYW5CYWNrUHJlc3N1cmVDb250cm9sbGVyIChyZWxhb2QpJ1xuICAgICRzY29wZS5ub3cgPSBEYXRlLm5vdygpXG5cbiAgICBpZiAkc2NvcGUubm9kZWlkXG4gICAgICBKb2JzU2VydmljZS5nZXRPcGVyYXRvckJhY2tQcmVzc3VyZSgkc2NvcGUubm9kZWlkKS50aGVuIChkYXRhKSAtPlxuICAgICAgICAkc2NvcGUuYmFja1ByZXNzdXJlT3BlcmF0b3JTdGF0c1skc2NvcGUubm9kZWlkXSA9IGRhdGFcblxuIyAtLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLVxuXG4uY29udHJvbGxlciAnSm9iVGltZWxpbmVWZXJ0ZXhDb250cm9sbGVyJywgKCRzY29wZSwgJHN0YXRlLCAkc3RhdGVQYXJhbXMsIEpvYnNTZXJ2aWNlKSAtPlxuICBjb25zb2xlLmxvZyAnSm9iVGltZWxpbmVWZXJ0ZXhDb250cm9sbGVyJ1xuXG4gIEpvYnNTZXJ2aWNlLmdldFZlcnRleCgkc3RhdGVQYXJhbXMudmVydGV4SWQpLnRoZW4gKGRhdGEpIC0+XG4gICAgJHNjb3BlLnZlcnRleCA9IGRhdGFcblxuICAkc2NvcGUuJG9uICdyZWxvYWQnLCAoZXZlbnQpIC0+XG4gICAgY29uc29sZS5sb2cgJ0pvYlRpbWVsaW5lVmVydGV4Q29udHJvbGxlcidcbiAgICBKb2JzU2VydmljZS5nZXRWZXJ0ZXgoJHN0YXRlUGFyYW1zLnZlcnRleElkKS50aGVuIChkYXRhKSAtPlxuICAgICAgJHNjb3BlLnZlcnRleCA9IGRhdGFcblxuIyAtLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLVxuXG4uY29udHJvbGxlciAnSm9iRXhjZXB0aW9uc0NvbnRyb2xsZXInLCAoJHNjb3BlLCAkc3RhdGUsICRzdGF0ZVBhcmFtcywgSm9ic1NlcnZpY2UpIC0+XG4gIEpvYnNTZXJ2aWNlLmxvYWRFeGNlcHRpb25zKCkudGhlbiAoZGF0YSkgLT5cbiAgICAkc2NvcGUuZXhjZXB0aW9ucyA9IGRhdGFcblxuIyAtLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLVxuXG4uY29udHJvbGxlciAnSm9iUHJvcGVydGllc0NvbnRyb2xsZXInLCAoJHNjb3BlLCBKb2JzU2VydmljZSkgLT5cbiAgY29uc29sZS5sb2cgJ0pvYlByb3BlcnRpZXNDb250cm9sbGVyJ1xuXG4gICRzY29wZS5jaGFuZ2VOb2RlID0gKG5vZGVpZCkgLT5cbiAgICBpZiBub2RlaWQgIT0gJHNjb3BlLm5vZGVpZFxuICAgICAgJHNjb3BlLm5vZGVpZCA9IG5vZGVpZFxuXG4gICAgICBKb2JzU2VydmljZS5nZXROb2RlKG5vZGVpZCkudGhlbiAoZGF0YSkgLT5cbiAgICAgICAgJHNjb3BlLm5vZGUgPSBkYXRhXG5cbiAgICBlbHNlXG4gICAgICAkc2NvcGUubm9kZWlkID0gbnVsbFxuICAgICAgJHNjb3BlLm5vZGUgPSBudWxsXG4iLCJhbmd1bGFyLm1vZHVsZSgnZmxpbmtBcHAnKS5jb250cm9sbGVyKCdSdW5uaW5nSm9ic0NvbnRyb2xsZXInLCBmdW5jdGlvbigkc2NvcGUsICRzdGF0ZSwgJHN0YXRlUGFyYW1zLCBKb2JzU2VydmljZSkge1xuICAkc2NvcGUuam9iT2JzZXJ2ZXIgPSBmdW5jdGlvbigpIHtcbiAgICByZXR1cm4gJHNjb3BlLmpvYnMgPSBKb2JzU2VydmljZS5nZXRKb2JzKCdydW5uaW5nJyk7XG4gIH07XG4gIEpvYnNTZXJ2aWNlLnJlZ2lzdGVyT2JzZXJ2ZXIoJHNjb3BlLmpvYk9ic2VydmVyKTtcbiAgJHNjb3BlLiRvbignJGRlc3Ryb3knLCBmdW5jdGlvbigpIHtcbiAgICByZXR1cm4gSm9ic1NlcnZpY2UudW5SZWdpc3Rlck9ic2VydmVyKCRzY29wZS5qb2JPYnNlcnZlcik7XG4gIH0pO1xuICByZXR1cm4gJHNjb3BlLmpvYk9ic2VydmVyKCk7XG59KS5jb250cm9sbGVyKCdDb21wbGV0ZWRKb2JzQ29udHJvbGxlcicsIGZ1bmN0aW9uKCRzY29wZSwgJHN0YXRlLCAkc3RhdGVQYXJhbXMsIEpvYnNTZXJ2aWNlKSB7XG4gICRzY29wZS5qb2JPYnNlcnZlciA9IGZ1bmN0aW9uKCkge1xuICAgIHJldHVybiAkc2NvcGUuam9icyA9IEpvYnNTZXJ2aWNlLmdldEpvYnMoJ2ZpbmlzaGVkJyk7XG4gIH07XG4gIEpvYnNTZXJ2aWNlLnJlZ2lzdGVyT2JzZXJ2ZXIoJHNjb3BlLmpvYk9ic2VydmVyKTtcbiAgJHNjb3BlLiRvbignJGRlc3Ryb3knLCBmdW5jdGlvbigpIHtcbiAgICByZXR1cm4gSm9ic1NlcnZpY2UudW5SZWdpc3Rlck9ic2VydmVyKCRzY29wZS5qb2JPYnNlcnZlcik7XG4gIH0pO1xuICByZXR1cm4gJHNjb3BlLmpvYk9ic2VydmVyKCk7XG59KS5jb250cm9sbGVyKCdTaW5nbGVKb2JDb250cm9sbGVyJywgZnVuY3Rpb24oJHNjb3BlLCAkc3RhdGUsICRzdGF0ZVBhcmFtcywgSm9ic1NlcnZpY2UsICRyb290U2NvcGUsIGZsaW5rQ29uZmlnLCAkaW50ZXJ2YWwpIHtcbiAgdmFyIHJlZnJlc2hlcjtcbiAgY29uc29sZS5sb2coJ1NpbmdsZUpvYkNvbnRyb2xsZXInKTtcbiAgJHNjb3BlLmpvYmlkID0gJHN0YXRlUGFyYW1zLmpvYmlkO1xuICAkc2NvcGUuam9iID0gbnVsbDtcbiAgJHNjb3BlLnBsYW4gPSBudWxsO1xuICAkc2NvcGUudmVydGljZXMgPSBudWxsO1xuICAkc2NvcGUuam9iQ2hlY2twb2ludFN0YXRzID0gbnVsbDtcbiAgJHNjb3BlLnNob3dIaXN0b3J5ID0gZmFsc2U7XG4gICRzY29wZS5iYWNrUHJlc3N1cmVPcGVyYXRvclN0YXRzID0ge307XG4gIEpvYnNTZXJ2aWNlLmxvYWRKb2IoJHN0YXRlUGFyYW1zLmpvYmlkKS50aGVuKGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAkc2NvcGUuam9iID0gZGF0YTtcbiAgICAkc2NvcGUucGxhbiA9IGRhdGEucGxhbjtcbiAgICByZXR1cm4gJHNjb3BlLnZlcnRpY2VzID0gZGF0YS52ZXJ0aWNlcztcbiAgfSk7XG4gIHJlZnJlc2hlciA9ICRpbnRlcnZhbChmdW5jdGlvbigpIHtcbiAgICByZXR1cm4gSm9ic1NlcnZpY2UubG9hZEpvYigkc3RhdGVQYXJhbXMuam9iaWQpLnRoZW4oZnVuY3Rpb24oZGF0YSkge1xuICAgICAgJHNjb3BlLmpvYiA9IGRhdGE7XG4gICAgICByZXR1cm4gJHNjb3BlLiRicm9hZGNhc3QoJ3JlbG9hZCcpO1xuICAgIH0pO1xuICB9LCBmbGlua0NvbmZpZ1tcInJlZnJlc2gtaW50ZXJ2YWxcIl0pO1xuICAkc2NvcGUuJG9uKCckZGVzdHJveScsIGZ1bmN0aW9uKCkge1xuICAgICRzY29wZS5qb2IgPSBudWxsO1xuICAgICRzY29wZS5wbGFuID0gbnVsbDtcbiAgICAkc2NvcGUudmVydGljZXMgPSBudWxsO1xuICAgICRzY29wZS5qb2JDaGVja3BvaW50U3RhdHMgPSBudWxsO1xuICAgICRzY29wZS5iYWNrUHJlc3N1cmVPcGVyYXRvclN0YXRzID0gbnVsbDtcbiAgICByZXR1cm4gJGludGVydmFsLmNhbmNlbChyZWZyZXNoZXIpO1xuICB9KTtcbiAgJHNjb3BlLmNhbmNlbEpvYiA9IGZ1bmN0aW9uKGNhbmNlbEV2ZW50KSB7XG4gICAgYW5ndWxhci5lbGVtZW50KGNhbmNlbEV2ZW50LmN1cnJlbnRUYXJnZXQpLnJlbW92ZUNsYXNzKFwiYnRuXCIpLnJlbW92ZUNsYXNzKFwiYnRuLWRlZmF1bHRcIikuaHRtbCgnQ2FuY2VsbGluZy4uLicpO1xuICAgIHJldHVybiBKb2JzU2VydmljZS5jYW5jZWxKb2IoJHN0YXRlUGFyYW1zLmpvYmlkKS50aGVuKGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgIHJldHVybiB7fTtcbiAgICB9KTtcbiAgfTtcbiAgcmV0dXJuICRzY29wZS50b2dnbGVIaXN0b3J5ID0gZnVuY3Rpb24oKSB7XG4gICAgcmV0dXJuICRzY29wZS5zaG93SGlzdG9yeSA9ICEkc2NvcGUuc2hvd0hpc3Rvcnk7XG4gIH07XG59KS5jb250cm9sbGVyKCdKb2JQbGFuQ29udHJvbGxlcicsIGZ1bmN0aW9uKCRzY29wZSwgJHN0YXRlLCAkc3RhdGVQYXJhbXMsIEpvYnNTZXJ2aWNlKSB7XG4gIGNvbnNvbGUubG9nKCdKb2JQbGFuQ29udHJvbGxlcicpO1xuICAkc2NvcGUubm9kZWlkID0gbnVsbDtcbiAgJHNjb3BlLm5vZGVVbmZvbGRlZCA9IGZhbHNlO1xuICAkc2NvcGUuc3RhdGVMaXN0ID0gSm9ic1NlcnZpY2Uuc3RhdGVMaXN0KCk7XG4gICRzY29wZS5jaGFuZ2VOb2RlID0gZnVuY3Rpb24obm9kZWlkKSB7XG4gICAgaWYgKG5vZGVpZCAhPT0gJHNjb3BlLm5vZGVpZCkge1xuICAgICAgJHNjb3BlLm5vZGVpZCA9IG5vZGVpZDtcbiAgICAgICRzY29wZS52ZXJ0ZXggPSBudWxsO1xuICAgICAgJHNjb3BlLnN1YnRhc2tzID0gbnVsbDtcbiAgICAgICRzY29wZS5hY2N1bXVsYXRvcnMgPSBudWxsO1xuICAgICAgJHNjb3BlLm9wZXJhdG9yQ2hlY2twb2ludFN0YXRzID0gbnVsbDtcbiAgICAgIHJldHVybiAkc2NvcGUuJGJyb2FkY2FzdCgncmVsb2FkJyk7XG4gICAgfSBlbHNlIHtcbiAgICAgICRzY29wZS5ub2RlaWQgPSBudWxsO1xuICAgICAgJHNjb3BlLm5vZGVVbmZvbGRlZCA9IGZhbHNlO1xuICAgICAgJHNjb3BlLnZlcnRleCA9IG51bGw7XG4gICAgICAkc2NvcGUuc3VidGFza3MgPSBudWxsO1xuICAgICAgJHNjb3BlLmFjY3VtdWxhdG9ycyA9IG51bGw7XG4gICAgICByZXR1cm4gJHNjb3BlLm9wZXJhdG9yQ2hlY2twb2ludFN0YXRzID0gbnVsbDtcbiAgICB9XG4gIH07XG4gICRzY29wZS5kZWFjdGl2YXRlTm9kZSA9IGZ1bmN0aW9uKCkge1xuICAgICRzY29wZS5ub2RlaWQgPSBudWxsO1xuICAgICRzY29wZS5ub2RlVW5mb2xkZWQgPSBmYWxzZTtcbiAgICAkc2NvcGUudmVydGV4ID0gbnVsbDtcbiAgICAkc2NvcGUuc3VidGFza3MgPSBudWxsO1xuICAgICRzY29wZS5hY2N1bXVsYXRvcnMgPSBudWxsO1xuICAgIHJldHVybiAkc2NvcGUub3BlcmF0b3JDaGVja3BvaW50U3RhdHMgPSBudWxsO1xuICB9O1xuICByZXR1cm4gJHNjb3BlLnRvZ2dsZUZvbGQgPSBmdW5jdGlvbigpIHtcbiAgICByZXR1cm4gJHNjb3BlLm5vZGVVbmZvbGRlZCA9ICEkc2NvcGUubm9kZVVuZm9sZGVkO1xuICB9O1xufSkuY29udHJvbGxlcignSm9iUGxhbk92ZXJ2aWV3Q29udHJvbGxlcicsIGZ1bmN0aW9uKCRzY29wZSwgSm9ic1NlcnZpY2UpIHtcbiAgY29uc29sZS5sb2coJ0pvYlBsYW5PdmVydmlld0NvbnRyb2xsZXInKTtcbiAgaWYgKCRzY29wZS5ub2RlaWQgJiYgKCEkc2NvcGUudmVydGV4IHx8ICEkc2NvcGUudmVydGV4LnN0KSkge1xuICAgIEpvYnNTZXJ2aWNlLmdldFN1YnRhc2tzKCRzY29wZS5ub2RlaWQpLnRoZW4oZnVuY3Rpb24oZGF0YSkge1xuICAgICAgcmV0dXJuICRzY29wZS5zdWJ0YXNrcyA9IGRhdGE7XG4gICAgfSk7XG4gIH1cbiAgcmV0dXJuICRzY29wZS4kb24oJ3JlbG9hZCcsIGZ1bmN0aW9uKGV2ZW50KSB7XG4gICAgY29uc29sZS5sb2coJ0pvYlBsYW5PdmVydmlld0NvbnRyb2xsZXInKTtcbiAgICBpZiAoJHNjb3BlLm5vZGVpZCkge1xuICAgICAgcmV0dXJuIEpvYnNTZXJ2aWNlLmdldFN1YnRhc2tzKCRzY29wZS5ub2RlaWQpLnRoZW4oZnVuY3Rpb24oZGF0YSkge1xuICAgICAgICByZXR1cm4gJHNjb3BlLnN1YnRhc2tzID0gZGF0YTtcbiAgICAgIH0pO1xuICAgIH1cbiAgfSk7XG59KS5jb250cm9sbGVyKCdKb2JQbGFuQWNjdW11bGF0b3JzQ29udHJvbGxlcicsIGZ1bmN0aW9uKCRzY29wZSwgSm9ic1NlcnZpY2UpIHtcbiAgY29uc29sZS5sb2coJ0pvYlBsYW5BY2N1bXVsYXRvcnNDb250cm9sbGVyJyk7XG4gIGlmICgkc2NvcGUubm9kZWlkICYmICghJHNjb3BlLnZlcnRleCB8fCAhJHNjb3BlLnZlcnRleC5hY2N1bXVsYXRvcnMpKSB7XG4gICAgSm9ic1NlcnZpY2UuZ2V0QWNjdW11bGF0b3JzKCRzY29wZS5ub2RlaWQpLnRoZW4oZnVuY3Rpb24oZGF0YSkge1xuICAgICAgJHNjb3BlLmFjY3VtdWxhdG9ycyA9IGRhdGEubWFpbjtcbiAgICAgIHJldHVybiAkc2NvcGUuc3VidGFza0FjY3VtdWxhdG9ycyA9IGRhdGEuc3VidGFza3M7XG4gICAgfSk7XG4gIH1cbiAgcmV0dXJuICRzY29wZS4kb24oJ3JlbG9hZCcsIGZ1bmN0aW9uKGV2ZW50KSB7XG4gICAgY29uc29sZS5sb2coJ0pvYlBsYW5BY2N1bXVsYXRvcnNDb250cm9sbGVyJyk7XG4gICAgaWYgKCRzY29wZS5ub2RlaWQpIHtcbiAgICAgIHJldHVybiBKb2JzU2VydmljZS5nZXRBY2N1bXVsYXRvcnMoJHNjb3BlLm5vZGVpZCkudGhlbihmdW5jdGlvbihkYXRhKSB7XG4gICAgICAgICRzY29wZS5hY2N1bXVsYXRvcnMgPSBkYXRhLm1haW47XG4gICAgICAgIHJldHVybiAkc2NvcGUuc3VidGFza0FjY3VtdWxhdG9ycyA9IGRhdGEuc3VidGFza3M7XG4gICAgICB9KTtcbiAgICB9XG4gIH0pO1xufSkuY29udHJvbGxlcignSm9iUGxhbkNoZWNrcG9pbnRzQ29udHJvbGxlcicsIGZ1bmN0aW9uKCRzY29wZSwgSm9ic1NlcnZpY2UpIHtcbiAgY29uc29sZS5sb2coJ0pvYlBsYW5DaGVja3BvaW50c0NvbnRyb2xsZXInKTtcbiAgSm9ic1NlcnZpY2UuZ2V0Sm9iQ2hlY2twb2ludFN0YXRzKCRzY29wZS5qb2JpZCkudGhlbihmdW5jdGlvbihkYXRhKSB7XG4gICAgcmV0dXJuICRzY29wZS5qb2JDaGVja3BvaW50U3RhdHMgPSBkYXRhO1xuICB9KTtcbiAgaWYgKCRzY29wZS5ub2RlaWQgJiYgKCEkc2NvcGUudmVydGV4IHx8ICEkc2NvcGUudmVydGV4Lm9wZXJhdG9yQ2hlY2twb2ludFN0YXRzKSkge1xuICAgIEpvYnNTZXJ2aWNlLmdldE9wZXJhdG9yQ2hlY2twb2ludFN0YXRzKCRzY29wZS5ub2RlaWQpLnRoZW4oZnVuY3Rpb24oZGF0YSkge1xuICAgICAgJHNjb3BlLm9wZXJhdG9yQ2hlY2twb2ludFN0YXRzID0gZGF0YS5vcGVyYXRvclN0YXRzO1xuICAgICAgcmV0dXJuICRzY29wZS5zdWJ0YXNrc0NoZWNrcG9pbnRTdGF0cyA9IGRhdGEuc3VidGFza3NTdGF0cztcbiAgICB9KTtcbiAgfVxuICByZXR1cm4gJHNjb3BlLiRvbigncmVsb2FkJywgZnVuY3Rpb24oZXZlbnQpIHtcbiAgICBjb25zb2xlLmxvZygnSm9iUGxhbkNoZWNrcG9pbnRzQ29udHJvbGxlcicpO1xuICAgIEpvYnNTZXJ2aWNlLmdldEpvYkNoZWNrcG9pbnRTdGF0cygkc2NvcGUuam9iaWQpLnRoZW4oZnVuY3Rpb24oZGF0YSkge1xuICAgICAgcmV0dXJuICRzY29wZS5qb2JDaGVja3BvaW50U3RhdHMgPSBkYXRhO1xuICAgIH0pO1xuICAgIGlmICgkc2NvcGUubm9kZWlkKSB7XG4gICAgICByZXR1cm4gSm9ic1NlcnZpY2UuZ2V0T3BlcmF0b3JDaGVja3BvaW50U3RhdHMoJHNjb3BlLm5vZGVpZCkudGhlbihmdW5jdGlvbihkYXRhKSB7XG4gICAgICAgICRzY29wZS5vcGVyYXRvckNoZWNrcG9pbnRTdGF0cyA9IGRhdGEub3BlcmF0b3JTdGF0cztcbiAgICAgICAgcmV0dXJuICRzY29wZS5zdWJ0YXNrc0NoZWNrcG9pbnRTdGF0cyA9IGRhdGEuc3VidGFza3NTdGF0cztcbiAgICAgIH0pO1xuICAgIH1cbiAgfSk7XG59KS5jb250cm9sbGVyKCdKb2JQbGFuQmFja1ByZXNzdXJlQ29udHJvbGxlcicsIGZ1bmN0aW9uKCRzY29wZSwgSm9ic1NlcnZpY2UpIHtcbiAgY29uc29sZS5sb2coJ0pvYlBsYW5CYWNrUHJlc3N1cmVDb250cm9sbGVyJyk7XG4gICRzY29wZS5ub3cgPSBEYXRlLm5vdygpO1xuICBpZiAoJHNjb3BlLm5vZGVpZCkge1xuICAgIEpvYnNTZXJ2aWNlLmdldE9wZXJhdG9yQmFja1ByZXNzdXJlKCRzY29wZS5ub2RlaWQpLnRoZW4oZnVuY3Rpb24oZGF0YSkge1xuICAgICAgcmV0dXJuICRzY29wZS5iYWNrUHJlc3N1cmVPcGVyYXRvclN0YXRzWyRzY29wZS5ub2RlaWRdID0gZGF0YTtcbiAgICB9KTtcbiAgfVxuICByZXR1cm4gJHNjb3BlLiRvbigncmVsb2FkJywgZnVuY3Rpb24oZXZlbnQpIHtcbiAgICBjb25zb2xlLmxvZygnSm9iUGxhbkJhY2tQcmVzc3VyZUNvbnRyb2xsZXIgKHJlbGFvZCknKTtcbiAgICAkc2NvcGUubm93ID0gRGF0ZS5ub3coKTtcbiAgICBpZiAoJHNjb3BlLm5vZGVpZCkge1xuICAgICAgcmV0dXJuIEpvYnNTZXJ2aWNlLmdldE9wZXJhdG9yQmFja1ByZXNzdXJlKCRzY29wZS5ub2RlaWQpLnRoZW4oZnVuY3Rpb24oZGF0YSkge1xuICAgICAgICByZXR1cm4gJHNjb3BlLmJhY2tQcmVzc3VyZU9wZXJhdG9yU3RhdHNbJHNjb3BlLm5vZGVpZF0gPSBkYXRhO1xuICAgICAgfSk7XG4gICAgfVxuICB9KTtcbn0pLmNvbnRyb2xsZXIoJ0pvYlRpbWVsaW5lVmVydGV4Q29udHJvbGxlcicsIGZ1bmN0aW9uKCRzY29wZSwgJHN0YXRlLCAkc3RhdGVQYXJhbXMsIEpvYnNTZXJ2aWNlKSB7XG4gIGNvbnNvbGUubG9nKCdKb2JUaW1lbGluZVZlcnRleENvbnRyb2xsZXInKTtcbiAgSm9ic1NlcnZpY2UuZ2V0VmVydGV4KCRzdGF0ZVBhcmFtcy52ZXJ0ZXhJZCkudGhlbihmdW5jdGlvbihkYXRhKSB7XG4gICAgcmV0dXJuICRzY29wZS52ZXJ0ZXggPSBkYXRhO1xuICB9KTtcbiAgcmV0dXJuICRzY29wZS4kb24oJ3JlbG9hZCcsIGZ1bmN0aW9uKGV2ZW50KSB7XG4gICAgY29uc29sZS5sb2coJ0pvYlRpbWVsaW5lVmVydGV4Q29udHJvbGxlcicpO1xuICAgIHJldHVybiBKb2JzU2VydmljZS5nZXRWZXJ0ZXgoJHN0YXRlUGFyYW1zLnZlcnRleElkKS50aGVuKGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgIHJldHVybiAkc2NvcGUudmVydGV4ID0gZGF0YTtcbiAgICB9KTtcbiAgfSk7XG59KS5jb250cm9sbGVyKCdKb2JFeGNlcHRpb25zQ29udHJvbGxlcicsIGZ1bmN0aW9uKCRzY29wZSwgJHN0YXRlLCAkc3RhdGVQYXJhbXMsIEpvYnNTZXJ2aWNlKSB7XG4gIHJldHVybiBKb2JzU2VydmljZS5sb2FkRXhjZXB0aW9ucygpLnRoZW4oZnVuY3Rpb24oZGF0YSkge1xuICAgIHJldHVybiAkc2NvcGUuZXhjZXB0aW9ucyA9IGRhdGE7XG4gIH0pO1xufSkuY29udHJvbGxlcignSm9iUHJvcGVydGllc0NvbnRyb2xsZXInLCBmdW5jdGlvbigkc2NvcGUsIEpvYnNTZXJ2aWNlKSB7XG4gIGNvbnNvbGUubG9nKCdKb2JQcm9wZXJ0aWVzQ29udHJvbGxlcicpO1xuICByZXR1cm4gJHNjb3BlLmNoYW5nZU5vZGUgPSBmdW5jdGlvbihub2RlaWQpIHtcbiAgICBpZiAobm9kZWlkICE9PSAkc2NvcGUubm9kZWlkKSB7XG4gICAgICAkc2NvcGUubm9kZWlkID0gbm9kZWlkO1xuICAgICAgcmV0dXJuIEpvYnNTZXJ2aWNlLmdldE5vZGUobm9kZWlkKS50aGVuKGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgICAgcmV0dXJuICRzY29wZS5ub2RlID0gZGF0YTtcbiAgICAgIH0pO1xuICAgIH0gZWxzZSB7XG4gICAgICAkc2NvcGUubm9kZWlkID0gbnVsbDtcbiAgICAgIHJldHVybiAkc2NvcGUubm9kZSA9IG51bGw7XG4gICAgfVxuICB9O1xufSk7XG4iLCIjXG4jIExpY2Vuc2VkIHRvIHRoZSBBcGFjaGUgU29mdHdhcmUgRm91bmRhdGlvbiAoQVNGKSB1bmRlciBvbmVcbiMgb3IgbW9yZSBjb250cmlidXRvciBsaWNlbnNlIGFncmVlbWVudHMuICBTZWUgdGhlIE5PVElDRSBmaWxlXG4jIGRpc3RyaWJ1dGVkIHdpdGggdGhpcyB3b3JrIGZvciBhZGRpdGlvbmFsIGluZm9ybWF0aW9uXG4jIHJlZ2FyZGluZyBjb3B5cmlnaHQgb3duZXJzaGlwLiAgVGhlIEFTRiBsaWNlbnNlcyB0aGlzIGZpbGVcbiMgdG8geW91IHVuZGVyIHRoZSBBcGFjaGUgTGljZW5zZSwgVmVyc2lvbiAyLjAgKHRoZVxuIyBcIkxpY2Vuc2VcIik7IHlvdSBtYXkgbm90IHVzZSB0aGlzIGZpbGUgZXhjZXB0IGluIGNvbXBsaWFuY2VcbiMgd2l0aCB0aGUgTGljZW5zZS4gIFlvdSBtYXkgb2J0YWluIGEgY29weSBvZiB0aGUgTGljZW5zZSBhdFxuI1xuIyAgICAgaHR0cDovL3d3dy5hcGFjaGUub3JnL2xpY2Vuc2VzL0xJQ0VOU0UtMi4wXG4jXG4jIFVubGVzcyByZXF1aXJlZCBieSBhcHBsaWNhYmxlIGxhdyBvciBhZ3JlZWQgdG8gaW4gd3JpdGluZywgc29mdHdhcmVcbiMgZGlzdHJpYnV0ZWQgdW5kZXIgdGhlIExpY2Vuc2UgaXMgZGlzdHJpYnV0ZWQgb24gYW4gXCJBUyBJU1wiIEJBU0lTLFxuIyBXSVRIT1VUIFdBUlJBTlRJRVMgT1IgQ09ORElUSU9OUyBPRiBBTlkgS0lORCwgZWl0aGVyIGV4cHJlc3Mgb3IgaW1wbGllZC5cbiMgU2VlIHRoZSBMaWNlbnNlIGZvciB0aGUgc3BlY2lmaWMgbGFuZ3VhZ2UgZ292ZXJuaW5nIHBlcm1pc3Npb25zIGFuZFxuIyBsaW1pdGF0aW9ucyB1bmRlciB0aGUgTGljZW5zZS5cbiNcblxuYW5ndWxhci5tb2R1bGUoJ2ZsaW5rQXBwJylcblxuIyAtLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tXG5cbi5kaXJlY3RpdmUgJ3ZlcnRleCcsICgkc3RhdGUpIC0+XG4gIHRlbXBsYXRlOiBcIjxzdmcgY2xhc3M9J3RpbWVsaW5lIHNlY29uZGFyeScgd2lkdGg9JzAnIGhlaWdodD0nMCc+PC9zdmc+XCJcblxuICBzY29wZTpcbiAgICBkYXRhOiBcIj1cIlxuXG4gIGxpbms6IChzY29wZSwgZWxlbSwgYXR0cnMpIC0+XG4gICAgc3ZnRWwgPSBlbGVtLmNoaWxkcmVuKClbMF1cblxuICAgIGNvbnRhaW5lclcgPSBlbGVtLndpZHRoKClcbiAgICBhbmd1bGFyLmVsZW1lbnQoc3ZnRWwpLmF0dHIoJ3dpZHRoJywgY29udGFpbmVyVylcblxuICAgIGFuYWx5emVUaW1lID0gKGRhdGEpIC0+XG4gICAgICBkMy5zZWxlY3Qoc3ZnRWwpLnNlbGVjdEFsbChcIipcIikucmVtb3ZlKClcblxuICAgICAgdGVzdERhdGEgPSBbXVxuXG4gICAgICBhbmd1bGFyLmZvckVhY2ggZGF0YS5zdWJ0YXNrcywgKHN1YnRhc2ssIGkpIC0+XG4gICAgICAgIHRpbWVzID0gW1xuICAgICAgICAgIHtcbiAgICAgICAgICAgIGxhYmVsOiBcIlNjaGVkdWxlZFwiXG4gICAgICAgICAgICBjb2xvcjogXCIjNjY2XCJcbiAgICAgICAgICAgIGJvcmRlckNvbG9yOiBcIiM1NTVcIlxuICAgICAgICAgICAgc3RhcnRpbmdfdGltZTogc3VidGFzay50aW1lc3RhbXBzW1wiU0NIRURVTEVEXCJdXG4gICAgICAgICAgICBlbmRpbmdfdGltZTogc3VidGFzay50aW1lc3RhbXBzW1wiREVQTE9ZSU5HXCJdXG4gICAgICAgICAgICB0eXBlOiAncmVndWxhcidcbiAgICAgICAgICB9XG4gICAgICAgICAge1xuICAgICAgICAgICAgbGFiZWw6IFwiRGVwbG95aW5nXCJcbiAgICAgICAgICAgIGNvbG9yOiBcIiNhYWFcIlxuICAgICAgICAgICAgYm9yZGVyQ29sb3I6IFwiIzU1NVwiXG4gICAgICAgICAgICBzdGFydGluZ190aW1lOiBzdWJ0YXNrLnRpbWVzdGFtcHNbXCJERVBMT1lJTkdcIl1cbiAgICAgICAgICAgIGVuZGluZ190aW1lOiBzdWJ0YXNrLnRpbWVzdGFtcHNbXCJSVU5OSU5HXCJdXG4gICAgICAgICAgICB0eXBlOiAncmVndWxhcidcbiAgICAgICAgICB9XG4gICAgICAgIF1cblxuICAgICAgICBpZiBzdWJ0YXNrLnRpbWVzdGFtcHNbXCJGSU5JU0hFRFwiXSA+IDBcbiAgICAgICAgICB0aW1lcy5wdXNoIHtcbiAgICAgICAgICAgIGxhYmVsOiBcIlJ1bm5pbmdcIlxuICAgICAgICAgICAgY29sb3I6IFwiI2RkZFwiXG4gICAgICAgICAgICBib3JkZXJDb2xvcjogXCIjNTU1XCJcbiAgICAgICAgICAgIHN0YXJ0aW5nX3RpbWU6IHN1YnRhc2sudGltZXN0YW1wc1tcIlJVTk5JTkdcIl1cbiAgICAgICAgICAgIGVuZGluZ190aW1lOiBzdWJ0YXNrLnRpbWVzdGFtcHNbXCJGSU5JU0hFRFwiXVxuICAgICAgICAgICAgdHlwZTogJ3JlZ3VsYXInXG4gICAgICAgICAgfVxuXG4gICAgICAgIHRlc3REYXRhLnB1c2gge1xuICAgICAgICAgIGxhYmVsOiBcIigje3N1YnRhc2suc3VidGFza30pICN7c3VidGFzay5ob3N0fVwiXG4gICAgICAgICAgdGltZXM6IHRpbWVzXG4gICAgICAgIH1cblxuICAgICAgY2hhcnQgPSBkMy50aW1lbGluZSgpLnN0YWNrKClcbiAgICAgIC50aWNrRm9ybWF0KHtcbiAgICAgICAgZm9ybWF0OiBkMy50aW1lLmZvcm1hdChcIiVMXCIpXG4gICAgICAgICMgdGlja0ludGVydmFsOiAxXG4gICAgICAgIHRpY2tTaXplOiAxXG4gICAgICB9KVxuICAgICAgLnByZWZpeChcInNpbmdsZVwiKVxuICAgICAgLmxhYmVsRm9ybWF0KChsYWJlbCkgLT5cbiAgICAgICAgbGFiZWxcbiAgICAgIClcbiAgICAgIC5tYXJnaW4oeyBsZWZ0OiAxMDAsIHJpZ2h0OiAwLCB0b3A6IDAsIGJvdHRvbTogMCB9KVxuICAgICAgLml0ZW1IZWlnaHQoMzApXG4gICAgICAucmVsYXRpdmVUaW1lKClcblxuICAgICAgc3ZnID0gZDMuc2VsZWN0KHN2Z0VsKVxuICAgICAgLmRhdHVtKHRlc3REYXRhKVxuICAgICAgLmNhbGwoY2hhcnQpXG5cbiAgICBhbmFseXplVGltZShzY29wZS5kYXRhKVxuXG4gICAgcmV0dXJuXG5cbiMgLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLVxuXG4uZGlyZWN0aXZlICd0aW1lbGluZScsICgkc3RhdGUpIC0+XG4gIHRlbXBsYXRlOiBcIjxzdmcgY2xhc3M9J3RpbWVsaW5lJyB3aWR0aD0nMCcgaGVpZ2h0PScwJz48L3N2Zz5cIlxuXG4gIHNjb3BlOlxuICAgIHZlcnRpY2VzOiBcIj1cIlxuICAgIGpvYmlkOiBcIj1cIlxuXG4gIGxpbms6IChzY29wZSwgZWxlbSwgYXR0cnMpIC0+XG4gICAgc3ZnRWwgPSBlbGVtLmNoaWxkcmVuKClbMF1cblxuICAgIGNvbnRhaW5lclcgPSBlbGVtLndpZHRoKClcbiAgICBhbmd1bGFyLmVsZW1lbnQoc3ZnRWwpLmF0dHIoJ3dpZHRoJywgY29udGFpbmVyVylcblxuICAgIHRyYW5zbGF0ZUxhYmVsID0gKGxhYmVsKSAtPlxuICAgICAgbGFiZWwucmVwbGFjZShcIiZndDtcIiwgXCI+XCIpXG5cbiAgICBhbmFseXplVGltZSA9IChkYXRhKSAtPlxuICAgICAgZDMuc2VsZWN0KHN2Z0VsKS5zZWxlY3RBbGwoXCIqXCIpLnJlbW92ZSgpXG5cbiAgICAgIHRlc3REYXRhID0gW11cblxuICAgICAgYW5ndWxhci5mb3JFYWNoIGRhdGEsICh2ZXJ0ZXgpIC0+XG4gICAgICAgIGlmIHZlcnRleFsnc3RhcnQtdGltZSddID4gLTFcbiAgICAgICAgICBpZiB2ZXJ0ZXgudHlwZSBpcyAnc2NoZWR1bGVkJ1xuICAgICAgICAgICAgdGVzdERhdGEucHVzaCBcbiAgICAgICAgICAgICAgdGltZXM6IFtcbiAgICAgICAgICAgICAgICBsYWJlbDogdHJhbnNsYXRlTGFiZWwodmVydGV4Lm5hbWUpXG4gICAgICAgICAgICAgICAgY29sb3I6IFwiI2NjY2NjY1wiXG4gICAgICAgICAgICAgICAgYm9yZGVyQ29sb3I6IFwiIzU1NTU1NVwiXG4gICAgICAgICAgICAgICAgc3RhcnRpbmdfdGltZTogdmVydGV4WydzdGFydC10aW1lJ11cbiAgICAgICAgICAgICAgICBlbmRpbmdfdGltZTogdmVydGV4WydlbmQtdGltZSddXG4gICAgICAgICAgICAgICAgdHlwZTogdmVydGV4LnR5cGVcbiAgICAgICAgICAgICAgXVxuICAgICAgICAgIGVsc2VcbiAgICAgICAgICAgIHRlc3REYXRhLnB1c2ggXG4gICAgICAgICAgICAgIHRpbWVzOiBbXG4gICAgICAgICAgICAgICAgbGFiZWw6IHRyYW5zbGF0ZUxhYmVsKHZlcnRleC5uYW1lKVxuICAgICAgICAgICAgICAgIGNvbG9yOiBcIiNkOWYxZjdcIlxuICAgICAgICAgICAgICAgIGJvcmRlckNvbG9yOiBcIiM2MmNkZWFcIlxuICAgICAgICAgICAgICAgIHN0YXJ0aW5nX3RpbWU6IHZlcnRleFsnc3RhcnQtdGltZSddXG4gICAgICAgICAgICAgICAgZW5kaW5nX3RpbWU6IHZlcnRleFsnZW5kLXRpbWUnXVxuICAgICAgICAgICAgICAgIGxpbms6IHZlcnRleC5pZFxuICAgICAgICAgICAgICAgIHR5cGU6IHZlcnRleC50eXBlXG4gICAgICAgICAgICAgIF1cblxuICAgICAgY2hhcnQgPSBkMy50aW1lbGluZSgpLnN0YWNrKCkuY2xpY2soKGQsIGksIGRhdHVtKSAtPlxuICAgICAgICBpZiBkLmxpbmtcbiAgICAgICAgICAkc3RhdGUuZ28gXCJzaW5nbGUtam9iLnRpbWVsaW5lLnZlcnRleFwiLCB7IGpvYmlkOiBzY29wZS5qb2JpZCwgdmVydGV4SWQ6IGQubGluayB9XG5cbiAgICAgIClcbiAgICAgIC50aWNrRm9ybWF0KHtcbiAgICAgICAgZm9ybWF0OiBkMy50aW1lLmZvcm1hdChcIiVMXCIpXG4gICAgICAgICMgdGlja1RpbWU6IGQzLnRpbWUuc2Vjb25kXG4gICAgICAgICMgdGlja0ludGVydmFsOiAwLjVcbiAgICAgICAgdGlja1NpemU6IDFcbiAgICAgIH0pXG4gICAgICAucHJlZml4KFwibWFpblwiKVxuICAgICAgLm1hcmdpbih7IGxlZnQ6IDAsIHJpZ2h0OiAwLCB0b3A6IDAsIGJvdHRvbTogMCB9KVxuICAgICAgLml0ZW1IZWlnaHQoMzApXG4gICAgICAuc2hvd0JvcmRlckxpbmUoKVxuICAgICAgLnNob3dIb3VyVGltZWxpbmUoKVxuXG4gICAgICBzdmcgPSBkMy5zZWxlY3Qoc3ZnRWwpXG4gICAgICAuZGF0dW0odGVzdERhdGEpXG4gICAgICAuY2FsbChjaGFydClcblxuICAgIHNjb3BlLiR3YXRjaCBhdHRycy52ZXJ0aWNlcywgKGRhdGEpIC0+XG4gICAgICBhbmFseXplVGltZShkYXRhKSBpZiBkYXRhXG5cbiAgICByZXR1cm5cblxuIyAtLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tXG5cbi5kaXJlY3RpdmUgJ2pvYlBsYW4nLCAoJHRpbWVvdXQpIC0+XG4gIHRlbXBsYXRlOiBcIlxuICAgIDxzdmcgY2xhc3M9J2dyYXBoJyB3aWR0aD0nNTAwJyBoZWlnaHQ9JzQwMCc+PGcgLz48L3N2Zz5cbiAgICA8c3ZnIGNsYXNzPSd0bXAnIHdpZHRoPScxJyBoZWlnaHQ9JzEnPjxnIC8+PC9zdmc+XG4gICAgPGRpdiBjbGFzcz0nYnRuLWdyb3VwIHpvb20tYnV0dG9ucyc+XG4gICAgICA8YSBjbGFzcz0nYnRuIGJ0bi1kZWZhdWx0IHpvb20taW4nIG5nLWNsaWNrPSd6b29tSW4oKSc+PGkgY2xhc3M9J2ZhIGZhLXBsdXMnIC8+PC9hPlxuICAgICAgPGEgY2xhc3M9J2J0biBidG4tZGVmYXVsdCB6b29tLW91dCcgbmctY2xpY2s9J3pvb21PdXQoKSc+PGkgY2xhc3M9J2ZhIGZhLW1pbnVzJyAvPjwvYT5cbiAgICA8L2Rpdj5cIlxuXG4gIHNjb3BlOlxuICAgIHBsYW46ICc9J1xuICAgIHNldE5vZGU6ICcmJ1xuXG4gIGxpbms6IChzY29wZSwgZWxlbSwgYXR0cnMpIC0+XG4gICAgZyA9IG51bGxcbiAgICBtYWluWm9vbSA9IGQzLmJlaGF2aW9yLnpvb20oKVxuICAgIHN1YmdyYXBocyA9IFtdXG4gICAgam9iaWQgPSBhdHRycy5qb2JpZFxuXG4gICAgbWFpblN2Z0VsZW1lbnQgPSBlbGVtLmNoaWxkcmVuKClbMF1cbiAgICBtYWluRyA9IGVsZW0uY2hpbGRyZW4oKS5jaGlsZHJlbigpWzBdXG4gICAgbWFpblRtcEVsZW1lbnQgPSBlbGVtLmNoaWxkcmVuKClbMV1cblxuICAgIGQzbWFpblN2ZyA9IGQzLnNlbGVjdChtYWluU3ZnRWxlbWVudClcbiAgICBkM21haW5TdmdHID0gZDMuc2VsZWN0KG1haW5HKVxuICAgIGQzdG1wU3ZnID0gZDMuc2VsZWN0KG1haW5UbXBFbGVtZW50KVxuXG4gICAgIyBhbmd1bGFyLmVsZW1lbnQobWFpbkcpLmVtcHR5KClcbiAgICAjIGQzbWFpblN2Z0cuc2VsZWN0QWxsKFwiKlwiKS5yZW1vdmUoKVxuXG4gICAgY29udGFpbmVyVyA9IGVsZW0ud2lkdGgoKVxuICAgIGFuZ3VsYXIuZWxlbWVudChlbGVtLmNoaWxkcmVuKClbMF0pLndpZHRoKGNvbnRhaW5lclcpXG5cbiAgICBzY29wZS56b29tSW4gPSAtPlxuICAgICAgaWYgbWFpblpvb20uc2NhbGUoKSA8IDIuOTlcbiAgICAgICAgXG4gICAgICAgICMgQ2FsY3VsYXRlIGFuZCBzdG9yZSBuZXcgdmFsdWVzIGluIHpvb20gb2JqZWN0XG4gICAgICAgIHRyYW5zbGF0ZSA9IG1haW5ab29tLnRyYW5zbGF0ZSgpXG4gICAgICAgIHYxID0gdHJhbnNsYXRlWzBdICogKG1haW5ab29tLnNjYWxlKCkgKyAwLjEgLyAobWFpblpvb20uc2NhbGUoKSkpXG4gICAgICAgIHYyID0gdHJhbnNsYXRlWzFdICogKG1haW5ab29tLnNjYWxlKCkgKyAwLjEgLyAobWFpblpvb20uc2NhbGUoKSkpXG4gICAgICAgIG1haW5ab29tLnNjYWxlIG1haW5ab29tLnNjYWxlKCkgKyAwLjFcbiAgICAgICAgbWFpblpvb20udHJhbnNsYXRlIFsgdjEsIHYyIF1cbiAgICAgICAgXG4gICAgICAgICMgVHJhbnNmb3JtIHN2Z1xuICAgICAgICBkM21haW5TdmdHLmF0dHIgXCJ0cmFuc2Zvcm1cIiwgXCJ0cmFuc2xhdGUoXCIgKyB2MSArIFwiLFwiICsgdjIgKyBcIikgc2NhbGUoXCIgKyBtYWluWm9vbS5zY2FsZSgpICsgXCIpXCJcblxuICAgIHNjb3BlLnpvb21PdXQgPSAtPlxuICAgICAgaWYgbWFpblpvb20uc2NhbGUoKSA+IDAuMzFcbiAgICAgICAgXG4gICAgICAgICMgQ2FsY3VsYXRlIGFuZCBzdG9yZSBuZXcgdmFsdWVzIGluIG1haW5ab29tIG9iamVjdFxuICAgICAgICBtYWluWm9vbS5zY2FsZSBtYWluWm9vbS5zY2FsZSgpIC0gMC4xXG4gICAgICAgIHRyYW5zbGF0ZSA9IG1haW5ab29tLnRyYW5zbGF0ZSgpXG4gICAgICAgIHYxID0gdHJhbnNsYXRlWzBdICogKG1haW5ab29tLnNjYWxlKCkgLSAwLjEgLyAobWFpblpvb20uc2NhbGUoKSkpXG4gICAgICAgIHYyID0gdHJhbnNsYXRlWzFdICogKG1haW5ab29tLnNjYWxlKCkgLSAwLjEgLyAobWFpblpvb20uc2NhbGUoKSkpXG4gICAgICAgIG1haW5ab29tLnRyYW5zbGF0ZSBbIHYxLCB2MiBdXG4gICAgICAgIFxuICAgICAgICAjIFRyYW5zZm9ybSBzdmdcbiAgICAgICAgZDNtYWluU3ZnRy5hdHRyIFwidHJhbnNmb3JtXCIsIFwidHJhbnNsYXRlKFwiICsgdjEgKyBcIixcIiArIHYyICsgXCIpIHNjYWxlKFwiICsgbWFpblpvb20uc2NhbGUoKSArIFwiKVwiXG5cbiAgICAjY3JlYXRlIGEgbGFiZWwgb2YgYW4gZWRnZVxuICAgIGNyZWF0ZUxhYmVsRWRnZSA9IChlbCkgLT5cbiAgICAgIGxhYmVsVmFsdWUgPSBcIlwiXG4gICAgICBpZiBlbC5zaGlwX3N0cmF0ZWd5PyBvciBlbC5sb2NhbF9zdHJhdGVneT9cbiAgICAgICAgbGFiZWxWYWx1ZSArPSBcIjxkaXYgY2xhc3M9J2VkZ2UtbGFiZWwnPlwiXG4gICAgICAgIGxhYmVsVmFsdWUgKz0gZWwuc2hpcF9zdHJhdGVneSAgaWYgZWwuc2hpcF9zdHJhdGVneT9cbiAgICAgICAgbGFiZWxWYWx1ZSArPSBcIiAoXCIgKyBlbC50ZW1wX21vZGUgKyBcIilcIiAgdW5sZXNzIGVsLnRlbXBfbW9kZSBpcyBgdW5kZWZpbmVkYFxuICAgICAgICBsYWJlbFZhbHVlICs9IFwiLDxicj5cIiArIGVsLmxvY2FsX3N0cmF0ZWd5ICB1bmxlc3MgZWwubG9jYWxfc3RyYXRlZ3kgaXMgYHVuZGVmaW5lZGBcbiAgICAgICAgbGFiZWxWYWx1ZSArPSBcIjwvZGl2PlwiXG4gICAgICBsYWJlbFZhbHVlXG5cblxuICAgICMgdHJ1ZSwgaWYgdGhlIG5vZGUgaXMgYSBzcGVjaWFsIG5vZGUgZnJvbSBhbiBpdGVyYXRpb25cbiAgICBpc1NwZWNpYWxJdGVyYXRpb25Ob2RlID0gKGluZm8pIC0+XG4gICAgICAoaW5mbyBpcyBcInBhcnRpYWxTb2x1dGlvblwiIG9yIGluZm8gaXMgXCJuZXh0UGFydGlhbFNvbHV0aW9uXCIgb3IgaW5mbyBpcyBcIndvcmtzZXRcIiBvciBpbmZvIGlzIFwibmV4dFdvcmtzZXRcIiBvciBpbmZvIGlzIFwic29sdXRpb25TZXRcIiBvciBpbmZvIGlzIFwic29sdXRpb25EZWx0YVwiKVxuXG4gICAgZ2V0Tm9kZVR5cGUgPSAoZWwsIGluZm8pIC0+XG4gICAgICBpZiBpbmZvIGlzIFwibWlycm9yXCJcbiAgICAgICAgJ25vZGUtbWlycm9yJ1xuXG4gICAgICBlbHNlIGlmIGlzU3BlY2lhbEl0ZXJhdGlvbk5vZGUoaW5mbylcbiAgICAgICAgJ25vZGUtaXRlcmF0aW9uJ1xuXG4gICAgICBlbHNlXG4gICAgICAgICAgJ25vZGUtbm9ybWFsJ1xuICAgICAgXG4gICAgIyBjcmVhdGVzIHRoZSBsYWJlbCBvZiBhIG5vZGUsIGluIGluZm8gaXMgc3RvcmVkLCB3aGV0aGVyIGl0IGlzIGEgc3BlY2lhbCBub2RlIChsaWtlIGEgbWlycm9yIGluIGFuIGl0ZXJhdGlvbilcbiAgICBjcmVhdGVMYWJlbE5vZGUgPSAoZWwsIGluZm8sIG1heFcsIG1heEgpIC0+XG4gICAgICAjIGxhYmVsVmFsdWUgPSBcIjxhIGhyZWY9JyMvam9icy9cIiArIGpvYmlkICsgXCIvdmVydGV4L1wiICsgZWwuaWQgKyBcIicgY2xhc3M9J25vZGUtbGFiZWwgXCIgKyBnZXROb2RlVHlwZShlbCwgaW5mbykgKyBcIic+XCJcbiAgICAgIGxhYmVsVmFsdWUgPSBcIjxkaXYgaHJlZj0nIy9qb2JzL1wiICsgam9iaWQgKyBcIi92ZXJ0ZXgvXCIgKyBlbC5pZCArIFwiJyBjbGFzcz0nbm9kZS1sYWJlbCBcIiArIGdldE5vZGVUeXBlKGVsLCBpbmZvKSArIFwiJz5cIlxuXG4gICAgICAjIE5vZGVuYW1lXG4gICAgICBpZiBpbmZvIGlzIFwibWlycm9yXCJcbiAgICAgICAgbGFiZWxWYWx1ZSArPSBcIjxoMyBjbGFzcz0nbm9kZS1uYW1lJz5NaXJyb3Igb2YgXCIgKyBlbC5vcGVyYXRvciArIFwiPC9oMz5cIlxuICAgICAgZWxzZVxuICAgICAgICBsYWJlbFZhbHVlICs9IFwiPGgzIGNsYXNzPSdub2RlLW5hbWUnPlwiICsgZWwub3BlcmF0b3IgKyBcIjwvaDM+XCJcbiAgICAgIGlmIGVsLmRlc2NyaXB0aW9uIGlzIFwiXCJcbiAgICAgICAgbGFiZWxWYWx1ZSArPSBcIlwiXG4gICAgICBlbHNlXG4gICAgICAgIHN0ZXBOYW1lID0gZWwuZGVzY3JpcHRpb25cbiAgICAgICAgXG4gICAgICAgICMgY2xlYW4gc3RlcE5hbWVcbiAgICAgICAgc3RlcE5hbWUgPSBzaG9ydGVuU3RyaW5nKHN0ZXBOYW1lKVxuICAgICAgICBsYWJlbFZhbHVlICs9IFwiPGg0IGNsYXNzPSdzdGVwLW5hbWUnPlwiICsgc3RlcE5hbWUgKyBcIjwvaDQ+XCJcbiAgICAgIFxuICAgICAgIyBJZiB0aGlzIG5vZGUgaXMgYW4gXCJpdGVyYXRpb25cIiB3ZSBuZWVkIGEgZGlmZmVyZW50IHBhbmVsLWJvZHlcbiAgICAgIGlmIGVsLnN0ZXBfZnVuY3Rpb24/XG4gICAgICAgIGxhYmVsVmFsdWUgKz0gZXh0ZW5kTGFiZWxOb2RlRm9ySXRlcmF0aW9uKGVsLmlkLCBtYXhXLCBtYXhIKVxuICAgICAgZWxzZVxuICAgICAgICBcbiAgICAgICAgIyBPdGhlcndpc2UgYWRkIGluZm9zICAgIFxuICAgICAgICBsYWJlbFZhbHVlICs9IFwiPGg1PlwiICsgaW5mbyArIFwiIE5vZGU8L2g1PlwiICBpZiBpc1NwZWNpYWxJdGVyYXRpb25Ob2RlKGluZm8pXG4gICAgICAgIGxhYmVsVmFsdWUgKz0gXCI8aDU+UGFyYWxsZWxpc206IFwiICsgZWwucGFyYWxsZWxpc20gKyBcIjwvaDU+XCIgIHVubGVzcyBlbC5wYXJhbGxlbGlzbSBpcyBcIlwiXG4gICAgICAgIGxhYmVsVmFsdWUgKz0gXCI8aDU+T3BlcmF0aW9uOiBcIiArIHNob3J0ZW5TdHJpbmcoZWwub3BlcmF0b3Jfc3RyYXRlZ3kpICsgXCI8L2g1PlwiICB1bmxlc3MgZWwub3BlcmF0b3IgaXMgYHVuZGVmaW5lZGBcbiAgICAgIFxuICAgICAgIyBsYWJlbFZhbHVlICs9IFwiPC9hPlwiXG4gICAgICBsYWJlbFZhbHVlICs9IFwiPC9kaXY+XCJcbiAgICAgIGxhYmVsVmFsdWVcblxuICAgICMgRXh0ZW5kcyB0aGUgbGFiZWwgb2YgYSBub2RlIHdpdGggYW4gYWRkaXRpb25hbCBzdmcgRWxlbWVudCB0byBwcmVzZW50IHRoZSBpdGVyYXRpb24uXG4gICAgZXh0ZW5kTGFiZWxOb2RlRm9ySXRlcmF0aW9uID0gKGlkLCBtYXhXLCBtYXhIKSAtPlxuICAgICAgc3ZnSUQgPSBcInN2Zy1cIiArIGlkXG5cbiAgICAgIGxhYmVsVmFsdWUgPSBcIjxzdmcgY2xhc3M9J1wiICsgc3ZnSUQgKyBcIicgd2lkdGg9XCIgKyBtYXhXICsgXCIgaGVpZ2h0PVwiICsgbWF4SCArIFwiPjxnIC8+PC9zdmc+XCJcbiAgICAgIGxhYmVsVmFsdWVcblxuICAgICMgU3BsaXQgYSBzdHJpbmcgaW50byBtdWx0aXBsZSBsaW5lcyBzbyB0aGF0IGVhY2ggbGluZSBoYXMgbGVzcyB0aGFuIDMwIGxldHRlcnMuXG4gICAgc2hvcnRlblN0cmluZyA9IChzKSAtPlxuICAgICAgIyBtYWtlIHN1cmUgdGhhdCBuYW1lIGRvZXMgbm90IGNvbnRhaW4gYSA8IChiZWNhdXNlIG9mIGh0bWwpXG4gICAgICBpZiBzLmNoYXJBdCgwKSBpcyBcIjxcIlxuICAgICAgICBzID0gcy5yZXBsYWNlKFwiPFwiLCBcIiZsdDtcIilcbiAgICAgICAgcyA9IHMucmVwbGFjZShcIj5cIiwgXCImZ3Q7XCIpXG4gICAgICBzYnIgPSBcIlwiXG4gICAgICB3aGlsZSBzLmxlbmd0aCA+IDMwXG4gICAgICAgIHNiciA9IHNiciArIHMuc3Vic3RyaW5nKDAsIDMwKSArIFwiPGJyPlwiXG4gICAgICAgIHMgPSBzLnN1YnN0cmluZygzMCwgcy5sZW5ndGgpXG4gICAgICBzYnIgPSBzYnIgKyBzXG4gICAgICBzYnJcblxuICAgIGNyZWF0ZU5vZGUgPSAoZywgZGF0YSwgZWwsIGlzUGFyZW50ID0gZmFsc2UsIG1heFcsIG1heEgpIC0+XG4gICAgICAjIGNyZWF0ZSBub2RlLCBzZW5kIGFkZGl0aW9uYWwgaW5mb3JtYXRpb25zIGFib3V0IHRoZSBub2RlIGlmIGl0IGlzIGEgc3BlY2lhbCBvbmVcbiAgICAgIGlmIGVsLmlkIGlzIGRhdGEucGFydGlhbF9zb2x1dGlvblxuICAgICAgICBnLnNldE5vZGUgZWwuaWQsXG4gICAgICAgICAgbGFiZWw6IGNyZWF0ZUxhYmVsTm9kZShlbCwgXCJwYXJ0aWFsU29sdXRpb25cIiwgbWF4VywgbWF4SClcbiAgICAgICAgICBsYWJlbFR5cGU6ICdodG1sJ1xuICAgICAgICAgIGNsYXNzOiBnZXROb2RlVHlwZShlbCwgXCJwYXJ0aWFsU29sdXRpb25cIilcblxuICAgICAgZWxzZSBpZiBlbC5pZCBpcyBkYXRhLm5leHRfcGFydGlhbF9zb2x1dGlvblxuICAgICAgICBnLnNldE5vZGUgZWwuaWQsXG4gICAgICAgICAgbGFiZWw6IGNyZWF0ZUxhYmVsTm9kZShlbCwgXCJuZXh0UGFydGlhbFNvbHV0aW9uXCIsIG1heFcsIG1heEgpXG4gICAgICAgICAgbGFiZWxUeXBlOiAnaHRtbCdcbiAgICAgICAgICBjbGFzczogZ2V0Tm9kZVR5cGUoZWwsIFwibmV4dFBhcnRpYWxTb2x1dGlvblwiKVxuXG4gICAgICBlbHNlIGlmIGVsLmlkIGlzIGRhdGEud29ya3NldFxuICAgICAgICBnLnNldE5vZGUgZWwuaWQsXG4gICAgICAgICAgbGFiZWw6IGNyZWF0ZUxhYmVsTm9kZShlbCwgXCJ3b3Jrc2V0XCIsIG1heFcsIG1heEgpXG4gICAgICAgICAgbGFiZWxUeXBlOiAnaHRtbCdcbiAgICAgICAgICBjbGFzczogZ2V0Tm9kZVR5cGUoZWwsIFwid29ya3NldFwiKVxuXG4gICAgICBlbHNlIGlmIGVsLmlkIGlzIGRhdGEubmV4dF93b3Jrc2V0XG4gICAgICAgIGcuc2V0Tm9kZSBlbC5pZCxcbiAgICAgICAgICBsYWJlbDogY3JlYXRlTGFiZWxOb2RlKGVsLCBcIm5leHRXb3Jrc2V0XCIsIG1heFcsIG1heEgpXG4gICAgICAgICAgbGFiZWxUeXBlOiAnaHRtbCdcbiAgICAgICAgICBjbGFzczogZ2V0Tm9kZVR5cGUoZWwsIFwibmV4dFdvcmtzZXRcIilcblxuICAgICAgZWxzZSBpZiBlbC5pZCBpcyBkYXRhLnNvbHV0aW9uX3NldFxuICAgICAgICBnLnNldE5vZGUgZWwuaWQsXG4gICAgICAgICAgbGFiZWw6IGNyZWF0ZUxhYmVsTm9kZShlbCwgXCJzb2x1dGlvblNldFwiLCBtYXhXLCBtYXhIKVxuICAgICAgICAgIGxhYmVsVHlwZTogJ2h0bWwnXG4gICAgICAgICAgY2xhc3M6IGdldE5vZGVUeXBlKGVsLCBcInNvbHV0aW9uU2V0XCIpXG5cbiAgICAgIGVsc2UgaWYgZWwuaWQgaXMgZGF0YS5zb2x1dGlvbl9kZWx0YVxuICAgICAgICBnLnNldE5vZGUgZWwuaWQsXG4gICAgICAgICAgbGFiZWw6IGNyZWF0ZUxhYmVsTm9kZShlbCwgXCJzb2x1dGlvbkRlbHRhXCIsIG1heFcsIG1heEgpXG4gICAgICAgICAgbGFiZWxUeXBlOiAnaHRtbCdcbiAgICAgICAgICBjbGFzczogZ2V0Tm9kZVR5cGUoZWwsIFwic29sdXRpb25EZWx0YVwiKVxuXG4gICAgICBlbHNlXG4gICAgICAgIGcuc2V0Tm9kZSBlbC5pZCxcbiAgICAgICAgICBsYWJlbDogY3JlYXRlTGFiZWxOb2RlKGVsLCBcIlwiLCBtYXhXLCBtYXhIKVxuICAgICAgICAgIGxhYmVsVHlwZTogJ2h0bWwnXG4gICAgICAgICAgY2xhc3M6IGdldE5vZGVUeXBlKGVsLCBcIlwiKVxuXG4gICAgY3JlYXRlRWRnZSA9IChnLCBkYXRhLCBlbCwgZXhpc3RpbmdOb2RlcywgcHJlZCkgLT5cbiAgICAgIHVubGVzcyBleGlzdGluZ05vZGVzLmluZGV4T2YocHJlZC5pZCkgaXMgLTFcbiAgICAgICAgZy5zZXRFZGdlIHByZWQuaWQsIGVsLmlkLFxuICAgICAgICAgIGxhYmVsOiBjcmVhdGVMYWJlbEVkZ2UocHJlZClcbiAgICAgICAgICBsYWJlbFR5cGU6ICdodG1sJ1xuICAgICAgICAgIGFycm93aGVhZDogJ25vcm1hbCdcblxuICAgICAgZWxzZVxuICAgICAgICBtaXNzaW5nTm9kZSA9IHNlYXJjaEZvck5vZGUoZGF0YSwgcHJlZC5pZClcbiAgICAgICAgdW5sZXNzICFtaXNzaW5nTm9kZVxuICAgICAgICAgIGcuc2V0RWRnZSBtaXNzaW5nTm9kZS5pZCwgZWwuaWQsXG4gICAgICAgICAgICBsYWJlbDogY3JlYXRlTGFiZWxFZGdlKG1pc3NpbmdOb2RlKVxuICAgICAgICAgICAgbGFiZWxUeXBlOiAnaHRtbCdcblxuICAgIGxvYWRKc29uVG9EYWdyZSA9IChnLCBkYXRhKSAtPlxuICAgICAgZXhpc3RpbmdOb2RlcyA9IFtdXG5cbiAgICAgIGlmIGRhdGEubm9kZXM/XG4gICAgICAgICMgVGhpcyBpcyB0aGUgbm9ybWFsIGpzb24gZGF0YVxuICAgICAgICB0b0l0ZXJhdGUgPSBkYXRhLm5vZGVzXG5cbiAgICAgIGVsc2VcbiAgICAgICAgIyBUaGlzIGlzIGFuIGl0ZXJhdGlvbiwgd2Ugbm93IHN0b3JlIHNwZWNpYWwgaXRlcmF0aW9uIG5vZGVzIGlmIHBvc3NpYmxlXG4gICAgICAgIHRvSXRlcmF0ZSA9IGRhdGEuc3RlcF9mdW5jdGlvblxuICAgICAgICBpc1BhcmVudCA9IHRydWVcblxuICAgICAgZm9yIGVsIGluIHRvSXRlcmF0ZVxuICAgICAgICBtYXhXID0gMFxuICAgICAgICBtYXhIID0gMFxuXG4gICAgICAgIGlmIGVsLnN0ZXBfZnVuY3Rpb25cbiAgICAgICAgICBzZyA9IG5ldyBkYWdyZUQzLmdyYXBobGliLkdyYXBoKHsgbXVsdGlncmFwaDogdHJ1ZSwgY29tcG91bmQ6IHRydWUgfSkuc2V0R3JhcGgoe1xuICAgICAgICAgICAgbm9kZXNlcDogMjBcbiAgICAgICAgICAgIGVkZ2VzZXA6IDBcbiAgICAgICAgICAgIHJhbmtzZXA6IDIwXG4gICAgICAgICAgICByYW5rZGlyOiBcIkxSXCJcbiAgICAgICAgICAgIG1hcmdpbng6IDEwXG4gICAgICAgICAgICBtYXJnaW55OiAxMFxuICAgICAgICAgICAgfSlcblxuICAgICAgICAgIHN1YmdyYXBoc1tlbC5pZF0gPSBzZ1xuXG4gICAgICAgICAgbG9hZEpzb25Ub0RhZ3JlKHNnLCBlbClcblxuICAgICAgICAgIHIgPSBuZXcgZGFncmVEMy5yZW5kZXIoKVxuICAgICAgICAgIGQzdG1wU3ZnLnNlbGVjdCgnZycpLmNhbGwociwgc2cpXG4gICAgICAgICAgbWF4VyA9IHNnLmdyYXBoKCkud2lkdGhcbiAgICAgICAgICBtYXhIID0gc2cuZ3JhcGgoKS5oZWlnaHRcblxuICAgICAgICAgIGFuZ3VsYXIuZWxlbWVudChtYWluVG1wRWxlbWVudCkuZW1wdHkoKVxuXG4gICAgICAgIGNyZWF0ZU5vZGUoZywgZGF0YSwgZWwsIGlzUGFyZW50LCBtYXhXLCBtYXhIKVxuXG4gICAgICAgIGV4aXN0aW5nTm9kZXMucHVzaCBlbC5pZFxuICAgICAgICBcbiAgICAgICAgIyBjcmVhdGUgZWRnZXMgZnJvbSBpbnB1dHMgdG8gY3VycmVudCBub2RlXG4gICAgICAgIGlmIGVsLmlucHV0cz9cbiAgICAgICAgICBmb3IgcHJlZCBpbiBlbC5pbnB1dHNcbiAgICAgICAgICAgIGNyZWF0ZUVkZ2UoZywgZGF0YSwgZWwsIGV4aXN0aW5nTm9kZXMsIHByZWQpXG5cbiAgICAgIGdcblxuICAgICMgc2VhcmNoZXMgaW4gdGhlIGdsb2JhbCBKU09ORGF0YSBmb3IgdGhlIG5vZGUgd2l0aCB0aGUgZ2l2ZW4gaWRcbiAgICBzZWFyY2hGb3JOb2RlID0gKGRhdGEsIG5vZGVJRCkgLT5cbiAgICAgIGZvciBpIG9mIGRhdGEubm9kZXNcbiAgICAgICAgZWwgPSBkYXRhLm5vZGVzW2ldXG4gICAgICAgIHJldHVybiBlbCAgaWYgZWwuaWQgaXMgbm9kZUlEXG4gICAgICAgIFxuICAgICAgICAjIGxvb2sgZm9yIG5vZGVzIHRoYXQgYXJlIGluIGl0ZXJhdGlvbnNcbiAgICAgICAgaWYgZWwuc3RlcF9mdW5jdGlvbj9cbiAgICAgICAgICBmb3IgaiBvZiBlbC5zdGVwX2Z1bmN0aW9uXG4gICAgICAgICAgICByZXR1cm4gZWwuc3RlcF9mdW5jdGlvbltqXSAgaWYgZWwuc3RlcF9mdW5jdGlvbltqXS5pZCBpcyBub2RlSURcblxuICAgIGRyYXdHcmFwaCA9IChkYXRhKSAtPlxuICAgICAgZyA9IG5ldyBkYWdyZUQzLmdyYXBobGliLkdyYXBoKHsgbXVsdGlncmFwaDogdHJ1ZSwgY29tcG91bmQ6IHRydWUgfSkuc2V0R3JhcGgoe1xuICAgICAgICBub2Rlc2VwOiA3MFxuICAgICAgICBlZGdlc2VwOiAwXG4gICAgICAgIHJhbmtzZXA6IDUwXG4gICAgICAgIHJhbmtkaXI6IFwiTFJcIlxuICAgICAgICBtYXJnaW54OiA0MFxuICAgICAgICBtYXJnaW55OiA0MFxuICAgICAgICB9KVxuXG4gICAgICBsb2FkSnNvblRvRGFncmUoZywgZGF0YSlcblxuICAgICAgcmVuZGVyZXIgPSBuZXcgZGFncmVEMy5yZW5kZXIoKVxuICAgICAgZDNtYWluU3ZnRy5jYWxsKHJlbmRlcmVyLCBnKVxuXG4gICAgICBmb3IgaSwgc2cgb2Ygc3ViZ3JhcGhzXG4gICAgICAgIGQzbWFpblN2Zy5zZWxlY3QoJ3N2Zy5zdmctJyArIGkgKyAnIGcnKS5jYWxsKHJlbmRlcmVyLCBzZylcblxuICAgICAgbmV3U2NhbGUgPSAwLjVcblxuICAgICAgeENlbnRlck9mZnNldCA9IE1hdGguZmxvb3IoKGFuZ3VsYXIuZWxlbWVudChtYWluU3ZnRWxlbWVudCkud2lkdGgoKSAtIGcuZ3JhcGgoKS53aWR0aCAqIG5ld1NjYWxlKSAvIDIpXG4gICAgICB5Q2VudGVyT2Zmc2V0ID0gTWF0aC5mbG9vcigoYW5ndWxhci5lbGVtZW50KG1haW5TdmdFbGVtZW50KS5oZWlnaHQoKSAtIGcuZ3JhcGgoKS5oZWlnaHQgKiBuZXdTY2FsZSkgLyAyKVxuXG4gICAgICBtYWluWm9vbS5zY2FsZShuZXdTY2FsZSkudHJhbnNsYXRlKFt4Q2VudGVyT2Zmc2V0LCB5Q2VudGVyT2Zmc2V0XSlcblxuICAgICAgZDNtYWluU3ZnRy5hdHRyKFwidHJhbnNmb3JtXCIsIFwidHJhbnNsYXRlKFwiICsgeENlbnRlck9mZnNldCArIFwiLCBcIiArIHlDZW50ZXJPZmZzZXQgKyBcIikgc2NhbGUoXCIgKyBtYWluWm9vbS5zY2FsZSgpICsgXCIpXCIpXG5cbiAgICAgIG1haW5ab29tLm9uKFwiem9vbVwiLCAtPlxuICAgICAgICBldiA9IGQzLmV2ZW50XG4gICAgICAgIGQzbWFpblN2Z0cuYXR0ciBcInRyYW5zZm9ybVwiLCBcInRyYW5zbGF0ZShcIiArIGV2LnRyYW5zbGF0ZSArIFwiKSBzY2FsZShcIiArIGV2LnNjYWxlICsgXCIpXCJcbiAgICAgIClcbiAgICAgIG1haW5ab29tKGQzbWFpblN2ZylcblxuICAgICAgZDNtYWluU3ZnRy5zZWxlY3RBbGwoJy5ub2RlJykub24gJ2NsaWNrJywgKGQpIC0+XG4gICAgICAgIHNjb3BlLnNldE5vZGUoeyBub2RlaWQ6IGQgfSlcblxuICAgIHNjb3BlLiR3YXRjaCBhdHRycy5wbGFuLCAobmV3UGxhbikgLT5cbiAgICAgIGRyYXdHcmFwaChuZXdQbGFuKSBpZiBuZXdQbGFuXG5cbiAgICByZXR1cm5cbiIsImFuZ3VsYXIubW9kdWxlKCdmbGlua0FwcCcpLmRpcmVjdGl2ZSgndmVydGV4JywgZnVuY3Rpb24oJHN0YXRlKSB7XG4gIHJldHVybiB7XG4gICAgdGVtcGxhdGU6IFwiPHN2ZyBjbGFzcz0ndGltZWxpbmUgc2Vjb25kYXJ5JyB3aWR0aD0nMCcgaGVpZ2h0PScwJz48L3N2Zz5cIixcbiAgICBzY29wZToge1xuICAgICAgZGF0YTogXCI9XCJcbiAgICB9LFxuICAgIGxpbms6IGZ1bmN0aW9uKHNjb3BlLCBlbGVtLCBhdHRycykge1xuICAgICAgdmFyIGFuYWx5emVUaW1lLCBjb250YWluZXJXLCBzdmdFbDtcbiAgICAgIHN2Z0VsID0gZWxlbS5jaGlsZHJlbigpWzBdO1xuICAgICAgY29udGFpbmVyVyA9IGVsZW0ud2lkdGgoKTtcbiAgICAgIGFuZ3VsYXIuZWxlbWVudChzdmdFbCkuYXR0cignd2lkdGgnLCBjb250YWluZXJXKTtcbiAgICAgIGFuYWx5emVUaW1lID0gZnVuY3Rpb24oZGF0YSkge1xuICAgICAgICB2YXIgY2hhcnQsIHN2ZywgdGVzdERhdGE7XG4gICAgICAgIGQzLnNlbGVjdChzdmdFbCkuc2VsZWN0QWxsKFwiKlwiKS5yZW1vdmUoKTtcbiAgICAgICAgdGVzdERhdGEgPSBbXTtcbiAgICAgICAgYW5ndWxhci5mb3JFYWNoKGRhdGEuc3VidGFza3MsIGZ1bmN0aW9uKHN1YnRhc2ssIGkpIHtcbiAgICAgICAgICB2YXIgdGltZXM7XG4gICAgICAgICAgdGltZXMgPSBbXG4gICAgICAgICAgICB7XG4gICAgICAgICAgICAgIGxhYmVsOiBcIlNjaGVkdWxlZFwiLFxuICAgICAgICAgICAgICBjb2xvcjogXCIjNjY2XCIsXG4gICAgICAgICAgICAgIGJvcmRlckNvbG9yOiBcIiM1NTVcIixcbiAgICAgICAgICAgICAgc3RhcnRpbmdfdGltZTogc3VidGFzay50aW1lc3RhbXBzW1wiU0NIRURVTEVEXCJdLFxuICAgICAgICAgICAgICBlbmRpbmdfdGltZTogc3VidGFzay50aW1lc3RhbXBzW1wiREVQTE9ZSU5HXCJdLFxuICAgICAgICAgICAgICB0eXBlOiAncmVndWxhcidcbiAgICAgICAgICAgIH0sIHtcbiAgICAgICAgICAgICAgbGFiZWw6IFwiRGVwbG95aW5nXCIsXG4gICAgICAgICAgICAgIGNvbG9yOiBcIiNhYWFcIixcbiAgICAgICAgICAgICAgYm9yZGVyQ29sb3I6IFwiIzU1NVwiLFxuICAgICAgICAgICAgICBzdGFydGluZ190aW1lOiBzdWJ0YXNrLnRpbWVzdGFtcHNbXCJERVBMT1lJTkdcIl0sXG4gICAgICAgICAgICAgIGVuZGluZ190aW1lOiBzdWJ0YXNrLnRpbWVzdGFtcHNbXCJSVU5OSU5HXCJdLFxuICAgICAgICAgICAgICB0eXBlOiAncmVndWxhcidcbiAgICAgICAgICAgIH1cbiAgICAgICAgICBdO1xuICAgICAgICAgIGlmIChzdWJ0YXNrLnRpbWVzdGFtcHNbXCJGSU5JU0hFRFwiXSA+IDApIHtcbiAgICAgICAgICAgIHRpbWVzLnB1c2goe1xuICAgICAgICAgICAgICBsYWJlbDogXCJSdW5uaW5nXCIsXG4gICAgICAgICAgICAgIGNvbG9yOiBcIiNkZGRcIixcbiAgICAgICAgICAgICAgYm9yZGVyQ29sb3I6IFwiIzU1NVwiLFxuICAgICAgICAgICAgICBzdGFydGluZ190aW1lOiBzdWJ0YXNrLnRpbWVzdGFtcHNbXCJSVU5OSU5HXCJdLFxuICAgICAgICAgICAgICBlbmRpbmdfdGltZTogc3VidGFzay50aW1lc3RhbXBzW1wiRklOSVNIRURcIl0sXG4gICAgICAgICAgICAgIHR5cGU6ICdyZWd1bGFyJ1xuICAgICAgICAgICAgfSk7XG4gICAgICAgICAgfVxuICAgICAgICAgIHJldHVybiB0ZXN0RGF0YS5wdXNoKHtcbiAgICAgICAgICAgIGxhYmVsOiBcIihcIiArIHN1YnRhc2suc3VidGFzayArIFwiKSBcIiArIHN1YnRhc2suaG9zdCxcbiAgICAgICAgICAgIHRpbWVzOiB0aW1lc1xuICAgICAgICAgIH0pO1xuICAgICAgICB9KTtcbiAgICAgICAgY2hhcnQgPSBkMy50aW1lbGluZSgpLnN0YWNrKCkudGlja0Zvcm1hdCh7XG4gICAgICAgICAgZm9ybWF0OiBkMy50aW1lLmZvcm1hdChcIiVMXCIpLFxuICAgICAgICAgIHRpY2tTaXplOiAxXG4gICAgICAgIH0pLnByZWZpeChcInNpbmdsZVwiKS5sYWJlbEZvcm1hdChmdW5jdGlvbihsYWJlbCkge1xuICAgICAgICAgIHJldHVybiBsYWJlbDtcbiAgICAgICAgfSkubWFyZ2luKHtcbiAgICAgICAgICBsZWZ0OiAxMDAsXG4gICAgICAgICAgcmlnaHQ6IDAsXG4gICAgICAgICAgdG9wOiAwLFxuICAgICAgICAgIGJvdHRvbTogMFxuICAgICAgICB9KS5pdGVtSGVpZ2h0KDMwKS5yZWxhdGl2ZVRpbWUoKTtcbiAgICAgICAgcmV0dXJuIHN2ZyA9IGQzLnNlbGVjdChzdmdFbCkuZGF0dW0odGVzdERhdGEpLmNhbGwoY2hhcnQpO1xuICAgICAgfTtcbiAgICAgIGFuYWx5emVUaW1lKHNjb3BlLmRhdGEpO1xuICAgIH1cbiAgfTtcbn0pLmRpcmVjdGl2ZSgndGltZWxpbmUnLCBmdW5jdGlvbigkc3RhdGUpIHtcbiAgcmV0dXJuIHtcbiAgICB0ZW1wbGF0ZTogXCI8c3ZnIGNsYXNzPSd0aW1lbGluZScgd2lkdGg9JzAnIGhlaWdodD0nMCc+PC9zdmc+XCIsXG4gICAgc2NvcGU6IHtcbiAgICAgIHZlcnRpY2VzOiBcIj1cIixcbiAgICAgIGpvYmlkOiBcIj1cIlxuICAgIH0sXG4gICAgbGluazogZnVuY3Rpb24oc2NvcGUsIGVsZW0sIGF0dHJzKSB7XG4gICAgICB2YXIgYW5hbHl6ZVRpbWUsIGNvbnRhaW5lclcsIHN2Z0VsLCB0cmFuc2xhdGVMYWJlbDtcbiAgICAgIHN2Z0VsID0gZWxlbS5jaGlsZHJlbigpWzBdO1xuICAgICAgY29udGFpbmVyVyA9IGVsZW0ud2lkdGgoKTtcbiAgICAgIGFuZ3VsYXIuZWxlbWVudChzdmdFbCkuYXR0cignd2lkdGgnLCBjb250YWluZXJXKTtcbiAgICAgIHRyYW5zbGF0ZUxhYmVsID0gZnVuY3Rpb24obGFiZWwpIHtcbiAgICAgICAgcmV0dXJuIGxhYmVsLnJlcGxhY2UoXCImZ3Q7XCIsIFwiPlwiKTtcbiAgICAgIH07XG4gICAgICBhbmFseXplVGltZSA9IGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgICAgdmFyIGNoYXJ0LCBzdmcsIHRlc3REYXRhO1xuICAgICAgICBkMy5zZWxlY3Qoc3ZnRWwpLnNlbGVjdEFsbChcIipcIikucmVtb3ZlKCk7XG4gICAgICAgIHRlc3REYXRhID0gW107XG4gICAgICAgIGFuZ3VsYXIuZm9yRWFjaChkYXRhLCBmdW5jdGlvbih2ZXJ0ZXgpIHtcbiAgICAgICAgICBpZiAodmVydGV4WydzdGFydC10aW1lJ10gPiAtMSkge1xuICAgICAgICAgICAgaWYgKHZlcnRleC50eXBlID09PSAnc2NoZWR1bGVkJykge1xuICAgICAgICAgICAgICByZXR1cm4gdGVzdERhdGEucHVzaCh7XG4gICAgICAgICAgICAgICAgdGltZXM6IFtcbiAgICAgICAgICAgICAgICAgIHtcbiAgICAgICAgICAgICAgICAgICAgbGFiZWw6IHRyYW5zbGF0ZUxhYmVsKHZlcnRleC5uYW1lKSxcbiAgICAgICAgICAgICAgICAgICAgY29sb3I6IFwiI2NjY2NjY1wiLFxuICAgICAgICAgICAgICAgICAgICBib3JkZXJDb2xvcjogXCIjNTU1NTU1XCIsXG4gICAgICAgICAgICAgICAgICAgIHN0YXJ0aW5nX3RpbWU6IHZlcnRleFsnc3RhcnQtdGltZSddLFxuICAgICAgICAgICAgICAgICAgICBlbmRpbmdfdGltZTogdmVydGV4WydlbmQtdGltZSddLFxuICAgICAgICAgICAgICAgICAgICB0eXBlOiB2ZXJ0ZXgudHlwZVxuICAgICAgICAgICAgICAgICAgfVxuICAgICAgICAgICAgICAgIF1cbiAgICAgICAgICAgICAgfSk7XG4gICAgICAgICAgICB9IGVsc2Uge1xuICAgICAgICAgICAgICByZXR1cm4gdGVzdERhdGEucHVzaCh7XG4gICAgICAgICAgICAgICAgdGltZXM6IFtcbiAgICAgICAgICAgICAgICAgIHtcbiAgICAgICAgICAgICAgICAgICAgbGFiZWw6IHRyYW5zbGF0ZUxhYmVsKHZlcnRleC5uYW1lKSxcbiAgICAgICAgICAgICAgICAgICAgY29sb3I6IFwiI2Q5ZjFmN1wiLFxuICAgICAgICAgICAgICAgICAgICBib3JkZXJDb2xvcjogXCIjNjJjZGVhXCIsXG4gICAgICAgICAgICAgICAgICAgIHN0YXJ0aW5nX3RpbWU6IHZlcnRleFsnc3RhcnQtdGltZSddLFxuICAgICAgICAgICAgICAgICAgICBlbmRpbmdfdGltZTogdmVydGV4WydlbmQtdGltZSddLFxuICAgICAgICAgICAgICAgICAgICBsaW5rOiB2ZXJ0ZXguaWQsXG4gICAgICAgICAgICAgICAgICAgIHR5cGU6IHZlcnRleC50eXBlXG4gICAgICAgICAgICAgICAgICB9XG4gICAgICAgICAgICAgICAgXVxuICAgICAgICAgICAgICB9KTtcbiAgICAgICAgICAgIH1cbiAgICAgICAgICB9XG4gICAgICAgIH0pO1xuICAgICAgICBjaGFydCA9IGQzLnRpbWVsaW5lKCkuc3RhY2soKS5jbGljayhmdW5jdGlvbihkLCBpLCBkYXR1bSkge1xuICAgICAgICAgIGlmIChkLmxpbmspIHtcbiAgICAgICAgICAgIHJldHVybiAkc3RhdGUuZ28oXCJzaW5nbGUtam9iLnRpbWVsaW5lLnZlcnRleFwiLCB7XG4gICAgICAgICAgICAgIGpvYmlkOiBzY29wZS5qb2JpZCxcbiAgICAgICAgICAgICAgdmVydGV4SWQ6IGQubGlua1xuICAgICAgICAgICAgfSk7XG4gICAgICAgICAgfVxuICAgICAgICB9KS50aWNrRm9ybWF0KHtcbiAgICAgICAgICBmb3JtYXQ6IGQzLnRpbWUuZm9ybWF0KFwiJUxcIiksXG4gICAgICAgICAgdGlja1NpemU6IDFcbiAgICAgICAgfSkucHJlZml4KFwibWFpblwiKS5tYXJnaW4oe1xuICAgICAgICAgIGxlZnQ6IDAsXG4gICAgICAgICAgcmlnaHQ6IDAsXG4gICAgICAgICAgdG9wOiAwLFxuICAgICAgICAgIGJvdHRvbTogMFxuICAgICAgICB9KS5pdGVtSGVpZ2h0KDMwKS5zaG93Qm9yZGVyTGluZSgpLnNob3dIb3VyVGltZWxpbmUoKTtcbiAgICAgICAgcmV0dXJuIHN2ZyA9IGQzLnNlbGVjdChzdmdFbCkuZGF0dW0odGVzdERhdGEpLmNhbGwoY2hhcnQpO1xuICAgICAgfTtcbiAgICAgIHNjb3BlLiR3YXRjaChhdHRycy52ZXJ0aWNlcywgZnVuY3Rpb24oZGF0YSkge1xuICAgICAgICBpZiAoZGF0YSkge1xuICAgICAgICAgIHJldHVybiBhbmFseXplVGltZShkYXRhKTtcbiAgICAgICAgfVxuICAgICAgfSk7XG4gICAgfVxuICB9O1xufSkuZGlyZWN0aXZlKCdqb2JQbGFuJywgZnVuY3Rpb24oJHRpbWVvdXQpIHtcbiAgcmV0dXJuIHtcbiAgICB0ZW1wbGF0ZTogXCI8c3ZnIGNsYXNzPSdncmFwaCcgd2lkdGg9JzUwMCcgaGVpZ2h0PSc0MDAnPjxnIC8+PC9zdmc+IDxzdmcgY2xhc3M9J3RtcCcgd2lkdGg9JzEnIGhlaWdodD0nMSc+PGcgLz48L3N2Zz4gPGRpdiBjbGFzcz0nYnRuLWdyb3VwIHpvb20tYnV0dG9ucyc+IDxhIGNsYXNzPSdidG4gYnRuLWRlZmF1bHQgem9vbS1pbicgbmctY2xpY2s9J3pvb21JbigpJz48aSBjbGFzcz0nZmEgZmEtcGx1cycgLz48L2E+IDxhIGNsYXNzPSdidG4gYnRuLWRlZmF1bHQgem9vbS1vdXQnIG5nLWNsaWNrPSd6b29tT3V0KCknPjxpIGNsYXNzPSdmYSBmYS1taW51cycgLz48L2E+IDwvZGl2PlwiLFxuICAgIHNjb3BlOiB7XG4gICAgICBwbGFuOiAnPScsXG4gICAgICBzZXROb2RlOiAnJidcbiAgICB9LFxuICAgIGxpbms6IGZ1bmN0aW9uKHNjb3BlLCBlbGVtLCBhdHRycykge1xuICAgICAgdmFyIGNvbnRhaW5lclcsIGNyZWF0ZUVkZ2UsIGNyZWF0ZUxhYmVsRWRnZSwgY3JlYXRlTGFiZWxOb2RlLCBjcmVhdGVOb2RlLCBkM21haW5TdmcsIGQzbWFpblN2Z0csIGQzdG1wU3ZnLCBkcmF3R3JhcGgsIGV4dGVuZExhYmVsTm9kZUZvckl0ZXJhdGlvbiwgZywgZ2V0Tm9kZVR5cGUsIGlzU3BlY2lhbEl0ZXJhdGlvbk5vZGUsIGpvYmlkLCBsb2FkSnNvblRvRGFncmUsIG1haW5HLCBtYWluU3ZnRWxlbWVudCwgbWFpblRtcEVsZW1lbnQsIG1haW5ab29tLCBzZWFyY2hGb3JOb2RlLCBzaG9ydGVuU3RyaW5nLCBzdWJncmFwaHM7XG4gICAgICBnID0gbnVsbDtcbiAgICAgIG1haW5ab29tID0gZDMuYmVoYXZpb3Iuem9vbSgpO1xuICAgICAgc3ViZ3JhcGhzID0gW107XG4gICAgICBqb2JpZCA9IGF0dHJzLmpvYmlkO1xuICAgICAgbWFpblN2Z0VsZW1lbnQgPSBlbGVtLmNoaWxkcmVuKClbMF07XG4gICAgICBtYWluRyA9IGVsZW0uY2hpbGRyZW4oKS5jaGlsZHJlbigpWzBdO1xuICAgICAgbWFpblRtcEVsZW1lbnQgPSBlbGVtLmNoaWxkcmVuKClbMV07XG4gICAgICBkM21haW5TdmcgPSBkMy5zZWxlY3QobWFpblN2Z0VsZW1lbnQpO1xuICAgICAgZDNtYWluU3ZnRyA9IGQzLnNlbGVjdChtYWluRyk7XG4gICAgICBkM3RtcFN2ZyA9IGQzLnNlbGVjdChtYWluVG1wRWxlbWVudCk7XG4gICAgICBjb250YWluZXJXID0gZWxlbS53aWR0aCgpO1xuICAgICAgYW5ndWxhci5lbGVtZW50KGVsZW0uY2hpbGRyZW4oKVswXSkud2lkdGgoY29udGFpbmVyVyk7XG4gICAgICBzY29wZS56b29tSW4gPSBmdW5jdGlvbigpIHtcbiAgICAgICAgdmFyIHRyYW5zbGF0ZSwgdjEsIHYyO1xuICAgICAgICBpZiAobWFpblpvb20uc2NhbGUoKSA8IDIuOTkpIHtcbiAgICAgICAgICB0cmFuc2xhdGUgPSBtYWluWm9vbS50cmFuc2xhdGUoKTtcbiAgICAgICAgICB2MSA9IHRyYW5zbGF0ZVswXSAqIChtYWluWm9vbS5zY2FsZSgpICsgMC4xIC8gKG1haW5ab29tLnNjYWxlKCkpKTtcbiAgICAgICAgICB2MiA9IHRyYW5zbGF0ZVsxXSAqIChtYWluWm9vbS5zY2FsZSgpICsgMC4xIC8gKG1haW5ab29tLnNjYWxlKCkpKTtcbiAgICAgICAgICBtYWluWm9vbS5zY2FsZShtYWluWm9vbS5zY2FsZSgpICsgMC4xKTtcbiAgICAgICAgICBtYWluWm9vbS50cmFuc2xhdGUoW3YxLCB2Ml0pO1xuICAgICAgICAgIHJldHVybiBkM21haW5TdmdHLmF0dHIoXCJ0cmFuc2Zvcm1cIiwgXCJ0cmFuc2xhdGUoXCIgKyB2MSArIFwiLFwiICsgdjIgKyBcIikgc2NhbGUoXCIgKyBtYWluWm9vbS5zY2FsZSgpICsgXCIpXCIpO1xuICAgICAgICB9XG4gICAgICB9O1xuICAgICAgc2NvcGUuem9vbU91dCA9IGZ1bmN0aW9uKCkge1xuICAgICAgICB2YXIgdHJhbnNsYXRlLCB2MSwgdjI7XG4gICAgICAgIGlmIChtYWluWm9vbS5zY2FsZSgpID4gMC4zMSkge1xuICAgICAgICAgIG1haW5ab29tLnNjYWxlKG1haW5ab29tLnNjYWxlKCkgLSAwLjEpO1xuICAgICAgICAgIHRyYW5zbGF0ZSA9IG1haW5ab29tLnRyYW5zbGF0ZSgpO1xuICAgICAgICAgIHYxID0gdHJhbnNsYXRlWzBdICogKG1haW5ab29tLnNjYWxlKCkgLSAwLjEgLyAobWFpblpvb20uc2NhbGUoKSkpO1xuICAgICAgICAgIHYyID0gdHJhbnNsYXRlWzFdICogKG1haW5ab29tLnNjYWxlKCkgLSAwLjEgLyAobWFpblpvb20uc2NhbGUoKSkpO1xuICAgICAgICAgIG1haW5ab29tLnRyYW5zbGF0ZShbdjEsIHYyXSk7XG4gICAgICAgICAgcmV0dXJuIGQzbWFpblN2Z0cuYXR0cihcInRyYW5zZm9ybVwiLCBcInRyYW5zbGF0ZShcIiArIHYxICsgXCIsXCIgKyB2MiArIFwiKSBzY2FsZShcIiArIG1haW5ab29tLnNjYWxlKCkgKyBcIilcIik7XG4gICAgICAgIH1cbiAgICAgIH07XG4gICAgICBjcmVhdGVMYWJlbEVkZ2UgPSBmdW5jdGlvbihlbCkge1xuICAgICAgICB2YXIgbGFiZWxWYWx1ZTtcbiAgICAgICAgbGFiZWxWYWx1ZSA9IFwiXCI7XG4gICAgICAgIGlmICgoZWwuc2hpcF9zdHJhdGVneSAhPSBudWxsKSB8fCAoZWwubG9jYWxfc3RyYXRlZ3kgIT0gbnVsbCkpIHtcbiAgICAgICAgICBsYWJlbFZhbHVlICs9IFwiPGRpdiBjbGFzcz0nZWRnZS1sYWJlbCc+XCI7XG4gICAgICAgICAgaWYgKGVsLnNoaXBfc3RyYXRlZ3kgIT0gbnVsbCkge1xuICAgICAgICAgICAgbGFiZWxWYWx1ZSArPSBlbC5zaGlwX3N0cmF0ZWd5O1xuICAgICAgICAgIH1cbiAgICAgICAgICBpZiAoZWwudGVtcF9tb2RlICE9PSB1bmRlZmluZWQpIHtcbiAgICAgICAgICAgIGxhYmVsVmFsdWUgKz0gXCIgKFwiICsgZWwudGVtcF9tb2RlICsgXCIpXCI7XG4gICAgICAgICAgfVxuICAgICAgICAgIGlmIChlbC5sb2NhbF9zdHJhdGVneSAhPT0gdW5kZWZpbmVkKSB7XG4gICAgICAgICAgICBsYWJlbFZhbHVlICs9IFwiLDxicj5cIiArIGVsLmxvY2FsX3N0cmF0ZWd5O1xuICAgICAgICAgIH1cbiAgICAgICAgICBsYWJlbFZhbHVlICs9IFwiPC9kaXY+XCI7XG4gICAgICAgIH1cbiAgICAgICAgcmV0dXJuIGxhYmVsVmFsdWU7XG4gICAgICB9O1xuICAgICAgaXNTcGVjaWFsSXRlcmF0aW9uTm9kZSA9IGZ1bmN0aW9uKGluZm8pIHtcbiAgICAgICAgcmV0dXJuIGluZm8gPT09IFwicGFydGlhbFNvbHV0aW9uXCIgfHwgaW5mbyA9PT0gXCJuZXh0UGFydGlhbFNvbHV0aW9uXCIgfHwgaW5mbyA9PT0gXCJ3b3Jrc2V0XCIgfHwgaW5mbyA9PT0gXCJuZXh0V29ya3NldFwiIHx8IGluZm8gPT09IFwic29sdXRpb25TZXRcIiB8fCBpbmZvID09PSBcInNvbHV0aW9uRGVsdGFcIjtcbiAgICAgIH07XG4gICAgICBnZXROb2RlVHlwZSA9IGZ1bmN0aW9uKGVsLCBpbmZvKSB7XG4gICAgICAgIGlmIChpbmZvID09PSBcIm1pcnJvclwiKSB7XG4gICAgICAgICAgcmV0dXJuICdub2RlLW1pcnJvcic7XG4gICAgICAgIH0gZWxzZSBpZiAoaXNTcGVjaWFsSXRlcmF0aW9uTm9kZShpbmZvKSkge1xuICAgICAgICAgIHJldHVybiAnbm9kZS1pdGVyYXRpb24nO1xuICAgICAgICB9IGVsc2Uge1xuICAgICAgICAgIHJldHVybiAnbm9kZS1ub3JtYWwnO1xuICAgICAgICB9XG4gICAgICB9O1xuICAgICAgY3JlYXRlTGFiZWxOb2RlID0gZnVuY3Rpb24oZWwsIGluZm8sIG1heFcsIG1heEgpIHtcbiAgICAgICAgdmFyIGxhYmVsVmFsdWUsIHN0ZXBOYW1lO1xuICAgICAgICBsYWJlbFZhbHVlID0gXCI8ZGl2IGhyZWY9JyMvam9icy9cIiArIGpvYmlkICsgXCIvdmVydGV4L1wiICsgZWwuaWQgKyBcIicgY2xhc3M9J25vZGUtbGFiZWwgXCIgKyBnZXROb2RlVHlwZShlbCwgaW5mbykgKyBcIic+XCI7XG4gICAgICAgIGlmIChpbmZvID09PSBcIm1pcnJvclwiKSB7XG4gICAgICAgICAgbGFiZWxWYWx1ZSArPSBcIjxoMyBjbGFzcz0nbm9kZS1uYW1lJz5NaXJyb3Igb2YgXCIgKyBlbC5vcGVyYXRvciArIFwiPC9oMz5cIjtcbiAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICBsYWJlbFZhbHVlICs9IFwiPGgzIGNsYXNzPSdub2RlLW5hbWUnPlwiICsgZWwub3BlcmF0b3IgKyBcIjwvaDM+XCI7XG4gICAgICAgIH1cbiAgICAgICAgaWYgKGVsLmRlc2NyaXB0aW9uID09PSBcIlwiKSB7XG4gICAgICAgICAgbGFiZWxWYWx1ZSArPSBcIlwiO1xuICAgICAgICB9IGVsc2Uge1xuICAgICAgICAgIHN0ZXBOYW1lID0gZWwuZGVzY3JpcHRpb247XG4gICAgICAgICAgc3RlcE5hbWUgPSBzaG9ydGVuU3RyaW5nKHN0ZXBOYW1lKTtcbiAgICAgICAgICBsYWJlbFZhbHVlICs9IFwiPGg0IGNsYXNzPSdzdGVwLW5hbWUnPlwiICsgc3RlcE5hbWUgKyBcIjwvaDQ+XCI7XG4gICAgICAgIH1cbiAgICAgICAgaWYgKGVsLnN0ZXBfZnVuY3Rpb24gIT0gbnVsbCkge1xuICAgICAgICAgIGxhYmVsVmFsdWUgKz0gZXh0ZW5kTGFiZWxOb2RlRm9ySXRlcmF0aW9uKGVsLmlkLCBtYXhXLCBtYXhIKTtcbiAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICBpZiAoaXNTcGVjaWFsSXRlcmF0aW9uTm9kZShpbmZvKSkge1xuICAgICAgICAgICAgbGFiZWxWYWx1ZSArPSBcIjxoNT5cIiArIGluZm8gKyBcIiBOb2RlPC9oNT5cIjtcbiAgICAgICAgICB9XG4gICAgICAgICAgaWYgKGVsLnBhcmFsbGVsaXNtICE9PSBcIlwiKSB7XG4gICAgICAgICAgICBsYWJlbFZhbHVlICs9IFwiPGg1PlBhcmFsbGVsaXNtOiBcIiArIGVsLnBhcmFsbGVsaXNtICsgXCI8L2g1PlwiO1xuICAgICAgICAgIH1cbiAgICAgICAgICBpZiAoZWwub3BlcmF0b3IgIT09IHVuZGVmaW5lZCkge1xuICAgICAgICAgICAgbGFiZWxWYWx1ZSArPSBcIjxoNT5PcGVyYXRpb246IFwiICsgc2hvcnRlblN0cmluZyhlbC5vcGVyYXRvcl9zdHJhdGVneSkgKyBcIjwvaDU+XCI7XG4gICAgICAgICAgfVxuICAgICAgICB9XG4gICAgICAgIGxhYmVsVmFsdWUgKz0gXCI8L2Rpdj5cIjtcbiAgICAgICAgcmV0dXJuIGxhYmVsVmFsdWU7XG4gICAgICB9O1xuICAgICAgZXh0ZW5kTGFiZWxOb2RlRm9ySXRlcmF0aW9uID0gZnVuY3Rpb24oaWQsIG1heFcsIG1heEgpIHtcbiAgICAgICAgdmFyIGxhYmVsVmFsdWUsIHN2Z0lEO1xuICAgICAgICBzdmdJRCA9IFwic3ZnLVwiICsgaWQ7XG4gICAgICAgIGxhYmVsVmFsdWUgPSBcIjxzdmcgY2xhc3M9J1wiICsgc3ZnSUQgKyBcIicgd2lkdGg9XCIgKyBtYXhXICsgXCIgaGVpZ2h0PVwiICsgbWF4SCArIFwiPjxnIC8+PC9zdmc+XCI7XG4gICAgICAgIHJldHVybiBsYWJlbFZhbHVlO1xuICAgICAgfTtcbiAgICAgIHNob3J0ZW5TdHJpbmcgPSBmdW5jdGlvbihzKSB7XG4gICAgICAgIHZhciBzYnI7XG4gICAgICAgIGlmIChzLmNoYXJBdCgwKSA9PT0gXCI8XCIpIHtcbiAgICAgICAgICBzID0gcy5yZXBsYWNlKFwiPFwiLCBcIiZsdDtcIik7XG4gICAgICAgICAgcyA9IHMucmVwbGFjZShcIj5cIiwgXCImZ3Q7XCIpO1xuICAgICAgICB9XG4gICAgICAgIHNiciA9IFwiXCI7XG4gICAgICAgIHdoaWxlIChzLmxlbmd0aCA+IDMwKSB7XG4gICAgICAgICAgc2JyID0gc2JyICsgcy5zdWJzdHJpbmcoMCwgMzApICsgXCI8YnI+XCI7XG4gICAgICAgICAgcyA9IHMuc3Vic3RyaW5nKDMwLCBzLmxlbmd0aCk7XG4gICAgICAgIH1cbiAgICAgICAgc2JyID0gc2JyICsgcztcbiAgICAgICAgcmV0dXJuIHNicjtcbiAgICAgIH07XG4gICAgICBjcmVhdGVOb2RlID0gZnVuY3Rpb24oZywgZGF0YSwgZWwsIGlzUGFyZW50LCBtYXhXLCBtYXhIKSB7XG4gICAgICAgIGlmIChpc1BhcmVudCA9PSBudWxsKSB7XG4gICAgICAgICAgaXNQYXJlbnQgPSBmYWxzZTtcbiAgICAgICAgfVxuICAgICAgICBpZiAoZWwuaWQgPT09IGRhdGEucGFydGlhbF9zb2x1dGlvbikge1xuICAgICAgICAgIHJldHVybiBnLnNldE5vZGUoZWwuaWQsIHtcbiAgICAgICAgICAgIGxhYmVsOiBjcmVhdGVMYWJlbE5vZGUoZWwsIFwicGFydGlhbFNvbHV0aW9uXCIsIG1heFcsIG1heEgpLFxuICAgICAgICAgICAgbGFiZWxUeXBlOiAnaHRtbCcsXG4gICAgICAgICAgICBcImNsYXNzXCI6IGdldE5vZGVUeXBlKGVsLCBcInBhcnRpYWxTb2x1dGlvblwiKVxuICAgICAgICAgIH0pO1xuICAgICAgICB9IGVsc2UgaWYgKGVsLmlkID09PSBkYXRhLm5leHRfcGFydGlhbF9zb2x1dGlvbikge1xuICAgICAgICAgIHJldHVybiBnLnNldE5vZGUoZWwuaWQsIHtcbiAgICAgICAgICAgIGxhYmVsOiBjcmVhdGVMYWJlbE5vZGUoZWwsIFwibmV4dFBhcnRpYWxTb2x1dGlvblwiLCBtYXhXLCBtYXhIKSxcbiAgICAgICAgICAgIGxhYmVsVHlwZTogJ2h0bWwnLFxuICAgICAgICAgICAgXCJjbGFzc1wiOiBnZXROb2RlVHlwZShlbCwgXCJuZXh0UGFydGlhbFNvbHV0aW9uXCIpXG4gICAgICAgICAgfSk7XG4gICAgICAgIH0gZWxzZSBpZiAoZWwuaWQgPT09IGRhdGEud29ya3NldCkge1xuICAgICAgICAgIHJldHVybiBnLnNldE5vZGUoZWwuaWQsIHtcbiAgICAgICAgICAgIGxhYmVsOiBjcmVhdGVMYWJlbE5vZGUoZWwsIFwid29ya3NldFwiLCBtYXhXLCBtYXhIKSxcbiAgICAgICAgICAgIGxhYmVsVHlwZTogJ2h0bWwnLFxuICAgICAgICAgICAgXCJjbGFzc1wiOiBnZXROb2RlVHlwZShlbCwgXCJ3b3Jrc2V0XCIpXG4gICAgICAgICAgfSk7XG4gICAgICAgIH0gZWxzZSBpZiAoZWwuaWQgPT09IGRhdGEubmV4dF93b3Jrc2V0KSB7XG4gICAgICAgICAgcmV0dXJuIGcuc2V0Tm9kZShlbC5pZCwge1xuICAgICAgICAgICAgbGFiZWw6IGNyZWF0ZUxhYmVsTm9kZShlbCwgXCJuZXh0V29ya3NldFwiLCBtYXhXLCBtYXhIKSxcbiAgICAgICAgICAgIGxhYmVsVHlwZTogJ2h0bWwnLFxuICAgICAgICAgICAgXCJjbGFzc1wiOiBnZXROb2RlVHlwZShlbCwgXCJuZXh0V29ya3NldFwiKVxuICAgICAgICAgIH0pO1xuICAgICAgICB9IGVsc2UgaWYgKGVsLmlkID09PSBkYXRhLnNvbHV0aW9uX3NldCkge1xuICAgICAgICAgIHJldHVybiBnLnNldE5vZGUoZWwuaWQsIHtcbiAgICAgICAgICAgIGxhYmVsOiBjcmVhdGVMYWJlbE5vZGUoZWwsIFwic29sdXRpb25TZXRcIiwgbWF4VywgbWF4SCksXG4gICAgICAgICAgICBsYWJlbFR5cGU6ICdodG1sJyxcbiAgICAgICAgICAgIFwiY2xhc3NcIjogZ2V0Tm9kZVR5cGUoZWwsIFwic29sdXRpb25TZXRcIilcbiAgICAgICAgICB9KTtcbiAgICAgICAgfSBlbHNlIGlmIChlbC5pZCA9PT0gZGF0YS5zb2x1dGlvbl9kZWx0YSkge1xuICAgICAgICAgIHJldHVybiBnLnNldE5vZGUoZWwuaWQsIHtcbiAgICAgICAgICAgIGxhYmVsOiBjcmVhdGVMYWJlbE5vZGUoZWwsIFwic29sdXRpb25EZWx0YVwiLCBtYXhXLCBtYXhIKSxcbiAgICAgICAgICAgIGxhYmVsVHlwZTogJ2h0bWwnLFxuICAgICAgICAgICAgXCJjbGFzc1wiOiBnZXROb2RlVHlwZShlbCwgXCJzb2x1dGlvbkRlbHRhXCIpXG4gICAgICAgICAgfSk7XG4gICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgcmV0dXJuIGcuc2V0Tm9kZShlbC5pZCwge1xuICAgICAgICAgICAgbGFiZWw6IGNyZWF0ZUxhYmVsTm9kZShlbCwgXCJcIiwgbWF4VywgbWF4SCksXG4gICAgICAgICAgICBsYWJlbFR5cGU6ICdodG1sJyxcbiAgICAgICAgICAgIFwiY2xhc3NcIjogZ2V0Tm9kZVR5cGUoZWwsIFwiXCIpXG4gICAgICAgICAgfSk7XG4gICAgICAgIH1cbiAgICAgIH07XG4gICAgICBjcmVhdGVFZGdlID0gZnVuY3Rpb24oZywgZGF0YSwgZWwsIGV4aXN0aW5nTm9kZXMsIHByZWQpIHtcbiAgICAgICAgdmFyIG1pc3NpbmdOb2RlO1xuICAgICAgICBpZiAoZXhpc3RpbmdOb2Rlcy5pbmRleE9mKHByZWQuaWQpICE9PSAtMSkge1xuICAgICAgICAgIHJldHVybiBnLnNldEVkZ2UocHJlZC5pZCwgZWwuaWQsIHtcbiAgICAgICAgICAgIGxhYmVsOiBjcmVhdGVMYWJlbEVkZ2UocHJlZCksXG4gICAgICAgICAgICBsYWJlbFR5cGU6ICdodG1sJyxcbiAgICAgICAgICAgIGFycm93aGVhZDogJ25vcm1hbCdcbiAgICAgICAgICB9KTtcbiAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICBtaXNzaW5nTm9kZSA9IHNlYXJjaEZvck5vZGUoZGF0YSwgcHJlZC5pZCk7XG4gICAgICAgICAgaWYgKCEhbWlzc2luZ05vZGUpIHtcbiAgICAgICAgICAgIHJldHVybiBnLnNldEVkZ2UobWlzc2luZ05vZGUuaWQsIGVsLmlkLCB7XG4gICAgICAgICAgICAgIGxhYmVsOiBjcmVhdGVMYWJlbEVkZ2UobWlzc2luZ05vZGUpLFxuICAgICAgICAgICAgICBsYWJlbFR5cGU6ICdodG1sJ1xuICAgICAgICAgICAgfSk7XG4gICAgICAgICAgfVxuICAgICAgICB9XG4gICAgICB9O1xuICAgICAgbG9hZEpzb25Ub0RhZ3JlID0gZnVuY3Rpb24oZywgZGF0YSkge1xuICAgICAgICB2YXIgZWwsIGV4aXN0aW5nTm9kZXMsIGlzUGFyZW50LCBrLCBsLCBsZW4sIGxlbjEsIG1heEgsIG1heFcsIHByZWQsIHIsIHJlZiwgc2csIHRvSXRlcmF0ZTtcbiAgICAgICAgZXhpc3RpbmdOb2RlcyA9IFtdO1xuICAgICAgICBpZiAoZGF0YS5ub2RlcyAhPSBudWxsKSB7XG4gICAgICAgICAgdG9JdGVyYXRlID0gZGF0YS5ub2RlcztcbiAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICB0b0l0ZXJhdGUgPSBkYXRhLnN0ZXBfZnVuY3Rpb247XG4gICAgICAgICAgaXNQYXJlbnQgPSB0cnVlO1xuICAgICAgICB9XG4gICAgICAgIGZvciAoayA9IDAsIGxlbiA9IHRvSXRlcmF0ZS5sZW5ndGg7IGsgPCBsZW47IGsrKykge1xuICAgICAgICAgIGVsID0gdG9JdGVyYXRlW2tdO1xuICAgICAgICAgIG1heFcgPSAwO1xuICAgICAgICAgIG1heEggPSAwO1xuICAgICAgICAgIGlmIChlbC5zdGVwX2Z1bmN0aW9uKSB7XG4gICAgICAgICAgICBzZyA9IG5ldyBkYWdyZUQzLmdyYXBobGliLkdyYXBoKHtcbiAgICAgICAgICAgICAgbXVsdGlncmFwaDogdHJ1ZSxcbiAgICAgICAgICAgICAgY29tcG91bmQ6IHRydWVcbiAgICAgICAgICAgIH0pLnNldEdyYXBoKHtcbiAgICAgICAgICAgICAgbm9kZXNlcDogMjAsXG4gICAgICAgICAgICAgIGVkZ2VzZXA6IDAsXG4gICAgICAgICAgICAgIHJhbmtzZXA6IDIwLFxuICAgICAgICAgICAgICByYW5rZGlyOiBcIkxSXCIsXG4gICAgICAgICAgICAgIG1hcmdpbng6IDEwLFxuICAgICAgICAgICAgICBtYXJnaW55OiAxMFxuICAgICAgICAgICAgfSk7XG4gICAgICAgICAgICBzdWJncmFwaHNbZWwuaWRdID0gc2c7XG4gICAgICAgICAgICBsb2FkSnNvblRvRGFncmUoc2csIGVsKTtcbiAgICAgICAgICAgIHIgPSBuZXcgZGFncmVEMy5yZW5kZXIoKTtcbiAgICAgICAgICAgIGQzdG1wU3ZnLnNlbGVjdCgnZycpLmNhbGwociwgc2cpO1xuICAgICAgICAgICAgbWF4VyA9IHNnLmdyYXBoKCkud2lkdGg7XG4gICAgICAgICAgICBtYXhIID0gc2cuZ3JhcGgoKS5oZWlnaHQ7XG4gICAgICAgICAgICBhbmd1bGFyLmVsZW1lbnQobWFpblRtcEVsZW1lbnQpLmVtcHR5KCk7XG4gICAgICAgICAgfVxuICAgICAgICAgIGNyZWF0ZU5vZGUoZywgZGF0YSwgZWwsIGlzUGFyZW50LCBtYXhXLCBtYXhIKTtcbiAgICAgICAgICBleGlzdGluZ05vZGVzLnB1c2goZWwuaWQpO1xuICAgICAgICAgIGlmIChlbC5pbnB1dHMgIT0gbnVsbCkge1xuICAgICAgICAgICAgcmVmID0gZWwuaW5wdXRzO1xuICAgICAgICAgICAgZm9yIChsID0gMCwgbGVuMSA9IHJlZi5sZW5ndGg7IGwgPCBsZW4xOyBsKyspIHtcbiAgICAgICAgICAgICAgcHJlZCA9IHJlZltsXTtcbiAgICAgICAgICAgICAgY3JlYXRlRWRnZShnLCBkYXRhLCBlbCwgZXhpc3RpbmdOb2RlcywgcHJlZCk7XG4gICAgICAgICAgICB9XG4gICAgICAgICAgfVxuICAgICAgICB9XG4gICAgICAgIHJldHVybiBnO1xuICAgICAgfTtcbiAgICAgIHNlYXJjaEZvck5vZGUgPSBmdW5jdGlvbihkYXRhLCBub2RlSUQpIHtcbiAgICAgICAgdmFyIGVsLCBpLCBqO1xuICAgICAgICBmb3IgKGkgaW4gZGF0YS5ub2Rlcykge1xuICAgICAgICAgIGVsID0gZGF0YS5ub2Rlc1tpXTtcbiAgICAgICAgICBpZiAoZWwuaWQgPT09IG5vZGVJRCkge1xuICAgICAgICAgICAgcmV0dXJuIGVsO1xuICAgICAgICAgIH1cbiAgICAgICAgICBpZiAoZWwuc3RlcF9mdW5jdGlvbiAhPSBudWxsKSB7XG4gICAgICAgICAgICBmb3IgKGogaW4gZWwuc3RlcF9mdW5jdGlvbikge1xuICAgICAgICAgICAgICBpZiAoZWwuc3RlcF9mdW5jdGlvbltqXS5pZCA9PT0gbm9kZUlEKSB7XG4gICAgICAgICAgICAgICAgcmV0dXJuIGVsLnN0ZXBfZnVuY3Rpb25bal07XG4gICAgICAgICAgICAgIH1cbiAgICAgICAgICAgIH1cbiAgICAgICAgICB9XG4gICAgICAgIH1cbiAgICAgIH07XG4gICAgICBkcmF3R3JhcGggPSBmdW5jdGlvbihkYXRhKSB7XG4gICAgICAgIHZhciBpLCBuZXdTY2FsZSwgcmVuZGVyZXIsIHNnLCB4Q2VudGVyT2Zmc2V0LCB5Q2VudGVyT2Zmc2V0O1xuICAgICAgICBnID0gbmV3IGRhZ3JlRDMuZ3JhcGhsaWIuR3JhcGgoe1xuICAgICAgICAgIG11bHRpZ3JhcGg6IHRydWUsXG4gICAgICAgICAgY29tcG91bmQ6IHRydWVcbiAgICAgICAgfSkuc2V0R3JhcGgoe1xuICAgICAgICAgIG5vZGVzZXA6IDcwLFxuICAgICAgICAgIGVkZ2VzZXA6IDAsXG4gICAgICAgICAgcmFua3NlcDogNTAsXG4gICAgICAgICAgcmFua2RpcjogXCJMUlwiLFxuICAgICAgICAgIG1hcmdpbng6IDQwLFxuICAgICAgICAgIG1hcmdpbnk6IDQwXG4gICAgICAgIH0pO1xuICAgICAgICBsb2FkSnNvblRvRGFncmUoZywgZGF0YSk7XG4gICAgICAgIHJlbmRlcmVyID0gbmV3IGRhZ3JlRDMucmVuZGVyKCk7XG4gICAgICAgIGQzbWFpblN2Z0cuY2FsbChyZW5kZXJlciwgZyk7XG4gICAgICAgIGZvciAoaSBpbiBzdWJncmFwaHMpIHtcbiAgICAgICAgICBzZyA9IHN1YmdyYXBoc1tpXTtcbiAgICAgICAgICBkM21haW5Tdmcuc2VsZWN0KCdzdmcuc3ZnLScgKyBpICsgJyBnJykuY2FsbChyZW5kZXJlciwgc2cpO1xuICAgICAgICB9XG4gICAgICAgIG5ld1NjYWxlID0gMC41O1xuICAgICAgICB4Q2VudGVyT2Zmc2V0ID0gTWF0aC5mbG9vcigoYW5ndWxhci5lbGVtZW50KG1haW5TdmdFbGVtZW50KS53aWR0aCgpIC0gZy5ncmFwaCgpLndpZHRoICogbmV3U2NhbGUpIC8gMik7XG4gICAgICAgIHlDZW50ZXJPZmZzZXQgPSBNYXRoLmZsb29yKChhbmd1bGFyLmVsZW1lbnQobWFpblN2Z0VsZW1lbnQpLmhlaWdodCgpIC0gZy5ncmFwaCgpLmhlaWdodCAqIG5ld1NjYWxlKSAvIDIpO1xuICAgICAgICBtYWluWm9vbS5zY2FsZShuZXdTY2FsZSkudHJhbnNsYXRlKFt4Q2VudGVyT2Zmc2V0LCB5Q2VudGVyT2Zmc2V0XSk7XG4gICAgICAgIGQzbWFpblN2Z0cuYXR0cihcInRyYW5zZm9ybVwiLCBcInRyYW5zbGF0ZShcIiArIHhDZW50ZXJPZmZzZXQgKyBcIiwgXCIgKyB5Q2VudGVyT2Zmc2V0ICsgXCIpIHNjYWxlKFwiICsgbWFpblpvb20uc2NhbGUoKSArIFwiKVwiKTtcbiAgICAgICAgbWFpblpvb20ub24oXCJ6b29tXCIsIGZ1bmN0aW9uKCkge1xuICAgICAgICAgIHZhciBldjtcbiAgICAgICAgICBldiA9IGQzLmV2ZW50O1xuICAgICAgICAgIHJldHVybiBkM21haW5TdmdHLmF0dHIoXCJ0cmFuc2Zvcm1cIiwgXCJ0cmFuc2xhdGUoXCIgKyBldi50cmFuc2xhdGUgKyBcIikgc2NhbGUoXCIgKyBldi5zY2FsZSArIFwiKVwiKTtcbiAgICAgICAgfSk7XG4gICAgICAgIG1haW5ab29tKGQzbWFpblN2Zyk7XG4gICAgICAgIHJldHVybiBkM21haW5TdmdHLnNlbGVjdEFsbCgnLm5vZGUnKS5vbignY2xpY2snLCBmdW5jdGlvbihkKSB7XG4gICAgICAgICAgcmV0dXJuIHNjb3BlLnNldE5vZGUoe1xuICAgICAgICAgICAgbm9kZWlkOiBkXG4gICAgICAgICAgfSk7XG4gICAgICAgIH0pO1xuICAgICAgfTtcbiAgICAgIHNjb3BlLiR3YXRjaChhdHRycy5wbGFuLCBmdW5jdGlvbihuZXdQbGFuKSB7XG4gICAgICAgIGlmIChuZXdQbGFuKSB7XG4gICAgICAgICAgcmV0dXJuIGRyYXdHcmFwaChuZXdQbGFuKTtcbiAgICAgICAgfVxuICAgICAgfSk7XG4gICAgfVxuICB9O1xufSk7XG4iLCIjXG4jIExpY2Vuc2VkIHRvIHRoZSBBcGFjaGUgU29mdHdhcmUgRm91bmRhdGlvbiAoQVNGKSB1bmRlciBvbmVcbiMgb3IgbW9yZSBjb250cmlidXRvciBsaWNlbnNlIGFncmVlbWVudHMuICBTZWUgdGhlIE5PVElDRSBmaWxlXG4jIGRpc3RyaWJ1dGVkIHdpdGggdGhpcyB3b3JrIGZvciBhZGRpdGlvbmFsIGluZm9ybWF0aW9uXG4jIHJlZ2FyZGluZyBjb3B5cmlnaHQgb3duZXJzaGlwLiAgVGhlIEFTRiBsaWNlbnNlcyB0aGlzIGZpbGVcbiMgdG8geW91IHVuZGVyIHRoZSBBcGFjaGUgTGljZW5zZSwgVmVyc2lvbiAyLjAgKHRoZVxuIyBcIkxpY2Vuc2VcIik7IHlvdSBtYXkgbm90IHVzZSB0aGlzIGZpbGUgZXhjZXB0IGluIGNvbXBsaWFuY2VcbiMgd2l0aCB0aGUgTGljZW5zZS4gIFlvdSBtYXkgb2J0YWluIGEgY29weSBvZiB0aGUgTGljZW5zZSBhdFxuI1xuIyAgICAgaHR0cDovL3d3dy5hcGFjaGUub3JnL2xpY2Vuc2VzL0xJQ0VOU0UtMi4wXG4jXG4jIFVubGVzcyByZXF1aXJlZCBieSBhcHBsaWNhYmxlIGxhdyBvciBhZ3JlZWQgdG8gaW4gd3JpdGluZywgc29mdHdhcmVcbiMgZGlzdHJpYnV0ZWQgdW5kZXIgdGhlIExpY2Vuc2UgaXMgZGlzdHJpYnV0ZWQgb24gYW4gXCJBUyBJU1wiIEJBU0lTLFxuIyBXSVRIT1VUIFdBUlJBTlRJRVMgT1IgQ09ORElUSU9OUyBPRiBBTlkgS0lORCwgZWl0aGVyIGV4cHJlc3Mgb3IgaW1wbGllZC5cbiMgU2VlIHRoZSBMaWNlbnNlIGZvciB0aGUgc3BlY2lmaWMgbGFuZ3VhZ2UgZ292ZXJuaW5nIHBlcm1pc3Npb25zIGFuZFxuIyBsaW1pdGF0aW9ucyB1bmRlciB0aGUgTGljZW5zZS5cbiNcblxuYW5ndWxhci5tb2R1bGUoJ2ZsaW5rQXBwJylcblxuLnNlcnZpY2UgJ0pvYnNTZXJ2aWNlJywgKCRodHRwLCBmbGlua0NvbmZpZywgJGxvZywgYW1Nb21lbnQsICRxLCAkdGltZW91dCkgLT5cbiAgY3VycmVudEpvYiA9IG51bGxcbiAgY3VycmVudFBsYW4gPSBudWxsXG5cbiAgZGVmZXJyZWRzID0ge31cbiAgam9icyA9IHtcbiAgICBydW5uaW5nOiBbXVxuICAgIGZpbmlzaGVkOiBbXVxuICAgIGNhbmNlbGxlZDogW11cbiAgICBmYWlsZWQ6IFtdXG4gIH1cblxuICBqb2JPYnNlcnZlcnMgPSBbXVxuXG4gIG5vdGlmeU9ic2VydmVycyA9IC0+XG4gICAgYW5ndWxhci5mb3JFYWNoIGpvYk9ic2VydmVycywgKGNhbGxiYWNrKSAtPlxuICAgICAgY2FsbGJhY2soKVxuXG4gIEByZWdpc3Rlck9ic2VydmVyID0gKGNhbGxiYWNrKSAtPlxuICAgIGpvYk9ic2VydmVycy5wdXNoKGNhbGxiYWNrKVxuXG4gIEB1blJlZ2lzdGVyT2JzZXJ2ZXIgPSAoY2FsbGJhY2spIC0+XG4gICAgaW5kZXggPSBqb2JPYnNlcnZlcnMuaW5kZXhPZihjYWxsYmFjaylcbiAgICBqb2JPYnNlcnZlcnMuc3BsaWNlKGluZGV4LCAxKVxuXG4gIEBzdGF0ZUxpc3QgPSAtPlxuICAgIFsgXG4gICAgICAjICdDUkVBVEVEJ1xuICAgICAgJ1NDSEVEVUxFRCdcbiAgICAgICdERVBMT1lJTkcnXG4gICAgICAnUlVOTklORydcbiAgICAgICdGSU5JU0hFRCdcbiAgICAgICdGQUlMRUQnXG4gICAgICAnQ0FOQ0VMSU5HJ1xuICAgICAgJ0NBTkNFTEVEJ1xuICAgIF1cblxuICBAdHJhbnNsYXRlTGFiZWxTdGF0ZSA9IChzdGF0ZSkgLT5cbiAgICBzd2l0Y2ggc3RhdGUudG9Mb3dlckNhc2UoKVxuICAgICAgd2hlbiAnZmluaXNoZWQnIHRoZW4gJ3N1Y2Nlc3MnXG4gICAgICB3aGVuICdmYWlsZWQnIHRoZW4gJ2RhbmdlcidcbiAgICAgIHdoZW4gJ3NjaGVkdWxlZCcgdGhlbiAnZGVmYXVsdCdcbiAgICAgIHdoZW4gJ2RlcGxveWluZycgdGhlbiAnaW5mbydcbiAgICAgIHdoZW4gJ3J1bm5pbmcnIHRoZW4gJ3ByaW1hcnknXG4gICAgICB3aGVuICdjYW5jZWxpbmcnIHRoZW4gJ3dhcm5pbmcnXG4gICAgICB3aGVuICdwZW5kaW5nJyB0aGVuICdpbmZvJ1xuICAgICAgd2hlbiAndG90YWwnIHRoZW4gJ2JsYWNrJ1xuICAgICAgZWxzZSAnZGVmYXVsdCdcblxuICBAc2V0RW5kVGltZXMgPSAobGlzdCkgLT5cbiAgICBhbmd1bGFyLmZvckVhY2ggbGlzdCwgKGl0ZW0sIGpvYktleSkgLT5cbiAgICAgIHVubGVzcyBpdGVtWydlbmQtdGltZSddID4gLTFcbiAgICAgICAgaXRlbVsnZW5kLXRpbWUnXSA9IGl0ZW1bJ3N0YXJ0LXRpbWUnXSArIGl0ZW1bJ2R1cmF0aW9uJ11cblxuICBAcHJvY2Vzc1ZlcnRpY2VzID0gKGRhdGEpIC0+XG4gICAgYW5ndWxhci5mb3JFYWNoIGRhdGEudmVydGljZXMsICh2ZXJ0ZXgsIGkpIC0+XG4gICAgICB2ZXJ0ZXgudHlwZSA9ICdyZWd1bGFyJ1xuXG4gICAgZGF0YS52ZXJ0aWNlcy51bnNoaWZ0KHtcbiAgICAgIG5hbWU6ICdTY2hlZHVsZWQnXG4gICAgICAnc3RhcnQtdGltZSc6IGRhdGEudGltZXN0YW1wc1snQ1JFQVRFRCddXG4gICAgICAnZW5kLXRpbWUnOiBkYXRhLnRpbWVzdGFtcHNbJ0NSRUFURUQnXSArIDFcbiAgICAgIHR5cGU6ICdzY2hlZHVsZWQnXG4gICAgfSlcblxuICBAbGlzdEpvYnMgPSAtPlxuICAgIGRlZmVycmVkID0gJHEuZGVmZXIoKVxuXG4gICAgJGh0dHAuZ2V0IGZsaW5rQ29uZmlnLmpvYlNlcnZlciArIFwiam9ib3ZlcnZpZXdcIlxuICAgIC5zdWNjZXNzIChkYXRhLCBzdGF0dXMsIGhlYWRlcnMsIGNvbmZpZykgPT5cbiAgICAgIGFuZ3VsYXIuZm9yRWFjaCBkYXRhLCAobGlzdCwgbGlzdEtleSkgPT5cbiAgICAgICAgc3dpdGNoIGxpc3RLZXlcbiAgICAgICAgICB3aGVuICdydW5uaW5nJyB0aGVuIGpvYnMucnVubmluZyA9IEBzZXRFbmRUaW1lcyhsaXN0KVxuICAgICAgICAgIHdoZW4gJ2ZpbmlzaGVkJyB0aGVuIGpvYnMuZmluaXNoZWQgPSBAc2V0RW5kVGltZXMobGlzdClcbiAgICAgICAgICB3aGVuICdjYW5jZWxsZWQnIHRoZW4gam9icy5jYW5jZWxsZWQgPSBAc2V0RW5kVGltZXMobGlzdClcbiAgICAgICAgICB3aGVuICdmYWlsZWQnIHRoZW4gam9icy5mYWlsZWQgPSBAc2V0RW5kVGltZXMobGlzdClcblxuICAgICAgZGVmZXJyZWQucmVzb2x2ZShqb2JzKVxuICAgICAgbm90aWZ5T2JzZXJ2ZXJzKClcblxuICAgIGRlZmVycmVkLnByb21pc2VcblxuICBAZ2V0Sm9icyA9ICh0eXBlKSAtPlxuICAgIGpvYnNbdHlwZV1cblxuICBAZ2V0QWxsSm9icyA9IC0+XG4gICAgam9ic1xuXG4gIEBsb2FkSm9iID0gKGpvYmlkKSAtPlxuICAgIGN1cnJlbnRKb2IgPSBudWxsXG4gICAgZGVmZXJyZWRzLmpvYiA9ICRxLmRlZmVyKClcblxuICAgICRodHRwLmdldCBmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcImpvYnMvXCIgKyBqb2JpZFxuICAgIC5zdWNjZXNzIChkYXRhLCBzdGF0dXMsIGhlYWRlcnMsIGNvbmZpZykgPT5cbiAgICAgIEBzZXRFbmRUaW1lcyhkYXRhLnZlcnRpY2VzKVxuICAgICAgQHByb2Nlc3NWZXJ0aWNlcyhkYXRhKVxuXG4gICAgICAkaHR0cC5nZXQgZmxpbmtDb25maWcuam9iU2VydmVyICsgXCJqb2JzL1wiICsgam9iaWQgKyBcIi9jb25maWdcIlxuICAgICAgLnN1Y2Nlc3MgKGpvYkNvbmZpZykgLT5cbiAgICAgICAgZGF0YSA9IGFuZ3VsYXIuZXh0ZW5kKGRhdGEsIGpvYkNvbmZpZylcblxuICAgICAgICBjdXJyZW50Sm9iID0gZGF0YVxuXG4gICAgICAgIGRlZmVycmVkcy5qb2IucmVzb2x2ZShjdXJyZW50Sm9iKVxuXG4gICAgZGVmZXJyZWRzLmpvYi5wcm9taXNlXG5cbiAgQGdldE5vZGUgPSAobm9kZWlkKSAtPlxuICAgIHNlZWtOb2RlID0gKG5vZGVpZCwgZGF0YSkgLT5cbiAgICAgIGZvciBub2RlIGluIGRhdGFcbiAgICAgICAgcmV0dXJuIG5vZGUgaWYgbm9kZS5pZCBpcyBub2RlaWRcbiAgICAgICAgc3ViID0gc2Vla05vZGUobm9kZWlkLCBub2RlLnN0ZXBfZnVuY3Rpb24pIGlmIG5vZGUuc3RlcF9mdW5jdGlvblxuICAgICAgICByZXR1cm4gc3ViIGlmIHN1YlxuXG4gICAgICBudWxsXG5cbiAgICBkZWZlcnJlZCA9ICRxLmRlZmVyKClcblxuICAgIGRlZmVycmVkcy5qb2IucHJvbWlzZS50aGVuIChkYXRhKSA9PlxuICAgICAgZm91bmROb2RlID0gc2Vla05vZGUobm9kZWlkLCBjdXJyZW50Sm9iLnBsYW4ubm9kZXMpXG5cbiAgICAgIGZvdW5kTm9kZS52ZXJ0ZXggPSBAc2Vla1ZlcnRleChub2RlaWQpXG5cbiAgICAgIGRlZmVycmVkLnJlc29sdmUoZm91bmROb2RlKVxuXG4gICAgZGVmZXJyZWQucHJvbWlzZVxuXG4gIEBzZWVrVmVydGV4ID0gKG5vZGVpZCkgLT5cbiAgICBmb3IgdmVydGV4IGluIGN1cnJlbnRKb2IudmVydGljZXNcbiAgICAgIHJldHVybiB2ZXJ0ZXggaWYgdmVydGV4LmlkIGlzIG5vZGVpZFxuXG4gICAgcmV0dXJuIG51bGxcblxuICBAZ2V0VmVydGV4ID0gKHZlcnRleGlkKSAtPlxuICAgIGRlZmVycmVkID0gJHEuZGVmZXIoKVxuXG4gICAgZGVmZXJyZWRzLmpvYi5wcm9taXNlLnRoZW4gKGRhdGEpID0+XG4gICAgICB2ZXJ0ZXggPSBAc2Vla1ZlcnRleCh2ZXJ0ZXhpZClcblxuICAgICAgJGh0dHAuZ2V0IGZsaW5rQ29uZmlnLmpvYlNlcnZlciArIFwiam9icy9cIiArIGN1cnJlbnRKb2IuamlkICsgXCIvdmVydGljZXMvXCIgKyB2ZXJ0ZXhpZCArIFwiL3N1YnRhc2t0aW1lc1wiXG4gICAgICAuc3VjY2VzcyAoZGF0YSkgPT5cbiAgICAgICAgIyBUT0RPOiBjaGFuZ2UgdG8gc3VidGFza3RpbWVzXG4gICAgICAgIHZlcnRleC5zdWJ0YXNrcyA9IGRhdGEuc3VidGFza3NcblxuICAgICAgICBkZWZlcnJlZC5yZXNvbHZlKHZlcnRleClcblxuICAgIGRlZmVycmVkLnByb21pc2VcblxuICBAZ2V0U3VidGFza3MgPSAodmVydGV4aWQpIC0+XG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpXG5cbiAgICBkZWZlcnJlZHMuam9iLnByb21pc2UudGhlbiAoZGF0YSkgPT5cbiAgICAgICMgdmVydGV4ID0gQHNlZWtWZXJ0ZXgodmVydGV4aWQpXG5cbiAgICAgICRodHRwLmdldCBmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcImpvYnMvXCIgKyBjdXJyZW50Sm9iLmppZCArIFwiL3ZlcnRpY2VzL1wiICsgdmVydGV4aWRcbiAgICAgIC5zdWNjZXNzIChkYXRhKSAtPlxuICAgICAgICBzdWJ0YXNrcyA9IGRhdGEuc3VidGFza3NcblxuICAgICAgICBkZWZlcnJlZC5yZXNvbHZlKHN1YnRhc2tzKVxuXG4gICAgZGVmZXJyZWQucHJvbWlzZVxuXG4gIEBnZXRBY2N1bXVsYXRvcnMgPSAodmVydGV4aWQpIC0+XG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpXG5cbiAgICBkZWZlcnJlZHMuam9iLnByb21pc2UudGhlbiAoZGF0YSkgPT5cbiAgICAgICMgdmVydGV4ID0gQHNlZWtWZXJ0ZXgodmVydGV4aWQpXG5cbiAgICAgICRodHRwLmdldCBmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcImpvYnMvXCIgKyBjdXJyZW50Sm9iLmppZCArIFwiL3ZlcnRpY2VzL1wiICsgdmVydGV4aWQgKyBcIi9hY2N1bXVsYXRvcnNcIlxuICAgICAgLnN1Y2Nlc3MgKGRhdGEpIC0+XG4gICAgICAgIGFjY3VtdWxhdG9ycyA9IGRhdGFbJ3VzZXItYWNjdW11bGF0b3JzJ11cblxuICAgICAgICAkaHR0cC5nZXQgZmxpbmtDb25maWcuam9iU2VydmVyICsgXCJqb2JzL1wiICsgY3VycmVudEpvYi5qaWQgKyBcIi92ZXJ0aWNlcy9cIiArIHZlcnRleGlkICsgXCIvc3VidGFza3MvYWNjdW11bGF0b3JzXCJcbiAgICAgICAgLnN1Y2Nlc3MgKGRhdGEpIC0+XG4gICAgICAgICAgc3VidGFza0FjY3VtdWxhdG9ycyA9IGRhdGEuc3VidGFza3NcblxuICAgICAgICAgIGRlZmVycmVkLnJlc29sdmUoeyBtYWluOiBhY2N1bXVsYXRvcnMsIHN1YnRhc2tzOiBzdWJ0YXNrQWNjdW11bGF0b3JzIH0pXG5cbiAgICBkZWZlcnJlZC5wcm9taXNlXG5cbiAgIyBKb2ItbGV2ZWwgY2hlY2twb2ludCBzdGF0c1xuICBAZ2V0Sm9iQ2hlY2twb2ludFN0YXRzID0gKGpvYmlkKSAtPlxuICAgIGRlZmVycmVkID0gJHEuZGVmZXIoKVxuXG4gICAgJGh0dHAuZ2V0IGZsaW5rQ29uZmlnLmpvYlNlcnZlciArIFwiam9icy9cIiArIGpvYmlkICsgXCIvY2hlY2twb2ludHNcIlxuICAgIC5zdWNjZXNzIChkYXRhLCBzdGF0dXMsIGhlYWRlcnMsIGNvbmZpZykgPT5cbiAgICAgIGlmIChhbmd1bGFyLmVxdWFscyh7fSwgZGF0YSkpXG4gICAgICAgIGRlZmVycmVkLnJlc29sdmUoZGVmZXJyZWQucmVzb2x2ZShudWxsKSlcbiAgICAgIGVsc2VcbiAgICAgICAgZGVmZXJyZWQucmVzb2x2ZShkYXRhKVxuXG4gICAgZGVmZXJyZWQucHJvbWlzZVxuXG4gICMgT3BlcmF0b3ItbGV2ZWwgY2hlY2twb2ludCBzdGF0c1xuICBAZ2V0T3BlcmF0b3JDaGVja3BvaW50U3RhdHMgPSAodmVydGV4aWQpIC0+XG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpXG5cbiAgICBkZWZlcnJlZHMuam9iLnByb21pc2UudGhlbiAoZGF0YSkgPT5cbiAgICAgICRodHRwLmdldCBmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcImpvYnMvXCIgKyBjdXJyZW50Sm9iLmppZCArIFwiL3ZlcnRpY2VzL1wiICsgdmVydGV4aWQgKyBcIi9jaGVja3BvaW50c1wiXG4gICAgICAuc3VjY2VzcyAoZGF0YSkgLT5cbiAgICAgICAgIyBJZiBubyBkYXRhIGF2YWlsYWJsZSwgd2UgYXJlIGRvbmUuXG4gICAgICAgIGlmIChhbmd1bGFyLmVxdWFscyh7fSwgZGF0YSkpXG4gICAgICAgICAgZGVmZXJyZWQucmVzb2x2ZSh7IG9wZXJhdG9yU3RhdHM6IG51bGwsIHN1YnRhc2tzU3RhdHM6IG51bGwgfSlcbiAgICAgICAgZWxzZVxuICAgICAgICAgIG9wZXJhdG9yU3RhdHMgPSB7IGlkOiBkYXRhWydpZCddLCB0aW1lc3RhbXA6IGRhdGFbJ3RpbWVzdGFtcCddLCBkdXJhdGlvbjogZGF0YVsnZHVyYXRpb24nXSwgc2l6ZTogZGF0YVsnc2l6ZSddIH1cblxuICAgICAgICAgIGlmIChhbmd1bGFyLmVxdWFscyh7fSwgZGF0YVsnc3VidGFza3MnXSkpXG4gICAgICAgICAgICBkZWZlcnJlZC5yZXNvbHZlKHsgb3BlcmF0b3JTdGF0czogb3BlcmF0b3JTdGF0cywgc3VidGFza3NTdGF0czogbnVsbCB9KVxuICAgICAgICAgIGVsc2VcbiAgICAgICAgICAgIHN1YnRhc2tTdGF0cyA9IGRhdGFbJ3N1YnRhc2tzJ11cbiAgICAgICAgICAgIGRlZmVycmVkLnJlc29sdmUoeyBvcGVyYXRvclN0YXRzOiBvcGVyYXRvclN0YXRzLCBzdWJ0YXNrc1N0YXRzOiBzdWJ0YXNrU3RhdHMgfSlcblxuICAgIGRlZmVycmVkLnByb21pc2VcblxuICAjIE9wZXJhdG9yLWxldmVsIGJhY2sgcHJlc3N1cmUgc3RhdHNcbiAgQGdldE9wZXJhdG9yQmFja1ByZXNzdXJlID0gKHZlcnRleGlkKSAtPlxuICAgIGRlZmVycmVkID0gJHEuZGVmZXIoKVxuXG4gICAgJGh0dHAuZ2V0IGZsaW5rQ29uZmlnLmpvYlNlcnZlciArIFwiam9icy9cIiArIGN1cnJlbnRKb2IuamlkICsgXCIvdmVydGljZXMvXCIgKyB2ZXJ0ZXhpZCArIFwiL2JhY2twcmVzc3VyZVwiXG4gICAgLnN1Y2Nlc3MgKGRhdGEpID0+XG4gICAgICBkZWZlcnJlZC5yZXNvbHZlKGRhdGEpXG5cbiAgICBkZWZlcnJlZC5wcm9taXNlXG5cbiAgQHRyYW5zbGF0ZUJhY2tQcmVzc3VyZUxhYmVsU3RhdGUgPSAoc3RhdGUpIC0+XG4gICAgc3dpdGNoIHN0YXRlLnRvTG93ZXJDYXNlKClcbiAgICAgIHdoZW4gJ2luLXByb2dyZXNzJyB0aGVuICdkYW5nZXInXG4gICAgICB3aGVuICdvaycgdGhlbiAnc3VjY2VzcydcbiAgICAgIHdoZW4gJ2xvdycgdGhlbiAnd2FybmluZydcbiAgICAgIHdoZW4gJ2hpZ2gnIHRoZW4gJ2RhbmdlcidcbiAgICAgIGVsc2UgJ2RlZmF1bHQnXG5cbiAgQGxvYWRFeGNlcHRpb25zID0gLT5cbiAgICBkZWZlcnJlZCA9ICRxLmRlZmVyKClcblxuICAgIGRlZmVycmVkcy5qb2IucHJvbWlzZS50aGVuIChkYXRhKSA9PlxuXG4gICAgICAkaHR0cC5nZXQgZmxpbmtDb25maWcuam9iU2VydmVyICsgXCJqb2JzL1wiICsgY3VycmVudEpvYi5qaWQgKyBcIi9leGNlcHRpb25zXCJcbiAgICAgIC5zdWNjZXNzIChleGNlcHRpb25zKSAtPlxuICAgICAgICBjdXJyZW50Sm9iLmV4Y2VwdGlvbnMgPSBleGNlcHRpb25zXG5cbiAgICAgICAgZGVmZXJyZWQucmVzb2x2ZShleGNlcHRpb25zKVxuXG4gICAgZGVmZXJyZWQucHJvbWlzZVxuXG4gIEBjYW5jZWxKb2IgPSAoam9iaWQpIC0+XG4gICAgIyB1c2VzIHRoZSBub24gUkVTVC1jb21wbGlhbnQgR0VUIHlhcm4tY2FuY2VsIGhhbmRsZXIgd2hpY2ggaXMgYXZhaWxhYmxlIGluIGFkZGl0aW9uIHRvIHRoZVxuICAgICMgcHJvcGVyIFwiREVMRVRFIGpvYnMvPGpvYmlkPi9cIlxuICAgICRodHRwLmdldCBmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcImpvYnMvXCIgKyBqb2JpZCArIFwiL3lhcm4tY2FuY2VsXCJcblxuICBAXG4iLCJhbmd1bGFyLm1vZHVsZSgnZmxpbmtBcHAnKS5zZXJ2aWNlKCdKb2JzU2VydmljZScsIGZ1bmN0aW9uKCRodHRwLCBmbGlua0NvbmZpZywgJGxvZywgYW1Nb21lbnQsICRxLCAkdGltZW91dCkge1xuICB2YXIgY3VycmVudEpvYiwgY3VycmVudFBsYW4sIGRlZmVycmVkcywgam9iT2JzZXJ2ZXJzLCBqb2JzLCBub3RpZnlPYnNlcnZlcnM7XG4gIGN1cnJlbnRKb2IgPSBudWxsO1xuICBjdXJyZW50UGxhbiA9IG51bGw7XG4gIGRlZmVycmVkcyA9IHt9O1xuICBqb2JzID0ge1xuICAgIHJ1bm5pbmc6IFtdLFxuICAgIGZpbmlzaGVkOiBbXSxcbiAgICBjYW5jZWxsZWQ6IFtdLFxuICAgIGZhaWxlZDogW11cbiAgfTtcbiAgam9iT2JzZXJ2ZXJzID0gW107XG4gIG5vdGlmeU9ic2VydmVycyA9IGZ1bmN0aW9uKCkge1xuICAgIHJldHVybiBhbmd1bGFyLmZvckVhY2goam9iT2JzZXJ2ZXJzLCBmdW5jdGlvbihjYWxsYmFjaykge1xuICAgICAgcmV0dXJuIGNhbGxiYWNrKCk7XG4gICAgfSk7XG4gIH07XG4gIHRoaXMucmVnaXN0ZXJPYnNlcnZlciA9IGZ1bmN0aW9uKGNhbGxiYWNrKSB7XG4gICAgcmV0dXJuIGpvYk9ic2VydmVycy5wdXNoKGNhbGxiYWNrKTtcbiAgfTtcbiAgdGhpcy51blJlZ2lzdGVyT2JzZXJ2ZXIgPSBmdW5jdGlvbihjYWxsYmFjaykge1xuICAgIHZhciBpbmRleDtcbiAgICBpbmRleCA9IGpvYk9ic2VydmVycy5pbmRleE9mKGNhbGxiYWNrKTtcbiAgICByZXR1cm4gam9iT2JzZXJ2ZXJzLnNwbGljZShpbmRleCwgMSk7XG4gIH07XG4gIHRoaXMuc3RhdGVMaXN0ID0gZnVuY3Rpb24oKSB7XG4gICAgcmV0dXJuIFsnU0NIRURVTEVEJywgJ0RFUExPWUlORycsICdSVU5OSU5HJywgJ0ZJTklTSEVEJywgJ0ZBSUxFRCcsICdDQU5DRUxJTkcnLCAnQ0FOQ0VMRUQnXTtcbiAgfTtcbiAgdGhpcy50cmFuc2xhdGVMYWJlbFN0YXRlID0gZnVuY3Rpb24oc3RhdGUpIHtcbiAgICBzd2l0Y2ggKHN0YXRlLnRvTG93ZXJDYXNlKCkpIHtcbiAgICAgIGNhc2UgJ2ZpbmlzaGVkJzpcbiAgICAgICAgcmV0dXJuICdzdWNjZXNzJztcbiAgICAgIGNhc2UgJ2ZhaWxlZCc6XG4gICAgICAgIHJldHVybiAnZGFuZ2VyJztcbiAgICAgIGNhc2UgJ3NjaGVkdWxlZCc6XG4gICAgICAgIHJldHVybiAnZGVmYXVsdCc7XG4gICAgICBjYXNlICdkZXBsb3lpbmcnOlxuICAgICAgICByZXR1cm4gJ2luZm8nO1xuICAgICAgY2FzZSAncnVubmluZyc6XG4gICAgICAgIHJldHVybiAncHJpbWFyeSc7XG4gICAgICBjYXNlICdjYW5jZWxpbmcnOlxuICAgICAgICByZXR1cm4gJ3dhcm5pbmcnO1xuICAgICAgY2FzZSAncGVuZGluZyc6XG4gICAgICAgIHJldHVybiAnaW5mbyc7XG4gICAgICBjYXNlICd0b3RhbCc6XG4gICAgICAgIHJldHVybiAnYmxhY2snO1xuICAgICAgZGVmYXVsdDpcbiAgICAgICAgcmV0dXJuICdkZWZhdWx0JztcbiAgICB9XG4gIH07XG4gIHRoaXMuc2V0RW5kVGltZXMgPSBmdW5jdGlvbihsaXN0KSB7XG4gICAgcmV0dXJuIGFuZ3VsYXIuZm9yRWFjaChsaXN0LCBmdW5jdGlvbihpdGVtLCBqb2JLZXkpIHtcbiAgICAgIGlmICghKGl0ZW1bJ2VuZC10aW1lJ10gPiAtMSkpIHtcbiAgICAgICAgcmV0dXJuIGl0ZW1bJ2VuZC10aW1lJ10gPSBpdGVtWydzdGFydC10aW1lJ10gKyBpdGVtWydkdXJhdGlvbiddO1xuICAgICAgfVxuICAgIH0pO1xuICB9O1xuICB0aGlzLnByb2Nlc3NWZXJ0aWNlcyA9IGZ1bmN0aW9uKGRhdGEpIHtcbiAgICBhbmd1bGFyLmZvckVhY2goZGF0YS52ZXJ0aWNlcywgZnVuY3Rpb24odmVydGV4LCBpKSB7XG4gICAgICByZXR1cm4gdmVydGV4LnR5cGUgPSAncmVndWxhcic7XG4gICAgfSk7XG4gICAgcmV0dXJuIGRhdGEudmVydGljZXMudW5zaGlmdCh7XG4gICAgICBuYW1lOiAnU2NoZWR1bGVkJyxcbiAgICAgICdzdGFydC10aW1lJzogZGF0YS50aW1lc3RhbXBzWydDUkVBVEVEJ10sXG4gICAgICAnZW5kLXRpbWUnOiBkYXRhLnRpbWVzdGFtcHNbJ0NSRUFURUQnXSArIDEsXG4gICAgICB0eXBlOiAnc2NoZWR1bGVkJ1xuICAgIH0pO1xuICB9O1xuICB0aGlzLmxpc3RKb2JzID0gZnVuY3Rpb24oKSB7XG4gICAgdmFyIGRlZmVycmVkO1xuICAgIGRlZmVycmVkID0gJHEuZGVmZXIoKTtcbiAgICAkaHR0cC5nZXQoZmxpbmtDb25maWcuam9iU2VydmVyICsgXCJqb2JvdmVydmlld1wiKS5zdWNjZXNzKChmdW5jdGlvbihfdGhpcykge1xuICAgICAgcmV0dXJuIGZ1bmN0aW9uKGRhdGEsIHN0YXR1cywgaGVhZGVycywgY29uZmlnKSB7XG4gICAgICAgIGFuZ3VsYXIuZm9yRWFjaChkYXRhLCBmdW5jdGlvbihsaXN0LCBsaXN0S2V5KSB7XG4gICAgICAgICAgc3dpdGNoIChsaXN0S2V5KSB7XG4gICAgICAgICAgICBjYXNlICdydW5uaW5nJzpcbiAgICAgICAgICAgICAgcmV0dXJuIGpvYnMucnVubmluZyA9IF90aGlzLnNldEVuZFRpbWVzKGxpc3QpO1xuICAgICAgICAgICAgY2FzZSAnZmluaXNoZWQnOlxuICAgICAgICAgICAgICByZXR1cm4gam9icy5maW5pc2hlZCA9IF90aGlzLnNldEVuZFRpbWVzKGxpc3QpO1xuICAgICAgICAgICAgY2FzZSAnY2FuY2VsbGVkJzpcbiAgICAgICAgICAgICAgcmV0dXJuIGpvYnMuY2FuY2VsbGVkID0gX3RoaXMuc2V0RW5kVGltZXMobGlzdCk7XG4gICAgICAgICAgICBjYXNlICdmYWlsZWQnOlxuICAgICAgICAgICAgICByZXR1cm4gam9icy5mYWlsZWQgPSBfdGhpcy5zZXRFbmRUaW1lcyhsaXN0KTtcbiAgICAgICAgICB9XG4gICAgICAgIH0pO1xuICAgICAgICBkZWZlcnJlZC5yZXNvbHZlKGpvYnMpO1xuICAgICAgICByZXR1cm4gbm90aWZ5T2JzZXJ2ZXJzKCk7XG4gICAgICB9O1xuICAgIH0pKHRoaXMpKTtcbiAgICByZXR1cm4gZGVmZXJyZWQucHJvbWlzZTtcbiAgfTtcbiAgdGhpcy5nZXRKb2JzID0gZnVuY3Rpb24odHlwZSkge1xuICAgIHJldHVybiBqb2JzW3R5cGVdO1xuICB9O1xuICB0aGlzLmdldEFsbEpvYnMgPSBmdW5jdGlvbigpIHtcbiAgICByZXR1cm4gam9icztcbiAgfTtcbiAgdGhpcy5sb2FkSm9iID0gZnVuY3Rpb24oam9iaWQpIHtcbiAgICBjdXJyZW50Sm9iID0gbnVsbDtcbiAgICBkZWZlcnJlZHMuam9iID0gJHEuZGVmZXIoKTtcbiAgICAkaHR0cC5nZXQoZmxpbmtDb25maWcuam9iU2VydmVyICsgXCJqb2JzL1wiICsgam9iaWQpLnN1Y2Nlc3MoKGZ1bmN0aW9uKF90aGlzKSB7XG4gICAgICByZXR1cm4gZnVuY3Rpb24oZGF0YSwgc3RhdHVzLCBoZWFkZXJzLCBjb25maWcpIHtcbiAgICAgICAgX3RoaXMuc2V0RW5kVGltZXMoZGF0YS52ZXJ0aWNlcyk7XG4gICAgICAgIF90aGlzLnByb2Nlc3NWZXJ0aWNlcyhkYXRhKTtcbiAgICAgICAgcmV0dXJuICRodHRwLmdldChmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcImpvYnMvXCIgKyBqb2JpZCArIFwiL2NvbmZpZ1wiKS5zdWNjZXNzKGZ1bmN0aW9uKGpvYkNvbmZpZykge1xuICAgICAgICAgIGRhdGEgPSBhbmd1bGFyLmV4dGVuZChkYXRhLCBqb2JDb25maWcpO1xuICAgICAgICAgIGN1cnJlbnRKb2IgPSBkYXRhO1xuICAgICAgICAgIHJldHVybiBkZWZlcnJlZHMuam9iLnJlc29sdmUoY3VycmVudEpvYik7XG4gICAgICAgIH0pO1xuICAgICAgfTtcbiAgICB9KSh0aGlzKSk7XG4gICAgcmV0dXJuIGRlZmVycmVkcy5qb2IucHJvbWlzZTtcbiAgfTtcbiAgdGhpcy5nZXROb2RlID0gZnVuY3Rpb24obm9kZWlkKSB7XG4gICAgdmFyIGRlZmVycmVkLCBzZWVrTm9kZTtcbiAgICBzZWVrTm9kZSA9IGZ1bmN0aW9uKG5vZGVpZCwgZGF0YSkge1xuICAgICAgdmFyIGosIGxlbiwgbm9kZSwgc3ViO1xuICAgICAgZm9yIChqID0gMCwgbGVuID0gZGF0YS5sZW5ndGg7IGogPCBsZW47IGorKykge1xuICAgICAgICBub2RlID0gZGF0YVtqXTtcbiAgICAgICAgaWYgKG5vZGUuaWQgPT09IG5vZGVpZCkge1xuICAgICAgICAgIHJldHVybiBub2RlO1xuICAgICAgICB9XG4gICAgICAgIGlmIChub2RlLnN0ZXBfZnVuY3Rpb24pIHtcbiAgICAgICAgICBzdWIgPSBzZWVrTm9kZShub2RlaWQsIG5vZGUuc3RlcF9mdW5jdGlvbik7XG4gICAgICAgIH1cbiAgICAgICAgaWYgKHN1Yikge1xuICAgICAgICAgIHJldHVybiBzdWI7XG4gICAgICAgIH1cbiAgICAgIH1cbiAgICAgIHJldHVybiBudWxsO1xuICAgIH07XG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpO1xuICAgIGRlZmVycmVkcy5qb2IucHJvbWlzZS50aGVuKChmdW5jdGlvbihfdGhpcykge1xuICAgICAgcmV0dXJuIGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgICAgdmFyIGZvdW5kTm9kZTtcbiAgICAgICAgZm91bmROb2RlID0gc2Vla05vZGUobm9kZWlkLCBjdXJyZW50Sm9iLnBsYW4ubm9kZXMpO1xuICAgICAgICBmb3VuZE5vZGUudmVydGV4ID0gX3RoaXMuc2Vla1ZlcnRleChub2RlaWQpO1xuICAgICAgICByZXR1cm4gZGVmZXJyZWQucmVzb2x2ZShmb3VuZE5vZGUpO1xuICAgICAgfTtcbiAgICB9KSh0aGlzKSk7XG4gICAgcmV0dXJuIGRlZmVycmVkLnByb21pc2U7XG4gIH07XG4gIHRoaXMuc2Vla1ZlcnRleCA9IGZ1bmN0aW9uKG5vZGVpZCkge1xuICAgIHZhciBqLCBsZW4sIHJlZiwgdmVydGV4O1xuICAgIHJlZiA9IGN1cnJlbnRKb2IudmVydGljZXM7XG4gICAgZm9yIChqID0gMCwgbGVuID0gcmVmLmxlbmd0aDsgaiA8IGxlbjsgaisrKSB7XG4gICAgICB2ZXJ0ZXggPSByZWZbal07XG4gICAgICBpZiAodmVydGV4LmlkID09PSBub2RlaWQpIHtcbiAgICAgICAgcmV0dXJuIHZlcnRleDtcbiAgICAgIH1cbiAgICB9XG4gICAgcmV0dXJuIG51bGw7XG4gIH07XG4gIHRoaXMuZ2V0VmVydGV4ID0gZnVuY3Rpb24odmVydGV4aWQpIHtcbiAgICB2YXIgZGVmZXJyZWQ7XG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpO1xuICAgIGRlZmVycmVkcy5qb2IucHJvbWlzZS50aGVuKChmdW5jdGlvbihfdGhpcykge1xuICAgICAgcmV0dXJuIGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgICAgdmFyIHZlcnRleDtcbiAgICAgICAgdmVydGV4ID0gX3RoaXMuc2Vla1ZlcnRleCh2ZXJ0ZXhpZCk7XG4gICAgICAgIHJldHVybiAkaHR0cC5nZXQoZmxpbmtDb25maWcuam9iU2VydmVyICsgXCJqb2JzL1wiICsgY3VycmVudEpvYi5qaWQgKyBcIi92ZXJ0aWNlcy9cIiArIHZlcnRleGlkICsgXCIvc3VidGFza3RpbWVzXCIpLnN1Y2Nlc3MoZnVuY3Rpb24oZGF0YSkge1xuICAgICAgICAgIHZlcnRleC5zdWJ0YXNrcyA9IGRhdGEuc3VidGFza3M7XG4gICAgICAgICAgcmV0dXJuIGRlZmVycmVkLnJlc29sdmUodmVydGV4KTtcbiAgICAgICAgfSk7XG4gICAgICB9O1xuICAgIH0pKHRoaXMpKTtcbiAgICByZXR1cm4gZGVmZXJyZWQucHJvbWlzZTtcbiAgfTtcbiAgdGhpcy5nZXRTdWJ0YXNrcyA9IGZ1bmN0aW9uKHZlcnRleGlkKSB7XG4gICAgdmFyIGRlZmVycmVkO1xuICAgIGRlZmVycmVkID0gJHEuZGVmZXIoKTtcbiAgICBkZWZlcnJlZHMuam9iLnByb21pc2UudGhlbigoZnVuY3Rpb24oX3RoaXMpIHtcbiAgICAgIHJldHVybiBmdW5jdGlvbihkYXRhKSB7XG4gICAgICAgIHJldHVybiAkaHR0cC5nZXQoZmxpbmtDb25maWcuam9iU2VydmVyICsgXCJqb2JzL1wiICsgY3VycmVudEpvYi5qaWQgKyBcIi92ZXJ0aWNlcy9cIiArIHZlcnRleGlkKS5zdWNjZXNzKGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgICAgICB2YXIgc3VidGFza3M7XG4gICAgICAgICAgc3VidGFza3MgPSBkYXRhLnN1YnRhc2tzO1xuICAgICAgICAgIHJldHVybiBkZWZlcnJlZC5yZXNvbHZlKHN1YnRhc2tzKTtcbiAgICAgICAgfSk7XG4gICAgICB9O1xuICAgIH0pKHRoaXMpKTtcbiAgICByZXR1cm4gZGVmZXJyZWQucHJvbWlzZTtcbiAgfTtcbiAgdGhpcy5nZXRBY2N1bXVsYXRvcnMgPSBmdW5jdGlvbih2ZXJ0ZXhpZCkge1xuICAgIHZhciBkZWZlcnJlZDtcbiAgICBkZWZlcnJlZCA9ICRxLmRlZmVyKCk7XG4gICAgZGVmZXJyZWRzLmpvYi5wcm9taXNlLnRoZW4oKGZ1bmN0aW9uKF90aGlzKSB7XG4gICAgICByZXR1cm4gZnVuY3Rpb24oZGF0YSkge1xuICAgICAgICByZXR1cm4gJGh0dHAuZ2V0KGZsaW5rQ29uZmlnLmpvYlNlcnZlciArIFwiam9icy9cIiArIGN1cnJlbnRKb2IuamlkICsgXCIvdmVydGljZXMvXCIgKyB2ZXJ0ZXhpZCArIFwiL2FjY3VtdWxhdG9yc1wiKS5zdWNjZXNzKGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgICAgICB2YXIgYWNjdW11bGF0b3JzO1xuICAgICAgICAgIGFjY3VtdWxhdG9ycyA9IGRhdGFbJ3VzZXItYWNjdW11bGF0b3JzJ107XG4gICAgICAgICAgcmV0dXJuICRodHRwLmdldChmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcImpvYnMvXCIgKyBjdXJyZW50Sm9iLmppZCArIFwiL3ZlcnRpY2VzL1wiICsgdmVydGV4aWQgKyBcIi9zdWJ0YXNrcy9hY2N1bXVsYXRvcnNcIikuc3VjY2VzcyhmdW5jdGlvbihkYXRhKSB7XG4gICAgICAgICAgICB2YXIgc3VidGFza0FjY3VtdWxhdG9ycztcbiAgICAgICAgICAgIHN1YnRhc2tBY2N1bXVsYXRvcnMgPSBkYXRhLnN1YnRhc2tzO1xuICAgICAgICAgICAgcmV0dXJuIGRlZmVycmVkLnJlc29sdmUoe1xuICAgICAgICAgICAgICBtYWluOiBhY2N1bXVsYXRvcnMsXG4gICAgICAgICAgICAgIHN1YnRhc2tzOiBzdWJ0YXNrQWNjdW11bGF0b3JzXG4gICAgICAgICAgICB9KTtcbiAgICAgICAgICB9KTtcbiAgICAgICAgfSk7XG4gICAgICB9O1xuICAgIH0pKHRoaXMpKTtcbiAgICByZXR1cm4gZGVmZXJyZWQucHJvbWlzZTtcbiAgfTtcbiAgdGhpcy5nZXRKb2JDaGVja3BvaW50U3RhdHMgPSBmdW5jdGlvbihqb2JpZCkge1xuICAgIHZhciBkZWZlcnJlZDtcbiAgICBkZWZlcnJlZCA9ICRxLmRlZmVyKCk7XG4gICAgJGh0dHAuZ2V0KGZsaW5rQ29uZmlnLmpvYlNlcnZlciArIFwiam9icy9cIiArIGpvYmlkICsgXCIvY2hlY2twb2ludHNcIikuc3VjY2VzcygoZnVuY3Rpb24oX3RoaXMpIHtcbiAgICAgIHJldHVybiBmdW5jdGlvbihkYXRhLCBzdGF0dXMsIGhlYWRlcnMsIGNvbmZpZykge1xuICAgICAgICBpZiAoYW5ndWxhci5lcXVhbHMoe30sIGRhdGEpKSB7XG4gICAgICAgICAgcmV0dXJuIGRlZmVycmVkLnJlc29sdmUoZGVmZXJyZWQucmVzb2x2ZShudWxsKSk7XG4gICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgcmV0dXJuIGRlZmVycmVkLnJlc29sdmUoZGF0YSk7XG4gICAgICAgIH1cbiAgICAgIH07XG4gICAgfSkodGhpcykpO1xuICAgIHJldHVybiBkZWZlcnJlZC5wcm9taXNlO1xuICB9O1xuICB0aGlzLmdldE9wZXJhdG9yQ2hlY2twb2ludFN0YXRzID0gZnVuY3Rpb24odmVydGV4aWQpIHtcbiAgICB2YXIgZGVmZXJyZWQ7XG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpO1xuICAgIGRlZmVycmVkcy5qb2IucHJvbWlzZS50aGVuKChmdW5jdGlvbihfdGhpcykge1xuICAgICAgcmV0dXJuIGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgICAgcmV0dXJuICRodHRwLmdldChmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcImpvYnMvXCIgKyBjdXJyZW50Sm9iLmppZCArIFwiL3ZlcnRpY2VzL1wiICsgdmVydGV4aWQgKyBcIi9jaGVja3BvaW50c1wiKS5zdWNjZXNzKGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgICAgICB2YXIgb3BlcmF0b3JTdGF0cywgc3VidGFza1N0YXRzO1xuICAgICAgICAgIGlmIChhbmd1bGFyLmVxdWFscyh7fSwgZGF0YSkpIHtcbiAgICAgICAgICAgIHJldHVybiBkZWZlcnJlZC5yZXNvbHZlKHtcbiAgICAgICAgICAgICAgb3BlcmF0b3JTdGF0czogbnVsbCxcbiAgICAgICAgICAgICAgc3VidGFza3NTdGF0czogbnVsbFxuICAgICAgICAgICAgfSk7XG4gICAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICAgIG9wZXJhdG9yU3RhdHMgPSB7XG4gICAgICAgICAgICAgIGlkOiBkYXRhWydpZCddLFxuICAgICAgICAgICAgICB0aW1lc3RhbXA6IGRhdGFbJ3RpbWVzdGFtcCddLFxuICAgICAgICAgICAgICBkdXJhdGlvbjogZGF0YVsnZHVyYXRpb24nXSxcbiAgICAgICAgICAgICAgc2l6ZTogZGF0YVsnc2l6ZSddXG4gICAgICAgICAgICB9O1xuICAgICAgICAgICAgaWYgKGFuZ3VsYXIuZXF1YWxzKHt9LCBkYXRhWydzdWJ0YXNrcyddKSkge1xuICAgICAgICAgICAgICByZXR1cm4gZGVmZXJyZWQucmVzb2x2ZSh7XG4gICAgICAgICAgICAgICAgb3BlcmF0b3JTdGF0czogb3BlcmF0b3JTdGF0cyxcbiAgICAgICAgICAgICAgICBzdWJ0YXNrc1N0YXRzOiBudWxsXG4gICAgICAgICAgICAgIH0pO1xuICAgICAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICAgICAgc3VidGFza1N0YXRzID0gZGF0YVsnc3VidGFza3MnXTtcbiAgICAgICAgICAgICAgcmV0dXJuIGRlZmVycmVkLnJlc29sdmUoe1xuICAgICAgICAgICAgICAgIG9wZXJhdG9yU3RhdHM6IG9wZXJhdG9yU3RhdHMsXG4gICAgICAgICAgICAgICAgc3VidGFza3NTdGF0czogc3VidGFza1N0YXRzXG4gICAgICAgICAgICAgIH0pO1xuICAgICAgICAgICAgfVxuICAgICAgICAgIH1cbiAgICAgICAgfSk7XG4gICAgICB9O1xuICAgIH0pKHRoaXMpKTtcbiAgICByZXR1cm4gZGVmZXJyZWQucHJvbWlzZTtcbiAgfTtcbiAgdGhpcy5nZXRPcGVyYXRvckJhY2tQcmVzc3VyZSA9IGZ1bmN0aW9uKHZlcnRleGlkKSB7XG4gICAgdmFyIGRlZmVycmVkO1xuICAgIGRlZmVycmVkID0gJHEuZGVmZXIoKTtcbiAgICAkaHR0cC5nZXQoZmxpbmtDb25maWcuam9iU2VydmVyICsgXCJqb2JzL1wiICsgY3VycmVudEpvYi5qaWQgKyBcIi92ZXJ0aWNlcy9cIiArIHZlcnRleGlkICsgXCIvYmFja3ByZXNzdXJlXCIpLnN1Y2Nlc3MoKGZ1bmN0aW9uKF90aGlzKSB7XG4gICAgICByZXR1cm4gZnVuY3Rpb24oZGF0YSkge1xuICAgICAgICByZXR1cm4gZGVmZXJyZWQucmVzb2x2ZShkYXRhKTtcbiAgICAgIH07XG4gICAgfSkodGhpcykpO1xuICAgIHJldHVybiBkZWZlcnJlZC5wcm9taXNlO1xuICB9O1xuICB0aGlzLnRyYW5zbGF0ZUJhY2tQcmVzc3VyZUxhYmVsU3RhdGUgPSBmdW5jdGlvbihzdGF0ZSkge1xuICAgIHN3aXRjaCAoc3RhdGUudG9Mb3dlckNhc2UoKSkge1xuICAgICAgY2FzZSAnaW4tcHJvZ3Jlc3MnOlxuICAgICAgICByZXR1cm4gJ2Rhbmdlcic7XG4gICAgICBjYXNlICdvayc6XG4gICAgICAgIHJldHVybiAnc3VjY2Vzcyc7XG4gICAgICBjYXNlICdsb3cnOlxuICAgICAgICByZXR1cm4gJ3dhcm5pbmcnO1xuICAgICAgY2FzZSAnaGlnaCc6XG4gICAgICAgIHJldHVybiAnZGFuZ2VyJztcbiAgICAgIGRlZmF1bHQ6XG4gICAgICAgIHJldHVybiAnZGVmYXVsdCc7XG4gICAgfVxuICB9O1xuICB0aGlzLmxvYWRFeGNlcHRpb25zID0gZnVuY3Rpb24oKSB7XG4gICAgdmFyIGRlZmVycmVkO1xuICAgIGRlZmVycmVkID0gJHEuZGVmZXIoKTtcbiAgICBkZWZlcnJlZHMuam9iLnByb21pc2UudGhlbigoZnVuY3Rpb24oX3RoaXMpIHtcbiAgICAgIHJldHVybiBmdW5jdGlvbihkYXRhKSB7XG4gICAgICAgIHJldHVybiAkaHR0cC5nZXQoZmxpbmtDb25maWcuam9iU2VydmVyICsgXCJqb2JzL1wiICsgY3VycmVudEpvYi5qaWQgKyBcIi9leGNlcHRpb25zXCIpLnN1Y2Nlc3MoZnVuY3Rpb24oZXhjZXB0aW9ucykge1xuICAgICAgICAgIGN1cnJlbnRKb2IuZXhjZXB0aW9ucyA9IGV4Y2VwdGlvbnM7XG4gICAgICAgICAgcmV0dXJuIGRlZmVycmVkLnJlc29sdmUoZXhjZXB0aW9ucyk7XG4gICAgICAgIH0pO1xuICAgICAgfTtcbiAgICB9KSh0aGlzKSk7XG4gICAgcmV0dXJuIGRlZmVycmVkLnByb21pc2U7XG4gIH07XG4gIHRoaXMuY2FuY2VsSm9iID0gZnVuY3Rpb24oam9iaWQpIHtcbiAgICByZXR1cm4gJGh0dHAuZ2V0KGZsaW5rQ29uZmlnLmpvYlNlcnZlciArIFwiam9icy9cIiArIGpvYmlkICsgXCIveWFybi1jYW5jZWxcIik7XG4gIH07XG4gIHJldHVybiB0aGlzO1xufSk7XG4iLCIjXG4jIExpY2Vuc2VkIHRvIHRoZSBBcGFjaGUgU29mdHdhcmUgRm91bmRhdGlvbiAoQVNGKSB1bmRlciBvbmVcbiMgb3IgbW9yZSBjb250cmlidXRvciBsaWNlbnNlIGFncmVlbWVudHMuICBTZWUgdGhlIE5PVElDRSBmaWxlXG4jIGRpc3RyaWJ1dGVkIHdpdGggdGhpcyB3b3JrIGZvciBhZGRpdGlvbmFsIGluZm9ybWF0aW9uXG4jIHJlZ2FyZGluZyBjb3B5cmlnaHQgb3duZXJzaGlwLiAgVGhlIEFTRiBsaWNlbnNlcyB0aGlzIGZpbGVcbiMgdG8geW91IHVuZGVyIHRoZSBBcGFjaGUgTGljZW5zZSwgVmVyc2lvbiAyLjAgKHRoZVxuIyBcIkxpY2Vuc2VcIik7IHlvdSBtYXkgbm90IHVzZSB0aGlzIGZpbGUgZXhjZXB0IGluIGNvbXBsaWFuY2VcbiMgd2l0aCB0aGUgTGljZW5zZS4gIFlvdSBtYXkgb2J0YWluIGEgY29weSBvZiB0aGUgTGljZW5zZSBhdFxuI1xuIyAgICAgaHR0cDovL3d3dy5hcGFjaGUub3JnL2xpY2Vuc2VzL0xJQ0VOU0UtMi4wXG4jXG4jIFVubGVzcyByZXF1aXJlZCBieSBhcHBsaWNhYmxlIGxhdyBvciBhZ3JlZWQgdG8gaW4gd3JpdGluZywgc29mdHdhcmVcbiMgZGlzdHJpYnV0ZWQgdW5kZXIgdGhlIExpY2Vuc2UgaXMgZGlzdHJpYnV0ZWQgb24gYW4gXCJBUyBJU1wiIEJBU0lTLFxuIyBXSVRIT1VUIFdBUlJBTlRJRVMgT1IgQ09ORElUSU9OUyBPRiBBTlkgS0lORCwgZWl0aGVyIGV4cHJlc3Mgb3IgaW1wbGllZC5cbiMgU2VlIHRoZSBMaWNlbnNlIGZvciB0aGUgc3BlY2lmaWMgbGFuZ3VhZ2UgZ292ZXJuaW5nIHBlcm1pc3Npb25zIGFuZFxuIyBsaW1pdGF0aW9ucyB1bmRlciB0aGUgTGljZW5zZS5cbiNcblxuYW5ndWxhci5tb2R1bGUoJ2ZsaW5rQXBwJylcblxuLmNvbnRyb2xsZXIgJ092ZXJ2aWV3Q29udHJvbGxlcicsICgkc2NvcGUsIE92ZXJ2aWV3U2VydmljZSwgSm9ic1NlcnZpY2UsICRpbnRlcnZhbCwgZmxpbmtDb25maWcpIC0+XG4gICRzY29wZS5qb2JPYnNlcnZlciA9IC0+XG4gICAgJHNjb3BlLnJ1bm5pbmdKb2JzID0gSm9ic1NlcnZpY2UuZ2V0Sm9icygncnVubmluZycpXG4gICAgJHNjb3BlLmZpbmlzaGVkSm9icyA9IEpvYnNTZXJ2aWNlLmdldEpvYnMoJ2ZpbmlzaGVkJylcblxuICBKb2JzU2VydmljZS5yZWdpc3Rlck9ic2VydmVyKCRzY29wZS5qb2JPYnNlcnZlcilcbiAgJHNjb3BlLiRvbiAnJGRlc3Ryb3knLCAtPlxuICAgIEpvYnNTZXJ2aWNlLnVuUmVnaXN0ZXJPYnNlcnZlcigkc2NvcGUuam9iT2JzZXJ2ZXIpXG5cbiAgJHNjb3BlLmpvYk9ic2VydmVyKClcblxuICBPdmVydmlld1NlcnZpY2UubG9hZE92ZXJ2aWV3KCkudGhlbiAoZGF0YSkgLT5cbiAgICAkc2NvcGUub3ZlcnZpZXcgPSBkYXRhXG5cbiAgcmVmcmVzaCA9ICRpbnRlcnZhbCAtPlxuICAgIE92ZXJ2aWV3U2VydmljZS5sb2FkT3ZlcnZpZXcoKS50aGVuIChkYXRhKSAtPlxuICAgICAgJHNjb3BlLm92ZXJ2aWV3ID0gZGF0YVxuICAsIGZsaW5rQ29uZmlnW1wicmVmcmVzaC1pbnRlcnZhbFwiXVxuXG4gICRzY29wZS4kb24gJyRkZXN0cm95JywgLT5cbiAgICAkaW50ZXJ2YWwuY2FuY2VsKHJlZnJlc2gpXG4iLCJhbmd1bGFyLm1vZHVsZSgnZmxpbmtBcHAnKS5jb250cm9sbGVyKCdPdmVydmlld0NvbnRyb2xsZXInLCBmdW5jdGlvbigkc2NvcGUsIE92ZXJ2aWV3U2VydmljZSwgSm9ic1NlcnZpY2UsICRpbnRlcnZhbCwgZmxpbmtDb25maWcpIHtcbiAgdmFyIHJlZnJlc2g7XG4gICRzY29wZS5qb2JPYnNlcnZlciA9IGZ1bmN0aW9uKCkge1xuICAgICRzY29wZS5ydW5uaW5nSm9icyA9IEpvYnNTZXJ2aWNlLmdldEpvYnMoJ3J1bm5pbmcnKTtcbiAgICByZXR1cm4gJHNjb3BlLmZpbmlzaGVkSm9icyA9IEpvYnNTZXJ2aWNlLmdldEpvYnMoJ2ZpbmlzaGVkJyk7XG4gIH07XG4gIEpvYnNTZXJ2aWNlLnJlZ2lzdGVyT2JzZXJ2ZXIoJHNjb3BlLmpvYk9ic2VydmVyKTtcbiAgJHNjb3BlLiRvbignJGRlc3Ryb3knLCBmdW5jdGlvbigpIHtcbiAgICByZXR1cm4gSm9ic1NlcnZpY2UudW5SZWdpc3Rlck9ic2VydmVyKCRzY29wZS5qb2JPYnNlcnZlcik7XG4gIH0pO1xuICAkc2NvcGUuam9iT2JzZXJ2ZXIoKTtcbiAgT3ZlcnZpZXdTZXJ2aWNlLmxvYWRPdmVydmlldygpLnRoZW4oZnVuY3Rpb24oZGF0YSkge1xuICAgIHJldHVybiAkc2NvcGUub3ZlcnZpZXcgPSBkYXRhO1xuICB9KTtcbiAgcmVmcmVzaCA9ICRpbnRlcnZhbChmdW5jdGlvbigpIHtcbiAgICByZXR1cm4gT3ZlcnZpZXdTZXJ2aWNlLmxvYWRPdmVydmlldygpLnRoZW4oZnVuY3Rpb24oZGF0YSkge1xuICAgICAgcmV0dXJuICRzY29wZS5vdmVydmlldyA9IGRhdGE7XG4gICAgfSk7XG4gIH0sIGZsaW5rQ29uZmlnW1wicmVmcmVzaC1pbnRlcnZhbFwiXSk7XG4gIHJldHVybiAkc2NvcGUuJG9uKCckZGVzdHJveScsIGZ1bmN0aW9uKCkge1xuICAgIHJldHVybiAkaW50ZXJ2YWwuY2FuY2VsKHJlZnJlc2gpO1xuICB9KTtcbn0pO1xuIiwiI1xuIyBMaWNlbnNlZCB0byB0aGUgQXBhY2hlIFNvZnR3YXJlIEZvdW5kYXRpb24gKEFTRikgdW5kZXIgb25lXG4jIG9yIG1vcmUgY29udHJpYnV0b3IgbGljZW5zZSBhZ3JlZW1lbnRzLiAgU2VlIHRoZSBOT1RJQ0UgZmlsZVxuIyBkaXN0cmlidXRlZCB3aXRoIHRoaXMgd29yayBmb3IgYWRkaXRpb25hbCBpbmZvcm1hdGlvblxuIyByZWdhcmRpbmcgY29weXJpZ2h0IG93bmVyc2hpcC4gIFRoZSBBU0YgbGljZW5zZXMgdGhpcyBmaWxlXG4jIHRvIHlvdSB1bmRlciB0aGUgQXBhY2hlIExpY2Vuc2UsIFZlcnNpb24gMi4wICh0aGVcbiMgXCJMaWNlbnNlXCIpOyB5b3UgbWF5IG5vdCB1c2UgdGhpcyBmaWxlIGV4Y2VwdCBpbiBjb21wbGlhbmNlXG4jIHdpdGggdGhlIExpY2Vuc2UuICBZb3UgbWF5IG9idGFpbiBhIGNvcHkgb2YgdGhlIExpY2Vuc2UgYXRcbiNcbiMgICAgIGh0dHA6Ly93d3cuYXBhY2hlLm9yZy9saWNlbnNlcy9MSUNFTlNFLTIuMFxuI1xuIyBVbmxlc3MgcmVxdWlyZWQgYnkgYXBwbGljYWJsZSBsYXcgb3IgYWdyZWVkIHRvIGluIHdyaXRpbmcsIHNvZnR3YXJlXG4jIGRpc3RyaWJ1dGVkIHVuZGVyIHRoZSBMaWNlbnNlIGlzIGRpc3RyaWJ1dGVkIG9uIGFuIFwiQVMgSVNcIiBCQVNJUyxcbiMgV0lUSE9VVCBXQVJSQU5USUVTIE9SIENPTkRJVElPTlMgT0YgQU5ZIEtJTkQsIGVpdGhlciBleHByZXNzIG9yIGltcGxpZWQuXG4jIFNlZSB0aGUgTGljZW5zZSBmb3IgdGhlIHNwZWNpZmljIGxhbmd1YWdlIGdvdmVybmluZyBwZXJtaXNzaW9ucyBhbmRcbiMgbGltaXRhdGlvbnMgdW5kZXIgdGhlIExpY2Vuc2UuXG4jXG5cbmFuZ3VsYXIubW9kdWxlKCdmbGlua0FwcCcpXG5cbi5zZXJ2aWNlICdPdmVydmlld1NlcnZpY2UnLCAoJGh0dHAsIGZsaW5rQ29uZmlnLCAkcSkgLT5cbiAgb3ZlcnZpZXcgPSB7fVxuXG4gIEBsb2FkT3ZlcnZpZXcgPSAtPlxuICAgIGRlZmVycmVkID0gJHEuZGVmZXIoKVxuXG4gICAgJGh0dHAuZ2V0KGZsaW5rQ29uZmlnLmpvYlNlcnZlciArIFwib3ZlcnZpZXdcIilcbiAgICAuc3VjY2VzcyAoZGF0YSwgc3RhdHVzLCBoZWFkZXJzLCBjb25maWcpIC0+XG4gICAgICBvdmVydmlldyA9IGRhdGFcbiAgICAgIGRlZmVycmVkLnJlc29sdmUoZGF0YSlcblxuICAgIGRlZmVycmVkLnByb21pc2VcblxuICBAXG4iLCJhbmd1bGFyLm1vZHVsZSgnZmxpbmtBcHAnKS5zZXJ2aWNlKCdPdmVydmlld1NlcnZpY2UnLCBmdW5jdGlvbigkaHR0cCwgZmxpbmtDb25maWcsICRxKSB7XG4gIHZhciBvdmVydmlldztcbiAgb3ZlcnZpZXcgPSB7fTtcbiAgdGhpcy5sb2FkT3ZlcnZpZXcgPSBmdW5jdGlvbigpIHtcbiAgICB2YXIgZGVmZXJyZWQ7XG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpO1xuICAgICRodHRwLmdldChmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcIm92ZXJ2aWV3XCIpLnN1Y2Nlc3MoZnVuY3Rpb24oZGF0YSwgc3RhdHVzLCBoZWFkZXJzLCBjb25maWcpIHtcbiAgICAgIG92ZXJ2aWV3ID0gZGF0YTtcbiAgICAgIHJldHVybiBkZWZlcnJlZC5yZXNvbHZlKGRhdGEpO1xuICAgIH0pO1xuICAgIHJldHVybiBkZWZlcnJlZC5wcm9taXNlO1xuICB9O1xuICByZXR1cm4gdGhpcztcbn0pO1xuIiwiI1xuIyBMaWNlbnNlZCB0byB0aGUgQXBhY2hlIFNvZnR3YXJlIEZvdW5kYXRpb24gKEFTRikgdW5kZXIgb25lXG4jIG9yIG1vcmUgY29udHJpYnV0b3IgbGljZW5zZSBhZ3JlZW1lbnRzLiAgU2VlIHRoZSBOT1RJQ0UgZmlsZVxuIyBkaXN0cmlidXRlZCB3aXRoIHRoaXMgd29yayBmb3IgYWRkaXRpb25hbCBpbmZvcm1hdGlvblxuIyByZWdhcmRpbmcgY29weXJpZ2h0IG93bmVyc2hpcC4gIFRoZSBBU0YgbGljZW5zZXMgdGhpcyBmaWxlXG4jIHRvIHlvdSB1bmRlciB0aGUgQXBhY2hlIExpY2Vuc2UsIFZlcnNpb24gMi4wICh0aGVcbiMgXCJMaWNlbnNlXCIpOyB5b3UgbWF5IG5vdCB1c2UgdGhpcyBmaWxlIGV4Y2VwdCBpbiBjb21wbGlhbmNlXG4jIHdpdGggdGhlIExpY2Vuc2UuICBZb3UgbWF5IG9idGFpbiBhIGNvcHkgb2YgdGhlIExpY2Vuc2UgYXRcbiNcbiMgICAgIGh0dHA6Ly93d3cuYXBhY2hlLm9yZy9saWNlbnNlcy9MSUNFTlNFLTIuMFxuI1xuIyBVbmxlc3MgcmVxdWlyZWQgYnkgYXBwbGljYWJsZSBsYXcgb3IgYWdyZWVkIHRvIGluIHdyaXRpbmcsIHNvZnR3YXJlXG4jIGRpc3RyaWJ1dGVkIHVuZGVyIHRoZSBMaWNlbnNlIGlzIGRpc3RyaWJ1dGVkIG9uIGFuIFwiQVMgSVNcIiBCQVNJUyxcbiMgV0lUSE9VVCBXQVJSQU5USUVTIE9SIENPTkRJVElPTlMgT0YgQU5ZIEtJTkQsIGVpdGhlciBleHByZXNzIG9yIGltcGxpZWQuXG4jIFNlZSB0aGUgTGljZW5zZSBmb3IgdGhlIHNwZWNpZmljIGxhbmd1YWdlIGdvdmVybmluZyBwZXJtaXNzaW9ucyBhbmRcbiMgbGltaXRhdGlvbnMgdW5kZXIgdGhlIExpY2Vuc2UuXG4jXG5cbmFuZ3VsYXIubW9kdWxlKCdmbGlua0FwcCcpXG5cbi5jb250cm9sbGVyICdKb2JTdWJtaXRDb250cm9sbGVyJywgKCRzY29wZSwgSm9iU3VibWl0U2VydmljZSwgJGludGVydmFsLCBmbGlua0NvbmZpZywgJHN0YXRlLCAkbG9jYXRpb24pIC0+XG4gICRzY29wZS55YXJuID0gJGxvY2F0aW9uLmFic1VybCgpLmluZGV4T2YoXCIvcHJveHkvYXBwbGljYXRpb25fXCIpICE9IC0xXG4gICRzY29wZS5sb2FkTGlzdCA9ICgpIC0+XG4gICAgSm9iU3VibWl0U2VydmljZS5sb2FkSmFyTGlzdCgpLnRoZW4gKGRhdGEpIC0+XG4gICAgICAkc2NvcGUuYWRkcmVzcyA9IGRhdGEuYWRkcmVzc1xuICAgICAgJHNjb3BlLm5vYWNjZXNzID0gZGF0YS5lcnJvclxuICAgICAgJHNjb3BlLmphcnMgPSBkYXRhLmZpbGVzXG5cbiAgJHNjb3BlLmRlZmF1bHRTdGF0ZSA9ICgpIC0+XG4gICAgJHNjb3BlLnBsYW4gPSBudWxsXG4gICAgJHNjb3BlLmVycm9yID0gbnVsbFxuICAgICRzY29wZS5zdGF0ZSA9IHtcbiAgICAgIHNlbGVjdGVkOiBudWxsLFxuICAgICAgcGFyYWxsZWxpc206IFwiXCIsXG4gICAgICAnZW50cnktY2xhc3MnOiBcIlwiLFxuICAgICAgJ3Byb2dyYW0tYXJncyc6IFwiXCIsXG4gICAgICAncGxhbi1idXR0b24nOiBcIlNob3cgUGxhblwiLFxuICAgICAgJ3N1Ym1pdC1idXR0b24nOiBcIlN1Ym1pdFwiLFxuICAgICAgJ2FjdGlvbi10aW1lJzogMFxuICAgIH1cblxuICAkc2NvcGUuZGVmYXVsdFN0YXRlKClcbiAgJHNjb3BlLnVwbG9hZGVyID0ge31cbiAgJHNjb3BlLmxvYWRMaXN0KClcblxuICByZWZyZXNoID0gJGludGVydmFsIC0+XG4gICAgJHNjb3BlLmxvYWRMaXN0KClcbiAgLCBmbGlua0NvbmZpZ1tcInJlZnJlc2gtaW50ZXJ2YWxcIl1cblxuICAkc2NvcGUuJG9uICckZGVzdHJveScsIC0+XG4gICAgJGludGVydmFsLmNhbmNlbChyZWZyZXNoKVxuXG4gICRzY29wZS5zZWxlY3RKYXIgPSAoaWQpIC0+XG4gICAgaWYgJHNjb3BlLnN0YXRlLnNlbGVjdGVkID09IGlkXG4gICAgICAkc2NvcGUuZGVmYXVsdFN0YXRlKClcbiAgICBlbHNlXG4gICAgICAkc2NvcGUuZGVmYXVsdFN0YXRlKClcbiAgICAgICRzY29wZS5zdGF0ZS5zZWxlY3RlZCA9IGlkXG5cbiAgJHNjb3BlLmRlbGV0ZUphciA9IChldmVudCwgaWQpIC0+XG4gICAgaWYgJHNjb3BlLnN0YXRlLnNlbGVjdGVkID09IGlkXG4gICAgICAkc2NvcGUuZGVmYXVsdFN0YXRlKClcbiAgICBhbmd1bGFyLmVsZW1lbnQoZXZlbnQuY3VycmVudFRhcmdldCkucmVtb3ZlQ2xhc3MoXCJmYS1yZW1vdmVcIikuYWRkQ2xhc3MoXCJmYS1zcGluIGZhLXNwaW5uZXJcIilcbiAgICBKb2JTdWJtaXRTZXJ2aWNlLmRlbGV0ZUphcihpZCkudGhlbiAoZGF0YSkgLT5cbiAgICAgIGFuZ3VsYXIuZWxlbWVudChldmVudC5jdXJyZW50VGFyZ2V0KS5yZW1vdmVDbGFzcyhcImZhLXNwaW4gZmEtc3Bpbm5lclwiKS5hZGRDbGFzcyhcImZhLXJlbW92ZVwiKVxuICAgICAgaWYgZGF0YS5lcnJvcj9cbiAgICAgICAgYWxlcnQoZGF0YS5lcnJvcilcblxuICAkc2NvcGUubG9hZEVudHJ5Q2xhc3MgPSAobmFtZSkgLT5cbiAgICAkc2NvcGUuc3RhdGVbJ2VudHJ5LWNsYXNzJ10gPSBuYW1lXG5cbiAgJHNjb3BlLmdldFBsYW4gPSAoKSAtPlxuICAgIGlmICRzY29wZS5zdGF0ZVsncGxhbi1idXR0b24nXSA9PSBcIlNob3cgUGxhblwiXG4gICAgICBhY3Rpb24gPSBuZXcgRGF0ZSgpLmdldFRpbWUoKVxuICAgICAgJHNjb3BlLnN0YXRlWydhY3Rpb24tdGltZSddID0gYWN0aW9uXG4gICAgICAkc2NvcGUuc3RhdGVbJ3N1Ym1pdC1idXR0b24nXSA9IFwiU3VibWl0XCJcbiAgICAgICRzY29wZS5zdGF0ZVsncGxhbi1idXR0b24nXSA9IFwiR2V0dGluZyBQbGFuXCJcbiAgICAgICRzY29wZS5lcnJvciA9IG51bGxcbiAgICAgICRzY29wZS5wbGFuID0gbnVsbFxuICAgICAgSm9iU3VibWl0U2VydmljZS5nZXRQbGFuKFxuICAgICAgICAkc2NvcGUuc3RhdGUuc2VsZWN0ZWQsIHtcbiAgICAgICAgICAnZW50cnktY2xhc3MnOiAkc2NvcGUuc3RhdGVbJ2VudHJ5LWNsYXNzJ10sXG4gICAgICAgICAgcGFyYWxsZWxpc206ICRzY29wZS5zdGF0ZS5wYXJhbGxlbGlzbSxcbiAgICAgICAgICAncHJvZ3JhbS1hcmdzJzogJHNjb3BlLnN0YXRlWydwcm9ncmFtLWFyZ3MnXVxuICAgICAgICB9XG4gICAgICApLnRoZW4gKGRhdGEpIC0+XG4gICAgICAgIGlmIGFjdGlvbiA9PSAkc2NvcGUuc3RhdGVbJ2FjdGlvbi10aW1lJ11cbiAgICAgICAgICAkc2NvcGUuc3RhdGVbJ3BsYW4tYnV0dG9uJ10gPSBcIlNob3cgUGxhblwiXG4gICAgICAgICAgJHNjb3BlLmVycm9yID0gZGF0YS5lcnJvclxuICAgICAgICAgICRzY29wZS5wbGFuID0gZGF0YS5wbGFuXG5cbiAgJHNjb3BlLnJ1bkpvYiA9ICgpIC0+XG4gICAgaWYgJHNjb3BlLnN0YXRlWydzdWJtaXQtYnV0dG9uJ10gPT0gXCJTdWJtaXRcIlxuICAgICAgYWN0aW9uID0gbmV3IERhdGUoKS5nZXRUaW1lKClcbiAgICAgICRzY29wZS5zdGF0ZVsnYWN0aW9uLXRpbWUnXSA9IGFjdGlvblxuICAgICAgJHNjb3BlLnN0YXRlWydzdWJtaXQtYnV0dG9uJ10gPSBcIlN1Ym1pdHRpbmdcIlxuICAgICAgJHNjb3BlLnN0YXRlWydwbGFuLWJ1dHRvbiddID0gXCJTaG93IFBsYW5cIlxuICAgICAgJHNjb3BlLmVycm9yID0gbnVsbFxuICAgICAgSm9iU3VibWl0U2VydmljZS5ydW5Kb2IoXG4gICAgICAgICRzY29wZS5zdGF0ZS5zZWxlY3RlZCwge1xuICAgICAgICAgICdlbnRyeS1jbGFzcyc6ICRzY29wZS5zdGF0ZVsnZW50cnktY2xhc3MnXSxcbiAgICAgICAgICBwYXJhbGxlbGlzbTogJHNjb3BlLnN0YXRlLnBhcmFsbGVsaXNtLFxuICAgICAgICAgICdwcm9ncmFtLWFyZ3MnOiAkc2NvcGUuc3RhdGVbJ3Byb2dyYW0tYXJncyddXG4gICAgICAgIH1cbiAgICAgICkudGhlbiAoZGF0YSkgLT5cbiAgICAgICAgaWYgYWN0aW9uID09ICRzY29wZS5zdGF0ZVsnYWN0aW9uLXRpbWUnXVxuICAgICAgICAgICRzY29wZS5zdGF0ZVsnc3VibWl0LWJ1dHRvbiddID0gXCJTdWJtaXRcIlxuICAgICAgICAgICRzY29wZS5lcnJvciA9IGRhdGEuZXJyb3JcbiAgICAgICAgICBpZiBkYXRhLmpvYmlkP1xuICAgICAgICAgICAgJHN0YXRlLmdvKFwic2luZ2xlLWpvYi5wbGFuLm92ZXJ2aWV3XCIsIHtqb2JpZDogZGF0YS5qb2JpZH0pXG5cbiAgIyBqb2IgcGxhbiBkaXNwbGF5IHJlbGF0ZWQgc3R1ZmZcbiAgJHNjb3BlLm5vZGVpZCA9IG51bGxcbiAgJHNjb3BlLmNoYW5nZU5vZGUgPSAobm9kZWlkKSAtPlxuICAgIGlmIG5vZGVpZCAhPSAkc2NvcGUubm9kZWlkXG4gICAgICAkc2NvcGUubm9kZWlkID0gbm9kZWlkXG4gICAgICAkc2NvcGUudmVydGV4ID0gbnVsbFxuICAgICAgJHNjb3BlLnN1YnRhc2tzID0gbnVsbFxuICAgICAgJHNjb3BlLmFjY3VtdWxhdG9ycyA9IG51bGxcblxuICAgICAgJHNjb3BlLiRicm9hZGNhc3QgJ3JlbG9hZCdcblxuICAgIGVsc2VcbiAgICAgICRzY29wZS5ub2RlaWQgPSBudWxsXG4gICAgICAkc2NvcGUubm9kZVVuZm9sZGVkID0gZmFsc2VcbiAgICAgICRzY29wZS52ZXJ0ZXggPSBudWxsXG4gICAgICAkc2NvcGUuc3VidGFza3MgPSBudWxsXG4gICAgICAkc2NvcGUuYWNjdW11bGF0b3JzID0gbnVsbFxuXG4gICRzY29wZS5jbGVhckZpbGVzID0gKCkgLT5cbiAgICAkc2NvcGUudXBsb2FkZXIgPSB7fVxuXG4gICRzY29wZS51cGxvYWRGaWxlcyA9IChmaWxlcykgLT5cbiAgICAjIG1ha2Ugc3VyZSBldmVyeXRoaW5nIGlzIGNsZWFyIGFnYWluLlxuICAgICRzY29wZS51cGxvYWRlciA9IHt9XG4gICAgaWYgZmlsZXMubGVuZ3RoID09IDFcbiAgICAgICRzY29wZS51cGxvYWRlclsnZmlsZSddID0gZmlsZXNbMF1cbiAgICAgICRzY29wZS51cGxvYWRlclsndXBsb2FkJ10gPSB0cnVlXG4gICAgZWxzZVxuICAgICAgJHNjb3BlLnVwbG9hZGVyWydlcnJvciddID0gXCJEaWQgeWEgZm9yZ2V0IHRvIHNlbGVjdCBhIGZpbGU/XCJcblxuICAkc2NvcGUuc3RhcnRVcGxvYWQgPSAoKSAtPlxuICAgIGlmICRzY29wZS51cGxvYWRlclsnZmlsZSddP1xuICAgICAgZm9ybWRhdGEgPSBuZXcgRm9ybURhdGEoKVxuICAgICAgZm9ybWRhdGEuYXBwZW5kKFwiamFyZmlsZVwiLCAkc2NvcGUudXBsb2FkZXJbJ2ZpbGUnXSlcbiAgICAgICRzY29wZS51cGxvYWRlclsndXBsb2FkJ10gPSBmYWxzZVxuICAgICAgJHNjb3BlLnVwbG9hZGVyWydzdWNjZXNzJ10gPSBcIkluaXRpYWxpemluZyB1cGxvYWQuLi5cIlxuICAgICAgeGhyID0gbmV3IFhNTEh0dHBSZXF1ZXN0KClcbiAgICAgIHhoci51cGxvYWQub25wcm9ncmVzcyA9IChldmVudCkgLT5cbiAgICAgICAgJHNjb3BlLnVwbG9hZGVyWydzdWNjZXNzJ10gPSBudWxsXG4gICAgICAgICRzY29wZS51cGxvYWRlclsncHJvZ3Jlc3MnXSA9IHBhcnNlSW50KDEwMCAqIGV2ZW50LmxvYWRlZCAvIGV2ZW50LnRvdGFsKVxuICAgICAgeGhyLnVwbG9hZC5vbmVycm9yID0gKGV2ZW50KSAtPlxuICAgICAgICAkc2NvcGUudXBsb2FkZXJbJ3Byb2dyZXNzJ10gPSBudWxsXG4gICAgICAgICRzY29wZS51cGxvYWRlclsnZXJyb3InXSA9IFwiQW4gZXJyb3Igb2NjdXJyZWQgd2hpbGUgdXBsb2FkaW5nIHlvdXIgZmlsZVwiXG4gICAgICB4aHIudXBsb2FkLm9ubG9hZCA9IChldmVudCkgLT5cbiAgICAgICAgJHNjb3BlLnVwbG9hZGVyWydwcm9ncmVzcyddID0gbnVsbFxuICAgICAgICAkc2NvcGUudXBsb2FkZXJbJ3N1Y2Nlc3MnXSA9IFwiU2F2aW5nLi4uXCJcbiAgICAgIHhoci5vbnJlYWR5c3RhdGVjaGFuZ2UgPSAoKSAtPlxuICAgICAgICBpZiB4aHIucmVhZHlTdGF0ZSA9PSA0XG4gICAgICAgICAgcmVzcG9uc2UgPSBKU09OLnBhcnNlKHhoci5yZXNwb25zZVRleHQpXG4gICAgICAgICAgaWYgcmVzcG9uc2UuZXJyb3I/XG4gICAgICAgICAgICAkc2NvcGUudXBsb2FkZXJbJ2Vycm9yJ10gPSByZXNwb25zZS5lcnJvclxuICAgICAgICAgICAgJHNjb3BlLnVwbG9hZGVyWydzdWNjZXNzJ10gPSBudWxsXG4gICAgICAgICAgZWxzZVxuICAgICAgICAgICAgJHNjb3BlLnVwbG9hZGVyWydzdWNjZXNzJ10gPSBcIlVwbG9hZGVkIVwiXG4gICAgICB4aHIub3BlbihcIlBPU1RcIiwgXCIvamFycy91cGxvYWRcIilcbiAgICAgIHhoci5zZW5kKGZvcm1kYXRhKVxuICAgIGVsc2VcbiAgICAgIGNvbnNvbGUubG9nKFwiVW5leHBlY3RlZCBFcnJvci4gVGhpcyBzaG91bGQgbm90IGhhcHBlblwiKVxuXG4uZmlsdGVyICdnZXRKYXJTZWxlY3RDbGFzcycsIC0+XG4gIChzZWxlY3RlZCwgYWN0dWFsKSAtPlxuICAgIGlmIHNlbGVjdGVkID09IGFjdHVhbFxuICAgICAgXCJmYS1jaGVjay1zcXVhcmVcIlxuICAgIGVsc2VcbiAgICAgIFwiZmEtc3F1YXJlLW9cIlxuIiwiYW5ndWxhci5tb2R1bGUoJ2ZsaW5rQXBwJykuY29udHJvbGxlcignSm9iU3VibWl0Q29udHJvbGxlcicsIGZ1bmN0aW9uKCRzY29wZSwgSm9iU3VibWl0U2VydmljZSwgJGludGVydmFsLCBmbGlua0NvbmZpZywgJHN0YXRlLCAkbG9jYXRpb24pIHtcbiAgdmFyIHJlZnJlc2g7XG4gICRzY29wZS55YXJuID0gJGxvY2F0aW9uLmFic1VybCgpLmluZGV4T2YoXCIvcHJveHkvYXBwbGljYXRpb25fXCIpICE9PSAtMTtcbiAgJHNjb3BlLmxvYWRMaXN0ID0gZnVuY3Rpb24oKSB7XG4gICAgcmV0dXJuIEpvYlN1Ym1pdFNlcnZpY2UubG9hZEphckxpc3QoKS50aGVuKGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgICRzY29wZS5hZGRyZXNzID0gZGF0YS5hZGRyZXNzO1xuICAgICAgJHNjb3BlLm5vYWNjZXNzID0gZGF0YS5lcnJvcjtcbiAgICAgIHJldHVybiAkc2NvcGUuamFycyA9IGRhdGEuZmlsZXM7XG4gICAgfSk7XG4gIH07XG4gICRzY29wZS5kZWZhdWx0U3RhdGUgPSBmdW5jdGlvbigpIHtcbiAgICAkc2NvcGUucGxhbiA9IG51bGw7XG4gICAgJHNjb3BlLmVycm9yID0gbnVsbDtcbiAgICByZXR1cm4gJHNjb3BlLnN0YXRlID0ge1xuICAgICAgc2VsZWN0ZWQ6IG51bGwsXG4gICAgICBwYXJhbGxlbGlzbTogXCJcIixcbiAgICAgICdlbnRyeS1jbGFzcyc6IFwiXCIsXG4gICAgICAncHJvZ3JhbS1hcmdzJzogXCJcIixcbiAgICAgICdwbGFuLWJ1dHRvbic6IFwiU2hvdyBQbGFuXCIsXG4gICAgICAnc3VibWl0LWJ1dHRvbic6IFwiU3VibWl0XCIsXG4gICAgICAnYWN0aW9uLXRpbWUnOiAwXG4gICAgfTtcbiAgfTtcbiAgJHNjb3BlLmRlZmF1bHRTdGF0ZSgpO1xuICAkc2NvcGUudXBsb2FkZXIgPSB7fTtcbiAgJHNjb3BlLmxvYWRMaXN0KCk7XG4gIHJlZnJlc2ggPSAkaW50ZXJ2YWwoZnVuY3Rpb24oKSB7XG4gICAgcmV0dXJuICRzY29wZS5sb2FkTGlzdCgpO1xuICB9LCBmbGlua0NvbmZpZ1tcInJlZnJlc2gtaW50ZXJ2YWxcIl0pO1xuICAkc2NvcGUuJG9uKCckZGVzdHJveScsIGZ1bmN0aW9uKCkge1xuICAgIHJldHVybiAkaW50ZXJ2YWwuY2FuY2VsKHJlZnJlc2gpO1xuICB9KTtcbiAgJHNjb3BlLnNlbGVjdEphciA9IGZ1bmN0aW9uKGlkKSB7XG4gICAgaWYgKCRzY29wZS5zdGF0ZS5zZWxlY3RlZCA9PT0gaWQpIHtcbiAgICAgIHJldHVybiAkc2NvcGUuZGVmYXVsdFN0YXRlKCk7XG4gICAgfSBlbHNlIHtcbiAgICAgICRzY29wZS5kZWZhdWx0U3RhdGUoKTtcbiAgICAgIHJldHVybiAkc2NvcGUuc3RhdGUuc2VsZWN0ZWQgPSBpZDtcbiAgICB9XG4gIH07XG4gICRzY29wZS5kZWxldGVKYXIgPSBmdW5jdGlvbihldmVudCwgaWQpIHtcbiAgICBpZiAoJHNjb3BlLnN0YXRlLnNlbGVjdGVkID09PSBpZCkge1xuICAgICAgJHNjb3BlLmRlZmF1bHRTdGF0ZSgpO1xuICAgIH1cbiAgICBhbmd1bGFyLmVsZW1lbnQoZXZlbnQuY3VycmVudFRhcmdldCkucmVtb3ZlQ2xhc3MoXCJmYS1yZW1vdmVcIikuYWRkQ2xhc3MoXCJmYS1zcGluIGZhLXNwaW5uZXJcIik7XG4gICAgcmV0dXJuIEpvYlN1Ym1pdFNlcnZpY2UuZGVsZXRlSmFyKGlkKS50aGVuKGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgIGFuZ3VsYXIuZWxlbWVudChldmVudC5jdXJyZW50VGFyZ2V0KS5yZW1vdmVDbGFzcyhcImZhLXNwaW4gZmEtc3Bpbm5lclwiKS5hZGRDbGFzcyhcImZhLXJlbW92ZVwiKTtcbiAgICAgIGlmIChkYXRhLmVycm9yICE9IG51bGwpIHtcbiAgICAgICAgcmV0dXJuIGFsZXJ0KGRhdGEuZXJyb3IpO1xuICAgICAgfVxuICAgIH0pO1xuICB9O1xuICAkc2NvcGUubG9hZEVudHJ5Q2xhc3MgPSBmdW5jdGlvbihuYW1lKSB7XG4gICAgcmV0dXJuICRzY29wZS5zdGF0ZVsnZW50cnktY2xhc3MnXSA9IG5hbWU7XG4gIH07XG4gICRzY29wZS5nZXRQbGFuID0gZnVuY3Rpb24oKSB7XG4gICAgdmFyIGFjdGlvbjtcbiAgICBpZiAoJHNjb3BlLnN0YXRlWydwbGFuLWJ1dHRvbiddID09PSBcIlNob3cgUGxhblwiKSB7XG4gICAgICBhY3Rpb24gPSBuZXcgRGF0ZSgpLmdldFRpbWUoKTtcbiAgICAgICRzY29wZS5zdGF0ZVsnYWN0aW9uLXRpbWUnXSA9IGFjdGlvbjtcbiAgICAgICRzY29wZS5zdGF0ZVsnc3VibWl0LWJ1dHRvbiddID0gXCJTdWJtaXRcIjtcbiAgICAgICRzY29wZS5zdGF0ZVsncGxhbi1idXR0b24nXSA9IFwiR2V0dGluZyBQbGFuXCI7XG4gICAgICAkc2NvcGUuZXJyb3IgPSBudWxsO1xuICAgICAgJHNjb3BlLnBsYW4gPSBudWxsO1xuICAgICAgcmV0dXJuIEpvYlN1Ym1pdFNlcnZpY2UuZ2V0UGxhbigkc2NvcGUuc3RhdGUuc2VsZWN0ZWQsIHtcbiAgICAgICAgJ2VudHJ5LWNsYXNzJzogJHNjb3BlLnN0YXRlWydlbnRyeS1jbGFzcyddLFxuICAgICAgICBwYXJhbGxlbGlzbTogJHNjb3BlLnN0YXRlLnBhcmFsbGVsaXNtLFxuICAgICAgICAncHJvZ3JhbS1hcmdzJzogJHNjb3BlLnN0YXRlWydwcm9ncmFtLWFyZ3MnXVxuICAgICAgfSkudGhlbihmdW5jdGlvbihkYXRhKSB7XG4gICAgICAgIGlmIChhY3Rpb24gPT09ICRzY29wZS5zdGF0ZVsnYWN0aW9uLXRpbWUnXSkge1xuICAgICAgICAgICRzY29wZS5zdGF0ZVsncGxhbi1idXR0b24nXSA9IFwiU2hvdyBQbGFuXCI7XG4gICAgICAgICAgJHNjb3BlLmVycm9yID0gZGF0YS5lcnJvcjtcbiAgICAgICAgICByZXR1cm4gJHNjb3BlLnBsYW4gPSBkYXRhLnBsYW47XG4gICAgICAgIH1cbiAgICAgIH0pO1xuICAgIH1cbiAgfTtcbiAgJHNjb3BlLnJ1bkpvYiA9IGZ1bmN0aW9uKCkge1xuICAgIHZhciBhY3Rpb247XG4gICAgaWYgKCRzY29wZS5zdGF0ZVsnc3VibWl0LWJ1dHRvbiddID09PSBcIlN1Ym1pdFwiKSB7XG4gICAgICBhY3Rpb24gPSBuZXcgRGF0ZSgpLmdldFRpbWUoKTtcbiAgICAgICRzY29wZS5zdGF0ZVsnYWN0aW9uLXRpbWUnXSA9IGFjdGlvbjtcbiAgICAgICRzY29wZS5zdGF0ZVsnc3VibWl0LWJ1dHRvbiddID0gXCJTdWJtaXR0aW5nXCI7XG4gICAgICAkc2NvcGUuc3RhdGVbJ3BsYW4tYnV0dG9uJ10gPSBcIlNob3cgUGxhblwiO1xuICAgICAgJHNjb3BlLmVycm9yID0gbnVsbDtcbiAgICAgIHJldHVybiBKb2JTdWJtaXRTZXJ2aWNlLnJ1bkpvYigkc2NvcGUuc3RhdGUuc2VsZWN0ZWQsIHtcbiAgICAgICAgJ2VudHJ5LWNsYXNzJzogJHNjb3BlLnN0YXRlWydlbnRyeS1jbGFzcyddLFxuICAgICAgICBwYXJhbGxlbGlzbTogJHNjb3BlLnN0YXRlLnBhcmFsbGVsaXNtLFxuICAgICAgICAncHJvZ3JhbS1hcmdzJzogJHNjb3BlLnN0YXRlWydwcm9ncmFtLWFyZ3MnXVxuICAgICAgfSkudGhlbihmdW5jdGlvbihkYXRhKSB7XG4gICAgICAgIGlmIChhY3Rpb24gPT09ICRzY29wZS5zdGF0ZVsnYWN0aW9uLXRpbWUnXSkge1xuICAgICAgICAgICRzY29wZS5zdGF0ZVsnc3VibWl0LWJ1dHRvbiddID0gXCJTdWJtaXRcIjtcbiAgICAgICAgICAkc2NvcGUuZXJyb3IgPSBkYXRhLmVycm9yO1xuICAgICAgICAgIGlmIChkYXRhLmpvYmlkICE9IG51bGwpIHtcbiAgICAgICAgICAgIHJldHVybiAkc3RhdGUuZ28oXCJzaW5nbGUtam9iLnBsYW4ub3ZlcnZpZXdcIiwge1xuICAgICAgICAgICAgICBqb2JpZDogZGF0YS5qb2JpZFxuICAgICAgICAgICAgfSk7XG4gICAgICAgICAgfVxuICAgICAgICB9XG4gICAgICB9KTtcbiAgICB9XG4gIH07XG4gICRzY29wZS5ub2RlaWQgPSBudWxsO1xuICAkc2NvcGUuY2hhbmdlTm9kZSA9IGZ1bmN0aW9uKG5vZGVpZCkge1xuICAgIGlmIChub2RlaWQgIT09ICRzY29wZS5ub2RlaWQpIHtcbiAgICAgICRzY29wZS5ub2RlaWQgPSBub2RlaWQ7XG4gICAgICAkc2NvcGUudmVydGV4ID0gbnVsbDtcbiAgICAgICRzY29wZS5zdWJ0YXNrcyA9IG51bGw7XG4gICAgICAkc2NvcGUuYWNjdW11bGF0b3JzID0gbnVsbDtcbiAgICAgIHJldHVybiAkc2NvcGUuJGJyb2FkY2FzdCgncmVsb2FkJyk7XG4gICAgfSBlbHNlIHtcbiAgICAgICRzY29wZS5ub2RlaWQgPSBudWxsO1xuICAgICAgJHNjb3BlLm5vZGVVbmZvbGRlZCA9IGZhbHNlO1xuICAgICAgJHNjb3BlLnZlcnRleCA9IG51bGw7XG4gICAgICAkc2NvcGUuc3VidGFza3MgPSBudWxsO1xuICAgICAgcmV0dXJuICRzY29wZS5hY2N1bXVsYXRvcnMgPSBudWxsO1xuICAgIH1cbiAgfTtcbiAgJHNjb3BlLmNsZWFyRmlsZXMgPSBmdW5jdGlvbigpIHtcbiAgICByZXR1cm4gJHNjb3BlLnVwbG9hZGVyID0ge307XG4gIH07XG4gICRzY29wZS51cGxvYWRGaWxlcyA9IGZ1bmN0aW9uKGZpbGVzKSB7XG4gICAgJHNjb3BlLnVwbG9hZGVyID0ge307XG4gICAgaWYgKGZpbGVzLmxlbmd0aCA9PT0gMSkge1xuICAgICAgJHNjb3BlLnVwbG9hZGVyWydmaWxlJ10gPSBmaWxlc1swXTtcbiAgICAgIHJldHVybiAkc2NvcGUudXBsb2FkZXJbJ3VwbG9hZCddID0gdHJ1ZTtcbiAgICB9IGVsc2Uge1xuICAgICAgcmV0dXJuICRzY29wZS51cGxvYWRlclsnZXJyb3InXSA9IFwiRGlkIHlhIGZvcmdldCB0byBzZWxlY3QgYSBmaWxlP1wiO1xuICAgIH1cbiAgfTtcbiAgcmV0dXJuICRzY29wZS5zdGFydFVwbG9hZCA9IGZ1bmN0aW9uKCkge1xuICAgIHZhciBmb3JtZGF0YSwgeGhyO1xuICAgIGlmICgkc2NvcGUudXBsb2FkZXJbJ2ZpbGUnXSAhPSBudWxsKSB7XG4gICAgICBmb3JtZGF0YSA9IG5ldyBGb3JtRGF0YSgpO1xuICAgICAgZm9ybWRhdGEuYXBwZW5kKFwiamFyZmlsZVwiLCAkc2NvcGUudXBsb2FkZXJbJ2ZpbGUnXSk7XG4gICAgICAkc2NvcGUudXBsb2FkZXJbJ3VwbG9hZCddID0gZmFsc2U7XG4gICAgICAkc2NvcGUudXBsb2FkZXJbJ3N1Y2Nlc3MnXSA9IFwiSW5pdGlhbGl6aW5nIHVwbG9hZC4uLlwiO1xuICAgICAgeGhyID0gbmV3IFhNTEh0dHBSZXF1ZXN0KCk7XG4gICAgICB4aHIudXBsb2FkLm9ucHJvZ3Jlc3MgPSBmdW5jdGlvbihldmVudCkge1xuICAgICAgICAkc2NvcGUudXBsb2FkZXJbJ3N1Y2Nlc3MnXSA9IG51bGw7XG4gICAgICAgIHJldHVybiAkc2NvcGUudXBsb2FkZXJbJ3Byb2dyZXNzJ10gPSBwYXJzZUludCgxMDAgKiBldmVudC5sb2FkZWQgLyBldmVudC50b3RhbCk7XG4gICAgICB9O1xuICAgICAgeGhyLnVwbG9hZC5vbmVycm9yID0gZnVuY3Rpb24oZXZlbnQpIHtcbiAgICAgICAgJHNjb3BlLnVwbG9hZGVyWydwcm9ncmVzcyddID0gbnVsbDtcbiAgICAgICAgcmV0dXJuICRzY29wZS51cGxvYWRlclsnZXJyb3InXSA9IFwiQW4gZXJyb3Igb2NjdXJyZWQgd2hpbGUgdXBsb2FkaW5nIHlvdXIgZmlsZVwiO1xuICAgICAgfTtcbiAgICAgIHhoci51cGxvYWQub25sb2FkID0gZnVuY3Rpb24oZXZlbnQpIHtcbiAgICAgICAgJHNjb3BlLnVwbG9hZGVyWydwcm9ncmVzcyddID0gbnVsbDtcbiAgICAgICAgcmV0dXJuICRzY29wZS51cGxvYWRlclsnc3VjY2VzcyddID0gXCJTYXZpbmcuLi5cIjtcbiAgICAgIH07XG4gICAgICB4aHIub25yZWFkeXN0YXRlY2hhbmdlID0gZnVuY3Rpb24oKSB7XG4gICAgICAgIHZhciByZXNwb25zZTtcbiAgICAgICAgaWYgKHhoci5yZWFkeVN0YXRlID09PSA0KSB7XG4gICAgICAgICAgcmVzcG9uc2UgPSBKU09OLnBhcnNlKHhoci5yZXNwb25zZVRleHQpO1xuICAgICAgICAgIGlmIChyZXNwb25zZS5lcnJvciAhPSBudWxsKSB7XG4gICAgICAgICAgICAkc2NvcGUudXBsb2FkZXJbJ2Vycm9yJ10gPSByZXNwb25zZS5lcnJvcjtcbiAgICAgICAgICAgIHJldHVybiAkc2NvcGUudXBsb2FkZXJbJ3N1Y2Nlc3MnXSA9IG51bGw7XG4gICAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICAgIHJldHVybiAkc2NvcGUudXBsb2FkZXJbJ3N1Y2Nlc3MnXSA9IFwiVXBsb2FkZWQhXCI7XG4gICAgICAgICAgfVxuICAgICAgICB9XG4gICAgICB9O1xuICAgICAgeGhyLm9wZW4oXCJQT1NUXCIsIFwiL2phcnMvdXBsb2FkXCIpO1xuICAgICAgcmV0dXJuIHhoci5zZW5kKGZvcm1kYXRhKTtcbiAgICB9IGVsc2Uge1xuICAgICAgcmV0dXJuIGNvbnNvbGUubG9nKFwiVW5leHBlY3RlZCBFcnJvci4gVGhpcyBzaG91bGQgbm90IGhhcHBlblwiKTtcbiAgICB9XG4gIH07XG59KS5maWx0ZXIoJ2dldEphclNlbGVjdENsYXNzJywgZnVuY3Rpb24oKSB7XG4gIHJldHVybiBmdW5jdGlvbihzZWxlY3RlZCwgYWN0dWFsKSB7XG4gICAgaWYgKHNlbGVjdGVkID09PSBhY3R1YWwpIHtcbiAgICAgIHJldHVybiBcImZhLWNoZWNrLXNxdWFyZVwiO1xuICAgIH0gZWxzZSB7XG4gICAgICByZXR1cm4gXCJmYS1zcXVhcmUtb1wiO1xuICAgIH1cbiAgfTtcbn0pO1xuIiwiI1xuIyBMaWNlbnNlZCB0byB0aGUgQXBhY2hlIFNvZnR3YXJlIEZvdW5kYXRpb24gKEFTRikgdW5kZXIgb25lXG4jIG9yIG1vcmUgY29udHJpYnV0b3IgbGljZW5zZSBhZ3JlZW1lbnRzLiAgU2VlIHRoZSBOT1RJQ0UgZmlsZVxuIyBkaXN0cmlidXRlZCB3aXRoIHRoaXMgd29yayBmb3IgYWRkaXRpb25hbCBpbmZvcm1hdGlvblxuIyByZWdhcmRpbmcgY29weXJpZ2h0IG93bmVyc2hpcC4gIFRoZSBBU0YgbGljZW5zZXMgdGhpcyBmaWxlXG4jIHRvIHlvdSB1bmRlciB0aGUgQXBhY2hlIExpY2Vuc2UsIFZlcnNpb24gMi4wICh0aGVcbiMgXCJMaWNlbnNlXCIpOyB5b3UgbWF5IG5vdCB1c2UgdGhpcyBmaWxlIGV4Y2VwdCBpbiBjb21wbGlhbmNlXG4jIHdpdGggdGhlIExpY2Vuc2UuICBZb3UgbWF5IG9idGFpbiBhIGNvcHkgb2YgdGhlIExpY2Vuc2UgYXRcbiNcbiMgICAgIGh0dHA6Ly93d3cuYXBhY2hlLm9yZy9saWNlbnNlcy9MSUNFTlNFLTIuMFxuI1xuIyBVbmxlc3MgcmVxdWlyZWQgYnkgYXBwbGljYWJsZSBsYXcgb3IgYWdyZWVkIHRvIGluIHdyaXRpbmcsIHNvZnR3YXJlXG4jIGRpc3RyaWJ1dGVkIHVuZGVyIHRoZSBMaWNlbnNlIGlzIGRpc3RyaWJ1dGVkIG9uIGFuIFwiQVMgSVNcIiBCQVNJUyxcbiMgV0lUSE9VVCBXQVJSQU5USUVTIE9SIENPTkRJVElPTlMgT0YgQU5ZIEtJTkQsIGVpdGhlciBleHByZXNzIG9yIGltcGxpZWQuXG4jIFNlZSB0aGUgTGljZW5zZSBmb3IgdGhlIHNwZWNpZmljIGxhbmd1YWdlIGdvdmVybmluZyBwZXJtaXNzaW9ucyBhbmRcbiMgbGltaXRhdGlvbnMgdW5kZXIgdGhlIExpY2Vuc2UuXG4jXG5cbmFuZ3VsYXIubW9kdWxlKCdmbGlua0FwcCcpXG5cbi5zZXJ2aWNlICdKb2JTdWJtaXRTZXJ2aWNlJywgKCRodHRwLCBmbGlua0NvbmZpZywgJHEpIC0+XG5cbiAgQGxvYWRKYXJMaXN0ID0gKCkgLT5cbiAgICBkZWZlcnJlZCA9ICRxLmRlZmVyKClcblxuICAgICRodHRwLmdldChcImphcnMvXCIpXG4gICAgLnN1Y2Nlc3MgKGRhdGEsIHN0YXR1cywgaGVhZGVycywgY29uZmlnKSAtPlxuICAgICAgZGVmZXJyZWQucmVzb2x2ZShkYXRhKVxuXG4gICAgZGVmZXJyZWQucHJvbWlzZVxuXG4gIEBkZWxldGVKYXIgPSAoaWQpIC0+XG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpXG5cbiAgICAkaHR0cC5kZWxldGUoXCJqYXJzL1wiICsgaWQpXG4gICAgLnN1Y2Nlc3MgKGRhdGEsIHN0YXR1cywgaGVhZGVycywgY29uZmlnKSAtPlxuICAgICAgIGRlZmVycmVkLnJlc29sdmUoZGF0YSlcblxuICAgIGRlZmVycmVkLnByb21pc2VcblxuICBAZ2V0UGxhbiA9IChpZCwgYXJncykgLT5cbiAgICBkZWZlcnJlZCA9ICRxLmRlZmVyKClcblxuICAgICRodHRwLmdldChcImphcnMvXCIgKyBpZCArIFwiL3BsYW5cIiwge3BhcmFtczogYXJnc30pXG4gICAgLnN1Y2Nlc3MgKGRhdGEsIHN0YXR1cywgaGVhZGVycywgY29uZmlnKSAtPlxuICAgICAgZGVmZXJyZWQucmVzb2x2ZShkYXRhKVxuXG4gICAgZGVmZXJyZWQucHJvbWlzZVxuXG4gIEBydW5Kb2IgPSAoaWQsIGFyZ3MpIC0+XG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpXG5cbiAgICAkaHR0cC5wb3N0KFwiamFycy9cIiArIGlkICsgXCIvcnVuXCIsIHt9LCB7cGFyYW1zOiBhcmdzfSlcbiAgICAuc3VjY2VzcyAoZGF0YSwgc3RhdHVzLCBoZWFkZXJzLCBjb25maWcpIC0+XG4gICAgICBkZWZlcnJlZC5yZXNvbHZlKGRhdGEpXG5cbiAgICBkZWZlcnJlZC5wcm9taXNlXG5cbiAgQFxuIiwiYW5ndWxhci5tb2R1bGUoJ2ZsaW5rQXBwJykuc2VydmljZSgnSm9iU3VibWl0U2VydmljZScsIGZ1bmN0aW9uKCRodHRwLCBmbGlua0NvbmZpZywgJHEpIHtcbiAgdGhpcy5sb2FkSmFyTGlzdCA9IGZ1bmN0aW9uKCkge1xuICAgIHZhciBkZWZlcnJlZDtcbiAgICBkZWZlcnJlZCA9ICRxLmRlZmVyKCk7XG4gICAgJGh0dHAuZ2V0KFwiamFycy9cIikuc3VjY2VzcyhmdW5jdGlvbihkYXRhLCBzdGF0dXMsIGhlYWRlcnMsIGNvbmZpZykge1xuICAgICAgcmV0dXJuIGRlZmVycmVkLnJlc29sdmUoZGF0YSk7XG4gICAgfSk7XG4gICAgcmV0dXJuIGRlZmVycmVkLnByb21pc2U7XG4gIH07XG4gIHRoaXMuZGVsZXRlSmFyID0gZnVuY3Rpb24oaWQpIHtcbiAgICB2YXIgZGVmZXJyZWQ7XG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpO1xuICAgICRodHRwW1wiZGVsZXRlXCJdKFwiamFycy9cIiArIGlkKS5zdWNjZXNzKGZ1bmN0aW9uKGRhdGEsIHN0YXR1cywgaGVhZGVycywgY29uZmlnKSB7XG4gICAgICByZXR1cm4gZGVmZXJyZWQucmVzb2x2ZShkYXRhKTtcbiAgICB9KTtcbiAgICByZXR1cm4gZGVmZXJyZWQucHJvbWlzZTtcbiAgfTtcbiAgdGhpcy5nZXRQbGFuID0gZnVuY3Rpb24oaWQsIGFyZ3MpIHtcbiAgICB2YXIgZGVmZXJyZWQ7XG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpO1xuICAgICRodHRwLmdldChcImphcnMvXCIgKyBpZCArIFwiL3BsYW5cIiwge1xuICAgICAgcGFyYW1zOiBhcmdzXG4gICAgfSkuc3VjY2VzcyhmdW5jdGlvbihkYXRhLCBzdGF0dXMsIGhlYWRlcnMsIGNvbmZpZykge1xuICAgICAgcmV0dXJuIGRlZmVycmVkLnJlc29sdmUoZGF0YSk7XG4gICAgfSk7XG4gICAgcmV0dXJuIGRlZmVycmVkLnByb21pc2U7XG4gIH07XG4gIHRoaXMucnVuSm9iID0gZnVuY3Rpb24oaWQsIGFyZ3MpIHtcbiAgICB2YXIgZGVmZXJyZWQ7XG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpO1xuICAgICRodHRwLnBvc3QoXCJqYXJzL1wiICsgaWQgKyBcIi9ydW5cIiwge30sIHtcbiAgICAgIHBhcmFtczogYXJnc1xuICAgIH0pLnN1Y2Nlc3MoZnVuY3Rpb24oZGF0YSwgc3RhdHVzLCBoZWFkZXJzLCBjb25maWcpIHtcbiAgICAgIHJldHVybiBkZWZlcnJlZC5yZXNvbHZlKGRhdGEpO1xuICAgIH0pO1xuICAgIHJldHVybiBkZWZlcnJlZC5wcm9taXNlO1xuICB9O1xuICByZXR1cm4gdGhpcztcbn0pO1xuIiwiI1xuIyBMaWNlbnNlZCB0byB0aGUgQXBhY2hlIFNvZnR3YXJlIEZvdW5kYXRpb24gKEFTRikgdW5kZXIgb25lXG4jIG9yIG1vcmUgY29udHJpYnV0b3IgbGljZW5zZSBhZ3JlZW1lbnRzLiAgU2VlIHRoZSBOT1RJQ0UgZmlsZVxuIyBkaXN0cmlidXRlZCB3aXRoIHRoaXMgd29yayBmb3IgYWRkaXRpb25hbCBpbmZvcm1hdGlvblxuIyByZWdhcmRpbmcgY29weXJpZ2h0IG93bmVyc2hpcC4gIFRoZSBBU0YgbGljZW5zZXMgdGhpcyBmaWxlXG4jIHRvIHlvdSB1bmRlciB0aGUgQXBhY2hlIExpY2Vuc2UsIFZlcnNpb24gMi4wICh0aGVcbiMgXCJMaWNlbnNlXCIpOyB5b3UgbWF5IG5vdCB1c2UgdGhpcyBmaWxlIGV4Y2VwdCBpbiBjb21wbGlhbmNlXG4jIHdpdGggdGhlIExpY2Vuc2UuICBZb3UgbWF5IG9idGFpbiBhIGNvcHkgb2YgdGhlIExpY2Vuc2UgYXRcbiNcbiMgICAgIGh0dHA6Ly93d3cuYXBhY2hlLm9yZy9saWNlbnNlcy9MSUNFTlNFLTIuMFxuI1xuIyBVbmxlc3MgcmVxdWlyZWQgYnkgYXBwbGljYWJsZSBsYXcgb3IgYWdyZWVkIHRvIGluIHdyaXRpbmcsIHNvZnR3YXJlXG4jIGRpc3RyaWJ1dGVkIHVuZGVyIHRoZSBMaWNlbnNlIGlzIGRpc3RyaWJ1dGVkIG9uIGFuIFwiQVMgSVNcIiBCQVNJUyxcbiMgV0lUSE9VVCBXQVJSQU5USUVTIE9SIENPTkRJVElPTlMgT0YgQU5ZIEtJTkQsIGVpdGhlciBleHByZXNzIG9yIGltcGxpZWQuXG4jIFNlZSB0aGUgTGljZW5zZSBmb3IgdGhlIHNwZWNpZmljIGxhbmd1YWdlIGdvdmVybmluZyBwZXJtaXNzaW9ucyBhbmRcbiMgbGltaXRhdGlvbnMgdW5kZXIgdGhlIExpY2Vuc2UuXG4jXG5cbmFuZ3VsYXIubW9kdWxlKCdmbGlua0FwcCcpXG5cbi5jb250cm9sbGVyICdBbGxUYXNrTWFuYWdlcnNDb250cm9sbGVyJywgKCRzY29wZSwgVGFza01hbmFnZXJzU2VydmljZSwgJGludGVydmFsLCBmbGlua0NvbmZpZykgLT5cbiAgVGFza01hbmFnZXJzU2VydmljZS5sb2FkTWFuYWdlcnMoKS50aGVuIChkYXRhKSAtPlxuICAgICRzY29wZS5tYW5hZ2VycyA9IGRhdGFcblxuICByZWZyZXNoID0gJGludGVydmFsIC0+XG4gICAgVGFza01hbmFnZXJzU2VydmljZS5sb2FkTWFuYWdlcnMoKS50aGVuIChkYXRhKSAtPlxuICAgICAgJHNjb3BlLm1hbmFnZXJzID0gZGF0YVxuICAsIGZsaW5rQ29uZmlnW1wicmVmcmVzaC1pbnRlcnZhbFwiXVxuXG4gICRzY29wZS4kb24gJyRkZXN0cm95JywgLT5cbiAgICAkaW50ZXJ2YWwuY2FuY2VsKHJlZnJlc2gpXG5cbi5jb250cm9sbGVyICdTaW5nbGVUYXNrTWFuYWdlckNvbnRyb2xsZXInLCAoJHNjb3BlLCAkc3RhdGVQYXJhbXMsIFNpbmdsZVRhc2tNYW5hZ2VyU2VydmljZSwgJGludGVydmFsLCBmbGlua0NvbmZpZykgLT5cbiAgJHNjb3BlLm1ldHJpY3MgPSB7fVxuICBTaW5nbGVUYXNrTWFuYWdlclNlcnZpY2UubG9hZE1ldHJpY3MoJHN0YXRlUGFyYW1zLnRhc2ttYW5hZ2VyaWQpLnRoZW4gKGRhdGEpIC0+XG4gICAgICAkc2NvcGUubWV0cmljcyA9IGRhdGFbMF1cblxuICAgIHJlZnJlc2ggPSAkaW50ZXJ2YWwgLT5cbiAgICAgIFNpbmdsZVRhc2tNYW5hZ2VyU2VydmljZS5sb2FkTWV0cmljcygkc3RhdGVQYXJhbXMudGFza21hbmFnZXJpZCkudGhlbiAoZGF0YSkgLT5cbiAgICAgICAgJHNjb3BlLm1ldHJpY3MgPSBkYXRhWzBdXG4gICAgLCBmbGlua0NvbmZpZ1tcInJlZnJlc2gtaW50ZXJ2YWxcIl1cblxuICAgICRzY29wZS4kb24gJyRkZXN0cm95JywgLT5cbiAgICAgICRpbnRlcnZhbC5jYW5jZWwocmVmcmVzaClcblxuIiwiYW5ndWxhci5tb2R1bGUoJ2ZsaW5rQXBwJykuY29udHJvbGxlcignQWxsVGFza01hbmFnZXJzQ29udHJvbGxlcicsIGZ1bmN0aW9uKCRzY29wZSwgVGFza01hbmFnZXJzU2VydmljZSwgJGludGVydmFsLCBmbGlua0NvbmZpZykge1xuICB2YXIgcmVmcmVzaDtcbiAgVGFza01hbmFnZXJzU2VydmljZS5sb2FkTWFuYWdlcnMoKS50aGVuKGZ1bmN0aW9uKGRhdGEpIHtcbiAgICByZXR1cm4gJHNjb3BlLm1hbmFnZXJzID0gZGF0YTtcbiAgfSk7XG4gIHJlZnJlc2ggPSAkaW50ZXJ2YWwoZnVuY3Rpb24oKSB7XG4gICAgcmV0dXJuIFRhc2tNYW5hZ2Vyc1NlcnZpY2UubG9hZE1hbmFnZXJzKCkudGhlbihmdW5jdGlvbihkYXRhKSB7XG4gICAgICByZXR1cm4gJHNjb3BlLm1hbmFnZXJzID0gZGF0YTtcbiAgICB9KTtcbiAgfSwgZmxpbmtDb25maWdbXCJyZWZyZXNoLWludGVydmFsXCJdKTtcbiAgcmV0dXJuICRzY29wZS4kb24oJyRkZXN0cm95JywgZnVuY3Rpb24oKSB7XG4gICAgcmV0dXJuICRpbnRlcnZhbC5jYW5jZWwocmVmcmVzaCk7XG4gIH0pO1xufSkuY29udHJvbGxlcignU2luZ2xlVGFza01hbmFnZXJDb250cm9sbGVyJywgZnVuY3Rpb24oJHNjb3BlLCAkc3RhdGVQYXJhbXMsIFNpbmdsZVRhc2tNYW5hZ2VyU2VydmljZSwgJGludGVydmFsLCBmbGlua0NvbmZpZykge1xuICB2YXIgcmVmcmVzaDtcbiAgJHNjb3BlLm1ldHJpY3MgPSB7fTtcbiAgU2luZ2xlVGFza01hbmFnZXJTZXJ2aWNlLmxvYWRNZXRyaWNzKCRzdGF0ZVBhcmFtcy50YXNrbWFuYWdlcmlkKS50aGVuKGZ1bmN0aW9uKGRhdGEpIHtcbiAgICByZXR1cm4gJHNjb3BlLm1ldHJpY3MgPSBkYXRhWzBdO1xuICB9KTtcbiAgcmVmcmVzaCA9ICRpbnRlcnZhbChmdW5jdGlvbigpIHtcbiAgICByZXR1cm4gU2luZ2xlVGFza01hbmFnZXJTZXJ2aWNlLmxvYWRNZXRyaWNzKCRzdGF0ZVBhcmFtcy50YXNrbWFuYWdlcmlkKS50aGVuKGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgIHJldHVybiAkc2NvcGUubWV0cmljcyA9IGRhdGFbMF07XG4gICAgfSk7XG4gIH0sIGZsaW5rQ29uZmlnW1wicmVmcmVzaC1pbnRlcnZhbFwiXSk7XG4gIHJldHVybiAkc2NvcGUuJG9uKCckZGVzdHJveScsIGZ1bmN0aW9uKCkge1xuICAgIHJldHVybiAkaW50ZXJ2YWwuY2FuY2VsKHJlZnJlc2gpO1xuICB9KTtcbn0pO1xuIiwiI1xuIyBMaWNlbnNlZCB0byB0aGUgQXBhY2hlIFNvZnR3YXJlIEZvdW5kYXRpb24gKEFTRikgdW5kZXIgb25lXG4jIG9yIG1vcmUgY29udHJpYnV0b3IgbGljZW5zZSBhZ3JlZW1lbnRzLiAgU2VlIHRoZSBOT1RJQ0UgZmlsZVxuIyBkaXN0cmlidXRlZCB3aXRoIHRoaXMgd29yayBmb3IgYWRkaXRpb25hbCBpbmZvcm1hdGlvblxuIyByZWdhcmRpbmcgY29weXJpZ2h0IG93bmVyc2hpcC4gIFRoZSBBU0YgbGljZW5zZXMgdGhpcyBmaWxlXG4jIHRvIHlvdSB1bmRlciB0aGUgQXBhY2hlIExpY2Vuc2UsIFZlcnNpb24gMi4wICh0aGVcbiMgXCJMaWNlbnNlXCIpOyB5b3UgbWF5IG5vdCB1c2UgdGhpcyBmaWxlIGV4Y2VwdCBpbiBjb21wbGlhbmNlXG4jIHdpdGggdGhlIExpY2Vuc2UuICBZb3UgbWF5IG9idGFpbiBhIGNvcHkgb2YgdGhlIExpY2Vuc2UgYXRcbiNcbiMgICAgIGh0dHA6Ly93d3cuYXBhY2hlLm9yZy9saWNlbnNlcy9MSUNFTlNFLTIuMFxuI1xuIyBVbmxlc3MgcmVxdWlyZWQgYnkgYXBwbGljYWJsZSBsYXcgb3IgYWdyZWVkIHRvIGluIHdyaXRpbmcsIHNvZnR3YXJlXG4jIGRpc3RyaWJ1dGVkIHVuZGVyIHRoZSBMaWNlbnNlIGlzIGRpc3RyaWJ1dGVkIG9uIGFuIFwiQVMgSVNcIiBCQVNJUyxcbiMgV0lUSE9VVCBXQVJSQU5USUVTIE9SIENPTkRJVElPTlMgT0YgQU5ZIEtJTkQsIGVpdGhlciBleHByZXNzIG9yIGltcGxpZWQuXG4jIFNlZSB0aGUgTGljZW5zZSBmb3IgdGhlIHNwZWNpZmljIGxhbmd1YWdlIGdvdmVybmluZyBwZXJtaXNzaW9ucyBhbmRcbiMgbGltaXRhdGlvbnMgdW5kZXIgdGhlIExpY2Vuc2UuXG4jXG5cbmFuZ3VsYXIubW9kdWxlKCdmbGlua0FwcCcpXG5cbi5zZXJ2aWNlICdUYXNrTWFuYWdlcnNTZXJ2aWNlJywgKCRodHRwLCBmbGlua0NvbmZpZywgJHEpIC0+XG4gIEBsb2FkTWFuYWdlcnMgPSAoKSAtPlxuICAgIGRlZmVycmVkID0gJHEuZGVmZXIoKVxuXG4gICAgJGh0dHAuZ2V0KGZsaW5rQ29uZmlnLmpvYlNlcnZlciArIFwidGFza21hbmFnZXJzXCIpXG4gICAgLnN1Y2Nlc3MgKGRhdGEsIHN0YXR1cywgaGVhZGVycywgY29uZmlnKSAtPlxuICAgICAgZGVmZXJyZWQucmVzb2x2ZShkYXRhWyd0YXNrbWFuYWdlcnMnXSlcblxuICAgIGRlZmVycmVkLnByb21pc2VcblxuICBAXG5cbi5zZXJ2aWNlICdTaW5nbGVUYXNrTWFuYWdlclNlcnZpY2UnLCAoJGh0dHAsIGZsaW5rQ29uZmlnLCAkcSkgLT5cbiAgQGxvYWRNZXRyaWNzID0gKHRhc2ttYW5hZ2VyaWQpIC0+XG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpXG5cbiAgICAkaHR0cC5nZXQoZmxpbmtDb25maWcuam9iU2VydmVyICsgXCJ0YXNrbWFuYWdlcnMvXCIgKyB0YXNrbWFuYWdlcmlkKVxuICAgIC5zdWNjZXNzIChkYXRhLCBzdGF0dXMsIGhlYWRlcnMsIGNvbmZpZykgLT5cbiAgICAgIGRlZmVycmVkLnJlc29sdmUoZGF0YVsndGFza21hbmFnZXJzJ10pXG5cbiAgICBkZWZlcnJlZC5wcm9taXNlXG5cbiAgQFxuXG4iLCJhbmd1bGFyLm1vZHVsZSgnZmxpbmtBcHAnKS5zZXJ2aWNlKCdUYXNrTWFuYWdlcnNTZXJ2aWNlJywgZnVuY3Rpb24oJGh0dHAsIGZsaW5rQ29uZmlnLCAkcSkge1xuICB0aGlzLmxvYWRNYW5hZ2VycyA9IGZ1bmN0aW9uKCkge1xuICAgIHZhciBkZWZlcnJlZDtcbiAgICBkZWZlcnJlZCA9ICRxLmRlZmVyKCk7XG4gICAgJGh0dHAuZ2V0KGZsaW5rQ29uZmlnLmpvYlNlcnZlciArIFwidGFza21hbmFnZXJzXCIpLnN1Y2Nlc3MoZnVuY3Rpb24oZGF0YSwgc3RhdHVzLCBoZWFkZXJzLCBjb25maWcpIHtcbiAgICAgIHJldHVybiBkZWZlcnJlZC5yZXNvbHZlKGRhdGFbJ3Rhc2ttYW5hZ2VycyddKTtcbiAgICB9KTtcbiAgICByZXR1cm4gZGVmZXJyZWQucHJvbWlzZTtcbiAgfTtcbiAgcmV0dXJuIHRoaXM7XG59KS5zZXJ2aWNlKCdTaW5nbGVUYXNrTWFuYWdlclNlcnZpY2UnLCBmdW5jdGlvbigkaHR0cCwgZmxpbmtDb25maWcsICRxKSB7XG4gIHRoaXMubG9hZE1ldHJpY3MgPSBmdW5jdGlvbih0YXNrbWFuYWdlcmlkKSB7XG4gICAgdmFyIGRlZmVycmVkO1xuICAgIGRlZmVycmVkID0gJHEuZGVmZXIoKTtcbiAgICAkaHR0cC5nZXQoZmxpbmtDb25maWcuam9iU2VydmVyICsgXCJ0YXNrbWFuYWdlcnMvXCIgKyB0YXNrbWFuYWdlcmlkKS5zdWNjZXNzKGZ1bmN0aW9uKGRhdGEsIHN0YXR1cywgaGVhZGVycywgY29uZmlnKSB7XG4gICAgICByZXR1cm4gZGVmZXJyZWQucmVzb2x2ZShkYXRhWyd0YXNrbWFuYWdlcnMnXSk7XG4gICAgfSk7XG4gICAgcmV0dXJuIGRlZmVycmVkLnByb21pc2U7XG4gIH07XG4gIHJldHVybiB0aGlzO1xufSk7XG4iXSwic291cmNlUm9vdCI6Ii9zb3VyY2UvIn0=
