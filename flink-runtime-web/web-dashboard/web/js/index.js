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
}]).run(["$rootScope", "$state", function($rootScope, $state) {
  return $rootScope.$on('$stateChangeStart', function(event, toState, toParams, fromState) {
    if (toState.redirectTo) {
      event.preventDefault();
      return $state.go(toState.redirectTo, toParams);
    }
  });
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
    redirectTo: "single-job.plan.subtasks",
    views: {
      details: {
        templateUrl: "partials/jobs/job.plan.html",
        controller: 'JobPlanController'
      }
    }
  }).state("single-job.plan.subtasks", {
    url: "",
    views: {
      'node-details': {
        templateUrl: "partials/jobs/job.plan.node-list.subtasks.html",
        controller: 'JobPlanSubtasksController'
      }
    }
  }).state("single-job.plan.taskmanagers", {
    url: "/taskmanagers",
    views: {
      'node-details': {
        templateUrl: "partials/jobs/job.plan.node-list.taskmanagers.html",
        controller: 'JobPlanTaskManagersController'
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
  $scope.stopJob = function(stopEvent) {
    angular.element(stopEvent.currentTarget).removeClass("btn").removeClass("btn-default").html('Stopping...');
    return JobsService.stopJob($stateParams.jobid).then(function(data) {
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
}]).controller('JobPlanSubtasksController', ["$scope", "JobsService", function($scope, JobsService) {
  console.log('JobPlanSubtasksController');
  if ($scope.nodeid && (!$scope.vertex || !$scope.vertex.st)) {
    JobsService.getSubtasks($scope.nodeid).then(function(data) {
      return $scope.subtasks = data;
    });
  }
  return $scope.$on('reload', function(event) {
    console.log('JobPlanSubtasksController');
    if ($scope.nodeid) {
      return JobsService.getSubtasks($scope.nodeid).then(function(data) {
        return $scope.subtasks = data;
      });
    }
  });
}]).controller('JobPlanTaskManagersController', ["$scope", "JobsService", function($scope, JobsService) {
  console.log('JobPlanTaskManagersController');
  if ($scope.nodeid && (!$scope.vertex || !$scope.vertex.st)) {
    JobsService.getTaskManagers($scope.nodeid).then(function(data) {
      return $scope.taskmanagers = data;
    });
  }
  return $scope.$on('reload', function(event) {
    console.log('JobPlanTaskManagersController');
    if ($scope.nodeid) {
      return JobsService.getTaskManagers($scope.nodeid).then(function(data) {
        return $scope.taskmanagers = data;
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
  this.getTaskManagers = function(vertexid) {
    var deferred;
    deferred = $q.defer();
    deferreds.job.promise.then((function(_this) {
      return function(data) {
        return $http.get(flinkConfig.jobServer + "jobs/" + currentJob.jid + "/vertices/" + vertexid + "/taskmanagers").success(function(data) {
          var taskmanagers;
          taskmanagers = data.taskmanagers;
          return deferred.resolve(taskmanagers);
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
  this.stopJob = function(jobid) {
    return $http.get("jobs/" + jobid + "/yarn-stop");
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
            return $state.go("single-job.plan.subtasks", {
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

//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJzb3VyY2VzIjpbImluZGV4LmNvZmZlZSIsImluZGV4LmpzIiwiY29tbW9uL2RpcmVjdGl2ZXMuY29mZmVlIiwiY29tbW9uL2RpcmVjdGl2ZXMuanMiLCJjb21tb24vZmlsdGVycy5jb2ZmZWUiLCJjb21tb24vZmlsdGVycy5qcyIsImNvbW1vbi9zZXJ2aWNlcy5jb2ZmZWUiLCJjb21tb24vc2VydmljZXMuanMiLCJtb2R1bGVzL2pvYm1hbmFnZXIvam9ibWFuYWdlci5jdHJsLmNvZmZlZSIsIm1vZHVsZXMvam9ibWFuYWdlci9qb2JtYW5hZ2VyLmN0cmwuanMiLCJtb2R1bGVzL2pvYm1hbmFnZXIvam9ibWFuYWdlci5zdmMuY29mZmVlIiwibW9kdWxlcy9qb2JtYW5hZ2VyL2pvYm1hbmFnZXIuc3ZjLmpzIiwibW9kdWxlcy9qb2JzL2pvYnMuY3RybC5jb2ZmZWUiLCJtb2R1bGVzL2pvYnMvam9icy5jdHJsLmpzIiwibW9kdWxlcy9qb2JzL2pvYnMuZGlyLmNvZmZlZSIsIm1vZHVsZXMvam9icy9qb2JzLmRpci5qcyIsIm1vZHVsZXMvam9icy9qb2JzLnN2Yy5jb2ZmZWUiLCJtb2R1bGVzL2pvYnMvam9icy5zdmMuanMiLCJtb2R1bGVzL292ZXJ2aWV3L292ZXJ2aWV3LmN0cmwuY29mZmVlIiwibW9kdWxlcy9vdmVydmlldy9vdmVydmlldy5jdHJsLmpzIiwibW9kdWxlcy9vdmVydmlldy9vdmVydmlldy5zdmMuY29mZmVlIiwibW9kdWxlcy9vdmVydmlldy9vdmVydmlldy5zdmMuanMiLCJtb2R1bGVzL3N1Ym1pdC9zdWJtaXQuY3RybC5jb2ZmZWUiLCJtb2R1bGVzL3N1Ym1pdC9zdWJtaXQuY3RybC5qcyIsIm1vZHVsZXMvc3VibWl0L3N1Ym1pdC5zdmMuY29mZmVlIiwibW9kdWxlcy9zdWJtaXQvc3VibWl0LnN2Yy5qcyIsIm1vZHVsZXMvdGFza21hbmFnZXIvdGFza21hbmFnZXIuY3RybC5jb2ZmZWUiLCJtb2R1bGVzL3Rhc2ttYW5hZ2VyL3Rhc2ttYW5hZ2VyLmN0cmwuanMiLCJtb2R1bGVzL3Rhc2ttYW5hZ2VyL3Rhc2ttYW5hZ2VyLnN2Yy5jb2ZmZWUiLCJtb2R1bGVzL3Rhc2ttYW5hZ2VyL3Rhc2ttYW5hZ2VyLnN2Yy5qcyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiQUFrQkEsUUFBUSxPQUFPLFlBQVksQ0FBQyxhQUFhLGtCQUl4QyxtQkFBSSxTQUFDLFlBQUQ7RUFDSCxXQUFXLGlCQUFpQjtFQ3JCNUIsT0RzQkEsV0FBVyxjQUFjLFdBQUE7SUFDdkIsV0FBVyxpQkFBaUIsQ0FBQyxXQUFXO0lDckJ4QyxPRHNCQSxXQUFXLGVBQWU7O0lBSTdCLE1BQU0sZUFBZTtFQUNwQixXQUFXO0VBRVgsb0JBQW9CO0dBS3JCLCtEQUFJLFNBQUMsYUFBYSxhQUFhLGFBQWEsV0FBeEM7RUM1QkgsT0Q2QkEsWUFBWSxhQUFhLEtBQUssU0FBQyxRQUFEO0lBQzVCLFFBQVEsT0FBTyxhQUFhO0lBRTVCLFlBQVk7SUM3QlosT0QrQkEsVUFBVSxXQUFBO01DOUJSLE9EK0JBLFlBQVk7T0FDWixZQUFZOztJQUtqQixpQ0FBTyxTQUFDLHVCQUFEO0VDakNOLE9Ea0NBLHNCQUFzQjtJQUl2Qiw2QkFBSSxTQUFDLFlBQVksUUFBYjtFQ3BDSCxPRHFDQSxXQUFXLElBQUkscUJBQXFCLFNBQUMsT0FBTyxTQUFTLFVBQVUsV0FBM0I7SUFDbEMsSUFBRyxRQUFRLFlBQVg7TUFDRSxNQUFNO01DcENOLE9EcUNBLE9BQU8sR0FBRyxRQUFRLFlBQVk7OztJQUluQyxnREFBTyxTQUFDLGdCQUFnQixvQkFBakI7RUFDTixlQUFlLE1BQU0sWUFDbkI7SUFBQSxLQUFLO0lBQ0wsT0FDRTtNQUFBLE1BQ0U7UUFBQSxhQUFhO1FBQ2IsWUFBWTs7O0tBRWpCLE1BQU0sZ0JBQ0w7SUFBQSxLQUFLO0lBQ0wsT0FDRTtNQUFBLE1BQ0U7UUFBQSxhQUFhO1FBQ2IsWUFBWTs7O0tBRWpCLE1BQU0sa0JBQ0w7SUFBQSxLQUFLO0lBQ0wsT0FDRTtNQUFBLE1BQ0U7UUFBQSxhQUFhO1FBQ2IsWUFBWTs7O0tBRWpCLE1BQU0sY0FDTDtJQUFBLEtBQUs7SUFDTCxVQUFVO0lBQ1YsT0FDRTtNQUFBLE1BQ0U7UUFBQSxhQUFhO1FBQ2IsWUFBWTs7O0tBRWpCLE1BQU0sbUJBQ0w7SUFBQSxLQUFLO0lBQ0wsWUFBWTtJQUNaLE9BQ0U7TUFBQSxTQUNFO1FBQUEsYUFBYTtRQUNiLFlBQVk7OztLQUVqQixNQUFNLDRCQUNMO0lBQUEsS0FBSztJQUNMLE9BQ0U7TUFBQSxnQkFDRTtRQUFBLGFBQWE7UUFDYixZQUFZOzs7S0FFakIsTUFBTSxnQ0FDTDtJQUFBLEtBQUs7SUFDTCxPQUNFO01BQUEsZ0JBQ0U7UUFBQSxhQUFhO1FBQ2IsWUFBWTs7O0tBRWpCLE1BQU0sZ0NBQ0w7SUFBQSxLQUFLO0lBQ0wsT0FDRTtNQUFBLGdCQUNFO1FBQUEsYUFBYTtRQUNiLFlBQVk7OztLQUVqQixNQUFNLCtCQUNMO0lBQUEsS0FBSztJQUNMLE9BQ0U7TUFBQSxnQkFDRTtRQUFBLGFBQWE7UUFDYixZQUFZOzs7S0FFakIsTUFBTSxnQ0FDTDtJQUFBLEtBQUs7SUFDTCxPQUNFO01BQUEsZ0JBQ0U7UUFBQSxhQUFhO1FBQ2IsWUFBWTs7O0tBRWpCLE1BQU0sdUJBQ0w7SUFBQSxLQUFLO0lBQ0wsT0FDRTtNQUFBLFNBQ0U7UUFBQSxhQUFhOzs7S0FFbEIsTUFBTSw4QkFDTDtJQUFBLEtBQUs7SUFDTCxPQUNFO01BQUEsUUFDRTtRQUFBLGFBQWE7UUFDYixZQUFZOzs7S0FFakIsTUFBTSx5QkFDTDtJQUFBLEtBQUs7SUFDTCxPQUNFO01BQUEsU0FDRTtRQUFBLGFBQWE7UUFDYixZQUFZOzs7S0FFakIsTUFBTSx5QkFDTDtJQUFBLEtBQUs7SUFDTCxPQUNFO01BQUEsU0FDRTtRQUFBLGFBQWE7UUFDYixZQUFZOzs7S0FFakIsTUFBTSxxQkFDTDtJQUFBLEtBQUs7SUFDTCxPQUNFO01BQUEsU0FDRTtRQUFBLGFBQWE7OztLQUVsQixNQUFNLGVBQ0w7SUFBQSxLQUFLO0lBQ0wsT0FDRTtNQUFBLE1BQ0U7UUFBQSxhQUFhO1FBQ2IsWUFBWTs7O0tBRWpCLE1BQU0sa0JBQ0g7SUFBQSxLQUFLO0lBQ0wsT0FDRTtNQUFBLE1BQ0U7UUFBQSxhQUFhO1FBQ2IsWUFBWTs7O0tBRW5CLE1BQU0sMEJBQ0w7SUFBQSxLQUFLO0lBQ0wsT0FDRTtNQUFBLFNBQ0U7UUFBQSxhQUFhOzs7S0FFbEIsTUFBTSxjQUNIO0lBQUEsS0FBSztJQUNMLE9BQ0U7TUFBQSxNQUNFO1FBQUEsYUFBYTs7O0tBRXBCLE1BQU0scUJBQ0w7SUFBQSxLQUFLO0lBQ0wsT0FDRTtNQUFBLFNBQ0U7UUFBQSxhQUFhO1FBQ2IsWUFBWTs7O0tBRWpCLE1BQU0scUJBQ0w7SUFBQSxLQUFLO0lBQ0wsT0FDRTtNQUFBLFNBQ0U7UUFBQSxhQUFhO1FBQ2IsWUFBWTs7O0tBRWpCLE1BQU0sa0JBQ0w7SUFBQSxLQUFLO0lBQ0wsT0FDRTtNQUFBLFNBQ0U7UUFBQSxhQUFhO1FBQ2IsWUFBWTs7O0tBRWpCLE1BQU0sVUFDSDtJQUFBLEtBQUs7SUFDTCxPQUNFO01BQUEsTUFDRTtRQUFBLGFBQWE7UUFDYixZQUFZOzs7O0VDWnBCLE9EY0EsbUJBQW1CLFVBQVU7O0FDWi9CO0FDbE1BLFFBQVEsT0FBTyxZQUlkLFVBQVUsMkJBQVcsU0FBQyxhQUFEO0VDckJwQixPRHNCQTtJQUFBLFlBQVk7SUFDWixTQUFTO0lBQ1QsT0FDRTtNQUFBLGVBQWU7TUFDZixRQUFROztJQUVWLFVBQVU7SUFFVixNQUFNLFNBQUMsT0FBTyxTQUFTLE9BQWpCO01DckJGLE9Ec0JGLE1BQU0sZ0JBQWdCLFdBQUE7UUNyQmxCLE9Ec0JGLGlCQUFpQixZQUFZLG9CQUFvQixNQUFNOzs7O0lBSTVELFVBQVUsMkJBQVcsU0FBQyxhQUFEO0VDckJwQixPRHNCQTtJQUFBLFlBQVk7SUFDWixTQUFTO0lBQ1QsT0FDRTtNQUFBLDJCQUEyQjtNQUMzQixRQUFROztJQUVWLFVBQVU7SUFFVixNQUFNLFNBQUMsT0FBTyxTQUFTLE9BQWpCO01DckJGLE9Ec0JGLE1BQU0sNEJBQTRCLFdBQUE7UUNyQjlCLE9Ec0JGLGlCQUFpQixZQUFZLGdDQUFnQyxNQUFNOzs7O0lBSXhFLFVBQVUsb0NBQW9CLFNBQUMsYUFBRDtFQ3JCN0IsT0RzQkE7SUFBQSxTQUFTO0lBQ1QsT0FDRTtNQUFBLGVBQWU7TUFDZixRQUFROztJQUVWLFVBQVU7SUFFVixNQUFNLFNBQUMsT0FBTyxTQUFTLE9BQWpCO01DckJGLE9Ec0JGLE1BQU0sZ0JBQWdCLFdBQUE7UUNyQmxCLE9Ec0JGLHNDQUFzQyxZQUFZLG9CQUFvQixNQUFNOzs7O0lBSWpGLFVBQVUsaUJBQWlCLFdBQUE7RUNyQjFCLE9Ec0JBO0lBQUEsU0FBUztJQUNULE9BQ0U7TUFBQSxPQUFPOztJQUVULFVBQVU7OztBQ2xCWjtBQ25DQSxRQUFRLE9BQU8sWUFFZCxPQUFPLG9EQUE0QixTQUFDLHFCQUFEO0VBQ2xDLElBQUE7RUFBQSxpQ0FBaUMsU0FBQyxPQUFPLFFBQVEsZ0JBQWhCO0lBQy9CLElBQWMsT0FBTyxVQUFTLGVBQWUsVUFBUyxNQUF0RDtNQUFBLE9BQU87O0lDaEJQLE9Ea0JBLE9BQU8sU0FBUyxPQUFPLFFBQVEsT0FBTyxnQkFBZ0I7TUFBRSxNQUFNOzs7RUFFaEUsK0JBQStCLFlBQVksb0JBQW9CO0VDZi9ELE9EaUJBO0lBRUQsT0FBTyxvQkFBb0IsV0FBQTtFQ2pCMUIsT0RrQkEsU0FBQyxPQUFPLE9BQVI7SUFDRSxJQUFBLE1BQUEsT0FBQSxTQUFBLElBQUEsU0FBQTtJQUFBLElBQWEsT0FBTyxVQUFTLGVBQWUsVUFBUyxNQUFyRDtNQUFBLE9BQU87O0lBQ1AsS0FBSyxRQUFRO0lBQ2IsSUFBSSxLQUFLLE1BQU0sUUFBUTtJQUN2QixVQUFVLElBQUk7SUFDZCxJQUFJLEtBQUssTUFBTSxJQUFJO0lBQ25CLFVBQVUsSUFBSTtJQUNkLElBQUksS0FBSyxNQUFNLElBQUk7SUFDbkIsUUFBUSxJQUFJO0lBQ1osSUFBSSxLQUFLLE1BQU0sSUFBSTtJQUNuQixPQUFPO0lBQ1AsSUFBRyxTQUFRLEdBQVg7TUFDRSxJQUFHLFVBQVMsR0FBWjtRQUNFLElBQUcsWUFBVyxHQUFkO1VBQ0UsSUFBRyxZQUFXLEdBQWQ7WUFDRSxPQUFPLEtBQUs7aUJBRGQ7WUFHRSxPQUFPLFVBQVU7O2VBSnJCO1VBTUUsT0FBTyxVQUFVLE9BQU8sVUFBVTs7YUFQdEM7UUFTRSxJQUFHLE9BQUg7VUFBYyxPQUFPLFFBQVEsT0FBTyxVQUFVO2VBQTlDO1VBQXVELE9BQU8sUUFBUSxPQUFPLFVBQVUsT0FBTyxVQUFVOzs7V0FWNUc7TUFZRSxJQUFHLE9BQUg7UUFBYyxPQUFPLE9BQU8sT0FBTyxRQUFRO2FBQTNDO1FBQW9ELE9BQU8sT0FBTyxPQUFPLFFBQVEsT0FBTyxVQUFVLE9BQU8sVUFBVTs7OztHQUV4SCxPQUFPLGdCQUFnQixXQUFBO0VDRnRCLE9ER0EsU0FBQyxNQUFEO0lBRUUsSUFBRyxNQUFIO01DSEUsT0RHVyxLQUFLLFFBQVEsU0FBUyxLQUFLLFFBQVEsV0FBVTtXQUExRDtNQ0RFLE9EQ2lFOzs7R0FFdEUsT0FBTyxpQkFBaUIsV0FBQTtFQ0N2QixPREFBLFNBQUMsT0FBRDtJQUNFLElBQUEsV0FBQTtJQUFBLFFBQVEsQ0FBQyxLQUFLLE1BQU0sTUFBTSxNQUFNLE1BQU0sTUFBTTtJQUM1QyxZQUFZLFNBQUMsT0FBTyxPQUFSO01BQ1YsSUFBQTtNQUFBLE9BQU8sS0FBSyxJQUFJLE1BQU07TUFDdEIsSUFBRyxRQUFRLE1BQVg7UUFDRSxPQUFPLENBQUMsUUFBUSxNQUFNLFFBQVEsS0FBSyxNQUFNLE1BQU07YUFDNUMsSUFBRyxRQUFRLE9BQU8sTUFBbEI7UUFDSCxPQUFPLENBQUMsUUFBUSxNQUFNLFlBQVksS0FBSyxNQUFNLE1BQU07YUFEaEQ7UUFHSCxPQUFPLFVBQVUsT0FBTyxRQUFROzs7SUFDcEMsSUFBYSxPQUFPLFVBQVMsZUFBZSxVQUFTLE1BQXJEO01BQUEsT0FBTzs7SUFDUCxJQUFHLFFBQVEsTUFBWDtNQ09FLE9EUG1CLFFBQVE7V0FBN0I7TUNTRSxPRFRxQyxVQUFVLE9BQU87OztHQUUzRCxPQUFPLGVBQWUsV0FBQTtFQ1dyQixPRFZBLFNBQUMsTUFBRDtJQ1dFLE9EWFEsS0FBSzs7O0FDY2pCO0FDeEVBLFFBQVEsT0FBTyxZQUVkLFFBQVEsOENBQWUsU0FBQyxPQUFPLGFBQWEsSUFBckI7RUFDdEIsS0FBQyxhQUFhLFdBQUE7SUFDWixJQUFBO0lBQUEsV0FBVyxHQUFHO0lBRWQsTUFBTSxJQUFJLFlBQVksWUFBWSxVQUNqQyxRQUFRLFNBQUMsTUFBTSxRQUFRLFNBQVMsUUFBeEI7TUNwQlAsT0RxQkEsU0FBUyxRQUFROztJQ25CbkIsT0RxQkEsU0FBUzs7RUNuQlgsT0RzQkE7O0FDcEJGO0FDT0EsUUFBUSxPQUFPLFlBRWQsV0FBVyxvRUFBOEIsU0FBQyxRQUFRLHlCQUFUO0VDbkJ4QyxPRG9CQSx3QkFBd0IsYUFBYSxLQUFLLFNBQUMsTUFBRDtJQUN4QyxJQUFJLE9BQUEsY0FBQSxNQUFKO01BQ0UsT0FBTyxhQUFhOztJQ2xCdEIsT0RtQkEsT0FBTyxXQUFXLFlBQVk7O0lBRWpDLFdBQVcsZ0VBQTRCLFNBQUMsUUFBUSx1QkFBVDtFQUN0QyxzQkFBc0IsV0FBVyxLQUFLLFNBQUMsTUFBRDtJQUNwQyxJQUFJLE9BQUEsY0FBQSxNQUFKO01BQ0UsT0FBTyxhQUFhOztJQ2pCdEIsT0RrQkEsT0FBTyxXQUFXLFNBQVM7O0VDaEI3QixPRGtCQSxPQUFPLGFBQWEsV0FBQTtJQ2pCbEIsT0RrQkEsc0JBQXNCLFdBQVcsS0FBSyxTQUFDLE1BQUQ7TUNqQnBDLE9Ea0JBLE9BQU8sV0FBVyxTQUFTOzs7SUFFaEMsV0FBVyxvRUFBOEIsU0FBQyxRQUFRLHlCQUFUO0VBQ3hDLHdCQUF3QixhQUFhLEtBQUssU0FBQyxNQUFEO0lBQ3hDLElBQUksT0FBQSxjQUFBLE1BQUo7TUFDRSxPQUFPLGFBQWE7O0lDZnRCLE9EZ0JBLE9BQU8sV0FBVyxZQUFZOztFQ2RoQyxPRGdCQSxPQUFPLGFBQWEsV0FBQTtJQ2ZsQixPRGdCQSx3QkFBd0IsYUFBYSxLQUFLLFNBQUMsTUFBRDtNQ2Z4QyxPRGdCQSxPQUFPLFdBQVcsWUFBWTs7OztBQ1pwQztBQ2RBLFFBQVEsT0FBTyxZQUVkLFFBQVEsMERBQTJCLFNBQUMsT0FBTyxhQUFhLElBQXJCO0VBQ2xDLElBQUE7RUFBQSxTQUFTO0VBRVQsS0FBQyxhQUFhLFdBQUE7SUFDWixJQUFBO0lBQUEsV0FBVyxHQUFHO0lBRWQsTUFBTSxJQUFJLFlBQVksWUFBWSxxQkFDakMsUUFBUSxTQUFDLE1BQU0sUUFBUSxTQUFTLFFBQXhCO01BQ1AsU0FBUztNQ3BCVCxPRHFCQSxTQUFTLFFBQVE7O0lDbkJuQixPRHFCQSxTQUFTOztFQ25CWCxPRHFCQTtJQUVELFFBQVEsd0RBQXlCLFNBQUMsT0FBTyxhQUFhLElBQXJCO0VBQ2hDLElBQUE7RUFBQSxPQUFPO0VBRVAsS0FBQyxXQUFXLFdBQUE7SUFDVixJQUFBO0lBQUEsV0FBVyxHQUFHO0lBRWQsTUFBTSxJQUFJLFlBQVksWUFBWSxrQkFDakMsUUFBUSxTQUFDLE1BQU0sUUFBUSxTQUFTLFFBQXhCO01BQ1AsT0FBTztNQ3RCUCxPRHVCQSxTQUFTLFFBQVE7O0lDckJuQixPRHVCQSxTQUFTOztFQ3JCWCxPRHVCQTtJQUVELFFBQVEsMERBQTJCLFNBQUMsT0FBTyxhQUFhLElBQXJCO0VBQ2xDLElBQUE7RUFBQSxTQUFTO0VBRVQsS0FBQyxhQUFhLFdBQUE7SUFDWixJQUFBO0lBQUEsV0FBVyxHQUFHO0lBRWQsTUFBTSxJQUFJLFlBQVksWUFBWSxxQkFDakMsUUFBUSxTQUFDLE1BQU0sUUFBUSxTQUFTLFFBQXhCO01BQ1AsU0FBUztNQ3hCVCxPRHlCQSxTQUFTLFFBQVE7O0lDdkJuQixPRHlCQSxTQUFTOztFQ3ZCWCxPRHlCQTs7QUN2QkY7QUN0QkEsUUFBUSxPQUFPLFlBRWQsV0FBVyw2RUFBeUIsU0FBQyxRQUFRLFFBQVEsY0FBYyxhQUEvQjtFQUNuQyxPQUFPLGNBQWMsV0FBQTtJQ25CbkIsT0RvQkEsT0FBTyxPQUFPLFlBQVksUUFBUTs7RUFFcEMsWUFBWSxpQkFBaUIsT0FBTztFQUNwQyxPQUFPLElBQUksWUFBWSxXQUFBO0lDbkJyQixPRG9CQSxZQUFZLG1CQUFtQixPQUFPOztFQ2xCeEMsT0RvQkEsT0FBTztJQUlSLFdBQVcsK0VBQTJCLFNBQUMsUUFBUSxRQUFRLGNBQWMsYUFBL0I7RUFDckMsT0FBTyxjQUFjLFdBQUE7SUN0Qm5CLE9EdUJBLE9BQU8sT0FBTyxZQUFZLFFBQVE7O0VBRXBDLFlBQVksaUJBQWlCLE9BQU87RUFDcEMsT0FBTyxJQUFJLFlBQVksV0FBQTtJQ3RCckIsT0R1QkEsWUFBWSxtQkFBbUIsT0FBTzs7RUNyQnhDLE9EdUJBLE9BQU87SUFJUixXQUFXLHFIQUF1QixTQUFDLFFBQVEsUUFBUSxjQUFjLGFBQWEsWUFBWSxhQUFhLFdBQXJFO0VBQ2pDLElBQUE7RUFBQSxRQUFRLElBQUk7RUFFWixPQUFPLFFBQVEsYUFBYTtFQUM1QixPQUFPLE1BQU07RUFDYixPQUFPLE9BQU87RUFDZCxPQUFPLFdBQVc7RUFDbEIsT0FBTyxxQkFBcUI7RUFDNUIsT0FBTyxjQUFjO0VBQ3JCLE9BQU8sNEJBQTRCO0VBRW5DLFlBQVksUUFBUSxhQUFhLE9BQU8sS0FBSyxTQUFDLE1BQUQ7SUFDM0MsT0FBTyxNQUFNO0lBQ2IsT0FBTyxPQUFPLEtBQUs7SUMxQm5CLE9EMkJBLE9BQU8sV0FBVyxLQUFLOztFQUV6QixZQUFZLFVBQVUsV0FBQTtJQzFCcEIsT0QyQkEsWUFBWSxRQUFRLGFBQWEsT0FBTyxLQUFLLFNBQUMsTUFBRDtNQUMzQyxPQUFPLE1BQU07TUMxQmIsT0Q0QkEsT0FBTyxXQUFXOztLQUVwQixZQUFZO0VBRWQsT0FBTyxJQUFJLFlBQVksV0FBQTtJQUNyQixPQUFPLE1BQU07SUFDYixPQUFPLE9BQU87SUFDZCxPQUFPLFdBQVc7SUFDbEIsT0FBTyxxQkFBcUI7SUFDNUIsT0FBTyw0QkFBNEI7SUM1Qm5DLE9EOEJBLFVBQVUsT0FBTzs7RUFFbkIsT0FBTyxZQUFZLFNBQUMsYUFBRDtJQUNqQixRQUFRLFFBQVEsWUFBWSxlQUFlLFlBQVksT0FBTyxZQUFZLGVBQWUsS0FBSztJQzdCOUYsT0Q4QkEsWUFBWSxVQUFVLGFBQWEsT0FBTyxLQUFLLFNBQUMsTUFBRDtNQzdCN0MsT0Q4QkE7OztFQUVKLE9BQU8sVUFBVSxTQUFDLFdBQUQ7SUFDZixRQUFRLFFBQVEsVUFBVSxlQUFlLFlBQVksT0FBTyxZQUFZLGVBQWUsS0FBSztJQzVCNUYsT0Q2QkEsWUFBWSxRQUFRLGFBQWEsT0FBTyxLQUFLLFNBQUMsTUFBRDtNQzVCM0MsT0Q2QkE7OztFQzFCSixPRDRCQSxPQUFPLGdCQUFnQixXQUFBO0lDM0JyQixPRDRCQSxPQUFPLGNBQWMsQ0FBQyxPQUFPOztJQUloQyxXQUFXLHlFQUFxQixTQUFDLFFBQVEsUUFBUSxjQUFjLGFBQS9CO0VBQy9CLFFBQVEsSUFBSTtFQUVaLE9BQU8sU0FBUztFQUNoQixPQUFPLGVBQWU7RUFDdEIsT0FBTyxZQUFZLFlBQVk7RUFFL0IsT0FBTyxhQUFhLFNBQUMsUUFBRDtJQUNsQixJQUFHLFdBQVUsT0FBTyxRQUFwQjtNQUNFLE9BQU8sU0FBUztNQUNoQixPQUFPLFNBQVM7TUFDaEIsT0FBTyxXQUFXO01BQ2xCLE9BQU8sZUFBZTtNQUN0QixPQUFPLDBCQUEwQjtNQy9CakMsT0RpQ0EsT0FBTyxXQUFXO1dBUHBCO01BVUUsT0FBTyxTQUFTO01BQ2hCLE9BQU8sZUFBZTtNQUN0QixPQUFPLFNBQVM7TUFDaEIsT0FBTyxXQUFXO01BQ2xCLE9BQU8sZUFBZTtNQ2pDdEIsT0RrQ0EsT0FBTywwQkFBMEI7OztFQUVyQyxPQUFPLGlCQUFpQixXQUFBO0lBQ3RCLE9BQU8sU0FBUztJQUNoQixPQUFPLGVBQWU7SUFDdEIsT0FBTyxTQUFTO0lBQ2hCLE9BQU8sV0FBVztJQUNsQixPQUFPLGVBQWU7SUNoQ3RCLE9EaUNBLE9BQU8sMEJBQTBCOztFQy9CbkMsT0RpQ0EsT0FBTyxhQUFhLFdBQUE7SUNoQ2xCLE9EaUNBLE9BQU8sZUFBZSxDQUFDLE9BQU87O0lBSWpDLFdBQVcsdURBQTZCLFNBQUMsUUFBUSxhQUFUO0VBQ3ZDLFFBQVEsSUFBSTtFQUVaLElBQUcsT0FBTyxXQUFZLENBQUMsT0FBTyxVQUFVLENBQUMsT0FBTyxPQUFPLEtBQXZEO0lBQ0UsWUFBWSxZQUFZLE9BQU8sUUFBUSxLQUFLLFNBQUMsTUFBRDtNQ25DMUMsT0RvQ0EsT0FBTyxXQUFXOzs7RUNqQ3RCLE9EbUNBLE9BQU8sSUFBSSxVQUFVLFNBQUMsT0FBRDtJQUNuQixRQUFRLElBQUk7SUFDWixJQUFHLE9BQU8sUUFBVjtNQ2xDRSxPRG1DQSxZQUFZLFlBQVksT0FBTyxRQUFRLEtBQUssU0FBQyxNQUFEO1FDbEMxQyxPRG1DQSxPQUFPLFdBQVc7Ozs7SUFJekIsV0FBVywyREFBaUMsU0FBQyxRQUFRLGFBQVQ7RUFDM0MsUUFBUSxJQUFJO0VBRVosSUFBRyxPQUFPLFdBQVksQ0FBQyxPQUFPLFVBQVUsQ0FBQyxPQUFPLE9BQU8sS0FBdkQ7SUFDRSxZQUFZLGdCQUFnQixPQUFPLFFBQVEsS0FBSyxTQUFDLE1BQUQ7TUNuQzlDLE9Eb0NBLE9BQU8sZUFBZTs7O0VDakMxQixPRG1DQSxPQUFPLElBQUksVUFBVSxTQUFDLE9BQUQ7SUFDbkIsUUFBUSxJQUFJO0lBQ1osSUFBRyxPQUFPLFFBQVY7TUNsQ0UsT0RtQ0EsWUFBWSxnQkFBZ0IsT0FBTyxRQUFRLEtBQUssU0FBQyxNQUFEO1FDbEM5QyxPRG1DQSxPQUFPLGVBQWU7Ozs7SUFJN0IsV0FBVywyREFBaUMsU0FBQyxRQUFRLGFBQVQ7RUFDM0MsUUFBUSxJQUFJO0VBRVosSUFBRyxPQUFPLFdBQVksQ0FBQyxPQUFPLFVBQVUsQ0FBQyxPQUFPLE9BQU8sZUFBdkQ7SUFDRSxZQUFZLGdCQUFnQixPQUFPLFFBQVEsS0FBSyxTQUFDLE1BQUQ7TUFDOUMsT0FBTyxlQUFlLEtBQUs7TUNuQzNCLE9Eb0NBLE9BQU8sc0JBQXNCLEtBQUs7OztFQ2pDdEMsT0RtQ0EsT0FBTyxJQUFJLFVBQVUsU0FBQyxPQUFEO0lBQ25CLFFBQVEsSUFBSTtJQUNaLElBQUcsT0FBTyxRQUFWO01DbENFLE9EbUNBLFlBQVksZ0JBQWdCLE9BQU8sUUFBUSxLQUFLLFNBQUMsTUFBRDtRQUM5QyxPQUFPLGVBQWUsS0FBSztRQ2xDM0IsT0RtQ0EsT0FBTyxzQkFBc0IsS0FBSzs7OztJQUl6QyxXQUFXLDBEQUFnQyxTQUFDLFFBQVEsYUFBVDtFQUMxQyxRQUFRLElBQUk7RUFHWixZQUFZLHNCQUFzQixPQUFPLE9BQU8sS0FBSyxTQUFDLE1BQUQ7SUNwQ25ELE9EcUNBLE9BQU8scUJBQXFCOztFQUc5QixJQUFHLE9BQU8sV0FBWSxDQUFDLE9BQU8sVUFBVSxDQUFDLE9BQU8sT0FBTywwQkFBdkQ7SUFDRSxZQUFZLDJCQUEyQixPQUFPLFFBQVEsS0FBSyxTQUFDLE1BQUQ7TUFDekQsT0FBTywwQkFBMEIsS0FBSztNQ3JDdEMsT0RzQ0EsT0FBTywwQkFBMEIsS0FBSzs7O0VDbkMxQyxPRHFDQSxPQUFPLElBQUksVUFBVSxTQUFDLE9BQUQ7SUFDbkIsUUFBUSxJQUFJO0lBRVosWUFBWSxzQkFBc0IsT0FBTyxPQUFPLEtBQUssU0FBQyxNQUFEO01DckNuRCxPRHNDQSxPQUFPLHFCQUFxQjs7SUFFOUIsSUFBRyxPQUFPLFFBQVY7TUNyQ0UsT0RzQ0EsWUFBWSwyQkFBMkIsT0FBTyxRQUFRLEtBQUssU0FBQyxNQUFEO1FBQ3pELE9BQU8sMEJBQTBCLEtBQUs7UUNyQ3RDLE9Ec0NBLE9BQU8sMEJBQTBCLEtBQUs7Ozs7SUFJN0MsV0FBVywyREFBaUMsU0FBQyxRQUFRLGFBQVQ7RUFDM0MsUUFBUSxJQUFJO0VBQ1osT0FBTyxNQUFNLEtBQUs7RUFFbEIsSUFBRyxPQUFPLFFBQVY7SUFDRSxZQUFZLHdCQUF3QixPQUFPLFFBQVEsS0FBSyxTQUFDLE1BQUQ7TUN0Q3RELE9EdUNBLE9BQU8sMEJBQTBCLE9BQU8sVUFBVTs7O0VDcEN0RCxPRHNDQSxPQUFPLElBQUksVUFBVSxTQUFDLE9BQUQ7SUFDbkIsUUFBUSxJQUFJO0lBQ1osT0FBTyxNQUFNLEtBQUs7SUFFbEIsSUFBRyxPQUFPLFFBQVY7TUN0Q0UsT0R1Q0EsWUFBWSx3QkFBd0IsT0FBTyxRQUFRLEtBQUssU0FBQyxNQUFEO1FDdEN0RCxPRHVDQSxPQUFPLDBCQUEwQixPQUFPLFVBQVU7Ozs7SUFJekQsV0FBVyxtRkFBK0IsU0FBQyxRQUFRLFFBQVEsY0FBYyxhQUEvQjtFQUN6QyxRQUFRLElBQUk7RUFFWixZQUFZLFVBQVUsYUFBYSxVQUFVLEtBQUssU0FBQyxNQUFEO0lDdkNoRCxPRHdDQSxPQUFPLFNBQVM7O0VDdENsQixPRHdDQSxPQUFPLElBQUksVUFBVSxTQUFDLE9BQUQ7SUFDbkIsUUFBUSxJQUFJO0lDdkNaLE9Ed0NBLFlBQVksVUFBVSxhQUFhLFVBQVUsS0FBSyxTQUFDLE1BQUQ7TUN2Q2hELE9Ed0NBLE9BQU8sU0FBUzs7O0lBSXJCLFdBQVcsK0VBQTJCLFNBQUMsUUFBUSxRQUFRLGNBQWMsYUFBL0I7RUN4Q3JDLE9EeUNBLFlBQVksaUJBQWlCLEtBQUssU0FBQyxNQUFEO0lDeENoQyxPRHlDQSxPQUFPLGFBQWE7O0lBSXZCLFdBQVcscURBQTJCLFNBQUMsUUFBUSxhQUFUO0VBQ3JDLFFBQVEsSUFBSTtFQzFDWixPRDRDQSxPQUFPLGFBQWEsU0FBQyxRQUFEO0lBQ2xCLElBQUcsV0FBVSxPQUFPLFFBQXBCO01BQ0UsT0FBTyxTQUFTO01DM0NoQixPRDZDQSxZQUFZLFFBQVEsUUFBUSxLQUFLLFNBQUMsTUFBRDtRQzVDL0IsT0Q2Q0EsT0FBTyxPQUFPOztXQUpsQjtNQU9FLE9BQU8sU0FBUztNQzVDaEIsT0Q2Q0EsT0FBTyxPQUFPOzs7O0FDekNwQjtBQ2pNQSxRQUFRLE9BQU8sWUFJZCxVQUFVLHFCQUFVLFNBQUMsUUFBRDtFQ3JCbkIsT0RzQkE7SUFBQSxVQUFVO0lBRVYsT0FDRTtNQUFBLE1BQU07O0lBRVIsTUFBTSxTQUFDLE9BQU8sTUFBTSxPQUFkO01BQ0osSUFBQSxhQUFBLFlBQUE7TUFBQSxRQUFRLEtBQUssV0FBVztNQUV4QixhQUFhLEtBQUs7TUFDbEIsUUFBUSxRQUFRLE9BQU8sS0FBSyxTQUFTO01BRXJDLGNBQWMsU0FBQyxNQUFEO1FBQ1osSUFBQSxPQUFBLEtBQUE7UUFBQSxHQUFHLE9BQU8sT0FBTyxVQUFVLEtBQUs7UUFFaEMsV0FBVztRQUVYLFFBQVEsUUFBUSxLQUFLLFVBQVUsU0FBQyxTQUFTLEdBQVY7VUFDN0IsSUFBQTtVQUFBLFFBQVE7WUFDTjtjQUNFLE9BQU87Y0FDUCxPQUFPO2NBQ1AsYUFBYTtjQUNiLGVBQWUsUUFBUSxXQUFXO2NBQ2xDLGFBQWEsUUFBUSxXQUFXO2NBQ2hDLE1BQU07ZUFFUjtjQUNFLE9BQU87Y0FDUCxPQUFPO2NBQ1AsYUFBYTtjQUNiLGVBQWUsUUFBUSxXQUFXO2NBQ2xDLGFBQWEsUUFBUSxXQUFXO2NBQ2hDLE1BQU07OztVQUlWLElBQUcsUUFBUSxXQUFXLGNBQWMsR0FBcEM7WUFDRSxNQUFNLEtBQUs7Y0FDVCxPQUFPO2NBQ1AsT0FBTztjQUNQLGFBQWE7Y0FDYixlQUFlLFFBQVEsV0FBVztjQUNsQyxhQUFhLFFBQVEsV0FBVztjQUNoQyxNQUFNOzs7VUN0QlIsT0R5QkYsU0FBUyxLQUFLO1lBQ1osT0FBTyxNQUFJLFFBQVEsVUFBUSxPQUFJLFFBQVE7WUFDdkMsT0FBTzs7O1FBR1gsUUFBUSxHQUFHLFdBQVcsUUFDckIsV0FBVztVQUNWLFFBQVEsR0FBRyxLQUFLLE9BQU87VUFFdkIsVUFBVTtXQUVYLE9BQU8sVUFDUCxZQUFZLFNBQUMsT0FBRDtVQzVCVCxPRDZCRjtXQUVELE9BQU87VUFBRSxNQUFNO1VBQUssT0FBTztVQUFHLEtBQUs7VUFBRyxRQUFRO1dBQzlDLFdBQVcsSUFDWDtRQzFCQyxPRDRCRixNQUFNLEdBQUcsT0FBTyxPQUNmLE1BQU0sVUFDTixLQUFLOztNQUVSLFlBQVksTUFBTTs7O0lBTXJCLFVBQVUsdUJBQVksU0FBQyxRQUFEO0VDaENyQixPRGlDQTtJQUFBLFVBQVU7SUFFVixPQUNFO01BQUEsVUFBVTtNQUNWLE9BQU87O0lBRVQsTUFBTSxTQUFDLE9BQU8sTUFBTSxPQUFkO01BQ0osSUFBQSxhQUFBLFlBQUEsT0FBQTtNQUFBLFFBQVEsS0FBSyxXQUFXO01BRXhCLGFBQWEsS0FBSztNQUNsQixRQUFRLFFBQVEsT0FBTyxLQUFLLFNBQVM7TUFFckMsaUJBQWlCLFNBQUMsT0FBRDtRQ2pDYixPRGtDRixNQUFNLFFBQVEsUUFBUTs7TUFFeEIsY0FBYyxTQUFDLE1BQUQ7UUFDWixJQUFBLE9BQUEsS0FBQTtRQUFBLEdBQUcsT0FBTyxPQUFPLFVBQVUsS0FBSztRQUVoQyxXQUFXO1FBRVgsUUFBUSxRQUFRLE1BQU0sU0FBQyxRQUFEO1VBQ3BCLElBQUcsT0FBTyxnQkFBZ0IsQ0FBQyxHQUEzQjtZQUNFLElBQUcsT0FBTyxTQUFRLGFBQWxCO2NDbENJLE9EbUNGLFNBQVMsS0FDUDtnQkFBQSxPQUFPO2tCQUNMO29CQUFBLE9BQU8sZUFBZSxPQUFPO29CQUM3QixPQUFPO29CQUNQLGFBQWE7b0JBQ2IsZUFBZSxPQUFPO29CQUN0QixhQUFhLE9BQU87b0JBQ3BCLE1BQU0sT0FBTzs7OzttQkFSbkI7Y0NyQkksT0RnQ0YsU0FBUyxLQUNQO2dCQUFBLE9BQU87a0JBQ0w7b0JBQUEsT0FBTyxlQUFlLE9BQU87b0JBQzdCLE9BQU87b0JBQ1AsYUFBYTtvQkFDYixlQUFlLE9BQU87b0JBQ3RCLGFBQWEsT0FBTztvQkFDcEIsTUFBTSxPQUFPO29CQUNiLE1BQU0sT0FBTzs7Ozs7OztRQUd2QixRQUFRLEdBQUcsV0FBVyxRQUFRLE1BQU0sU0FBQyxHQUFHLEdBQUcsT0FBUDtVQUNsQyxJQUFHLEVBQUUsTUFBTDtZQzFCSSxPRDJCRixPQUFPLEdBQUcsOEJBQThCO2NBQUUsT0FBTyxNQUFNO2NBQU8sVUFBVSxFQUFFOzs7V0FHN0UsV0FBVztVQUNWLFFBQVEsR0FBRyxLQUFLLE9BQU87VUFHdkIsVUFBVTtXQUVYLE9BQU8sUUFDUCxPQUFPO1VBQUUsTUFBTTtVQUFHLE9BQU87VUFBRyxLQUFLO1VBQUcsUUFBUTtXQUM1QyxXQUFXLElBQ1gsaUJBQ0E7UUMxQkMsT0Q0QkYsTUFBTSxHQUFHLE9BQU8sT0FDZixNQUFNLFVBQ04sS0FBSzs7TUFFUixNQUFNLE9BQU8sTUFBTSxVQUFVLFNBQUMsTUFBRDtRQUMzQixJQUFxQixNQUFyQjtVQzdCSSxPRDZCSixZQUFZOzs7OztJQU1qQixVQUFVLHdCQUFXLFNBQUMsVUFBRDtFQzdCcEIsT0Q4QkE7SUFBQSxVQUFVO0lBUVYsT0FDRTtNQUFBLE1BQU07TUFDTixTQUFTOztJQUVYLE1BQU0sU0FBQyxPQUFPLE1BQU0sT0FBZDtNQUNKLElBQUEsWUFBQSxZQUFBLGlCQUFBLGlCQUFBLFlBQUEsV0FBQSxZQUFBLFVBQUEsV0FBQSw2QkFBQSxHQUFBLGFBQUEsd0JBQUEsT0FBQSxpQkFBQSxPQUFBLGdCQUFBLGdCQUFBLFVBQUEsZUFBQSxlQUFBO01BQUEsSUFBSTtNQUNKLFdBQVcsR0FBRyxTQUFTO01BQ3ZCLFlBQVk7TUFDWixRQUFRLE1BQU07TUFFZCxpQkFBaUIsS0FBSyxXQUFXO01BQ2pDLFFBQVEsS0FBSyxXQUFXLFdBQVc7TUFDbkMsaUJBQWlCLEtBQUssV0FBVztNQUVqQyxZQUFZLEdBQUcsT0FBTztNQUN0QixhQUFhLEdBQUcsT0FBTztNQUN2QixXQUFXLEdBQUcsT0FBTztNQUtyQixhQUFhLEtBQUs7TUFDbEIsUUFBUSxRQUFRLEtBQUssV0FBVyxJQUFJLE1BQU07TUFFMUMsTUFBTSxTQUFTLFdBQUE7UUFDYixJQUFBLFdBQUEsSUFBQTtRQUFBLElBQUcsU0FBUyxVQUFVLE1BQXRCO1VBR0UsWUFBWSxTQUFTO1VBQ3JCLEtBQUssVUFBVSxNQUFNLFNBQVMsVUFBVSxPQUFPLFNBQVM7VUFDeEQsS0FBSyxVQUFVLE1BQU0sU0FBUyxVQUFVLE9BQU8sU0FBUztVQUN4RCxTQUFTLE1BQU0sU0FBUyxVQUFVO1VBQ2xDLFNBQVMsVUFBVSxDQUFFLElBQUk7VUMxQ3ZCLE9ENkNGLFdBQVcsS0FBSyxhQUFhLGVBQWUsS0FBSyxNQUFNLEtBQUssYUFBYSxTQUFTLFVBQVU7OztNQUVoRyxNQUFNLFVBQVUsV0FBQTtRQUNkLElBQUEsV0FBQSxJQUFBO1FBQUEsSUFBRyxTQUFTLFVBQVUsTUFBdEI7VUFHRSxTQUFTLE1BQU0sU0FBUyxVQUFVO1VBQ2xDLFlBQVksU0FBUztVQUNyQixLQUFLLFVBQVUsTUFBTSxTQUFTLFVBQVUsT0FBTyxTQUFTO1VBQ3hELEtBQUssVUFBVSxNQUFNLFNBQVMsVUFBVSxPQUFPLFNBQVM7VUFDeEQsU0FBUyxVQUFVLENBQUUsSUFBSTtVQzVDdkIsT0QrQ0YsV0FBVyxLQUFLLGFBQWEsZUFBZSxLQUFLLE1BQU0sS0FBSyxhQUFhLFNBQVMsVUFBVTs7O01BR2hHLGtCQUFrQixTQUFDLElBQUQ7UUFDaEIsSUFBQTtRQUFBLGFBQWE7UUFDYixJQUFHLENBQUEsR0FBQSxpQkFBQSxVQUFxQixHQUFBLGtCQUFBLE9BQXhCO1VBQ0UsY0FBYztVQUNkLElBQW1DLEdBQUEsaUJBQUEsTUFBbkM7WUFBQSxjQUFjLEdBQUc7O1VBQ2pCLElBQWdELEdBQUcsY0FBYSxXQUFoRTtZQUFBLGNBQWMsT0FBTyxHQUFHLFlBQVk7O1VBQ3BDLElBQWtELEdBQUcsbUJBQWtCLFdBQXZFO1lBQUEsY0FBYyxVQUFVLEdBQUc7O1VBQzNCLGNBQWM7O1FDdENkLE9EdUNGOztNQUlGLHlCQUF5QixTQUFDLE1BQUQ7UUN4Q3JCLE9EeUNELFNBQVEscUJBQXFCLFNBQVEseUJBQXlCLFNBQVEsYUFBYSxTQUFRLGlCQUFpQixTQUFRLGlCQUFpQixTQUFROztNQUVoSixjQUFjLFNBQUMsSUFBSSxNQUFMO1FBQ1osSUFBRyxTQUFRLFVBQVg7VUN4Q0ksT0R5Q0Y7ZUFFRyxJQUFHLHVCQUF1QixPQUExQjtVQ3pDRCxPRDBDRjtlQURHO1VDdkNELE9EMkNBOzs7TUFHTixrQkFBa0IsU0FBQyxJQUFJLE1BQU0sTUFBTSxNQUFqQjtRQUVoQixJQUFBLFlBQUE7UUFBQSxhQUFhLHVCQUF1QixRQUFRLGFBQWEsR0FBRyxLQUFLLHlCQUF5QixZQUFZLElBQUksUUFBUTtRQUdsSCxJQUFHLFNBQVEsVUFBWDtVQUNFLGNBQWMscUNBQXFDLEdBQUcsV0FBVztlQURuRTtVQUdFLGNBQWMsMkJBQTJCLEdBQUcsV0FBVzs7UUFDekQsSUFBRyxHQUFHLGdCQUFlLElBQXJCO1VBQ0UsY0FBYztlQURoQjtVQUdFLFdBQVcsR0FBRztVQUdkLFdBQVcsY0FBYztVQUN6QixjQUFjLDJCQUEyQixXQUFXOztRQUd0RCxJQUFHLEdBQUEsaUJBQUEsTUFBSDtVQUNFLGNBQWMsNEJBQTRCLEdBQUcsSUFBSSxNQUFNO2VBRHpEO1VBS0UsSUFBK0MsdUJBQXVCLE9BQXRFO1lBQUEsY0FBYyxTQUFTLE9BQU87O1VBQzlCLElBQXFFLEdBQUcsZ0JBQWUsSUFBdkY7WUFBQSxjQUFjLHNCQUFzQixHQUFHLGNBQWM7O1VBQ3JELElBQXdGLEdBQUcsYUFBWSxXQUF2RztZQUFBLGNBQWMsb0JBQW9CLGNBQWMsR0FBRyxxQkFBcUI7OztRQUcxRSxjQUFjO1FDM0NaLE9ENENGOztNQUdGLDhCQUE4QixTQUFDLElBQUksTUFBTSxNQUFYO1FBQzVCLElBQUEsWUFBQTtRQUFBLFFBQVEsU0FBUztRQUVqQixhQUFhLGlCQUFpQixRQUFRLGFBQWEsT0FBTyxhQUFhLE9BQU87UUM1QzVFLE9ENkNGOztNQUdGLGdCQUFnQixTQUFDLEdBQUQ7UUFFZCxJQUFBO1FBQUEsSUFBRyxFQUFFLE9BQU8sT0FBTSxLQUFsQjtVQUNFLElBQUksRUFBRSxRQUFRLEtBQUs7VUFDbkIsSUFBSSxFQUFFLFFBQVEsS0FBSzs7UUFDckIsTUFBTTtRQUNOLE9BQU0sRUFBRSxTQUFTLElBQWpCO1VBQ0UsTUFBTSxNQUFNLEVBQUUsVUFBVSxHQUFHLE1BQU07VUFDakMsSUFBSSxFQUFFLFVBQVUsSUFBSSxFQUFFOztRQUN4QixNQUFNLE1BQU07UUMzQ1YsT0Q0Q0Y7O01BRUYsYUFBYSxTQUFDLEdBQUcsTUFBTSxJQUFJLFVBQWtCLE1BQU0sTUFBdEM7UUMzQ1QsSUFBSSxZQUFZLE1BQU07VUQyQ0MsV0FBVzs7UUFFcEMsSUFBRyxHQUFHLE9BQU0sS0FBSyxrQkFBakI7VUN6Q0ksT0QwQ0YsRUFBRSxRQUFRLEdBQUcsSUFDWDtZQUFBLE9BQU8sZ0JBQWdCLElBQUksbUJBQW1CLE1BQU07WUFDcEQsV0FBVztZQUNYLFNBQU8sWUFBWSxJQUFJOztlQUV0QixJQUFHLEdBQUcsT0FBTSxLQUFLLHVCQUFqQjtVQ3pDRCxPRDBDRixFQUFFLFFBQVEsR0FBRyxJQUNYO1lBQUEsT0FBTyxnQkFBZ0IsSUFBSSx1QkFBdUIsTUFBTTtZQUN4RCxXQUFXO1lBQ1gsU0FBTyxZQUFZLElBQUk7O2VBRXRCLElBQUcsR0FBRyxPQUFNLEtBQUssU0FBakI7VUN6Q0QsT0QwQ0YsRUFBRSxRQUFRLEdBQUcsSUFDWDtZQUFBLE9BQU8sZ0JBQWdCLElBQUksV0FBVyxNQUFNO1lBQzVDLFdBQVc7WUFDWCxTQUFPLFlBQVksSUFBSTs7ZUFFdEIsSUFBRyxHQUFHLE9BQU0sS0FBSyxjQUFqQjtVQ3pDRCxPRDBDRixFQUFFLFFBQVEsR0FBRyxJQUNYO1lBQUEsT0FBTyxnQkFBZ0IsSUFBSSxlQUFlLE1BQU07WUFDaEQsV0FBVztZQUNYLFNBQU8sWUFBWSxJQUFJOztlQUV0QixJQUFHLEdBQUcsT0FBTSxLQUFLLGNBQWpCO1VDekNELE9EMENGLEVBQUUsUUFBUSxHQUFHLElBQ1g7WUFBQSxPQUFPLGdCQUFnQixJQUFJLGVBQWUsTUFBTTtZQUNoRCxXQUFXO1lBQ1gsU0FBTyxZQUFZLElBQUk7O2VBRXRCLElBQUcsR0FBRyxPQUFNLEtBQUssZ0JBQWpCO1VDekNELE9EMENGLEVBQUUsUUFBUSxHQUFHLElBQ1g7WUFBQSxPQUFPLGdCQUFnQixJQUFJLGlCQUFpQixNQUFNO1lBQ2xELFdBQVc7WUFDWCxTQUFPLFlBQVksSUFBSTs7ZUFKdEI7VUNuQ0QsT0QwQ0YsRUFBRSxRQUFRLEdBQUcsSUFDWDtZQUFBLE9BQU8sZ0JBQWdCLElBQUksSUFBSSxNQUFNO1lBQ3JDLFdBQVc7WUFDWCxTQUFPLFlBQVksSUFBSTs7OztNQUU3QixhQUFhLFNBQUMsR0FBRyxNQUFNLElBQUksZUFBZSxNQUE3QjtRQUNYLElBQUE7UUFBQSxJQUFPLGNBQWMsUUFBUSxLQUFLLFFBQU8sQ0FBQyxHQUExQztVQ3RDSSxPRHVDRixFQUFFLFFBQVEsS0FBSyxJQUFJLEdBQUcsSUFDcEI7WUFBQSxPQUFPLGdCQUFnQjtZQUN2QixXQUFXO1lBQ1gsV0FBVzs7ZUFKZjtVQU9FLGNBQWMsY0FBYyxNQUFNLEtBQUs7VUFDdkMsSUFBQSxDQUFPLENBQUMsYUFBUjtZQ3RDSSxPRHVDRixFQUFFLFFBQVEsWUFBWSxJQUFJLEdBQUcsSUFDM0I7Y0FBQSxPQUFPLGdCQUFnQjtjQUN2QixXQUFXOzs7OztNQUVuQixrQkFBa0IsU0FBQyxHQUFHLE1BQUo7UUFDaEIsSUFBQSxJQUFBLGVBQUEsVUFBQSxHQUFBLEdBQUEsS0FBQSxNQUFBLE1BQUEsTUFBQSxNQUFBLEdBQUEsS0FBQSxJQUFBO1FBQUEsZ0JBQWdCO1FBRWhCLElBQUcsS0FBQSxTQUFBLE1BQUg7VUFFRSxZQUFZLEtBQUs7ZUFGbkI7VUFNRSxZQUFZLEtBQUs7VUFDakIsV0FBVzs7UUFFYixLQUFBLElBQUEsR0FBQSxNQUFBLFVBQUEsUUFBQSxJQUFBLEtBQUEsS0FBQTtVQ3RDSSxLQUFLLFVBQVU7VUR1Q2pCLE9BQU87VUFDUCxPQUFPO1VBRVAsSUFBRyxHQUFHLGVBQU47WUFDRSxLQUFTLElBQUEsUUFBUSxTQUFTLE1BQU07Y0FBRSxZQUFZO2NBQU0sVUFBVTtlQUFRLFNBQVM7Y0FDN0UsU0FBUztjQUNULFNBQVM7Y0FDVCxTQUFTO2NBQ1QsU0FBUztjQUNULFNBQVM7Y0FDVCxTQUFTOztZQUdYLFVBQVUsR0FBRyxNQUFNO1lBRW5CLGdCQUFnQixJQUFJO1lBRXBCLElBQVEsSUFBQSxRQUFRO1lBQ2hCLFNBQVMsT0FBTyxLQUFLLEtBQUssR0FBRztZQUM3QixPQUFPLEdBQUcsUUFBUTtZQUNsQixPQUFPLEdBQUcsUUFBUTtZQUVsQixRQUFRLFFBQVEsZ0JBQWdCOztVQUVsQyxXQUFXLEdBQUcsTUFBTSxJQUFJLFVBQVUsTUFBTTtVQUV4QyxjQUFjLEtBQUssR0FBRztVQUd0QixJQUFHLEdBQUEsVUFBQSxNQUFIO1lBQ0UsTUFBQSxHQUFBO1lBQUEsS0FBQSxJQUFBLEdBQUEsT0FBQSxJQUFBLFFBQUEsSUFBQSxNQUFBLEtBQUE7Y0N6Q0ksT0FBTyxJQUFJO2NEMENiLFdBQVcsR0FBRyxNQUFNLElBQUksZUFBZTs7OztRQ3JDM0MsT0R1Q0Y7O01BR0YsZ0JBQWdCLFNBQUMsTUFBTSxRQUFQO1FBQ2QsSUFBQSxJQUFBLEdBQUE7UUFBQSxLQUFBLEtBQUEsS0FBQSxPQUFBO1VBQ0UsS0FBSyxLQUFLLE1BQU07VUFDaEIsSUFBYyxHQUFHLE9BQU0sUUFBdkI7WUFBQSxPQUFPOztVQUdQLElBQUcsR0FBQSxpQkFBQSxNQUFIO1lBQ0UsS0FBQSxLQUFBLEdBQUEsZUFBQTtjQUNFLElBQStCLEdBQUcsY0FBYyxHQUFHLE9BQU0sUUFBekQ7Z0JBQUEsT0FBTyxHQUFHLGNBQWM7Ozs7OztNQUVoQyxZQUFZLFNBQUMsTUFBRDtRQUNWLElBQUEsR0FBQSxVQUFBLFVBQUEsSUFBQSxlQUFBO1FBQUEsSUFBUSxJQUFBLFFBQVEsU0FBUyxNQUFNO1VBQUUsWUFBWTtVQUFNLFVBQVU7V0FBUSxTQUFTO1VBQzVFLFNBQVM7VUFDVCxTQUFTO1VBQ1QsU0FBUztVQUNULFNBQVM7VUFDVCxTQUFTO1VBQ1QsU0FBUzs7UUFHWCxnQkFBZ0IsR0FBRztRQUVuQixXQUFlLElBQUEsUUFBUTtRQUN2QixXQUFXLEtBQUssVUFBVTtRQUUxQixLQUFBLEtBQUEsV0FBQTtVQ2hDSSxLQUFLLFVBQVU7VURpQ2pCLFVBQVUsT0FBTyxhQUFhLElBQUksTUFBTSxLQUFLLFVBQVU7O1FBRXpELFdBQVc7UUFFWCxnQkFBZ0IsS0FBSyxNQUFNLENBQUMsUUFBUSxRQUFRLGdCQUFnQixVQUFVLEVBQUUsUUFBUSxRQUFRLFlBQVk7UUFDcEcsZ0JBQWdCLEtBQUssTUFBTSxDQUFDLFFBQVEsUUFBUSxnQkFBZ0IsV0FBVyxFQUFFLFFBQVEsU0FBUyxZQUFZO1FBRXRHLFNBQVMsTUFBTSxVQUFVLFVBQVUsQ0FBQyxlQUFlO1FBRW5ELFdBQVcsS0FBSyxhQUFhLGVBQWUsZ0JBQWdCLE9BQU8sZ0JBQWdCLGFBQWEsU0FBUyxVQUFVO1FBRW5ILFNBQVMsR0FBRyxRQUFRLFdBQUE7VUFDbEIsSUFBQTtVQUFBLEtBQUssR0FBRztVQ2xDTixPRG1DRixXQUFXLEtBQUssYUFBYSxlQUFlLEdBQUcsWUFBWSxhQUFhLEdBQUcsUUFBUTs7UUFFckYsU0FBUztRQ2xDUCxPRG9DRixXQUFXLFVBQVUsU0FBUyxHQUFHLFNBQVMsU0FBQyxHQUFEO1VDbkN0QyxPRG9DRixNQUFNLFFBQVE7WUFBRSxRQUFROzs7O01BRTVCLE1BQU0sT0FBTyxNQUFNLE1BQU0sU0FBQyxTQUFEO1FBQ3ZCLElBQXNCLFNBQXRCO1VDaENJLE9EZ0NKLFVBQVU7Ozs7OztBQzFCaEI7QUNuYUEsUUFBUSxPQUFPLFlBRWQsUUFBUSw4RUFBZSxTQUFDLE9BQU8sYUFBYSxNQUFNLFVBQVUsSUFBSSxVQUF6QztFQUN0QixJQUFBLFlBQUEsYUFBQSxXQUFBLGNBQUEsTUFBQTtFQUFBLGFBQWE7RUFDYixjQUFjO0VBRWQsWUFBWTtFQUNaLE9BQU87SUFDTCxTQUFTO0lBQ1QsVUFBVTtJQUNWLFdBQVc7SUFDWCxRQUFROztFQUdWLGVBQWU7RUFFZixrQkFBa0IsV0FBQTtJQ3JCaEIsT0RzQkEsUUFBUSxRQUFRLGNBQWMsU0FBQyxVQUFEO01DckI1QixPRHNCQTs7O0VBRUosS0FBQyxtQkFBbUIsU0FBQyxVQUFEO0lDcEJsQixPRHFCQSxhQUFhLEtBQUs7O0VBRXBCLEtBQUMscUJBQXFCLFNBQUMsVUFBRDtJQUNwQixJQUFBO0lBQUEsUUFBUSxhQUFhLFFBQVE7SUNuQjdCLE9Eb0JBLGFBQWEsT0FBTyxPQUFPOztFQUU3QixLQUFDLFlBQVksV0FBQTtJQ25CWCxPRG9CQSxDQUVFLGFBQ0EsYUFDQSxXQUNBLFlBQ0EsVUFDQSxhQUNBOztFQUdKLEtBQUMsc0JBQXNCLFNBQUMsT0FBRDtJQUNyQixRQUFPLE1BQU07TUFBYixLQUNPO1FDNUJILE9ENEJtQjtNQUR2QixLQUVPO1FDM0JILE9EMkJpQjtNQUZyQixLQUdPO1FDMUJILE9EMEJvQjtNQUh4QixLQUlPO1FDekJILE9EeUJvQjtNQUp4QixLQUtPO1FDeEJILE9Ed0JrQjtNQUx0QixLQU1PO1FDdkJILE9EdUJvQjtNQU54QixLQU9PO1FDdEJILE9Ec0JrQjtNQVB0QixLQVFPO1FDckJILE9EcUJnQjtNQVJwQjtRQ1hJLE9Eb0JHOzs7RUFFVCxLQUFDLGNBQWMsU0FBQyxNQUFEO0lDbEJiLE9EbUJBLFFBQVEsUUFBUSxNQUFNLFNBQUMsTUFBTSxRQUFQO01BQ3BCLElBQUEsRUFBTyxLQUFLLGNBQWMsQ0FBQyxJQUEzQjtRQ2xCRSxPRG1CQSxLQUFLLGNBQWMsS0FBSyxnQkFBZ0IsS0FBSzs7OztFQUVuRCxLQUFDLGtCQUFrQixTQUFDLE1BQUQ7SUFDakIsUUFBUSxRQUFRLEtBQUssVUFBVSxTQUFDLFFBQVEsR0FBVDtNQ2hCN0IsT0RpQkEsT0FBTyxPQUFPOztJQ2ZoQixPRGlCQSxLQUFLLFNBQVMsUUFBUTtNQUNwQixNQUFNO01BQ04sY0FBYyxLQUFLLFdBQVc7TUFDOUIsWUFBWSxLQUFLLFdBQVcsYUFBYTtNQUN6QyxNQUFNOzs7RUFHVixLQUFDLFdBQVcsV0FBQTtJQUNWLElBQUE7SUFBQSxXQUFXLEdBQUc7SUFFZCxNQUFNLElBQUksWUFBWSxZQUFZLGVBQ2pDLFFBQVEsQ0FBQSxTQUFBLE9BQUE7TUNqQlAsT0RpQk8sU0FBQyxNQUFNLFFBQVEsU0FBUyxRQUF4QjtRQUNQLFFBQVEsUUFBUSxNQUFNLFNBQUMsTUFBTSxTQUFQO1VBQ3BCLFFBQU87WUFBUCxLQUNPO2NDaEJELE9EZ0JnQixLQUFLLFVBQVUsTUFBQyxZQUFZO1lBRGxELEtBRU87Y0NmRCxPRGVpQixLQUFLLFdBQVcsTUFBQyxZQUFZO1lBRnBELEtBR087Y0NkRCxPRGNrQixLQUFLLFlBQVksTUFBQyxZQUFZO1lBSHRELEtBSU87Y0NiRCxPRGFlLEtBQUssU0FBUyxNQUFDLFlBQVk7OztRQUVsRCxTQUFTLFFBQVE7UUNYZixPRFlGOztPQVRPO0lDQVQsT0RXQSxTQUFTOztFQUVYLEtBQUMsVUFBVSxTQUFDLE1BQUQ7SUNWVCxPRFdBLEtBQUs7O0VBRVAsS0FBQyxhQUFhLFdBQUE7SUNWWixPRFdBOztFQUVGLEtBQUMsVUFBVSxTQUFDLE9BQUQ7SUFDVCxhQUFhO0lBQ2IsVUFBVSxNQUFNLEdBQUc7SUFFbkIsTUFBTSxJQUFJLFlBQVksWUFBWSxVQUFVLE9BQzNDLFFBQVEsQ0FBQSxTQUFBLE9BQUE7TUNaUCxPRFlPLFNBQUMsTUFBTSxRQUFRLFNBQVMsUUFBeEI7UUFDUCxNQUFDLFlBQVksS0FBSztRQUNsQixNQUFDLGdCQUFnQjtRQ1hmLE9EYUYsTUFBTSxJQUFJLFlBQVksWUFBWSxVQUFVLFFBQVEsV0FDbkQsUUFBUSxTQUFDLFdBQUQ7VUFDUCxPQUFPLFFBQVEsT0FBTyxNQUFNO1VBRTVCLGFBQWE7VUNkWCxPRGdCRixVQUFVLElBQUksUUFBUTs7O09BVmpCO0lDRlQsT0RjQSxVQUFVLElBQUk7O0VBRWhCLEtBQUMsVUFBVSxTQUFDLFFBQUQ7SUFDVCxJQUFBLFVBQUE7SUFBQSxXQUFXLFNBQUMsUUFBUSxNQUFUO01BQ1QsSUFBQSxHQUFBLEtBQUEsTUFBQTtNQUFBLEtBQUEsSUFBQSxHQUFBLE1BQUEsS0FBQSxRQUFBLElBQUEsS0FBQSxLQUFBO1FDWEUsT0FBTyxLQUFLO1FEWVosSUFBZSxLQUFLLE9BQU0sUUFBMUI7VUFBQSxPQUFPOztRQUNQLElBQThDLEtBQUssZUFBbkQ7VUFBQSxNQUFNLFNBQVMsUUFBUSxLQUFLOztRQUM1QixJQUFjLEtBQWQ7VUFBQSxPQUFPOzs7TUNIVCxPREtBOztJQUVGLFdBQVcsR0FBRztJQUVkLFVBQVUsSUFBSSxRQUFRLEtBQUssQ0FBQSxTQUFBLE9BQUE7TUNMekIsT0RLeUIsU0FBQyxNQUFEO1FBQ3pCLElBQUE7UUFBQSxZQUFZLFNBQVMsUUFBUSxXQUFXLEtBQUs7UUFFN0MsVUFBVSxTQUFTLE1BQUMsV0FBVztRQ0o3QixPRE1GLFNBQVMsUUFBUTs7T0FMUTtJQ0UzQixPREtBLFNBQVM7O0VBRVgsS0FBQyxhQUFhLFNBQUMsUUFBRDtJQUNaLElBQUEsR0FBQSxLQUFBLEtBQUE7SUFBQSxNQUFBLFdBQUE7SUFBQSxLQUFBLElBQUEsR0FBQSxNQUFBLElBQUEsUUFBQSxJQUFBLEtBQUEsS0FBQTtNQ0ZFLFNBQVMsSUFBSTtNREdiLElBQWlCLE9BQU8sT0FBTSxRQUE5QjtRQUFBLE9BQU87OztJQUVULE9BQU87O0VBRVQsS0FBQyxZQUFZLFNBQUMsVUFBRDtJQUNYLElBQUE7SUFBQSxXQUFXLEdBQUc7SUFFZCxVQUFVLElBQUksUUFBUSxLQUFLLENBQUEsU0FBQSxPQUFBO01DQ3pCLE9ERHlCLFNBQUMsTUFBRDtRQUN6QixJQUFBO1FBQUEsU0FBUyxNQUFDLFdBQVc7UUNHbkIsT0RERixNQUFNLElBQUksWUFBWSxZQUFZLFVBQVUsV0FBVyxNQUFNLGVBQWUsV0FBVyxpQkFDdEYsUUFBUSxTQUFDLE1BQUQ7VUFFUCxPQUFPLFdBQVcsS0FBSztVQ0FyQixPREVGLFNBQVMsUUFBUTs7O09BUk07SUNVM0IsT0RBQSxTQUFTOztFQUVYLEtBQUMsY0FBYyxTQUFDLFVBQUQ7SUFDYixJQUFBO0lBQUEsV0FBVyxHQUFHO0lBRWQsVUFBVSxJQUFJLFFBQVEsS0FBSyxDQUFBLFNBQUEsT0FBQTtNQ0N6QixPRER5QixTQUFDLE1BQUQ7UUNFdkIsT0RDRixNQUFNLElBQUksWUFBWSxZQUFZLFVBQVUsV0FBVyxNQUFNLGVBQWUsVUFDM0UsUUFBUSxTQUFDLE1BQUQ7VUFDUCxJQUFBO1VBQUEsV0FBVyxLQUFLO1VDQWQsT0RFRixTQUFTLFFBQVE7OztPQVBNO0lDUzNCLE9EQUEsU0FBUzs7RUFFWCxLQUFDLGtCQUFrQixTQUFDLFVBQUQ7SUFDakIsSUFBQTtJQUFBLFdBQVcsR0FBRztJQUVkLFVBQVUsSUFBSSxRQUFRLEtBQUssQ0FBQSxTQUFBLE9BQUE7TUNDekIsT0REeUIsU0FBQyxNQUFEO1FDRXZCLE9EQ0YsTUFBTSxJQUFJLFlBQVksWUFBWSxVQUFVLFdBQVcsTUFBTSxlQUFlLFdBQVcsaUJBQ3RGLFFBQVEsU0FBQyxNQUFEO1VBQ1AsSUFBQTtVQUFBLGVBQWUsS0FBSztVQ0FsQixPREVGLFNBQVMsUUFBUTs7O09BUE07SUNTM0IsT0RBQSxTQUFTOztFQUVYLEtBQUMsa0JBQWtCLFNBQUMsVUFBRDtJQUNqQixJQUFBO0lBQUEsV0FBVyxHQUFHO0lBRWQsVUFBVSxJQUFJLFFBQVEsS0FBSyxDQUFBLFNBQUEsT0FBQTtNQ0N6QixPRER5QixTQUFDLE1BQUQ7UUNFdkIsT0RDRixNQUFNLElBQUksWUFBWSxZQUFZLFVBQVUsV0FBVyxNQUFNLGVBQWUsV0FBVyxpQkFDdEYsUUFBUSxTQUFDLE1BQUQ7VUFDUCxJQUFBO1VBQUEsZUFBZSxLQUFLO1VDQWxCLE9ERUYsTUFBTSxJQUFJLFlBQVksWUFBWSxVQUFVLFdBQVcsTUFBTSxlQUFlLFdBQVcsMEJBQ3RGLFFBQVEsU0FBQyxNQUFEO1lBQ1AsSUFBQTtZQUFBLHNCQUFzQixLQUFLO1lDRHpCLE9ER0YsU0FBUyxRQUFRO2NBQUUsTUFBTTtjQUFjLFVBQVU7Ozs7O09BWDVCO0lDZ0IzQixPREhBLFNBQVM7O0VBR1gsS0FBQyx3QkFBd0IsU0FBQyxPQUFEO0lBQ3ZCLElBQUE7SUFBQSxXQUFXLEdBQUc7SUFFZCxNQUFNLElBQUksWUFBWSxZQUFZLFVBQVUsUUFBUSxnQkFDbkQsUUFBUSxDQUFBLFNBQUEsT0FBQTtNQ0VQLE9ERk8sU0FBQyxNQUFNLFFBQVEsU0FBUyxRQUF4QjtRQUNQLElBQUksUUFBUSxPQUFPLElBQUksT0FBdkI7VUNHSSxPREZGLFNBQVMsUUFBUSxTQUFTLFFBQVE7ZUFEcEM7VUNLSSxPREZGLFNBQVMsUUFBUTs7O09BSlo7SUNVVCxPREpBLFNBQVM7O0VBR1gsS0FBQyw2QkFBNkIsU0FBQyxVQUFEO0lBQzVCLElBQUE7SUFBQSxXQUFXLEdBQUc7SUFFZCxVQUFVLElBQUksUUFBUSxLQUFLLENBQUEsU0FBQSxPQUFBO01DSXpCLE9ESnlCLFNBQUMsTUFBRDtRQ0t2QixPREpGLE1BQU0sSUFBSSxZQUFZLFlBQVksVUFBVSxXQUFXLE1BQU0sZUFBZSxXQUFXLGdCQUN0RixRQUFRLFNBQUMsTUFBRDtVQUVQLElBQUEsZUFBQTtVQUFBLElBQUksUUFBUSxPQUFPLElBQUksT0FBdkI7WUNJSSxPREhGLFNBQVMsUUFBUTtjQUFFLGVBQWU7Y0FBTSxlQUFlOztpQkFEekQ7WUFHRSxnQkFBZ0I7Y0FBRSxJQUFJLEtBQUs7Y0FBTyxXQUFXLEtBQUs7Y0FBYyxVQUFVLEtBQUs7Y0FBYSxNQUFNLEtBQUs7O1lBRXZHLElBQUksUUFBUSxPQUFPLElBQUksS0FBSyxjQUE1QjtjQ1dJLE9EVkYsU0FBUyxRQUFRO2dCQUFFLGVBQWU7Z0JBQWUsZUFBZTs7bUJBRGxFO2NBR0UsZUFBZSxLQUFLO2NDY2xCLE9EYkYsU0FBUyxRQUFRO2dCQUFFLGVBQWU7Z0JBQWUsZUFBZTs7Ozs7O09BYjdDO0lDbUMzQixPRHBCQSxTQUFTOztFQUdYLEtBQUMsMEJBQTBCLFNBQUMsVUFBRDtJQUN6QixJQUFBO0lBQUEsV0FBVyxHQUFHO0lBRWQsTUFBTSxJQUFJLFlBQVksWUFBWSxVQUFVLFdBQVcsTUFBTSxlQUFlLFdBQVcsaUJBQ3RGLFFBQVEsQ0FBQSxTQUFBLE9BQUE7TUNtQlAsT0RuQk8sU0FBQyxNQUFEO1FDb0JMLE9EbkJGLFNBQVMsUUFBUTs7T0FEVjtJQ3VCVCxPRHBCQSxTQUFTOztFQUVYLEtBQUMsa0NBQWtDLFNBQUMsT0FBRDtJQUNqQyxRQUFPLE1BQU07TUFBYixLQUNPO1FDcUJILE9EckJzQjtNQUQxQixLQUVPO1FDc0JILE9EdEJhO01BRmpCLEtBR087UUN1QkgsT0R2QmM7TUFIbEIsS0FJTztRQ3dCSCxPRHhCZTtNQUpuQjtRQzhCSSxPRHpCRzs7O0VBRVQsS0FBQyxpQkFBaUIsV0FBQTtJQUNoQixJQUFBO0lBQUEsV0FBVyxHQUFHO0lBRWQsVUFBVSxJQUFJLFFBQVEsS0FBSyxDQUFBLFNBQUEsT0FBQTtNQzJCekIsT0QzQnlCLFNBQUMsTUFBRDtRQzRCdkIsT0QxQkYsTUFBTSxJQUFJLFlBQVksWUFBWSxVQUFVLFdBQVcsTUFBTSxlQUM1RCxRQUFRLFNBQUMsWUFBRDtVQUNQLFdBQVcsYUFBYTtVQzBCdEIsT0R4QkYsU0FBUyxRQUFROzs7T0FOTTtJQ2tDM0IsT0QxQkEsU0FBUzs7RUFFWCxLQUFDLFlBQVksU0FBQyxPQUFEO0lDMkJYLE9EeEJBLE1BQU0sSUFBSSxZQUFZLFlBQVksVUFBVSxRQUFROztFQUV0RCxLQUFDLFVBQVUsU0FBQyxPQUFEO0lDeUJULE9EdEJBLE1BQU0sSUFBSSxVQUFVLFFBQVE7O0VDd0I5QixPRHRCQTs7QUN3QkY7QUN2U0EsUUFBUSxPQUFPLFlBRWQsV0FBVywrRkFBc0IsU0FBQyxRQUFRLGlCQUFpQixhQUFhLFdBQVcsYUFBbEQ7RUFDaEMsSUFBQTtFQUFBLE9BQU8sY0FBYyxXQUFBO0lBQ25CLE9BQU8sY0FBYyxZQUFZLFFBQVE7SUNsQnpDLE9EbUJBLE9BQU8sZUFBZSxZQUFZLFFBQVE7O0VBRTVDLFlBQVksaUJBQWlCLE9BQU87RUFDcEMsT0FBTyxJQUFJLFlBQVksV0FBQTtJQ2xCckIsT0RtQkEsWUFBWSxtQkFBbUIsT0FBTzs7RUFFeEMsT0FBTztFQUVQLGdCQUFnQixlQUFlLEtBQUssU0FBQyxNQUFEO0lDbkJsQyxPRG9CQSxPQUFPLFdBQVc7O0VBRXBCLFVBQVUsVUFBVSxXQUFBO0lDbkJsQixPRG9CQSxnQkFBZ0IsZUFBZSxLQUFLLFNBQUMsTUFBRDtNQ25CbEMsT0RvQkEsT0FBTyxXQUFXOztLQUNwQixZQUFZO0VDbEJkLE9Eb0JBLE9BQU8sSUFBSSxZQUFZLFdBQUE7SUNuQnJCLE9Eb0JBLFVBQVUsT0FBTzs7O0FDakJyQjtBQ0xBLFFBQVEsT0FBTyxZQUVkLFFBQVEsa0RBQW1CLFNBQUMsT0FBTyxhQUFhLElBQXJCO0VBQzFCLElBQUE7RUFBQSxXQUFXO0VBRVgsS0FBQyxlQUFlLFdBQUE7SUFDZCxJQUFBO0lBQUEsV0FBVyxHQUFHO0lBRWQsTUFBTSxJQUFJLFlBQVksWUFBWSxZQUNqQyxRQUFRLFNBQUMsTUFBTSxRQUFRLFNBQVMsUUFBeEI7TUFDUCxXQUFXO01DcEJYLE9EcUJBLFNBQVMsUUFBUTs7SUNuQm5CLE9EcUJBLFNBQVM7O0VDbkJYLE9EcUJBOztBQ25CRjtBQ0lBLFFBQVEsT0FBTyxZQUVkLFdBQVcseUdBQXVCLFNBQUMsUUFBUSxrQkFBa0IsV0FBVyxhQUFhLFFBQVEsV0FBM0Q7RUFDakMsSUFBQTtFQUFBLE9BQU8sT0FBTyxVQUFVLFNBQVMsUUFBUSwyQkFBMEIsQ0FBQztFQUNwRSxPQUFPLFdBQVcsV0FBQTtJQ2xCaEIsT0RtQkEsaUJBQWlCLGNBQWMsS0FBSyxTQUFDLE1BQUQ7TUFDbEMsT0FBTyxVQUFVLEtBQUs7TUFDdEIsT0FBTyxXQUFXLEtBQUs7TUNsQnZCLE9EbUJBLE9BQU8sT0FBTyxLQUFLOzs7RUFFdkIsT0FBTyxlQUFlLFdBQUE7SUFDcEIsT0FBTyxPQUFPO0lBQ2QsT0FBTyxRQUFRO0lDakJmLE9Ea0JBLE9BQU8sUUFBUTtNQUNiLFVBQVU7TUFDVixhQUFhO01BQ2IsZUFBZTtNQUNmLGdCQUFnQjtNQUNoQixlQUFlO01BQ2YsaUJBQWlCO01BQ2pCLGVBQWU7OztFQUduQixPQUFPO0VBQ1AsT0FBTyxXQUFXO0VBQ2xCLE9BQU87RUFFUCxVQUFVLFVBQVUsV0FBQTtJQ2xCbEIsT0RtQkEsT0FBTztLQUNQLFlBQVk7RUFFZCxPQUFPLElBQUksWUFBWSxXQUFBO0lDbkJyQixPRG9CQSxVQUFVLE9BQU87O0VBRW5CLE9BQU8sWUFBWSxTQUFDLElBQUQ7SUFDakIsSUFBRyxPQUFPLE1BQU0sYUFBWSxJQUE1QjtNQ25CRSxPRG9CQSxPQUFPO1dBRFQ7TUFHRSxPQUFPO01DbkJQLE9Eb0JBLE9BQU8sTUFBTSxXQUFXOzs7RUFFNUIsT0FBTyxZQUFZLFNBQUMsT0FBTyxJQUFSO0lBQ2pCLElBQUcsT0FBTyxNQUFNLGFBQVksSUFBNUI7TUFDRSxPQUFPOztJQUNULFFBQVEsUUFBUSxNQUFNLGVBQWUsWUFBWSxhQUFhLFNBQVM7SUNqQnZFLE9Ea0JBLGlCQUFpQixVQUFVLElBQUksS0FBSyxTQUFDLE1BQUQ7TUFDbEMsUUFBUSxRQUFRLE1BQU0sZUFBZSxZQUFZLHNCQUFzQixTQUFTO01BQ2hGLElBQUcsS0FBQSxTQUFBLE1BQUg7UUNqQkUsT0RrQkEsTUFBTSxLQUFLOzs7O0VBRWpCLE9BQU8saUJBQWlCLFNBQUMsTUFBRDtJQ2Z0QixPRGdCQSxPQUFPLE1BQU0saUJBQWlCOztFQUVoQyxPQUFPLFVBQVUsV0FBQTtJQUNmLElBQUE7SUFBQSxJQUFHLE9BQU8sTUFBTSxtQkFBa0IsYUFBbEM7TUFDRSxTQUFhLElBQUEsT0FBTztNQUNwQixPQUFPLE1BQU0saUJBQWlCO01BQzlCLE9BQU8sTUFBTSxtQkFBbUI7TUFDaEMsT0FBTyxNQUFNLGlCQUFpQjtNQUM5QixPQUFPLFFBQVE7TUFDZixPQUFPLE9BQU87TUNkZCxPRGVBLGlCQUFpQixRQUNmLE9BQU8sTUFBTSxVQUFVO1FBQ3JCLGVBQWUsT0FBTyxNQUFNO1FBQzVCLGFBQWEsT0FBTyxNQUFNO1FBQzFCLGdCQUFnQixPQUFPLE1BQU07U0FFL0IsS0FBSyxTQUFDLE1BQUQ7UUFDTCxJQUFHLFdBQVUsT0FBTyxNQUFNLGdCQUExQjtVQUNFLE9BQU8sTUFBTSxpQkFBaUI7VUFDOUIsT0FBTyxRQUFRLEtBQUs7VUNoQnBCLE9EaUJBLE9BQU8sT0FBTyxLQUFLOzs7OztFQUUzQixPQUFPLFNBQVMsV0FBQTtJQUNkLElBQUE7SUFBQSxJQUFHLE9BQU8sTUFBTSxxQkFBb0IsVUFBcEM7TUFDRSxTQUFhLElBQUEsT0FBTztNQUNwQixPQUFPLE1BQU0saUJBQWlCO01BQzlCLE9BQU8sTUFBTSxtQkFBbUI7TUFDaEMsT0FBTyxNQUFNLGlCQUFpQjtNQUM5QixPQUFPLFFBQVE7TUNaZixPRGFBLGlCQUFpQixPQUNmLE9BQU8sTUFBTSxVQUFVO1FBQ3JCLGVBQWUsT0FBTyxNQUFNO1FBQzVCLGFBQWEsT0FBTyxNQUFNO1FBQzFCLGdCQUFnQixPQUFPLE1BQU07U0FFL0IsS0FBSyxTQUFDLE1BQUQ7UUFDTCxJQUFHLFdBQVUsT0FBTyxNQUFNLGdCQUExQjtVQUNFLE9BQU8sTUFBTSxtQkFBbUI7VUFDaEMsT0FBTyxRQUFRLEtBQUs7VUFDcEIsSUFBRyxLQUFBLFNBQUEsTUFBSDtZQ2RFLE9EZUEsT0FBTyxHQUFHLDRCQUE0QjtjQUFDLE9BQU8sS0FBSzs7Ozs7OztFQUc3RCxPQUFPLFNBQVM7RUFDaEIsT0FBTyxhQUFhLFNBQUMsUUFBRDtJQUNsQixJQUFHLFdBQVUsT0FBTyxRQUFwQjtNQUNFLE9BQU8sU0FBUztNQUNoQixPQUFPLFNBQVM7TUFDaEIsT0FBTyxXQUFXO01BQ2xCLE9BQU8sZUFBZTtNQ1R0QixPRFdBLE9BQU8sV0FBVztXQU5wQjtNQVNFLE9BQU8sU0FBUztNQUNoQixPQUFPLGVBQWU7TUFDdEIsT0FBTyxTQUFTO01BQ2hCLE9BQU8sV0FBVztNQ1hsQixPRFlBLE9BQU8sZUFBZTs7O0VBRTFCLE9BQU8sYUFBYSxXQUFBO0lDVmxCLE9EV0EsT0FBTyxXQUFXOztFQUVwQixPQUFPLGNBQWMsU0FBQyxPQUFEO0lBRW5CLE9BQU8sV0FBVztJQUNsQixJQUFHLE1BQU0sV0FBVSxHQUFuQjtNQUNFLE9BQU8sU0FBUyxVQUFVLE1BQU07TUNYaEMsT0RZQSxPQUFPLFNBQVMsWUFBWTtXQUY5QjtNQ1JFLE9EWUEsT0FBTyxTQUFTLFdBQVc7OztFQ1QvQixPRFdBLE9BQU8sY0FBYyxXQUFBO0lBQ25CLElBQUEsVUFBQTtJQUFBLElBQUcsT0FBQSxTQUFBLFdBQUEsTUFBSDtNQUNFLFdBQWUsSUFBQTtNQUNmLFNBQVMsT0FBTyxXQUFXLE9BQU8sU0FBUztNQUMzQyxPQUFPLFNBQVMsWUFBWTtNQUM1QixPQUFPLFNBQVMsYUFBYTtNQUM3QixNQUFVLElBQUE7TUFDVixJQUFJLE9BQU8sYUFBYSxTQUFDLE9BQUQ7UUFDdEIsT0FBTyxTQUFTLGFBQWE7UUNUN0IsT0RVQSxPQUFPLFNBQVMsY0FBYyxTQUFTLE1BQU0sTUFBTSxTQUFTLE1BQU07O01BQ3BFLElBQUksT0FBTyxVQUFVLFNBQUMsT0FBRDtRQUNuQixPQUFPLFNBQVMsY0FBYztRQ1I5QixPRFNBLE9BQU8sU0FBUyxXQUFXOztNQUM3QixJQUFJLE9BQU8sU0FBUyxTQUFDLE9BQUQ7UUFDbEIsT0FBTyxTQUFTLGNBQWM7UUNQOUIsT0RRQSxPQUFPLFNBQVMsYUFBYTs7TUFDL0IsSUFBSSxxQkFBcUIsV0FBQTtRQUN2QixJQUFBO1FBQUEsSUFBRyxJQUFJLGVBQWMsR0FBckI7VUFDRSxXQUFXLEtBQUssTUFBTSxJQUFJO1VBQzFCLElBQUcsU0FBQSxTQUFBLE1BQUg7WUFDRSxPQUFPLFNBQVMsV0FBVyxTQUFTO1lDTHBDLE9ETUEsT0FBTyxTQUFTLGFBQWE7aUJBRi9CO1lDRkUsT0RNQSxPQUFPLFNBQVMsYUFBYTs7OztNQUNuQyxJQUFJLEtBQUssUUFBUTtNQ0ZqQixPREdBLElBQUksS0FBSztXQXhCWDtNQ3VCRSxPREdBLFFBQVEsSUFBSTs7O0lBRWpCLE9BQU8scUJBQXFCLFdBQUE7RUNEM0IsT0RFQSxTQUFDLFVBQVUsUUFBWDtJQUNFLElBQUcsYUFBWSxRQUFmO01DREUsT0RFQTtXQURGO01DQ0UsT0RFQTs7OztBQ0VOO0FDL0pBLFFBQVEsT0FBTyxZQUVkLFFBQVEsbURBQW9CLFNBQUMsT0FBTyxhQUFhLElBQXJCO0VBRTNCLEtBQUMsY0FBYyxXQUFBO0lBQ2IsSUFBQTtJQUFBLFdBQVcsR0FBRztJQUVkLE1BQU0sSUFBSSxTQUNULFFBQVEsU0FBQyxNQUFNLFFBQVEsU0FBUyxRQUF4QjtNQ3JCUCxPRHNCQSxTQUFTLFFBQVE7O0lDcEJuQixPRHNCQSxTQUFTOztFQUVYLEtBQUMsWUFBWSxTQUFDLElBQUQ7SUFDWCxJQUFBO0lBQUEsV0FBVyxHQUFHO0lBRWQsTUFBTSxVQUFPLFVBQVUsSUFDdEIsUUFBUSxTQUFDLE1BQU0sUUFBUSxTQUFTLFFBQXhCO01DdEJQLE9EdUJDLFNBQVMsUUFBUTs7SUNyQnBCLE9EdUJBLFNBQVM7O0VBRVgsS0FBQyxVQUFVLFNBQUMsSUFBSSxNQUFMO0lBQ1QsSUFBQTtJQUFBLFdBQVcsR0FBRztJQUVkLE1BQU0sSUFBSSxVQUFVLEtBQUssU0FBUztNQUFDLFFBQVE7T0FDMUMsUUFBUSxTQUFDLE1BQU0sUUFBUSxTQUFTLFFBQXhCO01DckJQLE9Ec0JBLFNBQVMsUUFBUTs7SUNwQm5CLE9Ec0JBLFNBQVM7O0VBRVgsS0FBQyxTQUFTLFNBQUMsSUFBSSxNQUFMO0lBQ1IsSUFBQTtJQUFBLFdBQVcsR0FBRztJQUVkLE1BQU0sS0FBSyxVQUFVLEtBQUssUUFBUSxJQUFJO01BQUMsUUFBUTtPQUM5QyxRQUFRLFNBQUMsTUFBTSxRQUFRLFNBQVMsUUFBeEI7TUNwQlAsT0RxQkEsU0FBUyxRQUFROztJQ25CbkIsT0RxQkEsU0FBUzs7RUNuQlgsT0RxQkE7O0FDbkJGO0FDckJBLFFBQVEsT0FBTyxZQUVkLFdBQVcsMkZBQTZCLFNBQUMsUUFBUSxxQkFBcUIsV0FBVyxhQUF6QztFQUN2QyxJQUFBO0VBQUEsb0JBQW9CLGVBQWUsS0FBSyxTQUFDLE1BQUQ7SUNsQnRDLE9EbUJBLE9BQU8sV0FBVzs7RUFFcEIsVUFBVSxVQUFVLFdBQUE7SUNsQmxCLE9EbUJBLG9CQUFvQixlQUFlLEtBQUssU0FBQyxNQUFEO01DbEJ0QyxPRG1CQSxPQUFPLFdBQVc7O0tBQ3BCLFlBQVk7RUNqQmQsT0RtQkEsT0FBTyxJQUFJLFlBQVksV0FBQTtJQ2xCckIsT0RtQkEsVUFBVSxPQUFPOztJQUVwQixXQUFXLGtIQUErQixTQUFDLFFBQVEsY0FBYywwQkFBMEIsV0FBVyxhQUE1RDtFQUN6QyxJQUFBO0VBQUEsT0FBTyxVQUFVO0VBQ2pCLHlCQUF5QixZQUFZLGFBQWEsZUFBZSxLQUFLLFNBQUMsTUFBRDtJQ2pCcEUsT0RrQkUsT0FBTyxVQUFVLEtBQUs7O0VBRXhCLFVBQVUsVUFBVSxXQUFBO0lDakJwQixPRGtCRSx5QkFBeUIsWUFBWSxhQUFhLGVBQWUsS0FBSyxTQUFDLE1BQUQ7TUNqQnRFLE9Ea0JFLE9BQU8sVUFBVSxLQUFLOztLQUN4QixZQUFZO0VDaEJoQixPRGtCRSxPQUFPLElBQUksWUFBWSxXQUFBO0lDakJ2QixPRGtCRSxVQUFVLE9BQU87OztBQ2Z2QjtBQ1ZBLFFBQVEsT0FBTyxZQUVkLFFBQVEsc0RBQXVCLFNBQUMsT0FBTyxhQUFhLElBQXJCO0VBQzlCLEtBQUMsZUFBZSxXQUFBO0lBQ2QsSUFBQTtJQUFBLFdBQVcsR0FBRztJQUVkLE1BQU0sSUFBSSxZQUFZLFlBQVksZ0JBQ2pDLFFBQVEsU0FBQyxNQUFNLFFBQVEsU0FBUyxRQUF4QjtNQ3BCUCxPRHFCQSxTQUFTLFFBQVEsS0FBSzs7SUNuQnhCLE9EcUJBLFNBQVM7O0VDbkJYLE9EcUJBO0lBRUQsUUFBUSwyREFBNEIsU0FBQyxPQUFPLGFBQWEsSUFBckI7RUFDbkMsS0FBQyxjQUFjLFNBQUMsZUFBRDtJQUNiLElBQUE7SUFBQSxXQUFXLEdBQUc7SUFFZCxNQUFNLElBQUksWUFBWSxZQUFZLGtCQUFrQixlQUNuRCxRQUFRLFNBQUMsTUFBTSxRQUFRLFNBQVMsUUFBeEI7TUN0QlAsT0R1QkEsU0FBUyxRQUFRLEtBQUs7O0lDckJ4QixPRHVCQSxTQUFTOztFQ3JCWCxPRHVCQTs7QUNyQkYiLCJmaWxlIjoiaW5kZXguanMiLCJzb3VyY2VzQ29udGVudCI6WyIjXG4jIExpY2Vuc2VkIHRvIHRoZSBBcGFjaGUgU29mdHdhcmUgRm91bmRhdGlvbiAoQVNGKSB1bmRlciBvbmVcbiMgb3IgbW9yZSBjb250cmlidXRvciBsaWNlbnNlIGFncmVlbWVudHMuICBTZWUgdGhlIE5PVElDRSBmaWxlXG4jIGRpc3RyaWJ1dGVkIHdpdGggdGhpcyB3b3JrIGZvciBhZGRpdGlvbmFsIGluZm9ybWF0aW9uXG4jIHJlZ2FyZGluZyBjb3B5cmlnaHQgb3duZXJzaGlwLiAgVGhlIEFTRiBsaWNlbnNlcyB0aGlzIGZpbGVcbiMgdG8geW91IHVuZGVyIHRoZSBBcGFjaGUgTGljZW5zZSwgVmVyc2lvbiAyLjAgKHRoZVxuIyBcIkxpY2Vuc2VcIik7IHlvdSBtYXkgbm90IHVzZSB0aGlzIGZpbGUgZXhjZXB0IGluIGNvbXBsaWFuY2VcbiMgd2l0aCB0aGUgTGljZW5zZS4gIFlvdSBtYXkgb2J0YWluIGEgY29weSBvZiB0aGUgTGljZW5zZSBhdFxuI1xuIyAgICAgaHR0cDovL3d3dy5hcGFjaGUub3JnL2xpY2Vuc2VzL0xJQ0VOU0UtMi4wXG4jXG4jIFVubGVzcyByZXF1aXJlZCBieSBhcHBsaWNhYmxlIGxhdyBvciBhZ3JlZWQgdG8gaW4gd3JpdGluZywgc29mdHdhcmVcbiMgZGlzdHJpYnV0ZWQgdW5kZXIgdGhlIExpY2Vuc2UgaXMgZGlzdHJpYnV0ZWQgb24gYW4gXCJBUyBJU1wiIEJBU0lTLFxuIyBXSVRIT1VUIFdBUlJBTlRJRVMgT1IgQ09ORElUSU9OUyBPRiBBTlkgS0lORCwgZWl0aGVyIGV4cHJlc3Mgb3IgaW1wbGllZC5cbiMgU2VlIHRoZSBMaWNlbnNlIGZvciB0aGUgc3BlY2lmaWMgbGFuZ3VhZ2UgZ292ZXJuaW5nIHBlcm1pc3Npb25zIGFuZFxuIyBsaW1pdGF0aW9ucyB1bmRlciB0aGUgTGljZW5zZS5cbiNcblxuYW5ndWxhci5tb2R1bGUoJ2ZsaW5rQXBwJywgWyd1aS5yb3V0ZXInLCAnYW5ndWxhck1vbWVudCddKVxuXG4jIC0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tXG5cbi5ydW4gKCRyb290U2NvcGUpIC0+XG4gICRyb290U2NvcGUuc2lkZWJhclZpc2libGUgPSBmYWxzZVxuICAkcm9vdFNjb3BlLnNob3dTaWRlYmFyID0gLT5cbiAgICAkcm9vdFNjb3BlLnNpZGViYXJWaXNpYmxlID0gISRyb290U2NvcGUuc2lkZWJhclZpc2libGVcbiAgICAkcm9vdFNjb3BlLnNpZGViYXJDbGFzcyA9ICdmb3JjZS1zaG93J1xuXG4jIC0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tXG5cbi52YWx1ZSAnZmxpbmtDb25maWcnLCB7XG4gIGpvYlNlcnZlcjogJydcbiMgam9iU2VydmVyOiAnaHR0cDovL2xvY2FsaG9zdDo4MDgxLydcbiAgXCJyZWZyZXNoLWludGVydmFsXCI6IDEwMDAwXG59XG5cbiMgLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS1cblxuLnJ1biAoSm9ic1NlcnZpY2UsIE1haW5TZXJ2aWNlLCBmbGlua0NvbmZpZywgJGludGVydmFsKSAtPlxuICBNYWluU2VydmljZS5sb2FkQ29uZmlnKCkudGhlbiAoY29uZmlnKSAtPlxuICAgIGFuZ3VsYXIuZXh0ZW5kIGZsaW5rQ29uZmlnLCBjb25maWdcblxuICAgIEpvYnNTZXJ2aWNlLmxpc3RKb2JzKClcblxuICAgICRpbnRlcnZhbCAtPlxuICAgICAgSm9ic1NlcnZpY2UubGlzdEpvYnMoKVxuICAgICwgZmxpbmtDb25maWdbXCJyZWZyZXNoLWludGVydmFsXCJdXG5cblxuIyAtLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLVxuXG4uY29uZmlnICgkdWlWaWV3U2Nyb2xsUHJvdmlkZXIpIC0+XG4gICR1aVZpZXdTY3JvbGxQcm92aWRlci51c2VBbmNob3JTY3JvbGwoKVxuXG4jIC0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tXG5cbi5ydW4gKCRyb290U2NvcGUsICRzdGF0ZSkgLT5cbiAgJHJvb3RTY29wZS4kb24gJyRzdGF0ZUNoYW5nZVN0YXJ0JywgKGV2ZW50LCB0b1N0YXRlLCB0b1BhcmFtcywgZnJvbVN0YXRlKSAtPlxuICAgIGlmIHRvU3RhdGUucmVkaXJlY3RUb1xuICAgICAgZXZlbnQucHJldmVudERlZmF1bHQoKVxuICAgICAgJHN0YXRlLmdvIHRvU3RhdGUucmVkaXJlY3RUbywgdG9QYXJhbXNcblxuIyAtLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLVxuXG4uY29uZmlnICgkc3RhdGVQcm92aWRlciwgJHVybFJvdXRlclByb3ZpZGVyKSAtPlxuICAkc3RhdGVQcm92aWRlci5zdGF0ZSBcIm92ZXJ2aWV3XCIsXG4gICAgdXJsOiBcIi9vdmVydmlld1wiXG4gICAgdmlld3M6XG4gICAgICBtYWluOlxuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9vdmVydmlldy5odG1sXCJcbiAgICAgICAgY29udHJvbGxlcjogJ092ZXJ2aWV3Q29udHJvbGxlcidcblxuICAuc3RhdGUgXCJydW5uaW5nLWpvYnNcIixcbiAgICB1cmw6IFwiL3J1bm5pbmctam9ic1wiXG4gICAgdmlld3M6XG4gICAgICBtYWluOlxuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JzL3J1bm5pbmctam9icy5odG1sXCJcbiAgICAgICAgY29udHJvbGxlcjogJ1J1bm5pbmdKb2JzQ29udHJvbGxlcidcblxuICAuc3RhdGUgXCJjb21wbGV0ZWQtam9ic1wiLFxuICAgIHVybDogXCIvY29tcGxldGVkLWpvYnNcIlxuICAgIHZpZXdzOlxuICAgICAgbWFpbjpcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvam9icy9jb21wbGV0ZWQtam9icy5odG1sXCJcbiAgICAgICAgY29udHJvbGxlcjogJ0NvbXBsZXRlZEpvYnNDb250cm9sbGVyJ1xuXG4gIC5zdGF0ZSBcInNpbmdsZS1qb2JcIixcbiAgICB1cmw6IFwiL2pvYnMve2pvYmlkfVwiXG4gICAgYWJzdHJhY3Q6IHRydWVcbiAgICB2aWV3czpcbiAgICAgIG1haW46XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYnMvam9iLmh0bWxcIlxuICAgICAgICBjb250cm9sbGVyOiAnU2luZ2xlSm9iQ29udHJvbGxlcidcblxuICAuc3RhdGUgXCJzaW5nbGUtam9iLnBsYW5cIixcbiAgICB1cmw6IFwiXCJcbiAgICByZWRpcmVjdFRvOiBcInNpbmdsZS1qb2IucGxhbi5zdWJ0YXNrc1wiXG4gICAgdmlld3M6XG4gICAgICBkZXRhaWxzOlxuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JzL2pvYi5wbGFuLmh0bWxcIlxuICAgICAgICBjb250cm9sbGVyOiAnSm9iUGxhbkNvbnRyb2xsZXInXG5cbiAgLnN0YXRlIFwic2luZ2xlLWpvYi5wbGFuLnN1YnRhc2tzXCIsXG4gICAgdXJsOiBcIlwiXG4gICAgdmlld3M6XG4gICAgICAnbm9kZS1kZXRhaWxzJzpcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvam9icy9qb2IucGxhbi5ub2RlLWxpc3Quc3VidGFza3MuaHRtbFwiXG4gICAgICAgIGNvbnRyb2xsZXI6ICdKb2JQbGFuU3VidGFza3NDb250cm9sbGVyJ1xuXG4gIC5zdGF0ZSBcInNpbmdsZS1qb2IucGxhbi50YXNrbWFuYWdlcnNcIixcbiAgICB1cmw6IFwiL3Rhc2ttYW5hZ2Vyc1wiXG4gICAgdmlld3M6XG4gICAgICAnbm9kZS1kZXRhaWxzJzpcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvam9icy9qb2IucGxhbi5ub2RlLWxpc3QudGFza21hbmFnZXJzLmh0bWxcIlxuICAgICAgICBjb250cm9sbGVyOiAnSm9iUGxhblRhc2tNYW5hZ2Vyc0NvbnRyb2xsZXInXG5cbiAgLnN0YXRlIFwic2luZ2xlLWpvYi5wbGFuLmFjY3VtdWxhdG9yc1wiLFxuICAgIHVybDogXCIvYWNjdW11bGF0b3JzXCJcbiAgICB2aWV3czpcbiAgICAgICdub2RlLWRldGFpbHMnOlxuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JzL2pvYi5wbGFuLm5vZGUtbGlzdC5hY2N1bXVsYXRvcnMuaHRtbFwiXG4gICAgICAgIGNvbnRyb2xsZXI6ICdKb2JQbGFuQWNjdW11bGF0b3JzQ29udHJvbGxlcidcblxuICAuc3RhdGUgXCJzaW5nbGUtam9iLnBsYW4uY2hlY2twb2ludHNcIixcbiAgICB1cmw6IFwiL2NoZWNrcG9pbnRzXCJcbiAgICB2aWV3czpcbiAgICAgICdub2RlLWRldGFpbHMnOlxuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JzL2pvYi5wbGFuLm5vZGUtbGlzdC5jaGVja3BvaW50cy5odG1sXCJcbiAgICAgICAgY29udHJvbGxlcjogJ0pvYlBsYW5DaGVja3BvaW50c0NvbnRyb2xsZXInXG5cbiAgLnN0YXRlIFwic2luZ2xlLWpvYi5wbGFuLmJhY2twcmVzc3VyZVwiLFxuICAgIHVybDogXCIvYmFja3ByZXNzdXJlXCJcbiAgICB2aWV3czpcbiAgICAgICdub2RlLWRldGFpbHMnOlxuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JzL2pvYi5wbGFuLm5vZGUtbGlzdC5iYWNrcHJlc3N1cmUuaHRtbFwiXG4gICAgICAgIGNvbnRyb2xsZXI6ICdKb2JQbGFuQmFja1ByZXNzdXJlQ29udHJvbGxlcidcblxuICAuc3RhdGUgXCJzaW5nbGUtam9iLnRpbWVsaW5lXCIsXG4gICAgdXJsOiBcIi90aW1lbGluZVwiXG4gICAgdmlld3M6XG4gICAgICBkZXRhaWxzOlxuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JzL2pvYi50aW1lbGluZS5odG1sXCJcblxuICAuc3RhdGUgXCJzaW5nbGUtam9iLnRpbWVsaW5lLnZlcnRleFwiLFxuICAgIHVybDogXCIve3ZlcnRleElkfVwiXG4gICAgdmlld3M6XG4gICAgICB2ZXJ0ZXg6XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYnMvam9iLnRpbWVsaW5lLnZlcnRleC5odG1sXCJcbiAgICAgICAgY29udHJvbGxlcjogJ0pvYlRpbWVsaW5lVmVydGV4Q29udHJvbGxlcidcblxuICAuc3RhdGUgXCJzaW5nbGUtam9iLmV4Y2VwdGlvbnNcIixcbiAgICB1cmw6IFwiL2V4Y2VwdGlvbnNcIlxuICAgIHZpZXdzOlxuICAgICAgZGV0YWlsczpcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvam9icy9qb2IuZXhjZXB0aW9ucy5odG1sXCJcbiAgICAgICAgY29udHJvbGxlcjogJ0pvYkV4Y2VwdGlvbnNDb250cm9sbGVyJ1xuXG4gIC5zdGF0ZSBcInNpbmdsZS1qb2IucHJvcGVydGllc1wiLFxuICAgIHVybDogXCIvcHJvcGVydGllc1wiXG4gICAgdmlld3M6XG4gICAgICBkZXRhaWxzOlxuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JzL2pvYi5wcm9wZXJ0aWVzLmh0bWxcIlxuICAgICAgICBjb250cm9sbGVyOiAnSm9iUHJvcGVydGllc0NvbnRyb2xsZXInXG5cbiAgLnN0YXRlIFwic2luZ2xlLWpvYi5jb25maWdcIixcbiAgICB1cmw6IFwiL2NvbmZpZ1wiXG4gICAgdmlld3M6XG4gICAgICBkZXRhaWxzOlxuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JzL2pvYi5jb25maWcuaHRtbFwiXG5cbiAgLnN0YXRlIFwiYWxsLW1hbmFnZXJcIixcbiAgICB1cmw6IFwiL3Rhc2ttYW5hZ2Vyc1wiXG4gICAgdmlld3M6XG4gICAgICBtYWluOlxuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy90YXNrbWFuYWdlci9pbmRleC5odG1sXCJcbiAgICAgICAgY29udHJvbGxlcjogJ0FsbFRhc2tNYW5hZ2Vyc0NvbnRyb2xsZXInXG5cbiAgLnN0YXRlIFwic2luZ2xlLW1hbmFnZXJcIixcbiAgICAgIHVybDogXCIvdGFza21hbmFnZXIve3Rhc2ttYW5hZ2VyaWR9XCJcbiAgICAgIHZpZXdzOlxuICAgICAgICBtYWluOlxuICAgICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL3Rhc2ttYW5hZ2VyL3Rhc2ttYW5hZ2VyLmh0bWxcIlxuICAgICAgICAgIGNvbnRyb2xsZXI6ICdTaW5nbGVUYXNrTWFuYWdlckNvbnRyb2xsZXInXG5cbiAgLnN0YXRlIFwic2luZ2xlLW1hbmFnZXIubWV0cmljc1wiLFxuICAgIHVybDogXCIvbWV0cmljc1wiXG4gICAgdmlld3M6XG4gICAgICBkZXRhaWxzOlxuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy90YXNrbWFuYWdlci90YXNrbWFuYWdlci5tZXRyaWNzLmh0bWxcIlxuXG4gIC5zdGF0ZSBcImpvYm1hbmFnZXJcIixcbiAgICAgIHVybDogXCIvam9ibWFuYWdlclwiXG4gICAgICB2aWV3czpcbiAgICAgICAgbWFpbjpcbiAgICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JtYW5hZ2VyL2luZGV4Lmh0bWxcIlxuXG4gIC5zdGF0ZSBcImpvYm1hbmFnZXIuY29uZmlnXCIsXG4gICAgdXJsOiBcIi9jb25maWdcIlxuICAgIHZpZXdzOlxuICAgICAgZGV0YWlsczpcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvam9ibWFuYWdlci9jb25maWcuaHRtbFwiXG4gICAgICAgIGNvbnRyb2xsZXI6ICdKb2JNYW5hZ2VyQ29uZmlnQ29udHJvbGxlcidcblxuICAuc3RhdGUgXCJqb2JtYW5hZ2VyLnN0ZG91dFwiLFxuICAgIHVybDogXCIvc3Rkb3V0XCJcbiAgICB2aWV3czpcbiAgICAgIGRldGFpbHM6XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYm1hbmFnZXIvc3Rkb3V0Lmh0bWxcIlxuICAgICAgICBjb250cm9sbGVyOiAnSm9iTWFuYWdlclN0ZG91dENvbnRyb2xsZXInXG5cbiAgLnN0YXRlIFwiam9ibWFuYWdlci5sb2dcIixcbiAgICB1cmw6IFwiL2xvZ1wiXG4gICAgdmlld3M6XG4gICAgICBkZXRhaWxzOlxuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JtYW5hZ2VyL2xvZy5odG1sXCJcbiAgICAgICAgY29udHJvbGxlcjogJ0pvYk1hbmFnZXJMb2dzQ29udHJvbGxlcidcblxuICAuc3RhdGUgXCJzdWJtaXRcIixcbiAgICAgIHVybDogXCIvc3VibWl0XCJcbiAgICAgIHZpZXdzOlxuICAgICAgICBtYWluOlxuICAgICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL3N1Ym1pdC5odG1sXCJcbiAgICAgICAgICBjb250cm9sbGVyOiBcIkpvYlN1Ym1pdENvbnRyb2xsZXJcIlxuXG4gICR1cmxSb3V0ZXJQcm92aWRlci5vdGhlcndpc2UgXCIvb3ZlcnZpZXdcIlxuIiwiYW5ndWxhci5tb2R1bGUoJ2ZsaW5rQXBwJywgWyd1aS5yb3V0ZXInLCAnYW5ndWxhck1vbWVudCddKS5ydW4oZnVuY3Rpb24oJHJvb3RTY29wZSkge1xuICAkcm9vdFNjb3BlLnNpZGViYXJWaXNpYmxlID0gZmFsc2U7XG4gIHJldHVybiAkcm9vdFNjb3BlLnNob3dTaWRlYmFyID0gZnVuY3Rpb24oKSB7XG4gICAgJHJvb3RTY29wZS5zaWRlYmFyVmlzaWJsZSA9ICEkcm9vdFNjb3BlLnNpZGViYXJWaXNpYmxlO1xuICAgIHJldHVybiAkcm9vdFNjb3BlLnNpZGViYXJDbGFzcyA9ICdmb3JjZS1zaG93JztcbiAgfTtcbn0pLnZhbHVlKCdmbGlua0NvbmZpZycsIHtcbiAgam9iU2VydmVyOiAnJyxcbiAgXCJyZWZyZXNoLWludGVydmFsXCI6IDEwMDAwXG59KS5ydW4oZnVuY3Rpb24oSm9ic1NlcnZpY2UsIE1haW5TZXJ2aWNlLCBmbGlua0NvbmZpZywgJGludGVydmFsKSB7XG4gIHJldHVybiBNYWluU2VydmljZS5sb2FkQ29uZmlnKCkudGhlbihmdW5jdGlvbihjb25maWcpIHtcbiAgICBhbmd1bGFyLmV4dGVuZChmbGlua0NvbmZpZywgY29uZmlnKTtcbiAgICBKb2JzU2VydmljZS5saXN0Sm9icygpO1xuICAgIHJldHVybiAkaW50ZXJ2YWwoZnVuY3Rpb24oKSB7XG4gICAgICByZXR1cm4gSm9ic1NlcnZpY2UubGlzdEpvYnMoKTtcbiAgICB9LCBmbGlua0NvbmZpZ1tcInJlZnJlc2gtaW50ZXJ2YWxcIl0pO1xuICB9KTtcbn0pLmNvbmZpZyhmdW5jdGlvbigkdWlWaWV3U2Nyb2xsUHJvdmlkZXIpIHtcbiAgcmV0dXJuICR1aVZpZXdTY3JvbGxQcm92aWRlci51c2VBbmNob3JTY3JvbGwoKTtcbn0pLnJ1bihmdW5jdGlvbigkcm9vdFNjb3BlLCAkc3RhdGUpIHtcbiAgcmV0dXJuICRyb290U2NvcGUuJG9uKCckc3RhdGVDaGFuZ2VTdGFydCcsIGZ1bmN0aW9uKGV2ZW50LCB0b1N0YXRlLCB0b1BhcmFtcywgZnJvbVN0YXRlKSB7XG4gICAgaWYgKHRvU3RhdGUucmVkaXJlY3RUbykge1xuICAgICAgZXZlbnQucHJldmVudERlZmF1bHQoKTtcbiAgICAgIHJldHVybiAkc3RhdGUuZ28odG9TdGF0ZS5yZWRpcmVjdFRvLCB0b1BhcmFtcyk7XG4gICAgfVxuICB9KTtcbn0pLmNvbmZpZyhmdW5jdGlvbigkc3RhdGVQcm92aWRlciwgJHVybFJvdXRlclByb3ZpZGVyKSB7XG4gICRzdGF0ZVByb3ZpZGVyLnN0YXRlKFwib3ZlcnZpZXdcIiwge1xuICAgIHVybDogXCIvb3ZlcnZpZXdcIixcbiAgICB2aWV3czoge1xuICAgICAgbWFpbjoge1xuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9vdmVydmlldy5odG1sXCIsXG4gICAgICAgIGNvbnRyb2xsZXI6ICdPdmVydmlld0NvbnRyb2xsZXInXG4gICAgICB9XG4gICAgfVxuICB9KS5zdGF0ZShcInJ1bm5pbmctam9ic1wiLCB7XG4gICAgdXJsOiBcIi9ydW5uaW5nLWpvYnNcIixcbiAgICB2aWV3czoge1xuICAgICAgbWFpbjoge1xuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JzL3J1bm5pbmctam9icy5odG1sXCIsXG4gICAgICAgIGNvbnRyb2xsZXI6ICdSdW5uaW5nSm9ic0NvbnRyb2xsZXInXG4gICAgICB9XG4gICAgfVxuICB9KS5zdGF0ZShcImNvbXBsZXRlZC1qb2JzXCIsIHtcbiAgICB1cmw6IFwiL2NvbXBsZXRlZC1qb2JzXCIsXG4gICAgdmlld3M6IHtcbiAgICAgIG1haW46IHtcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvam9icy9jb21wbGV0ZWQtam9icy5odG1sXCIsXG4gICAgICAgIGNvbnRyb2xsZXI6ICdDb21wbGV0ZWRKb2JzQ29udHJvbGxlcidcbiAgICAgIH1cbiAgICB9XG4gIH0pLnN0YXRlKFwic2luZ2xlLWpvYlwiLCB7XG4gICAgdXJsOiBcIi9qb2JzL3tqb2JpZH1cIixcbiAgICBhYnN0cmFjdDogdHJ1ZSxcbiAgICB2aWV3czoge1xuICAgICAgbWFpbjoge1xuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JzL2pvYi5odG1sXCIsXG4gICAgICAgIGNvbnRyb2xsZXI6ICdTaW5nbGVKb2JDb250cm9sbGVyJ1xuICAgICAgfVxuICAgIH1cbiAgfSkuc3RhdGUoXCJzaW5nbGUtam9iLnBsYW5cIiwge1xuICAgIHVybDogXCJcIixcbiAgICByZWRpcmVjdFRvOiBcInNpbmdsZS1qb2IucGxhbi5zdWJ0YXNrc1wiLFxuICAgIHZpZXdzOiB7XG4gICAgICBkZXRhaWxzOiB7XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYnMvam9iLnBsYW4uaHRtbFwiLFxuICAgICAgICBjb250cm9sbGVyOiAnSm9iUGxhbkNvbnRyb2xsZXInXG4gICAgICB9XG4gICAgfVxuICB9KS5zdGF0ZShcInNpbmdsZS1qb2IucGxhbi5zdWJ0YXNrc1wiLCB7XG4gICAgdXJsOiBcIlwiLFxuICAgIHZpZXdzOiB7XG4gICAgICAnbm9kZS1kZXRhaWxzJzoge1xuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JzL2pvYi5wbGFuLm5vZGUtbGlzdC5zdWJ0YXNrcy5odG1sXCIsXG4gICAgICAgIGNvbnRyb2xsZXI6ICdKb2JQbGFuU3VidGFza3NDb250cm9sbGVyJ1xuICAgICAgfVxuICAgIH1cbiAgfSkuc3RhdGUoXCJzaW5nbGUtam9iLnBsYW4udGFza21hbmFnZXJzXCIsIHtcbiAgICB1cmw6IFwiL3Rhc2ttYW5hZ2Vyc1wiLFxuICAgIHZpZXdzOiB7XG4gICAgICAnbm9kZS1kZXRhaWxzJzoge1xuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JzL2pvYi5wbGFuLm5vZGUtbGlzdC50YXNrbWFuYWdlcnMuaHRtbFwiLFxuICAgICAgICBjb250cm9sbGVyOiAnSm9iUGxhblRhc2tNYW5hZ2Vyc0NvbnRyb2xsZXInXG4gICAgICB9XG4gICAgfVxuICB9KS5zdGF0ZShcInNpbmdsZS1qb2IucGxhbi5hY2N1bXVsYXRvcnNcIiwge1xuICAgIHVybDogXCIvYWNjdW11bGF0b3JzXCIsXG4gICAgdmlld3M6IHtcbiAgICAgICdub2RlLWRldGFpbHMnOiB7XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYnMvam9iLnBsYW4ubm9kZS1saXN0LmFjY3VtdWxhdG9ycy5odG1sXCIsXG4gICAgICAgIGNvbnRyb2xsZXI6ICdKb2JQbGFuQWNjdW11bGF0b3JzQ29udHJvbGxlcidcbiAgICAgIH1cbiAgICB9XG4gIH0pLnN0YXRlKFwic2luZ2xlLWpvYi5wbGFuLmNoZWNrcG9pbnRzXCIsIHtcbiAgICB1cmw6IFwiL2NoZWNrcG9pbnRzXCIsXG4gICAgdmlld3M6IHtcbiAgICAgICdub2RlLWRldGFpbHMnOiB7XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYnMvam9iLnBsYW4ubm9kZS1saXN0LmNoZWNrcG9pbnRzLmh0bWxcIixcbiAgICAgICAgY29udHJvbGxlcjogJ0pvYlBsYW5DaGVja3BvaW50c0NvbnRyb2xsZXInXG4gICAgICB9XG4gICAgfVxuICB9KS5zdGF0ZShcInNpbmdsZS1qb2IucGxhbi5iYWNrcHJlc3N1cmVcIiwge1xuICAgIHVybDogXCIvYmFja3ByZXNzdXJlXCIsXG4gICAgdmlld3M6IHtcbiAgICAgICdub2RlLWRldGFpbHMnOiB7XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYnMvam9iLnBsYW4ubm9kZS1saXN0LmJhY2twcmVzc3VyZS5odG1sXCIsXG4gICAgICAgIGNvbnRyb2xsZXI6ICdKb2JQbGFuQmFja1ByZXNzdXJlQ29udHJvbGxlcidcbiAgICAgIH1cbiAgICB9XG4gIH0pLnN0YXRlKFwic2luZ2xlLWpvYi50aW1lbGluZVwiLCB7XG4gICAgdXJsOiBcIi90aW1lbGluZVwiLFxuICAgIHZpZXdzOiB7XG4gICAgICBkZXRhaWxzOiB7XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYnMvam9iLnRpbWVsaW5lLmh0bWxcIlxuICAgICAgfVxuICAgIH1cbiAgfSkuc3RhdGUoXCJzaW5nbGUtam9iLnRpbWVsaW5lLnZlcnRleFwiLCB7XG4gICAgdXJsOiBcIi97dmVydGV4SWR9XCIsXG4gICAgdmlld3M6IHtcbiAgICAgIHZlcnRleDoge1xuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JzL2pvYi50aW1lbGluZS52ZXJ0ZXguaHRtbFwiLFxuICAgICAgICBjb250cm9sbGVyOiAnSm9iVGltZWxpbmVWZXJ0ZXhDb250cm9sbGVyJ1xuICAgICAgfVxuICAgIH1cbiAgfSkuc3RhdGUoXCJzaW5nbGUtam9iLmV4Y2VwdGlvbnNcIiwge1xuICAgIHVybDogXCIvZXhjZXB0aW9uc1wiLFxuICAgIHZpZXdzOiB7XG4gICAgICBkZXRhaWxzOiB7XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYnMvam9iLmV4Y2VwdGlvbnMuaHRtbFwiLFxuICAgICAgICBjb250cm9sbGVyOiAnSm9iRXhjZXB0aW9uc0NvbnRyb2xsZXInXG4gICAgICB9XG4gICAgfVxuICB9KS5zdGF0ZShcInNpbmdsZS1qb2IucHJvcGVydGllc1wiLCB7XG4gICAgdXJsOiBcIi9wcm9wZXJ0aWVzXCIsXG4gICAgdmlld3M6IHtcbiAgICAgIGRldGFpbHM6IHtcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvam9icy9qb2IucHJvcGVydGllcy5odG1sXCIsXG4gICAgICAgIGNvbnRyb2xsZXI6ICdKb2JQcm9wZXJ0aWVzQ29udHJvbGxlcidcbiAgICAgIH1cbiAgICB9XG4gIH0pLnN0YXRlKFwic2luZ2xlLWpvYi5jb25maWdcIiwge1xuICAgIHVybDogXCIvY29uZmlnXCIsXG4gICAgdmlld3M6IHtcbiAgICAgIGRldGFpbHM6IHtcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvam9icy9qb2IuY29uZmlnLmh0bWxcIlxuICAgICAgfVxuICAgIH1cbiAgfSkuc3RhdGUoXCJhbGwtbWFuYWdlclwiLCB7XG4gICAgdXJsOiBcIi90YXNrbWFuYWdlcnNcIixcbiAgICB2aWV3czoge1xuICAgICAgbWFpbjoge1xuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy90YXNrbWFuYWdlci9pbmRleC5odG1sXCIsXG4gICAgICAgIGNvbnRyb2xsZXI6ICdBbGxUYXNrTWFuYWdlcnNDb250cm9sbGVyJ1xuICAgICAgfVxuICAgIH1cbiAgfSkuc3RhdGUoXCJzaW5nbGUtbWFuYWdlclwiLCB7XG4gICAgdXJsOiBcIi90YXNrbWFuYWdlci97dGFza21hbmFnZXJpZH1cIixcbiAgICB2aWV3czoge1xuICAgICAgbWFpbjoge1xuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy90YXNrbWFuYWdlci90YXNrbWFuYWdlci5odG1sXCIsXG4gICAgICAgIGNvbnRyb2xsZXI6ICdTaW5nbGVUYXNrTWFuYWdlckNvbnRyb2xsZXInXG4gICAgICB9XG4gICAgfVxuICB9KS5zdGF0ZShcInNpbmdsZS1tYW5hZ2VyLm1ldHJpY3NcIiwge1xuICAgIHVybDogXCIvbWV0cmljc1wiLFxuICAgIHZpZXdzOiB7XG4gICAgICBkZXRhaWxzOiB7XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL3Rhc2ttYW5hZ2VyL3Rhc2ttYW5hZ2VyLm1ldHJpY3MuaHRtbFwiXG4gICAgICB9XG4gICAgfVxuICB9KS5zdGF0ZShcImpvYm1hbmFnZXJcIiwge1xuICAgIHVybDogXCIvam9ibWFuYWdlclwiLFxuICAgIHZpZXdzOiB7XG4gICAgICBtYWluOiB7XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYm1hbmFnZXIvaW5kZXguaHRtbFwiXG4gICAgICB9XG4gICAgfVxuICB9KS5zdGF0ZShcImpvYm1hbmFnZXIuY29uZmlnXCIsIHtcbiAgICB1cmw6IFwiL2NvbmZpZ1wiLFxuICAgIHZpZXdzOiB7XG4gICAgICBkZXRhaWxzOiB7XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYm1hbmFnZXIvY29uZmlnLmh0bWxcIixcbiAgICAgICAgY29udHJvbGxlcjogJ0pvYk1hbmFnZXJDb25maWdDb250cm9sbGVyJ1xuICAgICAgfVxuICAgIH1cbiAgfSkuc3RhdGUoXCJqb2JtYW5hZ2VyLnN0ZG91dFwiLCB7XG4gICAgdXJsOiBcIi9zdGRvdXRcIixcbiAgICB2aWV3czoge1xuICAgICAgZGV0YWlsczoge1xuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JtYW5hZ2VyL3N0ZG91dC5odG1sXCIsXG4gICAgICAgIGNvbnRyb2xsZXI6ICdKb2JNYW5hZ2VyU3Rkb3V0Q29udHJvbGxlcidcbiAgICAgIH1cbiAgICB9XG4gIH0pLnN0YXRlKFwiam9ibWFuYWdlci5sb2dcIiwge1xuICAgIHVybDogXCIvbG9nXCIsXG4gICAgdmlld3M6IHtcbiAgICAgIGRldGFpbHM6IHtcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvam9ibWFuYWdlci9sb2cuaHRtbFwiLFxuICAgICAgICBjb250cm9sbGVyOiAnSm9iTWFuYWdlckxvZ3NDb250cm9sbGVyJ1xuICAgICAgfVxuICAgIH1cbiAgfSkuc3RhdGUoXCJzdWJtaXRcIiwge1xuICAgIHVybDogXCIvc3VibWl0XCIsXG4gICAgdmlld3M6IHtcbiAgICAgIG1haW46IHtcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvc3VibWl0Lmh0bWxcIixcbiAgICAgICAgY29udHJvbGxlcjogXCJKb2JTdWJtaXRDb250cm9sbGVyXCJcbiAgICAgIH1cbiAgICB9XG4gIH0pO1xuICByZXR1cm4gJHVybFJvdXRlclByb3ZpZGVyLm90aGVyd2lzZShcIi9vdmVydmlld1wiKTtcbn0pO1xuIiwiI1xuIyBMaWNlbnNlZCB0byB0aGUgQXBhY2hlIFNvZnR3YXJlIEZvdW5kYXRpb24gKEFTRikgdW5kZXIgb25lXG4jIG9yIG1vcmUgY29udHJpYnV0b3IgbGljZW5zZSBhZ3JlZW1lbnRzLiAgU2VlIHRoZSBOT1RJQ0UgZmlsZVxuIyBkaXN0cmlidXRlZCB3aXRoIHRoaXMgd29yayBmb3IgYWRkaXRpb25hbCBpbmZvcm1hdGlvblxuIyByZWdhcmRpbmcgY29weXJpZ2h0IG93bmVyc2hpcC4gIFRoZSBBU0YgbGljZW5zZXMgdGhpcyBmaWxlXG4jIHRvIHlvdSB1bmRlciB0aGUgQXBhY2hlIExpY2Vuc2UsIFZlcnNpb24gMi4wICh0aGVcbiMgXCJMaWNlbnNlXCIpOyB5b3UgbWF5IG5vdCB1c2UgdGhpcyBmaWxlIGV4Y2VwdCBpbiBjb21wbGlhbmNlXG4jIHdpdGggdGhlIExpY2Vuc2UuICBZb3UgbWF5IG9idGFpbiBhIGNvcHkgb2YgdGhlIExpY2Vuc2UgYXRcbiNcbiMgICAgIGh0dHA6Ly93d3cuYXBhY2hlLm9yZy9saWNlbnNlcy9MSUNFTlNFLTIuMFxuI1xuIyBVbmxlc3MgcmVxdWlyZWQgYnkgYXBwbGljYWJsZSBsYXcgb3IgYWdyZWVkIHRvIGluIHdyaXRpbmcsIHNvZnR3YXJlXG4jIGRpc3RyaWJ1dGVkIHVuZGVyIHRoZSBMaWNlbnNlIGlzIGRpc3RyaWJ1dGVkIG9uIGFuIFwiQVMgSVNcIiBCQVNJUyxcbiMgV0lUSE9VVCBXQVJSQU5USUVTIE9SIENPTkRJVElPTlMgT0YgQU5ZIEtJTkQsIGVpdGhlciBleHByZXNzIG9yIGltcGxpZWQuXG4jIFNlZSB0aGUgTGljZW5zZSBmb3IgdGhlIHNwZWNpZmljIGxhbmd1YWdlIGdvdmVybmluZyBwZXJtaXNzaW9ucyBhbmRcbiMgbGltaXRhdGlvbnMgdW5kZXIgdGhlIExpY2Vuc2UuXG4jXG5cbmFuZ3VsYXIubW9kdWxlKCdmbGlua0FwcCcpXG5cbiMgLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLVxuXG4uZGlyZWN0aXZlICdic0xhYmVsJywgKEpvYnNTZXJ2aWNlKSAtPlxuICB0cmFuc2NsdWRlOiB0cnVlXG4gIHJlcGxhY2U6IHRydWVcbiAgc2NvcGU6IFxuICAgIGdldExhYmVsQ2xhc3M6IFwiJlwiXG4gICAgc3RhdHVzOiBcIkBcIlxuXG4gIHRlbXBsYXRlOiBcIjxzcGFuIHRpdGxlPSd7e3N0YXR1c319JyBuZy1jbGFzcz0nZ2V0TGFiZWxDbGFzcygpJz48bmctdHJhbnNjbHVkZT48L25nLXRyYW5zY2x1ZGU+PC9zcGFuPlwiXG4gIFxuICBsaW5rOiAoc2NvcGUsIGVsZW1lbnQsIGF0dHJzKSAtPlxuICAgIHNjb3BlLmdldExhYmVsQ2xhc3MgPSAtPlxuICAgICAgJ2xhYmVsIGxhYmVsLScgKyBKb2JzU2VydmljZS50cmFuc2xhdGVMYWJlbFN0YXRlKGF0dHJzLnN0YXR1cylcblxuIyAtLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tXG5cbi5kaXJlY3RpdmUgJ2JwTGFiZWwnLCAoSm9ic1NlcnZpY2UpIC0+XG4gIHRyYW5zY2x1ZGU6IHRydWVcbiAgcmVwbGFjZTogdHJ1ZVxuICBzY29wZTpcbiAgICBnZXRCYWNrUHJlc3N1cmVMYWJlbENsYXNzOiBcIiZcIlxuICAgIHN0YXR1czogXCJAXCJcblxuICB0ZW1wbGF0ZTogXCI8c3BhbiB0aXRsZT0ne3tzdGF0dXN9fScgbmctY2xhc3M9J2dldEJhY2tQcmVzc3VyZUxhYmVsQ2xhc3MoKSc+PG5nLXRyYW5zY2x1ZGU+PC9uZy10cmFuc2NsdWRlPjwvc3Bhbj5cIlxuXG4gIGxpbms6IChzY29wZSwgZWxlbWVudCwgYXR0cnMpIC0+XG4gICAgc2NvcGUuZ2V0QmFja1ByZXNzdXJlTGFiZWxDbGFzcyA9IC0+XG4gICAgICAnbGFiZWwgbGFiZWwtJyArIEpvYnNTZXJ2aWNlLnRyYW5zbGF0ZUJhY2tQcmVzc3VyZUxhYmVsU3RhdGUoYXR0cnMuc3RhdHVzKVxuXG4jIC0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS1cblxuLmRpcmVjdGl2ZSAnaW5kaWNhdG9yUHJpbWFyeScsIChKb2JzU2VydmljZSkgLT5cbiAgcmVwbGFjZTogdHJ1ZVxuICBzY29wZTogXG4gICAgZ2V0TGFiZWxDbGFzczogXCImXCJcbiAgICBzdGF0dXM6ICdAJ1xuXG4gIHRlbXBsYXRlOiBcIjxpIHRpdGxlPSd7e3N0YXR1c319JyBuZy1jbGFzcz0nZ2V0TGFiZWxDbGFzcygpJyAvPlwiXG4gIFxuICBsaW5rOiAoc2NvcGUsIGVsZW1lbnQsIGF0dHJzKSAtPlxuICAgIHNjb3BlLmdldExhYmVsQ2xhc3MgPSAtPlxuICAgICAgJ2ZhIGZhLWNpcmNsZSBpbmRpY2F0b3IgaW5kaWNhdG9yLScgKyBKb2JzU2VydmljZS50cmFuc2xhdGVMYWJlbFN0YXRlKGF0dHJzLnN0YXR1cylcblxuIyAtLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tXG5cbi5kaXJlY3RpdmUgJ3RhYmxlUHJvcGVydHknLCAtPlxuICByZXBsYWNlOiB0cnVlXG4gIHNjb3BlOlxuICAgIHZhbHVlOiAnPSdcblxuICB0ZW1wbGF0ZTogXCI8dGQgdGl0bGU9XFxcInt7dmFsdWUgfHwgJ05vbmUnfX1cXFwiPnt7dmFsdWUgfHwgJ05vbmUnfX08L3RkPlwiXG4iLCJhbmd1bGFyLm1vZHVsZSgnZmxpbmtBcHAnKS5kaXJlY3RpdmUoJ2JzTGFiZWwnLCBmdW5jdGlvbihKb2JzU2VydmljZSkge1xuICByZXR1cm4ge1xuICAgIHRyYW5zY2x1ZGU6IHRydWUsXG4gICAgcmVwbGFjZTogdHJ1ZSxcbiAgICBzY29wZToge1xuICAgICAgZ2V0TGFiZWxDbGFzczogXCImXCIsXG4gICAgICBzdGF0dXM6IFwiQFwiXG4gICAgfSxcbiAgICB0ZW1wbGF0ZTogXCI8c3BhbiB0aXRsZT0ne3tzdGF0dXN9fScgbmctY2xhc3M9J2dldExhYmVsQ2xhc3MoKSc+PG5nLXRyYW5zY2x1ZGU+PC9uZy10cmFuc2NsdWRlPjwvc3Bhbj5cIixcbiAgICBsaW5rOiBmdW5jdGlvbihzY29wZSwgZWxlbWVudCwgYXR0cnMpIHtcbiAgICAgIHJldHVybiBzY29wZS5nZXRMYWJlbENsYXNzID0gZnVuY3Rpb24oKSB7XG4gICAgICAgIHJldHVybiAnbGFiZWwgbGFiZWwtJyArIEpvYnNTZXJ2aWNlLnRyYW5zbGF0ZUxhYmVsU3RhdGUoYXR0cnMuc3RhdHVzKTtcbiAgICAgIH07XG4gICAgfVxuICB9O1xufSkuZGlyZWN0aXZlKCdicExhYmVsJywgZnVuY3Rpb24oSm9ic1NlcnZpY2UpIHtcbiAgcmV0dXJuIHtcbiAgICB0cmFuc2NsdWRlOiB0cnVlLFxuICAgIHJlcGxhY2U6IHRydWUsXG4gICAgc2NvcGU6IHtcbiAgICAgIGdldEJhY2tQcmVzc3VyZUxhYmVsQ2xhc3M6IFwiJlwiLFxuICAgICAgc3RhdHVzOiBcIkBcIlxuICAgIH0sXG4gICAgdGVtcGxhdGU6IFwiPHNwYW4gdGl0bGU9J3t7c3RhdHVzfX0nIG5nLWNsYXNzPSdnZXRCYWNrUHJlc3N1cmVMYWJlbENsYXNzKCknPjxuZy10cmFuc2NsdWRlPjwvbmctdHJhbnNjbHVkZT48L3NwYW4+XCIsXG4gICAgbGluazogZnVuY3Rpb24oc2NvcGUsIGVsZW1lbnQsIGF0dHJzKSB7XG4gICAgICByZXR1cm4gc2NvcGUuZ2V0QmFja1ByZXNzdXJlTGFiZWxDbGFzcyA9IGZ1bmN0aW9uKCkge1xuICAgICAgICByZXR1cm4gJ2xhYmVsIGxhYmVsLScgKyBKb2JzU2VydmljZS50cmFuc2xhdGVCYWNrUHJlc3N1cmVMYWJlbFN0YXRlKGF0dHJzLnN0YXR1cyk7XG4gICAgICB9O1xuICAgIH1cbiAgfTtcbn0pLmRpcmVjdGl2ZSgnaW5kaWNhdG9yUHJpbWFyeScsIGZ1bmN0aW9uKEpvYnNTZXJ2aWNlKSB7XG4gIHJldHVybiB7XG4gICAgcmVwbGFjZTogdHJ1ZSxcbiAgICBzY29wZToge1xuICAgICAgZ2V0TGFiZWxDbGFzczogXCImXCIsXG4gICAgICBzdGF0dXM6ICdAJ1xuICAgIH0sXG4gICAgdGVtcGxhdGU6IFwiPGkgdGl0bGU9J3t7c3RhdHVzfX0nIG5nLWNsYXNzPSdnZXRMYWJlbENsYXNzKCknIC8+XCIsXG4gICAgbGluazogZnVuY3Rpb24oc2NvcGUsIGVsZW1lbnQsIGF0dHJzKSB7XG4gICAgICByZXR1cm4gc2NvcGUuZ2V0TGFiZWxDbGFzcyA9IGZ1bmN0aW9uKCkge1xuICAgICAgICByZXR1cm4gJ2ZhIGZhLWNpcmNsZSBpbmRpY2F0b3IgaW5kaWNhdG9yLScgKyBKb2JzU2VydmljZS50cmFuc2xhdGVMYWJlbFN0YXRlKGF0dHJzLnN0YXR1cyk7XG4gICAgICB9O1xuICAgIH1cbiAgfTtcbn0pLmRpcmVjdGl2ZSgndGFibGVQcm9wZXJ0eScsIGZ1bmN0aW9uKCkge1xuICByZXR1cm4ge1xuICAgIHJlcGxhY2U6IHRydWUsXG4gICAgc2NvcGU6IHtcbiAgICAgIHZhbHVlOiAnPSdcbiAgICB9LFxuICAgIHRlbXBsYXRlOiBcIjx0ZCB0aXRsZT1cXFwie3t2YWx1ZSB8fCAnTm9uZSd9fVxcXCI+e3t2YWx1ZSB8fCAnTm9uZSd9fTwvdGQ+XCJcbiAgfTtcbn0pO1xuIiwiI1xuIyBMaWNlbnNlZCB0byB0aGUgQXBhY2hlIFNvZnR3YXJlIEZvdW5kYXRpb24gKEFTRikgdW5kZXIgb25lXG4jIG9yIG1vcmUgY29udHJpYnV0b3IgbGljZW5zZSBhZ3JlZW1lbnRzLiAgU2VlIHRoZSBOT1RJQ0UgZmlsZVxuIyBkaXN0cmlidXRlZCB3aXRoIHRoaXMgd29yayBmb3IgYWRkaXRpb25hbCBpbmZvcm1hdGlvblxuIyByZWdhcmRpbmcgY29weXJpZ2h0IG93bmVyc2hpcC4gIFRoZSBBU0YgbGljZW5zZXMgdGhpcyBmaWxlXG4jIHRvIHlvdSB1bmRlciB0aGUgQXBhY2hlIExpY2Vuc2UsIFZlcnNpb24gMi4wICh0aGVcbiMgXCJMaWNlbnNlXCIpOyB5b3UgbWF5IG5vdCB1c2UgdGhpcyBmaWxlIGV4Y2VwdCBpbiBjb21wbGlhbmNlXG4jIHdpdGggdGhlIExpY2Vuc2UuICBZb3UgbWF5IG9idGFpbiBhIGNvcHkgb2YgdGhlIExpY2Vuc2UgYXRcbiNcbiMgICAgIGh0dHA6Ly93d3cuYXBhY2hlLm9yZy9saWNlbnNlcy9MSUNFTlNFLTIuMFxuI1xuIyBVbmxlc3MgcmVxdWlyZWQgYnkgYXBwbGljYWJsZSBsYXcgb3IgYWdyZWVkIHRvIGluIHdyaXRpbmcsIHNvZnR3YXJlXG4jIGRpc3RyaWJ1dGVkIHVuZGVyIHRoZSBMaWNlbnNlIGlzIGRpc3RyaWJ1dGVkIG9uIGFuIFwiQVMgSVNcIiBCQVNJUyxcbiMgV0lUSE9VVCBXQVJSQU5USUVTIE9SIENPTkRJVElPTlMgT0YgQU5ZIEtJTkQsIGVpdGhlciBleHByZXNzIG9yIGltcGxpZWQuXG4jIFNlZSB0aGUgTGljZW5zZSBmb3IgdGhlIHNwZWNpZmljIGxhbmd1YWdlIGdvdmVybmluZyBwZXJtaXNzaW9ucyBhbmRcbiMgbGltaXRhdGlvbnMgdW5kZXIgdGhlIExpY2Vuc2UuXG4jXG5cbmFuZ3VsYXIubW9kdWxlKCdmbGlua0FwcCcpXG5cbi5maWx0ZXIgXCJhbUR1cmF0aW9uRm9ybWF0RXh0ZW5kZWRcIiwgKGFuZ3VsYXJNb21lbnRDb25maWcpIC0+XG4gIGFtRHVyYXRpb25Gb3JtYXRFeHRlbmRlZEZpbHRlciA9ICh2YWx1ZSwgZm9ybWF0LCBkdXJhdGlvbkZvcm1hdCkgLT5cbiAgICByZXR1cm4gXCJcIiAgaWYgdHlwZW9mIHZhbHVlIGlzIFwidW5kZWZpbmVkXCIgb3IgdmFsdWUgaXMgbnVsbFxuXG4gICAgbW9tZW50LmR1cmF0aW9uKHZhbHVlLCBmb3JtYXQpLmZvcm1hdChkdXJhdGlvbkZvcm1hdCwgeyB0cmltOiBmYWxzZSB9KVxuXG4gIGFtRHVyYXRpb25Gb3JtYXRFeHRlbmRlZEZpbHRlci4kc3RhdGVmdWwgPSBhbmd1bGFyTW9tZW50Q29uZmlnLnN0YXRlZnVsRmlsdGVyc1xuXG4gIGFtRHVyYXRpb25Gb3JtYXRFeHRlbmRlZEZpbHRlclxuXG4uZmlsdGVyIFwiaHVtYW5pemVEdXJhdGlvblwiLCAtPlxuICAodmFsdWUsIHNob3J0KSAtPlxuICAgIHJldHVybiBcIlwiIGlmIHR5cGVvZiB2YWx1ZSBpcyBcInVuZGVmaW5lZFwiIG9yIHZhbHVlIGlzIG51bGxcbiAgICBtcyA9IHZhbHVlICUgMTAwMFxuICAgIHggPSBNYXRoLmZsb29yKHZhbHVlIC8gMTAwMClcbiAgICBzZWNvbmRzID0geCAlIDYwXG4gICAgeCA9IE1hdGguZmxvb3IoeCAvIDYwKVxuICAgIG1pbnV0ZXMgPSB4ICUgNjBcbiAgICB4ID0gTWF0aC5mbG9vcih4IC8gNjApXG4gICAgaG91cnMgPSB4ICUgMjRcbiAgICB4ID0gTWF0aC5mbG9vcih4IC8gMjQpXG4gICAgZGF5cyA9IHhcbiAgICBpZiBkYXlzID09IDBcbiAgICAgIGlmIGhvdXJzID09IDBcbiAgICAgICAgaWYgbWludXRlcyA9PSAwXG4gICAgICAgICAgaWYgc2Vjb25kcyA9PSAwXG4gICAgICAgICAgICByZXR1cm4gbXMgKyBcIm1zXCJcbiAgICAgICAgICBlbHNlXG4gICAgICAgICAgICByZXR1cm4gc2Vjb25kcyArIFwicyBcIlxuICAgICAgICBlbHNlXG4gICAgICAgICAgcmV0dXJuIG1pbnV0ZXMgKyBcIm0gXCIgKyBzZWNvbmRzICsgXCJzXCJcbiAgICAgIGVsc2VcbiAgICAgICAgaWYgc2hvcnQgdGhlbiByZXR1cm4gaG91cnMgKyBcImggXCIgKyBtaW51dGVzICsgXCJtXCIgZWxzZSByZXR1cm4gaG91cnMgKyBcImggXCIgKyBtaW51dGVzICsgXCJtIFwiICsgc2Vjb25kcyArIFwic1wiXG4gICAgZWxzZVxuICAgICAgaWYgc2hvcnQgdGhlbiByZXR1cm4gZGF5cyArIFwiZCBcIiArIGhvdXJzICsgXCJoXCIgZWxzZSByZXR1cm4gZGF5cyArIFwiZCBcIiArIGhvdXJzICsgXCJoIFwiICsgbWludXRlcyArIFwibSBcIiArIHNlY29uZHMgKyBcInNcIlxuXG4uZmlsdGVyIFwiaHVtYW5pemVUZXh0XCIsIC0+XG4gICh0ZXh0KSAtPlxuICAgICMgVE9ETzogZXh0ZW5kLi4uIGEgbG90XG4gICAgaWYgdGV4dCB0aGVuIHRleHQucmVwbGFjZSgvJmd0Oy9nLCBcIj5cIikucmVwbGFjZSgvPGJyXFwvPi9nLFwiXCIpIGVsc2UgJydcblxuLmZpbHRlciBcImh1bWFuaXplQnl0ZXNcIiwgLT5cbiAgKGJ5dGVzKSAtPlxuICAgIHVuaXRzID0gW1wiQlwiLCBcIktCXCIsIFwiTUJcIiwgXCJHQlwiLCBcIlRCXCIsIFwiUEJcIiwgXCJFQlwiXVxuICAgIGNvbnZlcnRlciA9ICh2YWx1ZSwgcG93ZXIpIC0+XG4gICAgICBiYXNlID0gTWF0aC5wb3coMTAyNCwgcG93ZXIpXG4gICAgICBpZiB2YWx1ZSA8IGJhc2VcbiAgICAgICAgcmV0dXJuICh2YWx1ZSAvIGJhc2UpLnRvRml4ZWQoMikgKyBcIiBcIiArIHVuaXRzW3Bvd2VyXVxuICAgICAgZWxzZSBpZiB2YWx1ZSA8IGJhc2UgKiAxMDAwXG4gICAgICAgIHJldHVybiAodmFsdWUgLyBiYXNlKS50b1ByZWNpc2lvbigzKSArIFwiIFwiICsgdW5pdHNbcG93ZXJdXG4gICAgICBlbHNlXG4gICAgICAgIHJldHVybiBjb252ZXJ0ZXIodmFsdWUsIHBvd2VyICsgMSlcbiAgICByZXR1cm4gXCJcIiBpZiB0eXBlb2YgYnl0ZXMgaXMgXCJ1bmRlZmluZWRcIiBvciBieXRlcyBpcyBudWxsXG4gICAgaWYgYnl0ZXMgPCAxMDAwIHRoZW4gYnl0ZXMgKyBcIiBCXCIgZWxzZSBjb252ZXJ0ZXIoYnl0ZXMsIDEpXG5cbi5maWx0ZXIgXCJ0b1VwcGVyQ2FzZVwiLCAtPlxuICAodGV4dCkgLT4gdGV4dC50b1VwcGVyQ2FzZSgpXG4iLCJhbmd1bGFyLm1vZHVsZSgnZmxpbmtBcHAnKS5maWx0ZXIoXCJhbUR1cmF0aW9uRm9ybWF0RXh0ZW5kZWRcIiwgZnVuY3Rpb24oYW5ndWxhck1vbWVudENvbmZpZykge1xuICB2YXIgYW1EdXJhdGlvbkZvcm1hdEV4dGVuZGVkRmlsdGVyO1xuICBhbUR1cmF0aW9uRm9ybWF0RXh0ZW5kZWRGaWx0ZXIgPSBmdW5jdGlvbih2YWx1ZSwgZm9ybWF0LCBkdXJhdGlvbkZvcm1hdCkge1xuICAgIGlmICh0eXBlb2YgdmFsdWUgPT09IFwidW5kZWZpbmVkXCIgfHwgdmFsdWUgPT09IG51bGwpIHtcbiAgICAgIHJldHVybiBcIlwiO1xuICAgIH1cbiAgICByZXR1cm4gbW9tZW50LmR1cmF0aW9uKHZhbHVlLCBmb3JtYXQpLmZvcm1hdChkdXJhdGlvbkZvcm1hdCwge1xuICAgICAgdHJpbTogZmFsc2VcbiAgICB9KTtcbiAgfTtcbiAgYW1EdXJhdGlvbkZvcm1hdEV4dGVuZGVkRmlsdGVyLiRzdGF0ZWZ1bCA9IGFuZ3VsYXJNb21lbnRDb25maWcuc3RhdGVmdWxGaWx0ZXJzO1xuICByZXR1cm4gYW1EdXJhdGlvbkZvcm1hdEV4dGVuZGVkRmlsdGVyO1xufSkuZmlsdGVyKFwiaHVtYW5pemVEdXJhdGlvblwiLCBmdW5jdGlvbigpIHtcbiAgcmV0dXJuIGZ1bmN0aW9uKHZhbHVlLCBzaG9ydCkge1xuICAgIHZhciBkYXlzLCBob3VycywgbWludXRlcywgbXMsIHNlY29uZHMsIHg7XG4gICAgaWYgKHR5cGVvZiB2YWx1ZSA9PT0gXCJ1bmRlZmluZWRcIiB8fCB2YWx1ZSA9PT0gbnVsbCkge1xuICAgICAgcmV0dXJuIFwiXCI7XG4gICAgfVxuICAgIG1zID0gdmFsdWUgJSAxMDAwO1xuICAgIHggPSBNYXRoLmZsb29yKHZhbHVlIC8gMTAwMCk7XG4gICAgc2Vjb25kcyA9IHggJSA2MDtcbiAgICB4ID0gTWF0aC5mbG9vcih4IC8gNjApO1xuICAgIG1pbnV0ZXMgPSB4ICUgNjA7XG4gICAgeCA9IE1hdGguZmxvb3IoeCAvIDYwKTtcbiAgICBob3VycyA9IHggJSAyNDtcbiAgICB4ID0gTWF0aC5mbG9vcih4IC8gMjQpO1xuICAgIGRheXMgPSB4O1xuICAgIGlmIChkYXlzID09PSAwKSB7XG4gICAgICBpZiAoaG91cnMgPT09IDApIHtcbiAgICAgICAgaWYgKG1pbnV0ZXMgPT09IDApIHtcbiAgICAgICAgICBpZiAoc2Vjb25kcyA9PT0gMCkge1xuICAgICAgICAgICAgcmV0dXJuIG1zICsgXCJtc1wiO1xuICAgICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgICByZXR1cm4gc2Vjb25kcyArIFwicyBcIjtcbiAgICAgICAgICB9XG4gICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgcmV0dXJuIG1pbnV0ZXMgKyBcIm0gXCIgKyBzZWNvbmRzICsgXCJzXCI7XG4gICAgICAgIH1cbiAgICAgIH0gZWxzZSB7XG4gICAgICAgIGlmIChzaG9ydCkge1xuICAgICAgICAgIHJldHVybiBob3VycyArIFwiaCBcIiArIG1pbnV0ZXMgKyBcIm1cIjtcbiAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICByZXR1cm4gaG91cnMgKyBcImggXCIgKyBtaW51dGVzICsgXCJtIFwiICsgc2Vjb25kcyArIFwic1wiO1xuICAgICAgICB9XG4gICAgICB9XG4gICAgfSBlbHNlIHtcbiAgICAgIGlmIChzaG9ydCkge1xuICAgICAgICByZXR1cm4gZGF5cyArIFwiZCBcIiArIGhvdXJzICsgXCJoXCI7XG4gICAgICB9IGVsc2Uge1xuICAgICAgICByZXR1cm4gZGF5cyArIFwiZCBcIiArIGhvdXJzICsgXCJoIFwiICsgbWludXRlcyArIFwibSBcIiArIHNlY29uZHMgKyBcInNcIjtcbiAgICAgIH1cbiAgICB9XG4gIH07XG59KS5maWx0ZXIoXCJodW1hbml6ZVRleHRcIiwgZnVuY3Rpb24oKSB7XG4gIHJldHVybiBmdW5jdGlvbih0ZXh0KSB7XG4gICAgaWYgKHRleHQpIHtcbiAgICAgIHJldHVybiB0ZXh0LnJlcGxhY2UoLyZndDsvZywgXCI+XCIpLnJlcGxhY2UoLzxiclxcLz4vZywgXCJcIik7XG4gICAgfSBlbHNlIHtcbiAgICAgIHJldHVybiAnJztcbiAgICB9XG4gIH07XG59KS5maWx0ZXIoXCJodW1hbml6ZUJ5dGVzXCIsIGZ1bmN0aW9uKCkge1xuICByZXR1cm4gZnVuY3Rpb24oYnl0ZXMpIHtcbiAgICB2YXIgY29udmVydGVyLCB1bml0cztcbiAgICB1bml0cyA9IFtcIkJcIiwgXCJLQlwiLCBcIk1CXCIsIFwiR0JcIiwgXCJUQlwiLCBcIlBCXCIsIFwiRUJcIl07XG4gICAgY29udmVydGVyID0gZnVuY3Rpb24odmFsdWUsIHBvd2VyKSB7XG4gICAgICB2YXIgYmFzZTtcbiAgICAgIGJhc2UgPSBNYXRoLnBvdygxMDI0LCBwb3dlcik7XG4gICAgICBpZiAodmFsdWUgPCBiYXNlKSB7XG4gICAgICAgIHJldHVybiAodmFsdWUgLyBiYXNlKS50b0ZpeGVkKDIpICsgXCIgXCIgKyB1bml0c1twb3dlcl07XG4gICAgICB9IGVsc2UgaWYgKHZhbHVlIDwgYmFzZSAqIDEwMDApIHtcbiAgICAgICAgcmV0dXJuICh2YWx1ZSAvIGJhc2UpLnRvUHJlY2lzaW9uKDMpICsgXCIgXCIgKyB1bml0c1twb3dlcl07XG4gICAgICB9IGVsc2Uge1xuICAgICAgICByZXR1cm4gY29udmVydGVyKHZhbHVlLCBwb3dlciArIDEpO1xuICAgICAgfVxuICAgIH07XG4gICAgaWYgKHR5cGVvZiBieXRlcyA9PT0gXCJ1bmRlZmluZWRcIiB8fCBieXRlcyA9PT0gbnVsbCkge1xuICAgICAgcmV0dXJuIFwiXCI7XG4gICAgfVxuICAgIGlmIChieXRlcyA8IDEwMDApIHtcbiAgICAgIHJldHVybiBieXRlcyArIFwiIEJcIjtcbiAgICB9IGVsc2Uge1xuICAgICAgcmV0dXJuIGNvbnZlcnRlcihieXRlcywgMSk7XG4gICAgfVxuICB9O1xufSkuZmlsdGVyKFwidG9VcHBlckNhc2VcIiwgZnVuY3Rpb24oKSB7XG4gIHJldHVybiBmdW5jdGlvbih0ZXh0KSB7XG4gICAgcmV0dXJuIHRleHQudG9VcHBlckNhc2UoKTtcbiAgfTtcbn0pO1xuIiwiI1xuIyBMaWNlbnNlZCB0byB0aGUgQXBhY2hlIFNvZnR3YXJlIEZvdW5kYXRpb24gKEFTRikgdW5kZXIgb25lXG4jIG9yIG1vcmUgY29udHJpYnV0b3IgbGljZW5zZSBhZ3JlZW1lbnRzLiAgU2VlIHRoZSBOT1RJQ0UgZmlsZVxuIyBkaXN0cmlidXRlZCB3aXRoIHRoaXMgd29yayBmb3IgYWRkaXRpb25hbCBpbmZvcm1hdGlvblxuIyByZWdhcmRpbmcgY29weXJpZ2h0IG93bmVyc2hpcC4gIFRoZSBBU0YgbGljZW5zZXMgdGhpcyBmaWxlXG4jIHRvIHlvdSB1bmRlciB0aGUgQXBhY2hlIExpY2Vuc2UsIFZlcnNpb24gMi4wICh0aGVcbiMgXCJMaWNlbnNlXCIpOyB5b3UgbWF5IG5vdCB1c2UgdGhpcyBmaWxlIGV4Y2VwdCBpbiBjb21wbGlhbmNlXG4jIHdpdGggdGhlIExpY2Vuc2UuICBZb3UgbWF5IG9idGFpbiBhIGNvcHkgb2YgdGhlIExpY2Vuc2UgYXRcbiNcbiMgICAgIGh0dHA6Ly93d3cuYXBhY2hlLm9yZy9saWNlbnNlcy9MSUNFTlNFLTIuMFxuI1xuIyBVbmxlc3MgcmVxdWlyZWQgYnkgYXBwbGljYWJsZSBsYXcgb3IgYWdyZWVkIHRvIGluIHdyaXRpbmcsIHNvZnR3YXJlXG4jIGRpc3RyaWJ1dGVkIHVuZGVyIHRoZSBMaWNlbnNlIGlzIGRpc3RyaWJ1dGVkIG9uIGFuIFwiQVMgSVNcIiBCQVNJUyxcbiMgV0lUSE9VVCBXQVJSQU5USUVTIE9SIENPTkRJVElPTlMgT0YgQU5ZIEtJTkQsIGVpdGhlciBleHByZXNzIG9yIGltcGxpZWQuXG4jIFNlZSB0aGUgTGljZW5zZSBmb3IgdGhlIHNwZWNpZmljIGxhbmd1YWdlIGdvdmVybmluZyBwZXJtaXNzaW9ucyBhbmRcbiMgbGltaXRhdGlvbnMgdW5kZXIgdGhlIExpY2Vuc2UuXG4jXG5cbmFuZ3VsYXIubW9kdWxlKCdmbGlua0FwcCcpXG5cbi5zZXJ2aWNlICdNYWluU2VydmljZScsICgkaHR0cCwgZmxpbmtDb25maWcsICRxKSAtPlxuICBAbG9hZENvbmZpZyA9IC0+XG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpXG5cbiAgICAkaHR0cC5nZXQgZmxpbmtDb25maWcuam9iU2VydmVyICsgXCJjb25maWdcIlxuICAgIC5zdWNjZXNzIChkYXRhLCBzdGF0dXMsIGhlYWRlcnMsIGNvbmZpZykgLT5cbiAgICAgIGRlZmVycmVkLnJlc29sdmUoZGF0YSlcblxuICAgIGRlZmVycmVkLnByb21pc2VcblxuXG4gIEBcbiIsImFuZ3VsYXIubW9kdWxlKCdmbGlua0FwcCcpLnNlcnZpY2UoJ01haW5TZXJ2aWNlJywgZnVuY3Rpb24oJGh0dHAsIGZsaW5rQ29uZmlnLCAkcSkge1xuICB0aGlzLmxvYWRDb25maWcgPSBmdW5jdGlvbigpIHtcbiAgICB2YXIgZGVmZXJyZWQ7XG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpO1xuICAgICRodHRwLmdldChmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcImNvbmZpZ1wiKS5zdWNjZXNzKGZ1bmN0aW9uKGRhdGEsIHN0YXR1cywgaGVhZGVycywgY29uZmlnKSB7XG4gICAgICByZXR1cm4gZGVmZXJyZWQucmVzb2x2ZShkYXRhKTtcbiAgICB9KTtcbiAgICByZXR1cm4gZGVmZXJyZWQucHJvbWlzZTtcbiAgfTtcbiAgcmV0dXJuIHRoaXM7XG59KTtcbiIsIiNcbiMgTGljZW5zZWQgdG8gdGhlIEFwYWNoZSBTb2Z0d2FyZSBGb3VuZGF0aW9uIChBU0YpIHVuZGVyIG9uZVxuIyBvciBtb3JlIGNvbnRyaWJ1dG9yIGxpY2Vuc2UgYWdyZWVtZW50cy4gIFNlZSB0aGUgTk9USUNFIGZpbGVcbiMgZGlzdHJpYnV0ZWQgd2l0aCB0aGlzIHdvcmsgZm9yIGFkZGl0aW9uYWwgaW5mb3JtYXRpb25cbiMgcmVnYXJkaW5nIGNvcHlyaWdodCBvd25lcnNoaXAuICBUaGUgQVNGIGxpY2Vuc2VzIHRoaXMgZmlsZVxuIyB0byB5b3UgdW5kZXIgdGhlIEFwYWNoZSBMaWNlbnNlLCBWZXJzaW9uIDIuMCAodGhlXG4jIFwiTGljZW5zZVwiKTsgeW91IG1heSBub3QgdXNlIHRoaXMgZmlsZSBleGNlcHQgaW4gY29tcGxpYW5jZVxuIyB3aXRoIHRoZSBMaWNlbnNlLiAgWW91IG1heSBvYnRhaW4gYSBjb3B5IG9mIHRoZSBMaWNlbnNlIGF0XG4jXG4jICAgICBodHRwOi8vd3d3LmFwYWNoZS5vcmcvbGljZW5zZXMvTElDRU5TRS0yLjBcbiNcbiMgVW5sZXNzIHJlcXVpcmVkIGJ5IGFwcGxpY2FibGUgbGF3IG9yIGFncmVlZCB0byBpbiB3cml0aW5nLCBzb2Z0d2FyZVxuIyBkaXN0cmlidXRlZCB1bmRlciB0aGUgTGljZW5zZSBpcyBkaXN0cmlidXRlZCBvbiBhbiBcIkFTIElTXCIgQkFTSVMsXG4jIFdJVEhPVVQgV0FSUkFOVElFUyBPUiBDT05ESVRJT05TIE9GIEFOWSBLSU5ELCBlaXRoZXIgZXhwcmVzcyBvciBpbXBsaWVkLlxuIyBTZWUgdGhlIExpY2Vuc2UgZm9yIHRoZSBzcGVjaWZpYyBsYW5ndWFnZSBnb3Zlcm5pbmcgcGVybWlzc2lvbnMgYW5kXG4jIGxpbWl0YXRpb25zIHVuZGVyIHRoZSBMaWNlbnNlLlxuI1xuXG5hbmd1bGFyLm1vZHVsZSgnZmxpbmtBcHAnKVxuXG4uY29udHJvbGxlciAnSm9iTWFuYWdlckNvbmZpZ0NvbnRyb2xsZXInLCAoJHNjb3BlLCBKb2JNYW5hZ2VyQ29uZmlnU2VydmljZSkgLT5cbiAgSm9iTWFuYWdlckNvbmZpZ1NlcnZpY2UubG9hZENvbmZpZygpLnRoZW4gKGRhdGEpIC0+XG4gICAgaWYgISRzY29wZS5qb2JtYW5hZ2VyP1xuICAgICAgJHNjb3BlLmpvYm1hbmFnZXIgPSB7fVxuICAgICRzY29wZS5qb2JtYW5hZ2VyWydjb25maWcnXSA9IGRhdGFcblxuLmNvbnRyb2xsZXIgJ0pvYk1hbmFnZXJMb2dzQ29udHJvbGxlcicsICgkc2NvcGUsIEpvYk1hbmFnZXJMb2dzU2VydmljZSkgLT5cbiAgSm9iTWFuYWdlckxvZ3NTZXJ2aWNlLmxvYWRMb2dzKCkudGhlbiAoZGF0YSkgLT5cbiAgICBpZiAhJHNjb3BlLmpvYm1hbmFnZXI/XG4gICAgICAkc2NvcGUuam9ibWFuYWdlciA9IHt9XG4gICAgJHNjb3BlLmpvYm1hbmFnZXJbJ2xvZyddID0gZGF0YVxuXG4gICRzY29wZS5yZWxvYWREYXRhID0gKCkgLT5cbiAgICBKb2JNYW5hZ2VyTG9nc1NlcnZpY2UubG9hZExvZ3MoKS50aGVuIChkYXRhKSAtPlxuICAgICAgJHNjb3BlLmpvYm1hbmFnZXJbJ2xvZyddID0gZGF0YVxuXG4uY29udHJvbGxlciAnSm9iTWFuYWdlclN0ZG91dENvbnRyb2xsZXInLCAoJHNjb3BlLCBKb2JNYW5hZ2VyU3Rkb3V0U2VydmljZSkgLT5cbiAgSm9iTWFuYWdlclN0ZG91dFNlcnZpY2UubG9hZFN0ZG91dCgpLnRoZW4gKGRhdGEpIC0+XG4gICAgaWYgISRzY29wZS5qb2JtYW5hZ2VyP1xuICAgICAgJHNjb3BlLmpvYm1hbmFnZXIgPSB7fVxuICAgICRzY29wZS5qb2JtYW5hZ2VyWydzdGRvdXQnXSA9IGRhdGFcblxuICAkc2NvcGUucmVsb2FkRGF0YSA9ICgpIC0+XG4gICAgSm9iTWFuYWdlclN0ZG91dFNlcnZpY2UubG9hZFN0ZG91dCgpLnRoZW4gKGRhdGEpIC0+XG4gICAgICAkc2NvcGUuam9ibWFuYWdlclsnc3Rkb3V0J10gPSBkYXRhXG4iLCJhbmd1bGFyLm1vZHVsZSgnZmxpbmtBcHAnKS5jb250cm9sbGVyKCdKb2JNYW5hZ2VyQ29uZmlnQ29udHJvbGxlcicsIGZ1bmN0aW9uKCRzY29wZSwgSm9iTWFuYWdlckNvbmZpZ1NlcnZpY2UpIHtcbiAgcmV0dXJuIEpvYk1hbmFnZXJDb25maWdTZXJ2aWNlLmxvYWRDb25maWcoKS50aGVuKGZ1bmN0aW9uKGRhdGEpIHtcbiAgICBpZiAoJHNjb3BlLmpvYm1hbmFnZXIgPT0gbnVsbCkge1xuICAgICAgJHNjb3BlLmpvYm1hbmFnZXIgPSB7fTtcbiAgICB9XG4gICAgcmV0dXJuICRzY29wZS5qb2JtYW5hZ2VyWydjb25maWcnXSA9IGRhdGE7XG4gIH0pO1xufSkuY29udHJvbGxlcignSm9iTWFuYWdlckxvZ3NDb250cm9sbGVyJywgZnVuY3Rpb24oJHNjb3BlLCBKb2JNYW5hZ2VyTG9nc1NlcnZpY2UpIHtcbiAgSm9iTWFuYWdlckxvZ3NTZXJ2aWNlLmxvYWRMb2dzKCkudGhlbihmdW5jdGlvbihkYXRhKSB7XG4gICAgaWYgKCRzY29wZS5qb2JtYW5hZ2VyID09IG51bGwpIHtcbiAgICAgICRzY29wZS5qb2JtYW5hZ2VyID0ge307XG4gICAgfVxuICAgIHJldHVybiAkc2NvcGUuam9ibWFuYWdlclsnbG9nJ10gPSBkYXRhO1xuICB9KTtcbiAgcmV0dXJuICRzY29wZS5yZWxvYWREYXRhID0gZnVuY3Rpb24oKSB7XG4gICAgcmV0dXJuIEpvYk1hbmFnZXJMb2dzU2VydmljZS5sb2FkTG9ncygpLnRoZW4oZnVuY3Rpb24oZGF0YSkge1xuICAgICAgcmV0dXJuICRzY29wZS5qb2JtYW5hZ2VyWydsb2cnXSA9IGRhdGE7XG4gICAgfSk7XG4gIH07XG59KS5jb250cm9sbGVyKCdKb2JNYW5hZ2VyU3Rkb3V0Q29udHJvbGxlcicsIGZ1bmN0aW9uKCRzY29wZSwgSm9iTWFuYWdlclN0ZG91dFNlcnZpY2UpIHtcbiAgSm9iTWFuYWdlclN0ZG91dFNlcnZpY2UubG9hZFN0ZG91dCgpLnRoZW4oZnVuY3Rpb24oZGF0YSkge1xuICAgIGlmICgkc2NvcGUuam9ibWFuYWdlciA9PSBudWxsKSB7XG4gICAgICAkc2NvcGUuam9ibWFuYWdlciA9IHt9O1xuICAgIH1cbiAgICByZXR1cm4gJHNjb3BlLmpvYm1hbmFnZXJbJ3N0ZG91dCddID0gZGF0YTtcbiAgfSk7XG4gIHJldHVybiAkc2NvcGUucmVsb2FkRGF0YSA9IGZ1bmN0aW9uKCkge1xuICAgIHJldHVybiBKb2JNYW5hZ2VyU3Rkb3V0U2VydmljZS5sb2FkU3Rkb3V0KCkudGhlbihmdW5jdGlvbihkYXRhKSB7XG4gICAgICByZXR1cm4gJHNjb3BlLmpvYm1hbmFnZXJbJ3N0ZG91dCddID0gZGF0YTtcbiAgICB9KTtcbiAgfTtcbn0pO1xuIiwiI1xuIyBMaWNlbnNlZCB0byB0aGUgQXBhY2hlIFNvZnR3YXJlIEZvdW5kYXRpb24gKEFTRikgdW5kZXIgb25lXG4jIG9yIG1vcmUgY29udHJpYnV0b3IgbGljZW5zZSBhZ3JlZW1lbnRzLiAgU2VlIHRoZSBOT1RJQ0UgZmlsZVxuIyBkaXN0cmlidXRlZCB3aXRoIHRoaXMgd29yayBmb3IgYWRkaXRpb25hbCBpbmZvcm1hdGlvblxuIyByZWdhcmRpbmcgY29weXJpZ2h0IG93bmVyc2hpcC4gIFRoZSBBU0YgbGljZW5zZXMgdGhpcyBmaWxlXG4jIHRvIHlvdSB1bmRlciB0aGUgQXBhY2hlIExpY2Vuc2UsIFZlcnNpb24gMi4wICh0aGVcbiMgXCJMaWNlbnNlXCIpOyB5b3UgbWF5IG5vdCB1c2UgdGhpcyBmaWxlIGV4Y2VwdCBpbiBjb21wbGlhbmNlXG4jIHdpdGggdGhlIExpY2Vuc2UuICBZb3UgbWF5IG9idGFpbiBhIGNvcHkgb2YgdGhlIExpY2Vuc2UgYXRcbiNcbiMgICAgIGh0dHA6Ly93d3cuYXBhY2hlLm9yZy9saWNlbnNlcy9MSUNFTlNFLTIuMFxuI1xuIyBVbmxlc3MgcmVxdWlyZWQgYnkgYXBwbGljYWJsZSBsYXcgb3IgYWdyZWVkIHRvIGluIHdyaXRpbmcsIHNvZnR3YXJlXG4jIGRpc3RyaWJ1dGVkIHVuZGVyIHRoZSBMaWNlbnNlIGlzIGRpc3RyaWJ1dGVkIG9uIGFuIFwiQVMgSVNcIiBCQVNJUyxcbiMgV0lUSE9VVCBXQVJSQU5USUVTIE9SIENPTkRJVElPTlMgT0YgQU5ZIEtJTkQsIGVpdGhlciBleHByZXNzIG9yIGltcGxpZWQuXG4jIFNlZSB0aGUgTGljZW5zZSBmb3IgdGhlIHNwZWNpZmljIGxhbmd1YWdlIGdvdmVybmluZyBwZXJtaXNzaW9ucyBhbmRcbiMgbGltaXRhdGlvbnMgdW5kZXIgdGhlIExpY2Vuc2UuXG4jXG5cbmFuZ3VsYXIubW9kdWxlKCdmbGlua0FwcCcpXG5cbi5zZXJ2aWNlICdKb2JNYW5hZ2VyQ29uZmlnU2VydmljZScsICgkaHR0cCwgZmxpbmtDb25maWcsICRxKSAtPlxuICBjb25maWcgPSB7fVxuXG4gIEBsb2FkQ29uZmlnID0gLT5cbiAgICBkZWZlcnJlZCA9ICRxLmRlZmVyKClcblxuICAgICRodHRwLmdldChmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcImpvYm1hbmFnZXIvY29uZmlnXCIpXG4gICAgLnN1Y2Nlc3MgKGRhdGEsIHN0YXR1cywgaGVhZGVycywgY29uZmlnKSAtPlxuICAgICAgY29uZmlnID0gZGF0YVxuICAgICAgZGVmZXJyZWQucmVzb2x2ZShkYXRhKVxuXG4gICAgZGVmZXJyZWQucHJvbWlzZVxuXG4gIEBcblxuLnNlcnZpY2UgJ0pvYk1hbmFnZXJMb2dzU2VydmljZScsICgkaHR0cCwgZmxpbmtDb25maWcsICRxKSAtPlxuICBsb2dzID0ge31cblxuICBAbG9hZExvZ3MgPSAtPlxuICAgIGRlZmVycmVkID0gJHEuZGVmZXIoKVxuXG4gICAgJGh0dHAuZ2V0KGZsaW5rQ29uZmlnLmpvYlNlcnZlciArIFwiam9ibWFuYWdlci9sb2dcIilcbiAgICAuc3VjY2VzcyAoZGF0YSwgc3RhdHVzLCBoZWFkZXJzLCBjb25maWcpIC0+XG4gICAgICBsb2dzID0gZGF0YVxuICAgICAgZGVmZXJyZWQucmVzb2x2ZShkYXRhKVxuXG4gICAgZGVmZXJyZWQucHJvbWlzZVxuXG4gIEBcblxuLnNlcnZpY2UgJ0pvYk1hbmFnZXJTdGRvdXRTZXJ2aWNlJywgKCRodHRwLCBmbGlua0NvbmZpZywgJHEpIC0+XG4gIHN0ZG91dCA9IHt9XG5cbiAgQGxvYWRTdGRvdXQgPSAtPlxuICAgIGRlZmVycmVkID0gJHEuZGVmZXIoKVxuXG4gICAgJGh0dHAuZ2V0KGZsaW5rQ29uZmlnLmpvYlNlcnZlciArIFwiam9ibWFuYWdlci9zdGRvdXRcIilcbiAgICAuc3VjY2VzcyAoZGF0YSwgc3RhdHVzLCBoZWFkZXJzLCBjb25maWcpIC0+XG4gICAgICBzdGRvdXQgPSBkYXRhXG4gICAgICBkZWZlcnJlZC5yZXNvbHZlKGRhdGEpXG5cbiAgICBkZWZlcnJlZC5wcm9taXNlXG5cbiAgQFxuIiwiYW5ndWxhci5tb2R1bGUoJ2ZsaW5rQXBwJykuc2VydmljZSgnSm9iTWFuYWdlckNvbmZpZ1NlcnZpY2UnLCBmdW5jdGlvbigkaHR0cCwgZmxpbmtDb25maWcsICRxKSB7XG4gIHZhciBjb25maWc7XG4gIGNvbmZpZyA9IHt9O1xuICB0aGlzLmxvYWRDb25maWcgPSBmdW5jdGlvbigpIHtcbiAgICB2YXIgZGVmZXJyZWQ7XG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpO1xuICAgICRodHRwLmdldChmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcImpvYm1hbmFnZXIvY29uZmlnXCIpLnN1Y2Nlc3MoZnVuY3Rpb24oZGF0YSwgc3RhdHVzLCBoZWFkZXJzLCBjb25maWcpIHtcbiAgICAgIGNvbmZpZyA9IGRhdGE7XG4gICAgICByZXR1cm4gZGVmZXJyZWQucmVzb2x2ZShkYXRhKTtcbiAgICB9KTtcbiAgICByZXR1cm4gZGVmZXJyZWQucHJvbWlzZTtcbiAgfTtcbiAgcmV0dXJuIHRoaXM7XG59KS5zZXJ2aWNlKCdKb2JNYW5hZ2VyTG9nc1NlcnZpY2UnLCBmdW5jdGlvbigkaHR0cCwgZmxpbmtDb25maWcsICRxKSB7XG4gIHZhciBsb2dzO1xuICBsb2dzID0ge307XG4gIHRoaXMubG9hZExvZ3MgPSBmdW5jdGlvbigpIHtcbiAgICB2YXIgZGVmZXJyZWQ7XG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpO1xuICAgICRodHRwLmdldChmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcImpvYm1hbmFnZXIvbG9nXCIpLnN1Y2Nlc3MoZnVuY3Rpb24oZGF0YSwgc3RhdHVzLCBoZWFkZXJzLCBjb25maWcpIHtcbiAgICAgIGxvZ3MgPSBkYXRhO1xuICAgICAgcmV0dXJuIGRlZmVycmVkLnJlc29sdmUoZGF0YSk7XG4gICAgfSk7XG4gICAgcmV0dXJuIGRlZmVycmVkLnByb21pc2U7XG4gIH07XG4gIHJldHVybiB0aGlzO1xufSkuc2VydmljZSgnSm9iTWFuYWdlclN0ZG91dFNlcnZpY2UnLCBmdW5jdGlvbigkaHR0cCwgZmxpbmtDb25maWcsICRxKSB7XG4gIHZhciBzdGRvdXQ7XG4gIHN0ZG91dCA9IHt9O1xuICB0aGlzLmxvYWRTdGRvdXQgPSBmdW5jdGlvbigpIHtcbiAgICB2YXIgZGVmZXJyZWQ7XG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpO1xuICAgICRodHRwLmdldChmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcImpvYm1hbmFnZXIvc3Rkb3V0XCIpLnN1Y2Nlc3MoZnVuY3Rpb24oZGF0YSwgc3RhdHVzLCBoZWFkZXJzLCBjb25maWcpIHtcbiAgICAgIHN0ZG91dCA9IGRhdGE7XG4gICAgICByZXR1cm4gZGVmZXJyZWQucmVzb2x2ZShkYXRhKTtcbiAgICB9KTtcbiAgICByZXR1cm4gZGVmZXJyZWQucHJvbWlzZTtcbiAgfTtcbiAgcmV0dXJuIHRoaXM7XG59KTtcbiIsIiNcbiMgTGljZW5zZWQgdG8gdGhlIEFwYWNoZSBTb2Z0d2FyZSBGb3VuZGF0aW9uIChBU0YpIHVuZGVyIG9uZVxuIyBvciBtb3JlIGNvbnRyaWJ1dG9yIGxpY2Vuc2UgYWdyZWVtZW50cy4gIFNlZSB0aGUgTk9USUNFIGZpbGVcbiMgZGlzdHJpYnV0ZWQgd2l0aCB0aGlzIHdvcmsgZm9yIGFkZGl0aW9uYWwgaW5mb3JtYXRpb25cbiMgcmVnYXJkaW5nIGNvcHlyaWdodCBvd25lcnNoaXAuICBUaGUgQVNGIGxpY2Vuc2VzIHRoaXMgZmlsZVxuIyB0byB5b3UgdW5kZXIgdGhlIEFwYWNoZSBMaWNlbnNlLCBWZXJzaW9uIDIuMCAodGhlXG4jIFwiTGljZW5zZVwiKTsgeW91IG1heSBub3QgdXNlIHRoaXMgZmlsZSBleGNlcHQgaW4gY29tcGxpYW5jZVxuIyB3aXRoIHRoZSBMaWNlbnNlLiAgWW91IG1heSBvYnRhaW4gYSBjb3B5IG9mIHRoZSBMaWNlbnNlIGF0XG4jXG4jICAgICBodHRwOi8vd3d3LmFwYWNoZS5vcmcvbGljZW5zZXMvTElDRU5TRS0yLjBcbiNcbiMgVW5sZXNzIHJlcXVpcmVkIGJ5IGFwcGxpY2FibGUgbGF3IG9yIGFncmVlZCB0byBpbiB3cml0aW5nLCBzb2Z0d2FyZVxuIyBkaXN0cmlidXRlZCB1bmRlciB0aGUgTGljZW5zZSBpcyBkaXN0cmlidXRlZCBvbiBhbiBcIkFTIElTXCIgQkFTSVMsXG4jIFdJVEhPVVQgV0FSUkFOVElFUyBPUiBDT05ESVRJT05TIE9GIEFOWSBLSU5ELCBlaXRoZXIgZXhwcmVzcyBvciBpbXBsaWVkLlxuIyBTZWUgdGhlIExpY2Vuc2UgZm9yIHRoZSBzcGVjaWZpYyBsYW5ndWFnZSBnb3Zlcm5pbmcgcGVybWlzc2lvbnMgYW5kXG4jIGxpbWl0YXRpb25zIHVuZGVyIHRoZSBMaWNlbnNlLlxuI1xuXG5hbmd1bGFyLm1vZHVsZSgnZmxpbmtBcHAnKVxuXG4uY29udHJvbGxlciAnUnVubmluZ0pvYnNDb250cm9sbGVyJywgKCRzY29wZSwgJHN0YXRlLCAkc3RhdGVQYXJhbXMsIEpvYnNTZXJ2aWNlKSAtPlxuICAkc2NvcGUuam9iT2JzZXJ2ZXIgPSAtPlxuICAgICRzY29wZS5qb2JzID0gSm9ic1NlcnZpY2UuZ2V0Sm9icygncnVubmluZycpXG5cbiAgSm9ic1NlcnZpY2UucmVnaXN0ZXJPYnNlcnZlcigkc2NvcGUuam9iT2JzZXJ2ZXIpXG4gICRzY29wZS4kb24gJyRkZXN0cm95JywgLT5cbiAgICBKb2JzU2VydmljZS51blJlZ2lzdGVyT2JzZXJ2ZXIoJHNjb3BlLmpvYk9ic2VydmVyKVxuXG4gICRzY29wZS5qb2JPYnNlcnZlcigpXG5cbiMgLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS1cblxuLmNvbnRyb2xsZXIgJ0NvbXBsZXRlZEpvYnNDb250cm9sbGVyJywgKCRzY29wZSwgJHN0YXRlLCAkc3RhdGVQYXJhbXMsIEpvYnNTZXJ2aWNlKSAtPlxuICAkc2NvcGUuam9iT2JzZXJ2ZXIgPSAtPlxuICAgICRzY29wZS5qb2JzID0gSm9ic1NlcnZpY2UuZ2V0Sm9icygnZmluaXNoZWQnKVxuXG4gIEpvYnNTZXJ2aWNlLnJlZ2lzdGVyT2JzZXJ2ZXIoJHNjb3BlLmpvYk9ic2VydmVyKVxuICAkc2NvcGUuJG9uICckZGVzdHJveScsIC0+XG4gICAgSm9ic1NlcnZpY2UudW5SZWdpc3Rlck9ic2VydmVyKCRzY29wZS5qb2JPYnNlcnZlcilcblxuICAkc2NvcGUuam9iT2JzZXJ2ZXIoKVxuXG4jIC0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tXG5cbi5jb250cm9sbGVyICdTaW5nbGVKb2JDb250cm9sbGVyJywgKCRzY29wZSwgJHN0YXRlLCAkc3RhdGVQYXJhbXMsIEpvYnNTZXJ2aWNlLCAkcm9vdFNjb3BlLCBmbGlua0NvbmZpZywgJGludGVydmFsKSAtPlxuICBjb25zb2xlLmxvZyAnU2luZ2xlSm9iQ29udHJvbGxlcidcblxuICAkc2NvcGUuam9iaWQgPSAkc3RhdGVQYXJhbXMuam9iaWRcbiAgJHNjb3BlLmpvYiA9IG51bGxcbiAgJHNjb3BlLnBsYW4gPSBudWxsXG4gICRzY29wZS52ZXJ0aWNlcyA9IG51bGxcbiAgJHNjb3BlLmpvYkNoZWNrcG9pbnRTdGF0cyA9IG51bGxcbiAgJHNjb3BlLnNob3dIaXN0b3J5ID0gZmFsc2VcbiAgJHNjb3BlLmJhY2tQcmVzc3VyZU9wZXJhdG9yU3RhdHMgPSB7fVxuXG4gIEpvYnNTZXJ2aWNlLmxvYWRKb2IoJHN0YXRlUGFyYW1zLmpvYmlkKS50aGVuIChkYXRhKSAtPlxuICAgICRzY29wZS5qb2IgPSBkYXRhXG4gICAgJHNjb3BlLnBsYW4gPSBkYXRhLnBsYW5cbiAgICAkc2NvcGUudmVydGljZXMgPSBkYXRhLnZlcnRpY2VzXG5cbiAgcmVmcmVzaGVyID0gJGludGVydmFsIC0+XG4gICAgSm9ic1NlcnZpY2UubG9hZEpvYigkc3RhdGVQYXJhbXMuam9iaWQpLnRoZW4gKGRhdGEpIC0+XG4gICAgICAkc2NvcGUuam9iID0gZGF0YVxuXG4gICAgICAkc2NvcGUuJGJyb2FkY2FzdCAncmVsb2FkJ1xuXG4gICwgZmxpbmtDb25maWdbXCJyZWZyZXNoLWludGVydmFsXCJdXG5cbiAgJHNjb3BlLiRvbiAnJGRlc3Ryb3knLCAtPlxuICAgICRzY29wZS5qb2IgPSBudWxsXG4gICAgJHNjb3BlLnBsYW4gPSBudWxsXG4gICAgJHNjb3BlLnZlcnRpY2VzID0gbnVsbFxuICAgICRzY29wZS5qb2JDaGVja3BvaW50U3RhdHMgPSBudWxsXG4gICAgJHNjb3BlLmJhY2tQcmVzc3VyZU9wZXJhdG9yU3RhdHMgPSBudWxsXG5cbiAgICAkaW50ZXJ2YWwuY2FuY2VsKHJlZnJlc2hlcilcblxuICAkc2NvcGUuY2FuY2VsSm9iID0gKGNhbmNlbEV2ZW50KSAtPlxuICAgIGFuZ3VsYXIuZWxlbWVudChjYW5jZWxFdmVudC5jdXJyZW50VGFyZ2V0KS5yZW1vdmVDbGFzcyhcImJ0blwiKS5yZW1vdmVDbGFzcyhcImJ0bi1kZWZhdWx0XCIpLmh0bWwoJ0NhbmNlbGxpbmcuLi4nKVxuICAgIEpvYnNTZXJ2aWNlLmNhbmNlbEpvYigkc3RhdGVQYXJhbXMuam9iaWQpLnRoZW4gKGRhdGEpIC0+XG4gICAgICB7fVxuXG4gICRzY29wZS5zdG9wSm9iID0gKHN0b3BFdmVudCkgLT5cbiAgICBhbmd1bGFyLmVsZW1lbnQoc3RvcEV2ZW50LmN1cnJlbnRUYXJnZXQpLnJlbW92ZUNsYXNzKFwiYnRuXCIpLnJlbW92ZUNsYXNzKFwiYnRuLWRlZmF1bHRcIikuaHRtbCgnU3RvcHBpbmcuLi4nKVxuICAgIEpvYnNTZXJ2aWNlLnN0b3BKb2IoJHN0YXRlUGFyYW1zLmpvYmlkKS50aGVuIChkYXRhKSAtPlxuICAgICAge31cblxuICAkc2NvcGUudG9nZ2xlSGlzdG9yeSA9IC0+XG4gICAgJHNjb3BlLnNob3dIaXN0b3J5ID0gISRzY29wZS5zaG93SGlzdG9yeVxuXG4jIC0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tXG5cbi5jb250cm9sbGVyICdKb2JQbGFuQ29udHJvbGxlcicsICgkc2NvcGUsICRzdGF0ZSwgJHN0YXRlUGFyYW1zLCBKb2JzU2VydmljZSkgLT5cbiAgY29uc29sZS5sb2cgJ0pvYlBsYW5Db250cm9sbGVyJ1xuXG4gICRzY29wZS5ub2RlaWQgPSBudWxsXG4gICRzY29wZS5ub2RlVW5mb2xkZWQgPSBmYWxzZVxuICAkc2NvcGUuc3RhdGVMaXN0ID0gSm9ic1NlcnZpY2Uuc3RhdGVMaXN0KClcblxuICAkc2NvcGUuY2hhbmdlTm9kZSA9IChub2RlaWQpIC0+XG4gICAgaWYgbm9kZWlkICE9ICRzY29wZS5ub2RlaWRcbiAgICAgICRzY29wZS5ub2RlaWQgPSBub2RlaWRcbiAgICAgICRzY29wZS52ZXJ0ZXggPSBudWxsXG4gICAgICAkc2NvcGUuc3VidGFza3MgPSBudWxsXG4gICAgICAkc2NvcGUuYWNjdW11bGF0b3JzID0gbnVsbFxuICAgICAgJHNjb3BlLm9wZXJhdG9yQ2hlY2twb2ludFN0YXRzID0gbnVsbFxuXG4gICAgICAkc2NvcGUuJGJyb2FkY2FzdCAncmVsb2FkJ1xuXG4gICAgZWxzZVxuICAgICAgJHNjb3BlLm5vZGVpZCA9IG51bGxcbiAgICAgICRzY29wZS5ub2RlVW5mb2xkZWQgPSBmYWxzZVxuICAgICAgJHNjb3BlLnZlcnRleCA9IG51bGxcbiAgICAgICRzY29wZS5zdWJ0YXNrcyA9IG51bGxcbiAgICAgICRzY29wZS5hY2N1bXVsYXRvcnMgPSBudWxsXG4gICAgICAkc2NvcGUub3BlcmF0b3JDaGVja3BvaW50U3RhdHMgPSBudWxsXG5cbiAgJHNjb3BlLmRlYWN0aXZhdGVOb2RlID0gLT5cbiAgICAkc2NvcGUubm9kZWlkID0gbnVsbFxuICAgICRzY29wZS5ub2RlVW5mb2xkZWQgPSBmYWxzZVxuICAgICRzY29wZS52ZXJ0ZXggPSBudWxsXG4gICAgJHNjb3BlLnN1YnRhc2tzID0gbnVsbFxuICAgICRzY29wZS5hY2N1bXVsYXRvcnMgPSBudWxsXG4gICAgJHNjb3BlLm9wZXJhdG9yQ2hlY2twb2ludFN0YXRzID0gbnVsbFxuXG4gICRzY29wZS50b2dnbGVGb2xkID0gLT5cbiAgICAkc2NvcGUubm9kZVVuZm9sZGVkID0gISRzY29wZS5ub2RlVW5mb2xkZWRcblxuIyAtLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLVxuXG4uY29udHJvbGxlciAnSm9iUGxhblN1YnRhc2tzQ29udHJvbGxlcicsICgkc2NvcGUsIEpvYnNTZXJ2aWNlKSAtPlxuICBjb25zb2xlLmxvZyAnSm9iUGxhblN1YnRhc2tzQ29udHJvbGxlcidcblxuICBpZiAkc2NvcGUubm9kZWlkIGFuZCAoISRzY29wZS52ZXJ0ZXggb3IgISRzY29wZS52ZXJ0ZXguc3QpXG4gICAgSm9ic1NlcnZpY2UuZ2V0U3VidGFza3MoJHNjb3BlLm5vZGVpZCkudGhlbiAoZGF0YSkgLT5cbiAgICAgICRzY29wZS5zdWJ0YXNrcyA9IGRhdGFcblxuICAkc2NvcGUuJG9uICdyZWxvYWQnLCAoZXZlbnQpIC0+XG4gICAgY29uc29sZS5sb2cgJ0pvYlBsYW5TdWJ0YXNrc0NvbnRyb2xsZXInXG4gICAgaWYgJHNjb3BlLm5vZGVpZFxuICAgICAgSm9ic1NlcnZpY2UuZ2V0U3VidGFza3MoJHNjb3BlLm5vZGVpZCkudGhlbiAoZGF0YSkgLT5cbiAgICAgICAgJHNjb3BlLnN1YnRhc2tzID0gZGF0YVxuXG4jIC0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tXG5cbi5jb250cm9sbGVyICdKb2JQbGFuVGFza01hbmFnZXJzQ29udHJvbGxlcicsICgkc2NvcGUsIEpvYnNTZXJ2aWNlKSAtPlxuICBjb25zb2xlLmxvZyAnSm9iUGxhblRhc2tNYW5hZ2Vyc0NvbnRyb2xsZXInXG5cbiAgaWYgJHNjb3BlLm5vZGVpZCBhbmQgKCEkc2NvcGUudmVydGV4IG9yICEkc2NvcGUudmVydGV4LnN0KVxuICAgIEpvYnNTZXJ2aWNlLmdldFRhc2tNYW5hZ2Vycygkc2NvcGUubm9kZWlkKS50aGVuIChkYXRhKSAtPlxuICAgICAgJHNjb3BlLnRhc2ttYW5hZ2VycyA9IGRhdGFcblxuICAkc2NvcGUuJG9uICdyZWxvYWQnLCAoZXZlbnQpIC0+XG4gICAgY29uc29sZS5sb2cgJ0pvYlBsYW5UYXNrTWFuYWdlcnNDb250cm9sbGVyJ1xuICAgIGlmICRzY29wZS5ub2RlaWRcbiAgICAgIEpvYnNTZXJ2aWNlLmdldFRhc2tNYW5hZ2Vycygkc2NvcGUubm9kZWlkKS50aGVuIChkYXRhKSAtPlxuICAgICAgICAkc2NvcGUudGFza21hbmFnZXJzID0gZGF0YVxuXG4jIC0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tXG5cbi5jb250cm9sbGVyICdKb2JQbGFuQWNjdW11bGF0b3JzQ29udHJvbGxlcicsICgkc2NvcGUsIEpvYnNTZXJ2aWNlKSAtPlxuICBjb25zb2xlLmxvZyAnSm9iUGxhbkFjY3VtdWxhdG9yc0NvbnRyb2xsZXInXG5cbiAgaWYgJHNjb3BlLm5vZGVpZCBhbmQgKCEkc2NvcGUudmVydGV4IG9yICEkc2NvcGUudmVydGV4LmFjY3VtdWxhdG9ycylcbiAgICBKb2JzU2VydmljZS5nZXRBY2N1bXVsYXRvcnMoJHNjb3BlLm5vZGVpZCkudGhlbiAoZGF0YSkgLT5cbiAgICAgICRzY29wZS5hY2N1bXVsYXRvcnMgPSBkYXRhLm1haW5cbiAgICAgICRzY29wZS5zdWJ0YXNrQWNjdW11bGF0b3JzID0gZGF0YS5zdWJ0YXNrc1xuXG4gICRzY29wZS4kb24gJ3JlbG9hZCcsIChldmVudCkgLT5cbiAgICBjb25zb2xlLmxvZyAnSm9iUGxhbkFjY3VtdWxhdG9yc0NvbnRyb2xsZXInXG4gICAgaWYgJHNjb3BlLm5vZGVpZFxuICAgICAgSm9ic1NlcnZpY2UuZ2V0QWNjdW11bGF0b3JzKCRzY29wZS5ub2RlaWQpLnRoZW4gKGRhdGEpIC0+XG4gICAgICAgICRzY29wZS5hY2N1bXVsYXRvcnMgPSBkYXRhLm1haW5cbiAgICAgICAgJHNjb3BlLnN1YnRhc2tBY2N1bXVsYXRvcnMgPSBkYXRhLnN1YnRhc2tzXG5cbiMgLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS1cblxuLmNvbnRyb2xsZXIgJ0pvYlBsYW5DaGVja3BvaW50c0NvbnRyb2xsZXInLCAoJHNjb3BlLCBKb2JzU2VydmljZSkgLT5cbiAgY29uc29sZS5sb2cgJ0pvYlBsYW5DaGVja3BvaW50c0NvbnRyb2xsZXInXG5cbiAgIyBHZXQgdGhlIHBlciBqb2Igc3RhdHNcbiAgSm9ic1NlcnZpY2UuZ2V0Sm9iQ2hlY2twb2ludFN0YXRzKCRzY29wZS5qb2JpZCkudGhlbiAoZGF0YSkgLT5cbiAgICAkc2NvcGUuam9iQ2hlY2twb2ludFN0YXRzID0gZGF0YVxuXG4gICMgR2V0IHRoZSBwZXIgb3BlcmF0b3Igc3RhdHNcbiAgaWYgJHNjb3BlLm5vZGVpZCBhbmQgKCEkc2NvcGUudmVydGV4IG9yICEkc2NvcGUudmVydGV4Lm9wZXJhdG9yQ2hlY2twb2ludFN0YXRzKVxuICAgIEpvYnNTZXJ2aWNlLmdldE9wZXJhdG9yQ2hlY2twb2ludFN0YXRzKCRzY29wZS5ub2RlaWQpLnRoZW4gKGRhdGEpIC0+XG4gICAgICAkc2NvcGUub3BlcmF0b3JDaGVja3BvaW50U3RhdHMgPSBkYXRhLm9wZXJhdG9yU3RhdHNcbiAgICAgICRzY29wZS5zdWJ0YXNrc0NoZWNrcG9pbnRTdGF0cyA9IGRhdGEuc3VidGFza3NTdGF0c1xuXG4gICRzY29wZS4kb24gJ3JlbG9hZCcsIChldmVudCkgLT5cbiAgICBjb25zb2xlLmxvZyAnSm9iUGxhbkNoZWNrcG9pbnRzQ29udHJvbGxlcidcblxuICAgIEpvYnNTZXJ2aWNlLmdldEpvYkNoZWNrcG9pbnRTdGF0cygkc2NvcGUuam9iaWQpLnRoZW4gKGRhdGEpIC0+XG4gICAgICAkc2NvcGUuam9iQ2hlY2twb2ludFN0YXRzID0gZGF0YVxuXG4gICAgaWYgJHNjb3BlLm5vZGVpZFxuICAgICAgSm9ic1NlcnZpY2UuZ2V0T3BlcmF0b3JDaGVja3BvaW50U3RhdHMoJHNjb3BlLm5vZGVpZCkudGhlbiAoZGF0YSkgLT5cbiAgICAgICAgJHNjb3BlLm9wZXJhdG9yQ2hlY2twb2ludFN0YXRzID0gZGF0YS5vcGVyYXRvclN0YXRzXG4gICAgICAgICRzY29wZS5zdWJ0YXNrc0NoZWNrcG9pbnRTdGF0cyA9IGRhdGEuc3VidGFza3NTdGF0c1xuXG4jIC0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tXG5cbi5jb250cm9sbGVyICdKb2JQbGFuQmFja1ByZXNzdXJlQ29udHJvbGxlcicsICgkc2NvcGUsIEpvYnNTZXJ2aWNlKSAtPlxuICBjb25zb2xlLmxvZyAnSm9iUGxhbkJhY2tQcmVzc3VyZUNvbnRyb2xsZXInXG4gICRzY29wZS5ub3cgPSBEYXRlLm5vdygpXG5cbiAgaWYgJHNjb3BlLm5vZGVpZFxuICAgIEpvYnNTZXJ2aWNlLmdldE9wZXJhdG9yQmFja1ByZXNzdXJlKCRzY29wZS5ub2RlaWQpLnRoZW4gKGRhdGEpIC0+XG4gICAgICAkc2NvcGUuYmFja1ByZXNzdXJlT3BlcmF0b3JTdGF0c1skc2NvcGUubm9kZWlkXSA9IGRhdGFcblxuICAkc2NvcGUuJG9uICdyZWxvYWQnLCAoZXZlbnQpIC0+XG4gICAgY29uc29sZS5sb2cgJ0pvYlBsYW5CYWNrUHJlc3N1cmVDb250cm9sbGVyIChyZWxhb2QpJ1xuICAgICRzY29wZS5ub3cgPSBEYXRlLm5vdygpXG5cbiAgICBpZiAkc2NvcGUubm9kZWlkXG4gICAgICBKb2JzU2VydmljZS5nZXRPcGVyYXRvckJhY2tQcmVzc3VyZSgkc2NvcGUubm9kZWlkKS50aGVuIChkYXRhKSAtPlxuICAgICAgICAkc2NvcGUuYmFja1ByZXNzdXJlT3BlcmF0b3JTdGF0c1skc2NvcGUubm9kZWlkXSA9IGRhdGFcblxuIyAtLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLVxuXG4uY29udHJvbGxlciAnSm9iVGltZWxpbmVWZXJ0ZXhDb250cm9sbGVyJywgKCRzY29wZSwgJHN0YXRlLCAkc3RhdGVQYXJhbXMsIEpvYnNTZXJ2aWNlKSAtPlxuICBjb25zb2xlLmxvZyAnSm9iVGltZWxpbmVWZXJ0ZXhDb250cm9sbGVyJ1xuXG4gIEpvYnNTZXJ2aWNlLmdldFZlcnRleCgkc3RhdGVQYXJhbXMudmVydGV4SWQpLnRoZW4gKGRhdGEpIC0+XG4gICAgJHNjb3BlLnZlcnRleCA9IGRhdGFcblxuICAkc2NvcGUuJG9uICdyZWxvYWQnLCAoZXZlbnQpIC0+XG4gICAgY29uc29sZS5sb2cgJ0pvYlRpbWVsaW5lVmVydGV4Q29udHJvbGxlcidcbiAgICBKb2JzU2VydmljZS5nZXRWZXJ0ZXgoJHN0YXRlUGFyYW1zLnZlcnRleElkKS50aGVuIChkYXRhKSAtPlxuICAgICAgJHNjb3BlLnZlcnRleCA9IGRhdGFcblxuIyAtLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLVxuXG4uY29udHJvbGxlciAnSm9iRXhjZXB0aW9uc0NvbnRyb2xsZXInLCAoJHNjb3BlLCAkc3RhdGUsICRzdGF0ZVBhcmFtcywgSm9ic1NlcnZpY2UpIC0+XG4gIEpvYnNTZXJ2aWNlLmxvYWRFeGNlcHRpb25zKCkudGhlbiAoZGF0YSkgLT5cbiAgICAkc2NvcGUuZXhjZXB0aW9ucyA9IGRhdGFcblxuIyAtLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLVxuXG4uY29udHJvbGxlciAnSm9iUHJvcGVydGllc0NvbnRyb2xsZXInLCAoJHNjb3BlLCBKb2JzU2VydmljZSkgLT5cbiAgY29uc29sZS5sb2cgJ0pvYlByb3BlcnRpZXNDb250cm9sbGVyJ1xuXG4gICRzY29wZS5jaGFuZ2VOb2RlID0gKG5vZGVpZCkgLT5cbiAgICBpZiBub2RlaWQgIT0gJHNjb3BlLm5vZGVpZFxuICAgICAgJHNjb3BlLm5vZGVpZCA9IG5vZGVpZFxuXG4gICAgICBKb2JzU2VydmljZS5nZXROb2RlKG5vZGVpZCkudGhlbiAoZGF0YSkgLT5cbiAgICAgICAgJHNjb3BlLm5vZGUgPSBkYXRhXG5cbiAgICBlbHNlXG4gICAgICAkc2NvcGUubm9kZWlkID0gbnVsbFxuICAgICAgJHNjb3BlLm5vZGUgPSBudWxsXG4iLCJhbmd1bGFyLm1vZHVsZSgnZmxpbmtBcHAnKS5jb250cm9sbGVyKCdSdW5uaW5nSm9ic0NvbnRyb2xsZXInLCBmdW5jdGlvbigkc2NvcGUsICRzdGF0ZSwgJHN0YXRlUGFyYW1zLCBKb2JzU2VydmljZSkge1xuICAkc2NvcGUuam9iT2JzZXJ2ZXIgPSBmdW5jdGlvbigpIHtcbiAgICByZXR1cm4gJHNjb3BlLmpvYnMgPSBKb2JzU2VydmljZS5nZXRKb2JzKCdydW5uaW5nJyk7XG4gIH07XG4gIEpvYnNTZXJ2aWNlLnJlZ2lzdGVyT2JzZXJ2ZXIoJHNjb3BlLmpvYk9ic2VydmVyKTtcbiAgJHNjb3BlLiRvbignJGRlc3Ryb3knLCBmdW5jdGlvbigpIHtcbiAgICByZXR1cm4gSm9ic1NlcnZpY2UudW5SZWdpc3Rlck9ic2VydmVyKCRzY29wZS5qb2JPYnNlcnZlcik7XG4gIH0pO1xuICByZXR1cm4gJHNjb3BlLmpvYk9ic2VydmVyKCk7XG59KS5jb250cm9sbGVyKCdDb21wbGV0ZWRKb2JzQ29udHJvbGxlcicsIGZ1bmN0aW9uKCRzY29wZSwgJHN0YXRlLCAkc3RhdGVQYXJhbXMsIEpvYnNTZXJ2aWNlKSB7XG4gICRzY29wZS5qb2JPYnNlcnZlciA9IGZ1bmN0aW9uKCkge1xuICAgIHJldHVybiAkc2NvcGUuam9icyA9IEpvYnNTZXJ2aWNlLmdldEpvYnMoJ2ZpbmlzaGVkJyk7XG4gIH07XG4gIEpvYnNTZXJ2aWNlLnJlZ2lzdGVyT2JzZXJ2ZXIoJHNjb3BlLmpvYk9ic2VydmVyKTtcbiAgJHNjb3BlLiRvbignJGRlc3Ryb3knLCBmdW5jdGlvbigpIHtcbiAgICByZXR1cm4gSm9ic1NlcnZpY2UudW5SZWdpc3Rlck9ic2VydmVyKCRzY29wZS5qb2JPYnNlcnZlcik7XG4gIH0pO1xuICByZXR1cm4gJHNjb3BlLmpvYk9ic2VydmVyKCk7XG59KS5jb250cm9sbGVyKCdTaW5nbGVKb2JDb250cm9sbGVyJywgZnVuY3Rpb24oJHNjb3BlLCAkc3RhdGUsICRzdGF0ZVBhcmFtcywgSm9ic1NlcnZpY2UsICRyb290U2NvcGUsIGZsaW5rQ29uZmlnLCAkaW50ZXJ2YWwpIHtcbiAgdmFyIHJlZnJlc2hlcjtcbiAgY29uc29sZS5sb2coJ1NpbmdsZUpvYkNvbnRyb2xsZXInKTtcbiAgJHNjb3BlLmpvYmlkID0gJHN0YXRlUGFyYW1zLmpvYmlkO1xuICAkc2NvcGUuam9iID0gbnVsbDtcbiAgJHNjb3BlLnBsYW4gPSBudWxsO1xuICAkc2NvcGUudmVydGljZXMgPSBudWxsO1xuICAkc2NvcGUuam9iQ2hlY2twb2ludFN0YXRzID0gbnVsbDtcbiAgJHNjb3BlLnNob3dIaXN0b3J5ID0gZmFsc2U7XG4gICRzY29wZS5iYWNrUHJlc3N1cmVPcGVyYXRvclN0YXRzID0ge307XG4gIEpvYnNTZXJ2aWNlLmxvYWRKb2IoJHN0YXRlUGFyYW1zLmpvYmlkKS50aGVuKGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAkc2NvcGUuam9iID0gZGF0YTtcbiAgICAkc2NvcGUucGxhbiA9IGRhdGEucGxhbjtcbiAgICByZXR1cm4gJHNjb3BlLnZlcnRpY2VzID0gZGF0YS52ZXJ0aWNlcztcbiAgfSk7XG4gIHJlZnJlc2hlciA9ICRpbnRlcnZhbChmdW5jdGlvbigpIHtcbiAgICByZXR1cm4gSm9ic1NlcnZpY2UubG9hZEpvYigkc3RhdGVQYXJhbXMuam9iaWQpLnRoZW4oZnVuY3Rpb24oZGF0YSkge1xuICAgICAgJHNjb3BlLmpvYiA9IGRhdGE7XG4gICAgICByZXR1cm4gJHNjb3BlLiRicm9hZGNhc3QoJ3JlbG9hZCcpO1xuICAgIH0pO1xuICB9LCBmbGlua0NvbmZpZ1tcInJlZnJlc2gtaW50ZXJ2YWxcIl0pO1xuICAkc2NvcGUuJG9uKCckZGVzdHJveScsIGZ1bmN0aW9uKCkge1xuICAgICRzY29wZS5qb2IgPSBudWxsO1xuICAgICRzY29wZS5wbGFuID0gbnVsbDtcbiAgICAkc2NvcGUudmVydGljZXMgPSBudWxsO1xuICAgICRzY29wZS5qb2JDaGVja3BvaW50U3RhdHMgPSBudWxsO1xuICAgICRzY29wZS5iYWNrUHJlc3N1cmVPcGVyYXRvclN0YXRzID0gbnVsbDtcbiAgICByZXR1cm4gJGludGVydmFsLmNhbmNlbChyZWZyZXNoZXIpO1xuICB9KTtcbiAgJHNjb3BlLmNhbmNlbEpvYiA9IGZ1bmN0aW9uKGNhbmNlbEV2ZW50KSB7XG4gICAgYW5ndWxhci5lbGVtZW50KGNhbmNlbEV2ZW50LmN1cnJlbnRUYXJnZXQpLnJlbW92ZUNsYXNzKFwiYnRuXCIpLnJlbW92ZUNsYXNzKFwiYnRuLWRlZmF1bHRcIikuaHRtbCgnQ2FuY2VsbGluZy4uLicpO1xuICAgIHJldHVybiBKb2JzU2VydmljZS5jYW5jZWxKb2IoJHN0YXRlUGFyYW1zLmpvYmlkKS50aGVuKGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgIHJldHVybiB7fTtcbiAgICB9KTtcbiAgfTtcbiAgJHNjb3BlLnN0b3BKb2IgPSBmdW5jdGlvbihzdG9wRXZlbnQpIHtcbiAgICBhbmd1bGFyLmVsZW1lbnQoc3RvcEV2ZW50LmN1cnJlbnRUYXJnZXQpLnJlbW92ZUNsYXNzKFwiYnRuXCIpLnJlbW92ZUNsYXNzKFwiYnRuLWRlZmF1bHRcIikuaHRtbCgnU3RvcHBpbmcuLi4nKTtcbiAgICByZXR1cm4gSm9ic1NlcnZpY2Uuc3RvcEpvYigkc3RhdGVQYXJhbXMuam9iaWQpLnRoZW4oZnVuY3Rpb24oZGF0YSkge1xuICAgICAgcmV0dXJuIHt9O1xuICAgIH0pO1xuICB9O1xuICByZXR1cm4gJHNjb3BlLnRvZ2dsZUhpc3RvcnkgPSBmdW5jdGlvbigpIHtcbiAgICByZXR1cm4gJHNjb3BlLnNob3dIaXN0b3J5ID0gISRzY29wZS5zaG93SGlzdG9yeTtcbiAgfTtcbn0pLmNvbnRyb2xsZXIoJ0pvYlBsYW5Db250cm9sbGVyJywgZnVuY3Rpb24oJHNjb3BlLCAkc3RhdGUsICRzdGF0ZVBhcmFtcywgSm9ic1NlcnZpY2UpIHtcbiAgY29uc29sZS5sb2coJ0pvYlBsYW5Db250cm9sbGVyJyk7XG4gICRzY29wZS5ub2RlaWQgPSBudWxsO1xuICAkc2NvcGUubm9kZVVuZm9sZGVkID0gZmFsc2U7XG4gICRzY29wZS5zdGF0ZUxpc3QgPSBKb2JzU2VydmljZS5zdGF0ZUxpc3QoKTtcbiAgJHNjb3BlLmNoYW5nZU5vZGUgPSBmdW5jdGlvbihub2RlaWQpIHtcbiAgICBpZiAobm9kZWlkICE9PSAkc2NvcGUubm9kZWlkKSB7XG4gICAgICAkc2NvcGUubm9kZWlkID0gbm9kZWlkO1xuICAgICAgJHNjb3BlLnZlcnRleCA9IG51bGw7XG4gICAgICAkc2NvcGUuc3VidGFza3MgPSBudWxsO1xuICAgICAgJHNjb3BlLmFjY3VtdWxhdG9ycyA9IG51bGw7XG4gICAgICAkc2NvcGUub3BlcmF0b3JDaGVja3BvaW50U3RhdHMgPSBudWxsO1xuICAgICAgcmV0dXJuICRzY29wZS4kYnJvYWRjYXN0KCdyZWxvYWQnKTtcbiAgICB9IGVsc2Uge1xuICAgICAgJHNjb3BlLm5vZGVpZCA9IG51bGw7XG4gICAgICAkc2NvcGUubm9kZVVuZm9sZGVkID0gZmFsc2U7XG4gICAgICAkc2NvcGUudmVydGV4ID0gbnVsbDtcbiAgICAgICRzY29wZS5zdWJ0YXNrcyA9IG51bGw7XG4gICAgICAkc2NvcGUuYWNjdW11bGF0b3JzID0gbnVsbDtcbiAgICAgIHJldHVybiAkc2NvcGUub3BlcmF0b3JDaGVja3BvaW50U3RhdHMgPSBudWxsO1xuICAgIH1cbiAgfTtcbiAgJHNjb3BlLmRlYWN0aXZhdGVOb2RlID0gZnVuY3Rpb24oKSB7XG4gICAgJHNjb3BlLm5vZGVpZCA9IG51bGw7XG4gICAgJHNjb3BlLm5vZGVVbmZvbGRlZCA9IGZhbHNlO1xuICAgICRzY29wZS52ZXJ0ZXggPSBudWxsO1xuICAgICRzY29wZS5zdWJ0YXNrcyA9IG51bGw7XG4gICAgJHNjb3BlLmFjY3VtdWxhdG9ycyA9IG51bGw7XG4gICAgcmV0dXJuICRzY29wZS5vcGVyYXRvckNoZWNrcG9pbnRTdGF0cyA9IG51bGw7XG4gIH07XG4gIHJldHVybiAkc2NvcGUudG9nZ2xlRm9sZCA9IGZ1bmN0aW9uKCkge1xuICAgIHJldHVybiAkc2NvcGUubm9kZVVuZm9sZGVkID0gISRzY29wZS5ub2RlVW5mb2xkZWQ7XG4gIH07XG59KS5jb250cm9sbGVyKCdKb2JQbGFuU3VidGFza3NDb250cm9sbGVyJywgZnVuY3Rpb24oJHNjb3BlLCBKb2JzU2VydmljZSkge1xuICBjb25zb2xlLmxvZygnSm9iUGxhblN1YnRhc2tzQ29udHJvbGxlcicpO1xuICBpZiAoJHNjb3BlLm5vZGVpZCAmJiAoISRzY29wZS52ZXJ0ZXggfHwgISRzY29wZS52ZXJ0ZXguc3QpKSB7XG4gICAgSm9ic1NlcnZpY2UuZ2V0U3VidGFza3MoJHNjb3BlLm5vZGVpZCkudGhlbihmdW5jdGlvbihkYXRhKSB7XG4gICAgICByZXR1cm4gJHNjb3BlLnN1YnRhc2tzID0gZGF0YTtcbiAgICB9KTtcbiAgfVxuICByZXR1cm4gJHNjb3BlLiRvbigncmVsb2FkJywgZnVuY3Rpb24oZXZlbnQpIHtcbiAgICBjb25zb2xlLmxvZygnSm9iUGxhblN1YnRhc2tzQ29udHJvbGxlcicpO1xuICAgIGlmICgkc2NvcGUubm9kZWlkKSB7XG4gICAgICByZXR1cm4gSm9ic1NlcnZpY2UuZ2V0U3VidGFza3MoJHNjb3BlLm5vZGVpZCkudGhlbihmdW5jdGlvbihkYXRhKSB7XG4gICAgICAgIHJldHVybiAkc2NvcGUuc3VidGFza3MgPSBkYXRhO1xuICAgICAgfSk7XG4gICAgfVxuICB9KTtcbn0pLmNvbnRyb2xsZXIoJ0pvYlBsYW5UYXNrTWFuYWdlcnNDb250cm9sbGVyJywgZnVuY3Rpb24oJHNjb3BlLCBKb2JzU2VydmljZSkge1xuICBjb25zb2xlLmxvZygnSm9iUGxhblRhc2tNYW5hZ2Vyc0NvbnRyb2xsZXInKTtcbiAgaWYgKCRzY29wZS5ub2RlaWQgJiYgKCEkc2NvcGUudmVydGV4IHx8ICEkc2NvcGUudmVydGV4LnN0KSkge1xuICAgIEpvYnNTZXJ2aWNlLmdldFRhc2tNYW5hZ2Vycygkc2NvcGUubm9kZWlkKS50aGVuKGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgIHJldHVybiAkc2NvcGUudGFza21hbmFnZXJzID0gZGF0YTtcbiAgICB9KTtcbiAgfVxuICByZXR1cm4gJHNjb3BlLiRvbigncmVsb2FkJywgZnVuY3Rpb24oZXZlbnQpIHtcbiAgICBjb25zb2xlLmxvZygnSm9iUGxhblRhc2tNYW5hZ2Vyc0NvbnRyb2xsZXInKTtcbiAgICBpZiAoJHNjb3BlLm5vZGVpZCkge1xuICAgICAgcmV0dXJuIEpvYnNTZXJ2aWNlLmdldFRhc2tNYW5hZ2Vycygkc2NvcGUubm9kZWlkKS50aGVuKGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgICAgcmV0dXJuICRzY29wZS50YXNrbWFuYWdlcnMgPSBkYXRhO1xuICAgICAgfSk7XG4gICAgfVxuICB9KTtcbn0pLmNvbnRyb2xsZXIoJ0pvYlBsYW5BY2N1bXVsYXRvcnNDb250cm9sbGVyJywgZnVuY3Rpb24oJHNjb3BlLCBKb2JzU2VydmljZSkge1xuICBjb25zb2xlLmxvZygnSm9iUGxhbkFjY3VtdWxhdG9yc0NvbnRyb2xsZXInKTtcbiAgaWYgKCRzY29wZS5ub2RlaWQgJiYgKCEkc2NvcGUudmVydGV4IHx8ICEkc2NvcGUudmVydGV4LmFjY3VtdWxhdG9ycykpIHtcbiAgICBKb2JzU2VydmljZS5nZXRBY2N1bXVsYXRvcnMoJHNjb3BlLm5vZGVpZCkudGhlbihmdW5jdGlvbihkYXRhKSB7XG4gICAgICAkc2NvcGUuYWNjdW11bGF0b3JzID0gZGF0YS5tYWluO1xuICAgICAgcmV0dXJuICRzY29wZS5zdWJ0YXNrQWNjdW11bGF0b3JzID0gZGF0YS5zdWJ0YXNrcztcbiAgICB9KTtcbiAgfVxuICByZXR1cm4gJHNjb3BlLiRvbigncmVsb2FkJywgZnVuY3Rpb24oZXZlbnQpIHtcbiAgICBjb25zb2xlLmxvZygnSm9iUGxhbkFjY3VtdWxhdG9yc0NvbnRyb2xsZXInKTtcbiAgICBpZiAoJHNjb3BlLm5vZGVpZCkge1xuICAgICAgcmV0dXJuIEpvYnNTZXJ2aWNlLmdldEFjY3VtdWxhdG9ycygkc2NvcGUubm9kZWlkKS50aGVuKGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgICAgJHNjb3BlLmFjY3VtdWxhdG9ycyA9IGRhdGEubWFpbjtcbiAgICAgICAgcmV0dXJuICRzY29wZS5zdWJ0YXNrQWNjdW11bGF0b3JzID0gZGF0YS5zdWJ0YXNrcztcbiAgICAgIH0pO1xuICAgIH1cbiAgfSk7XG59KS5jb250cm9sbGVyKCdKb2JQbGFuQ2hlY2twb2ludHNDb250cm9sbGVyJywgZnVuY3Rpb24oJHNjb3BlLCBKb2JzU2VydmljZSkge1xuICBjb25zb2xlLmxvZygnSm9iUGxhbkNoZWNrcG9pbnRzQ29udHJvbGxlcicpO1xuICBKb2JzU2VydmljZS5nZXRKb2JDaGVja3BvaW50U3RhdHMoJHNjb3BlLmpvYmlkKS50aGVuKGZ1bmN0aW9uKGRhdGEpIHtcbiAgICByZXR1cm4gJHNjb3BlLmpvYkNoZWNrcG9pbnRTdGF0cyA9IGRhdGE7XG4gIH0pO1xuICBpZiAoJHNjb3BlLm5vZGVpZCAmJiAoISRzY29wZS52ZXJ0ZXggfHwgISRzY29wZS52ZXJ0ZXgub3BlcmF0b3JDaGVja3BvaW50U3RhdHMpKSB7XG4gICAgSm9ic1NlcnZpY2UuZ2V0T3BlcmF0b3JDaGVja3BvaW50U3RhdHMoJHNjb3BlLm5vZGVpZCkudGhlbihmdW5jdGlvbihkYXRhKSB7XG4gICAgICAkc2NvcGUub3BlcmF0b3JDaGVja3BvaW50U3RhdHMgPSBkYXRhLm9wZXJhdG9yU3RhdHM7XG4gICAgICByZXR1cm4gJHNjb3BlLnN1YnRhc2tzQ2hlY2twb2ludFN0YXRzID0gZGF0YS5zdWJ0YXNrc1N0YXRzO1xuICAgIH0pO1xuICB9XG4gIHJldHVybiAkc2NvcGUuJG9uKCdyZWxvYWQnLCBmdW5jdGlvbihldmVudCkge1xuICAgIGNvbnNvbGUubG9nKCdKb2JQbGFuQ2hlY2twb2ludHNDb250cm9sbGVyJyk7XG4gICAgSm9ic1NlcnZpY2UuZ2V0Sm9iQ2hlY2twb2ludFN0YXRzKCRzY29wZS5qb2JpZCkudGhlbihmdW5jdGlvbihkYXRhKSB7XG4gICAgICByZXR1cm4gJHNjb3BlLmpvYkNoZWNrcG9pbnRTdGF0cyA9IGRhdGE7XG4gICAgfSk7XG4gICAgaWYgKCRzY29wZS5ub2RlaWQpIHtcbiAgICAgIHJldHVybiBKb2JzU2VydmljZS5nZXRPcGVyYXRvckNoZWNrcG9pbnRTdGF0cygkc2NvcGUubm9kZWlkKS50aGVuKGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgICAgJHNjb3BlLm9wZXJhdG9yQ2hlY2twb2ludFN0YXRzID0gZGF0YS5vcGVyYXRvclN0YXRzO1xuICAgICAgICByZXR1cm4gJHNjb3BlLnN1YnRhc2tzQ2hlY2twb2ludFN0YXRzID0gZGF0YS5zdWJ0YXNrc1N0YXRzO1xuICAgICAgfSk7XG4gICAgfVxuICB9KTtcbn0pLmNvbnRyb2xsZXIoJ0pvYlBsYW5CYWNrUHJlc3N1cmVDb250cm9sbGVyJywgZnVuY3Rpb24oJHNjb3BlLCBKb2JzU2VydmljZSkge1xuICBjb25zb2xlLmxvZygnSm9iUGxhbkJhY2tQcmVzc3VyZUNvbnRyb2xsZXInKTtcbiAgJHNjb3BlLm5vdyA9IERhdGUubm93KCk7XG4gIGlmICgkc2NvcGUubm9kZWlkKSB7XG4gICAgSm9ic1NlcnZpY2UuZ2V0T3BlcmF0b3JCYWNrUHJlc3N1cmUoJHNjb3BlLm5vZGVpZCkudGhlbihmdW5jdGlvbihkYXRhKSB7XG4gICAgICByZXR1cm4gJHNjb3BlLmJhY2tQcmVzc3VyZU9wZXJhdG9yU3RhdHNbJHNjb3BlLm5vZGVpZF0gPSBkYXRhO1xuICAgIH0pO1xuICB9XG4gIHJldHVybiAkc2NvcGUuJG9uKCdyZWxvYWQnLCBmdW5jdGlvbihldmVudCkge1xuICAgIGNvbnNvbGUubG9nKCdKb2JQbGFuQmFja1ByZXNzdXJlQ29udHJvbGxlciAocmVsYW9kKScpO1xuICAgICRzY29wZS5ub3cgPSBEYXRlLm5vdygpO1xuICAgIGlmICgkc2NvcGUubm9kZWlkKSB7XG4gICAgICByZXR1cm4gSm9ic1NlcnZpY2UuZ2V0T3BlcmF0b3JCYWNrUHJlc3N1cmUoJHNjb3BlLm5vZGVpZCkudGhlbihmdW5jdGlvbihkYXRhKSB7XG4gICAgICAgIHJldHVybiAkc2NvcGUuYmFja1ByZXNzdXJlT3BlcmF0b3JTdGF0c1skc2NvcGUubm9kZWlkXSA9IGRhdGE7XG4gICAgICB9KTtcbiAgICB9XG4gIH0pO1xufSkuY29udHJvbGxlcignSm9iVGltZWxpbmVWZXJ0ZXhDb250cm9sbGVyJywgZnVuY3Rpb24oJHNjb3BlLCAkc3RhdGUsICRzdGF0ZVBhcmFtcywgSm9ic1NlcnZpY2UpIHtcbiAgY29uc29sZS5sb2coJ0pvYlRpbWVsaW5lVmVydGV4Q29udHJvbGxlcicpO1xuICBKb2JzU2VydmljZS5nZXRWZXJ0ZXgoJHN0YXRlUGFyYW1zLnZlcnRleElkKS50aGVuKGZ1bmN0aW9uKGRhdGEpIHtcbiAgICByZXR1cm4gJHNjb3BlLnZlcnRleCA9IGRhdGE7XG4gIH0pO1xuICByZXR1cm4gJHNjb3BlLiRvbigncmVsb2FkJywgZnVuY3Rpb24oZXZlbnQpIHtcbiAgICBjb25zb2xlLmxvZygnSm9iVGltZWxpbmVWZXJ0ZXhDb250cm9sbGVyJyk7XG4gICAgcmV0dXJuIEpvYnNTZXJ2aWNlLmdldFZlcnRleCgkc3RhdGVQYXJhbXMudmVydGV4SWQpLnRoZW4oZnVuY3Rpb24oZGF0YSkge1xuICAgICAgcmV0dXJuICRzY29wZS52ZXJ0ZXggPSBkYXRhO1xuICAgIH0pO1xuICB9KTtcbn0pLmNvbnRyb2xsZXIoJ0pvYkV4Y2VwdGlvbnNDb250cm9sbGVyJywgZnVuY3Rpb24oJHNjb3BlLCAkc3RhdGUsICRzdGF0ZVBhcmFtcywgSm9ic1NlcnZpY2UpIHtcbiAgcmV0dXJuIEpvYnNTZXJ2aWNlLmxvYWRFeGNlcHRpb25zKCkudGhlbihmdW5jdGlvbihkYXRhKSB7XG4gICAgcmV0dXJuICRzY29wZS5leGNlcHRpb25zID0gZGF0YTtcbiAgfSk7XG59KS5jb250cm9sbGVyKCdKb2JQcm9wZXJ0aWVzQ29udHJvbGxlcicsIGZ1bmN0aW9uKCRzY29wZSwgSm9ic1NlcnZpY2UpIHtcbiAgY29uc29sZS5sb2coJ0pvYlByb3BlcnRpZXNDb250cm9sbGVyJyk7XG4gIHJldHVybiAkc2NvcGUuY2hhbmdlTm9kZSA9IGZ1bmN0aW9uKG5vZGVpZCkge1xuICAgIGlmIChub2RlaWQgIT09ICRzY29wZS5ub2RlaWQpIHtcbiAgICAgICRzY29wZS5ub2RlaWQgPSBub2RlaWQ7XG4gICAgICByZXR1cm4gSm9ic1NlcnZpY2UuZ2V0Tm9kZShub2RlaWQpLnRoZW4oZnVuY3Rpb24oZGF0YSkge1xuICAgICAgICByZXR1cm4gJHNjb3BlLm5vZGUgPSBkYXRhO1xuICAgICAgfSk7XG4gICAgfSBlbHNlIHtcbiAgICAgICRzY29wZS5ub2RlaWQgPSBudWxsO1xuICAgICAgcmV0dXJuICRzY29wZS5ub2RlID0gbnVsbDtcbiAgICB9XG4gIH07XG59KTtcbiIsIiNcbiMgTGljZW5zZWQgdG8gdGhlIEFwYWNoZSBTb2Z0d2FyZSBGb3VuZGF0aW9uIChBU0YpIHVuZGVyIG9uZVxuIyBvciBtb3JlIGNvbnRyaWJ1dG9yIGxpY2Vuc2UgYWdyZWVtZW50cy4gIFNlZSB0aGUgTk9USUNFIGZpbGVcbiMgZGlzdHJpYnV0ZWQgd2l0aCB0aGlzIHdvcmsgZm9yIGFkZGl0aW9uYWwgaW5mb3JtYXRpb25cbiMgcmVnYXJkaW5nIGNvcHlyaWdodCBvd25lcnNoaXAuICBUaGUgQVNGIGxpY2Vuc2VzIHRoaXMgZmlsZVxuIyB0byB5b3UgdW5kZXIgdGhlIEFwYWNoZSBMaWNlbnNlLCBWZXJzaW9uIDIuMCAodGhlXG4jIFwiTGljZW5zZVwiKTsgeW91IG1heSBub3QgdXNlIHRoaXMgZmlsZSBleGNlcHQgaW4gY29tcGxpYW5jZVxuIyB3aXRoIHRoZSBMaWNlbnNlLiAgWW91IG1heSBvYnRhaW4gYSBjb3B5IG9mIHRoZSBMaWNlbnNlIGF0XG4jXG4jICAgICBodHRwOi8vd3d3LmFwYWNoZS5vcmcvbGljZW5zZXMvTElDRU5TRS0yLjBcbiNcbiMgVW5sZXNzIHJlcXVpcmVkIGJ5IGFwcGxpY2FibGUgbGF3IG9yIGFncmVlZCB0byBpbiB3cml0aW5nLCBzb2Z0d2FyZVxuIyBkaXN0cmlidXRlZCB1bmRlciB0aGUgTGljZW5zZSBpcyBkaXN0cmlidXRlZCBvbiBhbiBcIkFTIElTXCIgQkFTSVMsXG4jIFdJVEhPVVQgV0FSUkFOVElFUyBPUiBDT05ESVRJT05TIE9GIEFOWSBLSU5ELCBlaXRoZXIgZXhwcmVzcyBvciBpbXBsaWVkLlxuIyBTZWUgdGhlIExpY2Vuc2UgZm9yIHRoZSBzcGVjaWZpYyBsYW5ndWFnZSBnb3Zlcm5pbmcgcGVybWlzc2lvbnMgYW5kXG4jIGxpbWl0YXRpb25zIHVuZGVyIHRoZSBMaWNlbnNlLlxuI1xuXG5hbmd1bGFyLm1vZHVsZSgnZmxpbmtBcHAnKVxuXG4jIC0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS1cblxuLmRpcmVjdGl2ZSAndmVydGV4JywgKCRzdGF0ZSkgLT5cbiAgdGVtcGxhdGU6IFwiPHN2ZyBjbGFzcz0ndGltZWxpbmUgc2Vjb25kYXJ5JyB3aWR0aD0nMCcgaGVpZ2h0PScwJz48L3N2Zz5cIlxuXG4gIHNjb3BlOlxuICAgIGRhdGE6IFwiPVwiXG5cbiAgbGluazogKHNjb3BlLCBlbGVtLCBhdHRycykgLT5cbiAgICBzdmdFbCA9IGVsZW0uY2hpbGRyZW4oKVswXVxuXG4gICAgY29udGFpbmVyVyA9IGVsZW0ud2lkdGgoKVxuICAgIGFuZ3VsYXIuZWxlbWVudChzdmdFbCkuYXR0cignd2lkdGgnLCBjb250YWluZXJXKVxuXG4gICAgYW5hbHl6ZVRpbWUgPSAoZGF0YSkgLT5cbiAgICAgIGQzLnNlbGVjdChzdmdFbCkuc2VsZWN0QWxsKFwiKlwiKS5yZW1vdmUoKVxuXG4gICAgICB0ZXN0RGF0YSA9IFtdXG5cbiAgICAgIGFuZ3VsYXIuZm9yRWFjaCBkYXRhLnN1YnRhc2tzLCAoc3VidGFzaywgaSkgLT5cbiAgICAgICAgdGltZXMgPSBbXG4gICAgICAgICAge1xuICAgICAgICAgICAgbGFiZWw6IFwiU2NoZWR1bGVkXCJcbiAgICAgICAgICAgIGNvbG9yOiBcIiM2NjZcIlxuICAgICAgICAgICAgYm9yZGVyQ29sb3I6IFwiIzU1NVwiXG4gICAgICAgICAgICBzdGFydGluZ190aW1lOiBzdWJ0YXNrLnRpbWVzdGFtcHNbXCJTQ0hFRFVMRURcIl1cbiAgICAgICAgICAgIGVuZGluZ190aW1lOiBzdWJ0YXNrLnRpbWVzdGFtcHNbXCJERVBMT1lJTkdcIl1cbiAgICAgICAgICAgIHR5cGU6ICdyZWd1bGFyJ1xuICAgICAgICAgIH1cbiAgICAgICAgICB7XG4gICAgICAgICAgICBsYWJlbDogXCJEZXBsb3lpbmdcIlxuICAgICAgICAgICAgY29sb3I6IFwiI2FhYVwiXG4gICAgICAgICAgICBib3JkZXJDb2xvcjogXCIjNTU1XCJcbiAgICAgICAgICAgIHN0YXJ0aW5nX3RpbWU6IHN1YnRhc2sudGltZXN0YW1wc1tcIkRFUExPWUlOR1wiXVxuICAgICAgICAgICAgZW5kaW5nX3RpbWU6IHN1YnRhc2sudGltZXN0YW1wc1tcIlJVTk5JTkdcIl1cbiAgICAgICAgICAgIHR5cGU6ICdyZWd1bGFyJ1xuICAgICAgICAgIH1cbiAgICAgICAgXVxuXG4gICAgICAgIGlmIHN1YnRhc2sudGltZXN0YW1wc1tcIkZJTklTSEVEXCJdID4gMFxuICAgICAgICAgIHRpbWVzLnB1c2gge1xuICAgICAgICAgICAgbGFiZWw6IFwiUnVubmluZ1wiXG4gICAgICAgICAgICBjb2xvcjogXCIjZGRkXCJcbiAgICAgICAgICAgIGJvcmRlckNvbG9yOiBcIiM1NTVcIlxuICAgICAgICAgICAgc3RhcnRpbmdfdGltZTogc3VidGFzay50aW1lc3RhbXBzW1wiUlVOTklOR1wiXVxuICAgICAgICAgICAgZW5kaW5nX3RpbWU6IHN1YnRhc2sudGltZXN0YW1wc1tcIkZJTklTSEVEXCJdXG4gICAgICAgICAgICB0eXBlOiAncmVndWxhcidcbiAgICAgICAgICB9XG5cbiAgICAgICAgdGVzdERhdGEucHVzaCB7XG4gICAgICAgICAgbGFiZWw6IFwiKCN7c3VidGFzay5zdWJ0YXNrfSkgI3tzdWJ0YXNrLmhvc3R9XCJcbiAgICAgICAgICB0aW1lczogdGltZXNcbiAgICAgICAgfVxuXG4gICAgICBjaGFydCA9IGQzLnRpbWVsaW5lKCkuc3RhY2soKVxuICAgICAgLnRpY2tGb3JtYXQoe1xuICAgICAgICBmb3JtYXQ6IGQzLnRpbWUuZm9ybWF0KFwiJUxcIilcbiAgICAgICAgIyB0aWNrSW50ZXJ2YWw6IDFcbiAgICAgICAgdGlja1NpemU6IDFcbiAgICAgIH0pXG4gICAgICAucHJlZml4KFwic2luZ2xlXCIpXG4gICAgICAubGFiZWxGb3JtYXQoKGxhYmVsKSAtPlxuICAgICAgICBsYWJlbFxuICAgICAgKVxuICAgICAgLm1hcmdpbih7IGxlZnQ6IDEwMCwgcmlnaHQ6IDAsIHRvcDogMCwgYm90dG9tOiAwIH0pXG4gICAgICAuaXRlbUhlaWdodCgzMClcbiAgICAgIC5yZWxhdGl2ZVRpbWUoKVxuXG4gICAgICBzdmcgPSBkMy5zZWxlY3Qoc3ZnRWwpXG4gICAgICAuZGF0dW0odGVzdERhdGEpXG4gICAgICAuY2FsbChjaGFydClcblxuICAgIGFuYWx5emVUaW1lKHNjb3BlLmRhdGEpXG5cbiAgICByZXR1cm5cblxuIyAtLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tXG5cbi5kaXJlY3RpdmUgJ3RpbWVsaW5lJywgKCRzdGF0ZSkgLT5cbiAgdGVtcGxhdGU6IFwiPHN2ZyBjbGFzcz0ndGltZWxpbmUnIHdpZHRoPScwJyBoZWlnaHQ9JzAnPjwvc3ZnPlwiXG5cbiAgc2NvcGU6XG4gICAgdmVydGljZXM6IFwiPVwiXG4gICAgam9iaWQ6IFwiPVwiXG5cbiAgbGluazogKHNjb3BlLCBlbGVtLCBhdHRycykgLT5cbiAgICBzdmdFbCA9IGVsZW0uY2hpbGRyZW4oKVswXVxuXG4gICAgY29udGFpbmVyVyA9IGVsZW0ud2lkdGgoKVxuICAgIGFuZ3VsYXIuZWxlbWVudChzdmdFbCkuYXR0cignd2lkdGgnLCBjb250YWluZXJXKVxuXG4gICAgdHJhbnNsYXRlTGFiZWwgPSAobGFiZWwpIC0+XG4gICAgICBsYWJlbC5yZXBsYWNlKFwiJmd0O1wiLCBcIj5cIilcblxuICAgIGFuYWx5emVUaW1lID0gKGRhdGEpIC0+XG4gICAgICBkMy5zZWxlY3Qoc3ZnRWwpLnNlbGVjdEFsbChcIipcIikucmVtb3ZlKClcblxuICAgICAgdGVzdERhdGEgPSBbXVxuXG4gICAgICBhbmd1bGFyLmZvckVhY2ggZGF0YSwgKHZlcnRleCkgLT5cbiAgICAgICAgaWYgdmVydGV4WydzdGFydC10aW1lJ10gPiAtMVxuICAgICAgICAgIGlmIHZlcnRleC50eXBlIGlzICdzY2hlZHVsZWQnXG4gICAgICAgICAgICB0ZXN0RGF0YS5wdXNoIFxuICAgICAgICAgICAgICB0aW1lczogW1xuICAgICAgICAgICAgICAgIGxhYmVsOiB0cmFuc2xhdGVMYWJlbCh2ZXJ0ZXgubmFtZSlcbiAgICAgICAgICAgICAgICBjb2xvcjogXCIjY2NjY2NjXCJcbiAgICAgICAgICAgICAgICBib3JkZXJDb2xvcjogXCIjNTU1NTU1XCJcbiAgICAgICAgICAgICAgICBzdGFydGluZ190aW1lOiB2ZXJ0ZXhbJ3N0YXJ0LXRpbWUnXVxuICAgICAgICAgICAgICAgIGVuZGluZ190aW1lOiB2ZXJ0ZXhbJ2VuZC10aW1lJ11cbiAgICAgICAgICAgICAgICB0eXBlOiB2ZXJ0ZXgudHlwZVxuICAgICAgICAgICAgICBdXG4gICAgICAgICAgZWxzZVxuICAgICAgICAgICAgdGVzdERhdGEucHVzaCBcbiAgICAgICAgICAgICAgdGltZXM6IFtcbiAgICAgICAgICAgICAgICBsYWJlbDogdHJhbnNsYXRlTGFiZWwodmVydGV4Lm5hbWUpXG4gICAgICAgICAgICAgICAgY29sb3I6IFwiI2Q5ZjFmN1wiXG4gICAgICAgICAgICAgICAgYm9yZGVyQ29sb3I6IFwiIzYyY2RlYVwiXG4gICAgICAgICAgICAgICAgc3RhcnRpbmdfdGltZTogdmVydGV4WydzdGFydC10aW1lJ11cbiAgICAgICAgICAgICAgICBlbmRpbmdfdGltZTogdmVydGV4WydlbmQtdGltZSddXG4gICAgICAgICAgICAgICAgbGluazogdmVydGV4LmlkXG4gICAgICAgICAgICAgICAgdHlwZTogdmVydGV4LnR5cGVcbiAgICAgICAgICAgICAgXVxuXG4gICAgICBjaGFydCA9IGQzLnRpbWVsaW5lKCkuc3RhY2soKS5jbGljaygoZCwgaSwgZGF0dW0pIC0+XG4gICAgICAgIGlmIGQubGlua1xuICAgICAgICAgICRzdGF0ZS5nbyBcInNpbmdsZS1qb2IudGltZWxpbmUudmVydGV4XCIsIHsgam9iaWQ6IHNjb3BlLmpvYmlkLCB2ZXJ0ZXhJZDogZC5saW5rIH1cblxuICAgICAgKVxuICAgICAgLnRpY2tGb3JtYXQoe1xuICAgICAgICBmb3JtYXQ6IGQzLnRpbWUuZm9ybWF0KFwiJUxcIilcbiAgICAgICAgIyB0aWNrVGltZTogZDMudGltZS5zZWNvbmRcbiAgICAgICAgIyB0aWNrSW50ZXJ2YWw6IDAuNVxuICAgICAgICB0aWNrU2l6ZTogMVxuICAgICAgfSlcbiAgICAgIC5wcmVmaXgoXCJtYWluXCIpXG4gICAgICAubWFyZ2luKHsgbGVmdDogMCwgcmlnaHQ6IDAsIHRvcDogMCwgYm90dG9tOiAwIH0pXG4gICAgICAuaXRlbUhlaWdodCgzMClcbiAgICAgIC5zaG93Qm9yZGVyTGluZSgpXG4gICAgICAuc2hvd0hvdXJUaW1lbGluZSgpXG5cbiAgICAgIHN2ZyA9IGQzLnNlbGVjdChzdmdFbClcbiAgICAgIC5kYXR1bSh0ZXN0RGF0YSlcbiAgICAgIC5jYWxsKGNoYXJ0KVxuXG4gICAgc2NvcGUuJHdhdGNoIGF0dHJzLnZlcnRpY2VzLCAoZGF0YSkgLT5cbiAgICAgIGFuYWx5emVUaW1lKGRhdGEpIGlmIGRhdGFcblxuICAgIHJldHVyblxuXG4jIC0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS1cblxuLmRpcmVjdGl2ZSAnam9iUGxhbicsICgkdGltZW91dCkgLT5cbiAgdGVtcGxhdGU6IFwiXG4gICAgPHN2ZyBjbGFzcz0nZ3JhcGgnIHdpZHRoPSc1MDAnIGhlaWdodD0nNDAwJz48ZyAvPjwvc3ZnPlxuICAgIDxzdmcgY2xhc3M9J3RtcCcgd2lkdGg9JzEnIGhlaWdodD0nMSc+PGcgLz48L3N2Zz5cbiAgICA8ZGl2IGNsYXNzPSdidG4tZ3JvdXAgem9vbS1idXR0b25zJz5cbiAgICAgIDxhIGNsYXNzPSdidG4gYnRuLWRlZmF1bHQgem9vbS1pbicgbmctY2xpY2s9J3pvb21JbigpJz48aSBjbGFzcz0nZmEgZmEtcGx1cycgLz48L2E+XG4gICAgICA8YSBjbGFzcz0nYnRuIGJ0bi1kZWZhdWx0IHpvb20tb3V0JyBuZy1jbGljaz0nem9vbU91dCgpJz48aSBjbGFzcz0nZmEgZmEtbWludXMnIC8+PC9hPlxuICAgIDwvZGl2PlwiXG5cbiAgc2NvcGU6XG4gICAgcGxhbjogJz0nXG4gICAgc2V0Tm9kZTogJyYnXG5cbiAgbGluazogKHNjb3BlLCBlbGVtLCBhdHRycykgLT5cbiAgICBnID0gbnVsbFxuICAgIG1haW5ab29tID0gZDMuYmVoYXZpb3Iuem9vbSgpXG4gICAgc3ViZ3JhcGhzID0gW11cbiAgICBqb2JpZCA9IGF0dHJzLmpvYmlkXG5cbiAgICBtYWluU3ZnRWxlbWVudCA9IGVsZW0uY2hpbGRyZW4oKVswXVxuICAgIG1haW5HID0gZWxlbS5jaGlsZHJlbigpLmNoaWxkcmVuKClbMF1cbiAgICBtYWluVG1wRWxlbWVudCA9IGVsZW0uY2hpbGRyZW4oKVsxXVxuXG4gICAgZDNtYWluU3ZnID0gZDMuc2VsZWN0KG1haW5TdmdFbGVtZW50KVxuICAgIGQzbWFpblN2Z0cgPSBkMy5zZWxlY3QobWFpbkcpXG4gICAgZDN0bXBTdmcgPSBkMy5zZWxlY3QobWFpblRtcEVsZW1lbnQpXG5cbiAgICAjIGFuZ3VsYXIuZWxlbWVudChtYWluRykuZW1wdHkoKVxuICAgICMgZDNtYWluU3ZnRy5zZWxlY3RBbGwoXCIqXCIpLnJlbW92ZSgpXG5cbiAgICBjb250YWluZXJXID0gZWxlbS53aWR0aCgpXG4gICAgYW5ndWxhci5lbGVtZW50KGVsZW0uY2hpbGRyZW4oKVswXSkud2lkdGgoY29udGFpbmVyVylcblxuICAgIHNjb3BlLnpvb21JbiA9IC0+XG4gICAgICBpZiBtYWluWm9vbS5zY2FsZSgpIDwgMi45OVxuICAgICAgICBcbiAgICAgICAgIyBDYWxjdWxhdGUgYW5kIHN0b3JlIG5ldyB2YWx1ZXMgaW4gem9vbSBvYmplY3RcbiAgICAgICAgdHJhbnNsYXRlID0gbWFpblpvb20udHJhbnNsYXRlKClcbiAgICAgICAgdjEgPSB0cmFuc2xhdGVbMF0gKiAobWFpblpvb20uc2NhbGUoKSArIDAuMSAvIChtYWluWm9vbS5zY2FsZSgpKSlcbiAgICAgICAgdjIgPSB0cmFuc2xhdGVbMV0gKiAobWFpblpvb20uc2NhbGUoKSArIDAuMSAvIChtYWluWm9vbS5zY2FsZSgpKSlcbiAgICAgICAgbWFpblpvb20uc2NhbGUgbWFpblpvb20uc2NhbGUoKSArIDAuMVxuICAgICAgICBtYWluWm9vbS50cmFuc2xhdGUgWyB2MSwgdjIgXVxuICAgICAgICBcbiAgICAgICAgIyBUcmFuc2Zvcm0gc3ZnXG4gICAgICAgIGQzbWFpblN2Z0cuYXR0ciBcInRyYW5zZm9ybVwiLCBcInRyYW5zbGF0ZShcIiArIHYxICsgXCIsXCIgKyB2MiArIFwiKSBzY2FsZShcIiArIG1haW5ab29tLnNjYWxlKCkgKyBcIilcIlxuXG4gICAgc2NvcGUuem9vbU91dCA9IC0+XG4gICAgICBpZiBtYWluWm9vbS5zY2FsZSgpID4gMC4zMVxuICAgICAgICBcbiAgICAgICAgIyBDYWxjdWxhdGUgYW5kIHN0b3JlIG5ldyB2YWx1ZXMgaW4gbWFpblpvb20gb2JqZWN0XG4gICAgICAgIG1haW5ab29tLnNjYWxlIG1haW5ab29tLnNjYWxlKCkgLSAwLjFcbiAgICAgICAgdHJhbnNsYXRlID0gbWFpblpvb20udHJhbnNsYXRlKClcbiAgICAgICAgdjEgPSB0cmFuc2xhdGVbMF0gKiAobWFpblpvb20uc2NhbGUoKSAtIDAuMSAvIChtYWluWm9vbS5zY2FsZSgpKSlcbiAgICAgICAgdjIgPSB0cmFuc2xhdGVbMV0gKiAobWFpblpvb20uc2NhbGUoKSAtIDAuMSAvIChtYWluWm9vbS5zY2FsZSgpKSlcbiAgICAgICAgbWFpblpvb20udHJhbnNsYXRlIFsgdjEsIHYyIF1cbiAgICAgICAgXG4gICAgICAgICMgVHJhbnNmb3JtIHN2Z1xuICAgICAgICBkM21haW5TdmdHLmF0dHIgXCJ0cmFuc2Zvcm1cIiwgXCJ0cmFuc2xhdGUoXCIgKyB2MSArIFwiLFwiICsgdjIgKyBcIikgc2NhbGUoXCIgKyBtYWluWm9vbS5zY2FsZSgpICsgXCIpXCJcblxuICAgICNjcmVhdGUgYSBsYWJlbCBvZiBhbiBlZGdlXG4gICAgY3JlYXRlTGFiZWxFZGdlID0gKGVsKSAtPlxuICAgICAgbGFiZWxWYWx1ZSA9IFwiXCJcbiAgICAgIGlmIGVsLnNoaXBfc3RyYXRlZ3k/IG9yIGVsLmxvY2FsX3N0cmF0ZWd5P1xuICAgICAgICBsYWJlbFZhbHVlICs9IFwiPGRpdiBjbGFzcz0nZWRnZS1sYWJlbCc+XCJcbiAgICAgICAgbGFiZWxWYWx1ZSArPSBlbC5zaGlwX3N0cmF0ZWd5ICBpZiBlbC5zaGlwX3N0cmF0ZWd5P1xuICAgICAgICBsYWJlbFZhbHVlICs9IFwiIChcIiArIGVsLnRlbXBfbW9kZSArIFwiKVwiICB1bmxlc3MgZWwudGVtcF9tb2RlIGlzIGB1bmRlZmluZWRgXG4gICAgICAgIGxhYmVsVmFsdWUgKz0gXCIsPGJyPlwiICsgZWwubG9jYWxfc3RyYXRlZ3kgIHVubGVzcyBlbC5sb2NhbF9zdHJhdGVneSBpcyBgdW5kZWZpbmVkYFxuICAgICAgICBsYWJlbFZhbHVlICs9IFwiPC9kaXY+XCJcbiAgICAgIGxhYmVsVmFsdWVcblxuXG4gICAgIyB0cnVlLCBpZiB0aGUgbm9kZSBpcyBhIHNwZWNpYWwgbm9kZSBmcm9tIGFuIGl0ZXJhdGlvblxuICAgIGlzU3BlY2lhbEl0ZXJhdGlvbk5vZGUgPSAoaW5mbykgLT5cbiAgICAgIChpbmZvIGlzIFwicGFydGlhbFNvbHV0aW9uXCIgb3IgaW5mbyBpcyBcIm5leHRQYXJ0aWFsU29sdXRpb25cIiBvciBpbmZvIGlzIFwid29ya3NldFwiIG9yIGluZm8gaXMgXCJuZXh0V29ya3NldFwiIG9yIGluZm8gaXMgXCJzb2x1dGlvblNldFwiIG9yIGluZm8gaXMgXCJzb2x1dGlvbkRlbHRhXCIpXG5cbiAgICBnZXROb2RlVHlwZSA9IChlbCwgaW5mbykgLT5cbiAgICAgIGlmIGluZm8gaXMgXCJtaXJyb3JcIlxuICAgICAgICAnbm9kZS1taXJyb3InXG5cbiAgICAgIGVsc2UgaWYgaXNTcGVjaWFsSXRlcmF0aW9uTm9kZShpbmZvKVxuICAgICAgICAnbm9kZS1pdGVyYXRpb24nXG5cbiAgICAgIGVsc2VcbiAgICAgICAgICAnbm9kZS1ub3JtYWwnXG4gICAgICBcbiAgICAjIGNyZWF0ZXMgdGhlIGxhYmVsIG9mIGEgbm9kZSwgaW4gaW5mbyBpcyBzdG9yZWQsIHdoZXRoZXIgaXQgaXMgYSBzcGVjaWFsIG5vZGUgKGxpa2UgYSBtaXJyb3IgaW4gYW4gaXRlcmF0aW9uKVxuICAgIGNyZWF0ZUxhYmVsTm9kZSA9IChlbCwgaW5mbywgbWF4VywgbWF4SCkgLT5cbiAgICAgICMgbGFiZWxWYWx1ZSA9IFwiPGEgaHJlZj0nIy9qb2JzL1wiICsgam9iaWQgKyBcIi92ZXJ0ZXgvXCIgKyBlbC5pZCArIFwiJyBjbGFzcz0nbm9kZS1sYWJlbCBcIiArIGdldE5vZGVUeXBlKGVsLCBpbmZvKSArIFwiJz5cIlxuICAgICAgbGFiZWxWYWx1ZSA9IFwiPGRpdiBocmVmPScjL2pvYnMvXCIgKyBqb2JpZCArIFwiL3ZlcnRleC9cIiArIGVsLmlkICsgXCInIGNsYXNzPSdub2RlLWxhYmVsIFwiICsgZ2V0Tm9kZVR5cGUoZWwsIGluZm8pICsgXCInPlwiXG5cbiAgICAgICMgTm9kZW5hbWVcbiAgICAgIGlmIGluZm8gaXMgXCJtaXJyb3JcIlxuICAgICAgICBsYWJlbFZhbHVlICs9IFwiPGgzIGNsYXNzPSdub2RlLW5hbWUnPk1pcnJvciBvZiBcIiArIGVsLm9wZXJhdG9yICsgXCI8L2gzPlwiXG4gICAgICBlbHNlXG4gICAgICAgIGxhYmVsVmFsdWUgKz0gXCI8aDMgY2xhc3M9J25vZGUtbmFtZSc+XCIgKyBlbC5vcGVyYXRvciArIFwiPC9oMz5cIlxuICAgICAgaWYgZWwuZGVzY3JpcHRpb24gaXMgXCJcIlxuICAgICAgICBsYWJlbFZhbHVlICs9IFwiXCJcbiAgICAgIGVsc2VcbiAgICAgICAgc3RlcE5hbWUgPSBlbC5kZXNjcmlwdGlvblxuICAgICAgICBcbiAgICAgICAgIyBjbGVhbiBzdGVwTmFtZVxuICAgICAgICBzdGVwTmFtZSA9IHNob3J0ZW5TdHJpbmcoc3RlcE5hbWUpXG4gICAgICAgIGxhYmVsVmFsdWUgKz0gXCI8aDQgY2xhc3M9J3N0ZXAtbmFtZSc+XCIgKyBzdGVwTmFtZSArIFwiPC9oND5cIlxuICAgICAgXG4gICAgICAjIElmIHRoaXMgbm9kZSBpcyBhbiBcIml0ZXJhdGlvblwiIHdlIG5lZWQgYSBkaWZmZXJlbnQgcGFuZWwtYm9keVxuICAgICAgaWYgZWwuc3RlcF9mdW5jdGlvbj9cbiAgICAgICAgbGFiZWxWYWx1ZSArPSBleHRlbmRMYWJlbE5vZGVGb3JJdGVyYXRpb24oZWwuaWQsIG1heFcsIG1heEgpXG4gICAgICBlbHNlXG4gICAgICAgIFxuICAgICAgICAjIE90aGVyd2lzZSBhZGQgaW5mb3MgICAgXG4gICAgICAgIGxhYmVsVmFsdWUgKz0gXCI8aDU+XCIgKyBpbmZvICsgXCIgTm9kZTwvaDU+XCIgIGlmIGlzU3BlY2lhbEl0ZXJhdGlvbk5vZGUoaW5mbylcbiAgICAgICAgbGFiZWxWYWx1ZSArPSBcIjxoNT5QYXJhbGxlbGlzbTogXCIgKyBlbC5wYXJhbGxlbGlzbSArIFwiPC9oNT5cIiAgdW5sZXNzIGVsLnBhcmFsbGVsaXNtIGlzIFwiXCJcbiAgICAgICAgbGFiZWxWYWx1ZSArPSBcIjxoNT5PcGVyYXRpb246IFwiICsgc2hvcnRlblN0cmluZyhlbC5vcGVyYXRvcl9zdHJhdGVneSkgKyBcIjwvaDU+XCIgIHVubGVzcyBlbC5vcGVyYXRvciBpcyBgdW5kZWZpbmVkYFxuICAgICAgXG4gICAgICAjIGxhYmVsVmFsdWUgKz0gXCI8L2E+XCJcbiAgICAgIGxhYmVsVmFsdWUgKz0gXCI8L2Rpdj5cIlxuICAgICAgbGFiZWxWYWx1ZVxuXG4gICAgIyBFeHRlbmRzIHRoZSBsYWJlbCBvZiBhIG5vZGUgd2l0aCBhbiBhZGRpdGlvbmFsIHN2ZyBFbGVtZW50IHRvIHByZXNlbnQgdGhlIGl0ZXJhdGlvbi5cbiAgICBleHRlbmRMYWJlbE5vZGVGb3JJdGVyYXRpb24gPSAoaWQsIG1heFcsIG1heEgpIC0+XG4gICAgICBzdmdJRCA9IFwic3ZnLVwiICsgaWRcblxuICAgICAgbGFiZWxWYWx1ZSA9IFwiPHN2ZyBjbGFzcz0nXCIgKyBzdmdJRCArIFwiJyB3aWR0aD1cIiArIG1heFcgKyBcIiBoZWlnaHQ9XCIgKyBtYXhIICsgXCI+PGcgLz48L3N2Zz5cIlxuICAgICAgbGFiZWxWYWx1ZVxuXG4gICAgIyBTcGxpdCBhIHN0cmluZyBpbnRvIG11bHRpcGxlIGxpbmVzIHNvIHRoYXQgZWFjaCBsaW5lIGhhcyBsZXNzIHRoYW4gMzAgbGV0dGVycy5cbiAgICBzaG9ydGVuU3RyaW5nID0gKHMpIC0+XG4gICAgICAjIG1ha2Ugc3VyZSB0aGF0IG5hbWUgZG9lcyBub3QgY29udGFpbiBhIDwgKGJlY2F1c2Ugb2YgaHRtbClcbiAgICAgIGlmIHMuY2hhckF0KDApIGlzIFwiPFwiXG4gICAgICAgIHMgPSBzLnJlcGxhY2UoXCI8XCIsIFwiJmx0O1wiKVxuICAgICAgICBzID0gcy5yZXBsYWNlKFwiPlwiLCBcIiZndDtcIilcbiAgICAgIHNiciA9IFwiXCJcbiAgICAgIHdoaWxlIHMubGVuZ3RoID4gMzBcbiAgICAgICAgc2JyID0gc2JyICsgcy5zdWJzdHJpbmcoMCwgMzApICsgXCI8YnI+XCJcbiAgICAgICAgcyA9IHMuc3Vic3RyaW5nKDMwLCBzLmxlbmd0aClcbiAgICAgIHNiciA9IHNiciArIHNcbiAgICAgIHNiclxuXG4gICAgY3JlYXRlTm9kZSA9IChnLCBkYXRhLCBlbCwgaXNQYXJlbnQgPSBmYWxzZSwgbWF4VywgbWF4SCkgLT5cbiAgICAgICMgY3JlYXRlIG5vZGUsIHNlbmQgYWRkaXRpb25hbCBpbmZvcm1hdGlvbnMgYWJvdXQgdGhlIG5vZGUgaWYgaXQgaXMgYSBzcGVjaWFsIG9uZVxuICAgICAgaWYgZWwuaWQgaXMgZGF0YS5wYXJ0aWFsX3NvbHV0aW9uXG4gICAgICAgIGcuc2V0Tm9kZSBlbC5pZCxcbiAgICAgICAgICBsYWJlbDogY3JlYXRlTGFiZWxOb2RlKGVsLCBcInBhcnRpYWxTb2x1dGlvblwiLCBtYXhXLCBtYXhIKVxuICAgICAgICAgIGxhYmVsVHlwZTogJ2h0bWwnXG4gICAgICAgICAgY2xhc3M6IGdldE5vZGVUeXBlKGVsLCBcInBhcnRpYWxTb2x1dGlvblwiKVxuXG4gICAgICBlbHNlIGlmIGVsLmlkIGlzIGRhdGEubmV4dF9wYXJ0aWFsX3NvbHV0aW9uXG4gICAgICAgIGcuc2V0Tm9kZSBlbC5pZCxcbiAgICAgICAgICBsYWJlbDogY3JlYXRlTGFiZWxOb2RlKGVsLCBcIm5leHRQYXJ0aWFsU29sdXRpb25cIiwgbWF4VywgbWF4SClcbiAgICAgICAgICBsYWJlbFR5cGU6ICdodG1sJ1xuICAgICAgICAgIGNsYXNzOiBnZXROb2RlVHlwZShlbCwgXCJuZXh0UGFydGlhbFNvbHV0aW9uXCIpXG5cbiAgICAgIGVsc2UgaWYgZWwuaWQgaXMgZGF0YS53b3Jrc2V0XG4gICAgICAgIGcuc2V0Tm9kZSBlbC5pZCxcbiAgICAgICAgICBsYWJlbDogY3JlYXRlTGFiZWxOb2RlKGVsLCBcIndvcmtzZXRcIiwgbWF4VywgbWF4SClcbiAgICAgICAgICBsYWJlbFR5cGU6ICdodG1sJ1xuICAgICAgICAgIGNsYXNzOiBnZXROb2RlVHlwZShlbCwgXCJ3b3Jrc2V0XCIpXG5cbiAgICAgIGVsc2UgaWYgZWwuaWQgaXMgZGF0YS5uZXh0X3dvcmtzZXRcbiAgICAgICAgZy5zZXROb2RlIGVsLmlkLFxuICAgICAgICAgIGxhYmVsOiBjcmVhdGVMYWJlbE5vZGUoZWwsIFwibmV4dFdvcmtzZXRcIiwgbWF4VywgbWF4SClcbiAgICAgICAgICBsYWJlbFR5cGU6ICdodG1sJ1xuICAgICAgICAgIGNsYXNzOiBnZXROb2RlVHlwZShlbCwgXCJuZXh0V29ya3NldFwiKVxuXG4gICAgICBlbHNlIGlmIGVsLmlkIGlzIGRhdGEuc29sdXRpb25fc2V0XG4gICAgICAgIGcuc2V0Tm9kZSBlbC5pZCxcbiAgICAgICAgICBsYWJlbDogY3JlYXRlTGFiZWxOb2RlKGVsLCBcInNvbHV0aW9uU2V0XCIsIG1heFcsIG1heEgpXG4gICAgICAgICAgbGFiZWxUeXBlOiAnaHRtbCdcbiAgICAgICAgICBjbGFzczogZ2V0Tm9kZVR5cGUoZWwsIFwic29sdXRpb25TZXRcIilcblxuICAgICAgZWxzZSBpZiBlbC5pZCBpcyBkYXRhLnNvbHV0aW9uX2RlbHRhXG4gICAgICAgIGcuc2V0Tm9kZSBlbC5pZCxcbiAgICAgICAgICBsYWJlbDogY3JlYXRlTGFiZWxOb2RlKGVsLCBcInNvbHV0aW9uRGVsdGFcIiwgbWF4VywgbWF4SClcbiAgICAgICAgICBsYWJlbFR5cGU6ICdodG1sJ1xuICAgICAgICAgIGNsYXNzOiBnZXROb2RlVHlwZShlbCwgXCJzb2x1dGlvbkRlbHRhXCIpXG5cbiAgICAgIGVsc2VcbiAgICAgICAgZy5zZXROb2RlIGVsLmlkLFxuICAgICAgICAgIGxhYmVsOiBjcmVhdGVMYWJlbE5vZGUoZWwsIFwiXCIsIG1heFcsIG1heEgpXG4gICAgICAgICAgbGFiZWxUeXBlOiAnaHRtbCdcbiAgICAgICAgICBjbGFzczogZ2V0Tm9kZVR5cGUoZWwsIFwiXCIpXG5cbiAgICBjcmVhdGVFZGdlID0gKGcsIGRhdGEsIGVsLCBleGlzdGluZ05vZGVzLCBwcmVkKSAtPlxuICAgICAgdW5sZXNzIGV4aXN0aW5nTm9kZXMuaW5kZXhPZihwcmVkLmlkKSBpcyAtMVxuICAgICAgICBnLnNldEVkZ2UgcHJlZC5pZCwgZWwuaWQsXG4gICAgICAgICAgbGFiZWw6IGNyZWF0ZUxhYmVsRWRnZShwcmVkKVxuICAgICAgICAgIGxhYmVsVHlwZTogJ2h0bWwnXG4gICAgICAgICAgYXJyb3doZWFkOiAnbm9ybWFsJ1xuXG4gICAgICBlbHNlXG4gICAgICAgIG1pc3NpbmdOb2RlID0gc2VhcmNoRm9yTm9kZShkYXRhLCBwcmVkLmlkKVxuICAgICAgICB1bmxlc3MgIW1pc3NpbmdOb2RlXG4gICAgICAgICAgZy5zZXRFZGdlIG1pc3NpbmdOb2RlLmlkLCBlbC5pZCxcbiAgICAgICAgICAgIGxhYmVsOiBjcmVhdGVMYWJlbEVkZ2UobWlzc2luZ05vZGUpXG4gICAgICAgICAgICBsYWJlbFR5cGU6ICdodG1sJ1xuXG4gICAgbG9hZEpzb25Ub0RhZ3JlID0gKGcsIGRhdGEpIC0+XG4gICAgICBleGlzdGluZ05vZGVzID0gW11cblxuICAgICAgaWYgZGF0YS5ub2Rlcz9cbiAgICAgICAgIyBUaGlzIGlzIHRoZSBub3JtYWwganNvbiBkYXRhXG4gICAgICAgIHRvSXRlcmF0ZSA9IGRhdGEubm9kZXNcblxuICAgICAgZWxzZVxuICAgICAgICAjIFRoaXMgaXMgYW4gaXRlcmF0aW9uLCB3ZSBub3cgc3RvcmUgc3BlY2lhbCBpdGVyYXRpb24gbm9kZXMgaWYgcG9zc2libGVcbiAgICAgICAgdG9JdGVyYXRlID0gZGF0YS5zdGVwX2Z1bmN0aW9uXG4gICAgICAgIGlzUGFyZW50ID0gdHJ1ZVxuXG4gICAgICBmb3IgZWwgaW4gdG9JdGVyYXRlXG4gICAgICAgIG1heFcgPSAwXG4gICAgICAgIG1heEggPSAwXG5cbiAgICAgICAgaWYgZWwuc3RlcF9mdW5jdGlvblxuICAgICAgICAgIHNnID0gbmV3IGRhZ3JlRDMuZ3JhcGhsaWIuR3JhcGgoeyBtdWx0aWdyYXBoOiB0cnVlLCBjb21wb3VuZDogdHJ1ZSB9KS5zZXRHcmFwaCh7XG4gICAgICAgICAgICBub2Rlc2VwOiAyMFxuICAgICAgICAgICAgZWRnZXNlcDogMFxuICAgICAgICAgICAgcmFua3NlcDogMjBcbiAgICAgICAgICAgIHJhbmtkaXI6IFwiTFJcIlxuICAgICAgICAgICAgbWFyZ2lueDogMTBcbiAgICAgICAgICAgIG1hcmdpbnk6IDEwXG4gICAgICAgICAgICB9KVxuXG4gICAgICAgICAgc3ViZ3JhcGhzW2VsLmlkXSA9IHNnXG5cbiAgICAgICAgICBsb2FkSnNvblRvRGFncmUoc2csIGVsKVxuXG4gICAgICAgICAgciA9IG5ldyBkYWdyZUQzLnJlbmRlcigpXG4gICAgICAgICAgZDN0bXBTdmcuc2VsZWN0KCdnJykuY2FsbChyLCBzZylcbiAgICAgICAgICBtYXhXID0gc2cuZ3JhcGgoKS53aWR0aFxuICAgICAgICAgIG1heEggPSBzZy5ncmFwaCgpLmhlaWdodFxuXG4gICAgICAgICAgYW5ndWxhci5lbGVtZW50KG1haW5UbXBFbGVtZW50KS5lbXB0eSgpXG5cbiAgICAgICAgY3JlYXRlTm9kZShnLCBkYXRhLCBlbCwgaXNQYXJlbnQsIG1heFcsIG1heEgpXG5cbiAgICAgICAgZXhpc3RpbmdOb2Rlcy5wdXNoIGVsLmlkXG4gICAgICAgIFxuICAgICAgICAjIGNyZWF0ZSBlZGdlcyBmcm9tIGlucHV0cyB0byBjdXJyZW50IG5vZGVcbiAgICAgICAgaWYgZWwuaW5wdXRzP1xuICAgICAgICAgIGZvciBwcmVkIGluIGVsLmlucHV0c1xuICAgICAgICAgICAgY3JlYXRlRWRnZShnLCBkYXRhLCBlbCwgZXhpc3RpbmdOb2RlcywgcHJlZClcblxuICAgICAgZ1xuXG4gICAgIyBzZWFyY2hlcyBpbiB0aGUgZ2xvYmFsIEpTT05EYXRhIGZvciB0aGUgbm9kZSB3aXRoIHRoZSBnaXZlbiBpZFxuICAgIHNlYXJjaEZvck5vZGUgPSAoZGF0YSwgbm9kZUlEKSAtPlxuICAgICAgZm9yIGkgb2YgZGF0YS5ub2Rlc1xuICAgICAgICBlbCA9IGRhdGEubm9kZXNbaV1cbiAgICAgICAgcmV0dXJuIGVsICBpZiBlbC5pZCBpcyBub2RlSURcbiAgICAgICAgXG4gICAgICAgICMgbG9vayBmb3Igbm9kZXMgdGhhdCBhcmUgaW4gaXRlcmF0aW9uc1xuICAgICAgICBpZiBlbC5zdGVwX2Z1bmN0aW9uP1xuICAgICAgICAgIGZvciBqIG9mIGVsLnN0ZXBfZnVuY3Rpb25cbiAgICAgICAgICAgIHJldHVybiBlbC5zdGVwX2Z1bmN0aW9uW2pdICBpZiBlbC5zdGVwX2Z1bmN0aW9uW2pdLmlkIGlzIG5vZGVJRFxuXG4gICAgZHJhd0dyYXBoID0gKGRhdGEpIC0+XG4gICAgICBnID0gbmV3IGRhZ3JlRDMuZ3JhcGhsaWIuR3JhcGgoeyBtdWx0aWdyYXBoOiB0cnVlLCBjb21wb3VuZDogdHJ1ZSB9KS5zZXRHcmFwaCh7XG4gICAgICAgIG5vZGVzZXA6IDcwXG4gICAgICAgIGVkZ2VzZXA6IDBcbiAgICAgICAgcmFua3NlcDogNTBcbiAgICAgICAgcmFua2RpcjogXCJMUlwiXG4gICAgICAgIG1hcmdpbng6IDQwXG4gICAgICAgIG1hcmdpbnk6IDQwXG4gICAgICAgIH0pXG5cbiAgICAgIGxvYWRKc29uVG9EYWdyZShnLCBkYXRhKVxuXG4gICAgICByZW5kZXJlciA9IG5ldyBkYWdyZUQzLnJlbmRlcigpXG4gICAgICBkM21haW5TdmdHLmNhbGwocmVuZGVyZXIsIGcpXG5cbiAgICAgIGZvciBpLCBzZyBvZiBzdWJncmFwaHNcbiAgICAgICAgZDNtYWluU3ZnLnNlbGVjdCgnc3ZnLnN2Zy0nICsgaSArICcgZycpLmNhbGwocmVuZGVyZXIsIHNnKVxuXG4gICAgICBuZXdTY2FsZSA9IDAuNVxuXG4gICAgICB4Q2VudGVyT2Zmc2V0ID0gTWF0aC5mbG9vcigoYW5ndWxhci5lbGVtZW50KG1haW5TdmdFbGVtZW50KS53aWR0aCgpIC0gZy5ncmFwaCgpLndpZHRoICogbmV3U2NhbGUpIC8gMilcbiAgICAgIHlDZW50ZXJPZmZzZXQgPSBNYXRoLmZsb29yKChhbmd1bGFyLmVsZW1lbnQobWFpblN2Z0VsZW1lbnQpLmhlaWdodCgpIC0gZy5ncmFwaCgpLmhlaWdodCAqIG5ld1NjYWxlKSAvIDIpXG5cbiAgICAgIG1haW5ab29tLnNjYWxlKG5ld1NjYWxlKS50cmFuc2xhdGUoW3hDZW50ZXJPZmZzZXQsIHlDZW50ZXJPZmZzZXRdKVxuXG4gICAgICBkM21haW5TdmdHLmF0dHIoXCJ0cmFuc2Zvcm1cIiwgXCJ0cmFuc2xhdGUoXCIgKyB4Q2VudGVyT2Zmc2V0ICsgXCIsIFwiICsgeUNlbnRlck9mZnNldCArIFwiKSBzY2FsZShcIiArIG1haW5ab29tLnNjYWxlKCkgKyBcIilcIilcblxuICAgICAgbWFpblpvb20ub24oXCJ6b29tXCIsIC0+XG4gICAgICAgIGV2ID0gZDMuZXZlbnRcbiAgICAgICAgZDNtYWluU3ZnRy5hdHRyIFwidHJhbnNmb3JtXCIsIFwidHJhbnNsYXRlKFwiICsgZXYudHJhbnNsYXRlICsgXCIpIHNjYWxlKFwiICsgZXYuc2NhbGUgKyBcIilcIlxuICAgICAgKVxuICAgICAgbWFpblpvb20oZDNtYWluU3ZnKVxuXG4gICAgICBkM21haW5TdmdHLnNlbGVjdEFsbCgnLm5vZGUnKS5vbiAnY2xpY2snLCAoZCkgLT5cbiAgICAgICAgc2NvcGUuc2V0Tm9kZSh7IG5vZGVpZDogZCB9KVxuXG4gICAgc2NvcGUuJHdhdGNoIGF0dHJzLnBsYW4sIChuZXdQbGFuKSAtPlxuICAgICAgZHJhd0dyYXBoKG5ld1BsYW4pIGlmIG5ld1BsYW5cblxuICAgIHJldHVyblxuIiwiYW5ndWxhci5tb2R1bGUoJ2ZsaW5rQXBwJykuZGlyZWN0aXZlKCd2ZXJ0ZXgnLCBmdW5jdGlvbigkc3RhdGUpIHtcbiAgcmV0dXJuIHtcbiAgICB0ZW1wbGF0ZTogXCI8c3ZnIGNsYXNzPSd0aW1lbGluZSBzZWNvbmRhcnknIHdpZHRoPScwJyBoZWlnaHQ9JzAnPjwvc3ZnPlwiLFxuICAgIHNjb3BlOiB7XG4gICAgICBkYXRhOiBcIj1cIlxuICAgIH0sXG4gICAgbGluazogZnVuY3Rpb24oc2NvcGUsIGVsZW0sIGF0dHJzKSB7XG4gICAgICB2YXIgYW5hbHl6ZVRpbWUsIGNvbnRhaW5lclcsIHN2Z0VsO1xuICAgICAgc3ZnRWwgPSBlbGVtLmNoaWxkcmVuKClbMF07XG4gICAgICBjb250YWluZXJXID0gZWxlbS53aWR0aCgpO1xuICAgICAgYW5ndWxhci5lbGVtZW50KHN2Z0VsKS5hdHRyKCd3aWR0aCcsIGNvbnRhaW5lclcpO1xuICAgICAgYW5hbHl6ZVRpbWUgPSBmdW5jdGlvbihkYXRhKSB7XG4gICAgICAgIHZhciBjaGFydCwgc3ZnLCB0ZXN0RGF0YTtcbiAgICAgICAgZDMuc2VsZWN0KHN2Z0VsKS5zZWxlY3RBbGwoXCIqXCIpLnJlbW92ZSgpO1xuICAgICAgICB0ZXN0RGF0YSA9IFtdO1xuICAgICAgICBhbmd1bGFyLmZvckVhY2goZGF0YS5zdWJ0YXNrcywgZnVuY3Rpb24oc3VidGFzaywgaSkge1xuICAgICAgICAgIHZhciB0aW1lcztcbiAgICAgICAgICB0aW1lcyA9IFtcbiAgICAgICAgICAgIHtcbiAgICAgICAgICAgICAgbGFiZWw6IFwiU2NoZWR1bGVkXCIsXG4gICAgICAgICAgICAgIGNvbG9yOiBcIiM2NjZcIixcbiAgICAgICAgICAgICAgYm9yZGVyQ29sb3I6IFwiIzU1NVwiLFxuICAgICAgICAgICAgICBzdGFydGluZ190aW1lOiBzdWJ0YXNrLnRpbWVzdGFtcHNbXCJTQ0hFRFVMRURcIl0sXG4gICAgICAgICAgICAgIGVuZGluZ190aW1lOiBzdWJ0YXNrLnRpbWVzdGFtcHNbXCJERVBMT1lJTkdcIl0sXG4gICAgICAgICAgICAgIHR5cGU6ICdyZWd1bGFyJ1xuICAgICAgICAgICAgfSwge1xuICAgICAgICAgICAgICBsYWJlbDogXCJEZXBsb3lpbmdcIixcbiAgICAgICAgICAgICAgY29sb3I6IFwiI2FhYVwiLFxuICAgICAgICAgICAgICBib3JkZXJDb2xvcjogXCIjNTU1XCIsXG4gICAgICAgICAgICAgIHN0YXJ0aW5nX3RpbWU6IHN1YnRhc2sudGltZXN0YW1wc1tcIkRFUExPWUlOR1wiXSxcbiAgICAgICAgICAgICAgZW5kaW5nX3RpbWU6IHN1YnRhc2sudGltZXN0YW1wc1tcIlJVTk5JTkdcIl0sXG4gICAgICAgICAgICAgIHR5cGU6ICdyZWd1bGFyJ1xuICAgICAgICAgICAgfVxuICAgICAgICAgIF07XG4gICAgICAgICAgaWYgKHN1YnRhc2sudGltZXN0YW1wc1tcIkZJTklTSEVEXCJdID4gMCkge1xuICAgICAgICAgICAgdGltZXMucHVzaCh7XG4gICAgICAgICAgICAgIGxhYmVsOiBcIlJ1bm5pbmdcIixcbiAgICAgICAgICAgICAgY29sb3I6IFwiI2RkZFwiLFxuICAgICAgICAgICAgICBib3JkZXJDb2xvcjogXCIjNTU1XCIsXG4gICAgICAgICAgICAgIHN0YXJ0aW5nX3RpbWU6IHN1YnRhc2sudGltZXN0YW1wc1tcIlJVTk5JTkdcIl0sXG4gICAgICAgICAgICAgIGVuZGluZ190aW1lOiBzdWJ0YXNrLnRpbWVzdGFtcHNbXCJGSU5JU0hFRFwiXSxcbiAgICAgICAgICAgICAgdHlwZTogJ3JlZ3VsYXInXG4gICAgICAgICAgICB9KTtcbiAgICAgICAgICB9XG4gICAgICAgICAgcmV0dXJuIHRlc3REYXRhLnB1c2goe1xuICAgICAgICAgICAgbGFiZWw6IFwiKFwiICsgc3VidGFzay5zdWJ0YXNrICsgXCIpIFwiICsgc3VidGFzay5ob3N0LFxuICAgICAgICAgICAgdGltZXM6IHRpbWVzXG4gICAgICAgICAgfSk7XG4gICAgICAgIH0pO1xuICAgICAgICBjaGFydCA9IGQzLnRpbWVsaW5lKCkuc3RhY2soKS50aWNrRm9ybWF0KHtcbiAgICAgICAgICBmb3JtYXQ6IGQzLnRpbWUuZm9ybWF0KFwiJUxcIiksXG4gICAgICAgICAgdGlja1NpemU6IDFcbiAgICAgICAgfSkucHJlZml4KFwic2luZ2xlXCIpLmxhYmVsRm9ybWF0KGZ1bmN0aW9uKGxhYmVsKSB7XG4gICAgICAgICAgcmV0dXJuIGxhYmVsO1xuICAgICAgICB9KS5tYXJnaW4oe1xuICAgICAgICAgIGxlZnQ6IDEwMCxcbiAgICAgICAgICByaWdodDogMCxcbiAgICAgICAgICB0b3A6IDAsXG4gICAgICAgICAgYm90dG9tOiAwXG4gICAgICAgIH0pLml0ZW1IZWlnaHQoMzApLnJlbGF0aXZlVGltZSgpO1xuICAgICAgICByZXR1cm4gc3ZnID0gZDMuc2VsZWN0KHN2Z0VsKS5kYXR1bSh0ZXN0RGF0YSkuY2FsbChjaGFydCk7XG4gICAgICB9O1xuICAgICAgYW5hbHl6ZVRpbWUoc2NvcGUuZGF0YSk7XG4gICAgfVxuICB9O1xufSkuZGlyZWN0aXZlKCd0aW1lbGluZScsIGZ1bmN0aW9uKCRzdGF0ZSkge1xuICByZXR1cm4ge1xuICAgIHRlbXBsYXRlOiBcIjxzdmcgY2xhc3M9J3RpbWVsaW5lJyB3aWR0aD0nMCcgaGVpZ2h0PScwJz48L3N2Zz5cIixcbiAgICBzY29wZToge1xuICAgICAgdmVydGljZXM6IFwiPVwiLFxuICAgICAgam9iaWQ6IFwiPVwiXG4gICAgfSxcbiAgICBsaW5rOiBmdW5jdGlvbihzY29wZSwgZWxlbSwgYXR0cnMpIHtcbiAgICAgIHZhciBhbmFseXplVGltZSwgY29udGFpbmVyVywgc3ZnRWwsIHRyYW5zbGF0ZUxhYmVsO1xuICAgICAgc3ZnRWwgPSBlbGVtLmNoaWxkcmVuKClbMF07XG4gICAgICBjb250YWluZXJXID0gZWxlbS53aWR0aCgpO1xuICAgICAgYW5ndWxhci5lbGVtZW50KHN2Z0VsKS5hdHRyKCd3aWR0aCcsIGNvbnRhaW5lclcpO1xuICAgICAgdHJhbnNsYXRlTGFiZWwgPSBmdW5jdGlvbihsYWJlbCkge1xuICAgICAgICByZXR1cm4gbGFiZWwucmVwbGFjZShcIiZndDtcIiwgXCI+XCIpO1xuICAgICAgfTtcbiAgICAgIGFuYWx5emVUaW1lID0gZnVuY3Rpb24oZGF0YSkge1xuICAgICAgICB2YXIgY2hhcnQsIHN2ZywgdGVzdERhdGE7XG4gICAgICAgIGQzLnNlbGVjdChzdmdFbCkuc2VsZWN0QWxsKFwiKlwiKS5yZW1vdmUoKTtcbiAgICAgICAgdGVzdERhdGEgPSBbXTtcbiAgICAgICAgYW5ndWxhci5mb3JFYWNoKGRhdGEsIGZ1bmN0aW9uKHZlcnRleCkge1xuICAgICAgICAgIGlmICh2ZXJ0ZXhbJ3N0YXJ0LXRpbWUnXSA+IC0xKSB7XG4gICAgICAgICAgICBpZiAodmVydGV4LnR5cGUgPT09ICdzY2hlZHVsZWQnKSB7XG4gICAgICAgICAgICAgIHJldHVybiB0ZXN0RGF0YS5wdXNoKHtcbiAgICAgICAgICAgICAgICB0aW1lczogW1xuICAgICAgICAgICAgICAgICAge1xuICAgICAgICAgICAgICAgICAgICBsYWJlbDogdHJhbnNsYXRlTGFiZWwodmVydGV4Lm5hbWUpLFxuICAgICAgICAgICAgICAgICAgICBjb2xvcjogXCIjY2NjY2NjXCIsXG4gICAgICAgICAgICAgICAgICAgIGJvcmRlckNvbG9yOiBcIiM1NTU1NTVcIixcbiAgICAgICAgICAgICAgICAgICAgc3RhcnRpbmdfdGltZTogdmVydGV4WydzdGFydC10aW1lJ10sXG4gICAgICAgICAgICAgICAgICAgIGVuZGluZ190aW1lOiB2ZXJ0ZXhbJ2VuZC10aW1lJ10sXG4gICAgICAgICAgICAgICAgICAgIHR5cGU6IHZlcnRleC50eXBlXG4gICAgICAgICAgICAgICAgICB9XG4gICAgICAgICAgICAgICAgXVxuICAgICAgICAgICAgICB9KTtcbiAgICAgICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgICAgIHJldHVybiB0ZXN0RGF0YS5wdXNoKHtcbiAgICAgICAgICAgICAgICB0aW1lczogW1xuICAgICAgICAgICAgICAgICAge1xuICAgICAgICAgICAgICAgICAgICBsYWJlbDogdHJhbnNsYXRlTGFiZWwodmVydGV4Lm5hbWUpLFxuICAgICAgICAgICAgICAgICAgICBjb2xvcjogXCIjZDlmMWY3XCIsXG4gICAgICAgICAgICAgICAgICAgIGJvcmRlckNvbG9yOiBcIiM2MmNkZWFcIixcbiAgICAgICAgICAgICAgICAgICAgc3RhcnRpbmdfdGltZTogdmVydGV4WydzdGFydC10aW1lJ10sXG4gICAgICAgICAgICAgICAgICAgIGVuZGluZ190aW1lOiB2ZXJ0ZXhbJ2VuZC10aW1lJ10sXG4gICAgICAgICAgICAgICAgICAgIGxpbms6IHZlcnRleC5pZCxcbiAgICAgICAgICAgICAgICAgICAgdHlwZTogdmVydGV4LnR5cGVcbiAgICAgICAgICAgICAgICAgIH1cbiAgICAgICAgICAgICAgICBdXG4gICAgICAgICAgICAgIH0pO1xuICAgICAgICAgICAgfVxuICAgICAgICAgIH1cbiAgICAgICAgfSk7XG4gICAgICAgIGNoYXJ0ID0gZDMudGltZWxpbmUoKS5zdGFjaygpLmNsaWNrKGZ1bmN0aW9uKGQsIGksIGRhdHVtKSB7XG4gICAgICAgICAgaWYgKGQubGluaykge1xuICAgICAgICAgICAgcmV0dXJuICRzdGF0ZS5nbyhcInNpbmdsZS1qb2IudGltZWxpbmUudmVydGV4XCIsIHtcbiAgICAgICAgICAgICAgam9iaWQ6IHNjb3BlLmpvYmlkLFxuICAgICAgICAgICAgICB2ZXJ0ZXhJZDogZC5saW5rXG4gICAgICAgICAgICB9KTtcbiAgICAgICAgICB9XG4gICAgICAgIH0pLnRpY2tGb3JtYXQoe1xuICAgICAgICAgIGZvcm1hdDogZDMudGltZS5mb3JtYXQoXCIlTFwiKSxcbiAgICAgICAgICB0aWNrU2l6ZTogMVxuICAgICAgICB9KS5wcmVmaXgoXCJtYWluXCIpLm1hcmdpbih7XG4gICAgICAgICAgbGVmdDogMCxcbiAgICAgICAgICByaWdodDogMCxcbiAgICAgICAgICB0b3A6IDAsXG4gICAgICAgICAgYm90dG9tOiAwXG4gICAgICAgIH0pLml0ZW1IZWlnaHQoMzApLnNob3dCb3JkZXJMaW5lKCkuc2hvd0hvdXJUaW1lbGluZSgpO1xuICAgICAgICByZXR1cm4gc3ZnID0gZDMuc2VsZWN0KHN2Z0VsKS5kYXR1bSh0ZXN0RGF0YSkuY2FsbChjaGFydCk7XG4gICAgICB9O1xuICAgICAgc2NvcGUuJHdhdGNoKGF0dHJzLnZlcnRpY2VzLCBmdW5jdGlvbihkYXRhKSB7XG4gICAgICAgIGlmIChkYXRhKSB7XG4gICAgICAgICAgcmV0dXJuIGFuYWx5emVUaW1lKGRhdGEpO1xuICAgICAgICB9XG4gICAgICB9KTtcbiAgICB9XG4gIH07XG59KS5kaXJlY3RpdmUoJ2pvYlBsYW4nLCBmdW5jdGlvbigkdGltZW91dCkge1xuICByZXR1cm4ge1xuICAgIHRlbXBsYXRlOiBcIjxzdmcgY2xhc3M9J2dyYXBoJyB3aWR0aD0nNTAwJyBoZWlnaHQ9JzQwMCc+PGcgLz48L3N2Zz4gPHN2ZyBjbGFzcz0ndG1wJyB3aWR0aD0nMScgaGVpZ2h0PScxJz48ZyAvPjwvc3ZnPiA8ZGl2IGNsYXNzPSdidG4tZ3JvdXAgem9vbS1idXR0b25zJz4gPGEgY2xhc3M9J2J0biBidG4tZGVmYXVsdCB6b29tLWluJyBuZy1jbGljaz0nem9vbUluKCknPjxpIGNsYXNzPSdmYSBmYS1wbHVzJyAvPjwvYT4gPGEgY2xhc3M9J2J0biBidG4tZGVmYXVsdCB6b29tLW91dCcgbmctY2xpY2s9J3pvb21PdXQoKSc+PGkgY2xhc3M9J2ZhIGZhLW1pbnVzJyAvPjwvYT4gPC9kaXY+XCIsXG4gICAgc2NvcGU6IHtcbiAgICAgIHBsYW46ICc9JyxcbiAgICAgIHNldE5vZGU6ICcmJ1xuICAgIH0sXG4gICAgbGluazogZnVuY3Rpb24oc2NvcGUsIGVsZW0sIGF0dHJzKSB7XG4gICAgICB2YXIgY29udGFpbmVyVywgY3JlYXRlRWRnZSwgY3JlYXRlTGFiZWxFZGdlLCBjcmVhdGVMYWJlbE5vZGUsIGNyZWF0ZU5vZGUsIGQzbWFpblN2ZywgZDNtYWluU3ZnRywgZDN0bXBTdmcsIGRyYXdHcmFwaCwgZXh0ZW5kTGFiZWxOb2RlRm9ySXRlcmF0aW9uLCBnLCBnZXROb2RlVHlwZSwgaXNTcGVjaWFsSXRlcmF0aW9uTm9kZSwgam9iaWQsIGxvYWRKc29uVG9EYWdyZSwgbWFpbkcsIG1haW5TdmdFbGVtZW50LCBtYWluVG1wRWxlbWVudCwgbWFpblpvb20sIHNlYXJjaEZvck5vZGUsIHNob3J0ZW5TdHJpbmcsIHN1YmdyYXBocztcbiAgICAgIGcgPSBudWxsO1xuICAgICAgbWFpblpvb20gPSBkMy5iZWhhdmlvci56b29tKCk7XG4gICAgICBzdWJncmFwaHMgPSBbXTtcbiAgICAgIGpvYmlkID0gYXR0cnMuam9iaWQ7XG4gICAgICBtYWluU3ZnRWxlbWVudCA9IGVsZW0uY2hpbGRyZW4oKVswXTtcbiAgICAgIG1haW5HID0gZWxlbS5jaGlsZHJlbigpLmNoaWxkcmVuKClbMF07XG4gICAgICBtYWluVG1wRWxlbWVudCA9IGVsZW0uY2hpbGRyZW4oKVsxXTtcbiAgICAgIGQzbWFpblN2ZyA9IGQzLnNlbGVjdChtYWluU3ZnRWxlbWVudCk7XG4gICAgICBkM21haW5TdmdHID0gZDMuc2VsZWN0KG1haW5HKTtcbiAgICAgIGQzdG1wU3ZnID0gZDMuc2VsZWN0KG1haW5UbXBFbGVtZW50KTtcbiAgICAgIGNvbnRhaW5lclcgPSBlbGVtLndpZHRoKCk7XG4gICAgICBhbmd1bGFyLmVsZW1lbnQoZWxlbS5jaGlsZHJlbigpWzBdKS53aWR0aChjb250YWluZXJXKTtcbiAgICAgIHNjb3BlLnpvb21JbiA9IGZ1bmN0aW9uKCkge1xuICAgICAgICB2YXIgdHJhbnNsYXRlLCB2MSwgdjI7XG4gICAgICAgIGlmIChtYWluWm9vbS5zY2FsZSgpIDwgMi45OSkge1xuICAgICAgICAgIHRyYW5zbGF0ZSA9IG1haW5ab29tLnRyYW5zbGF0ZSgpO1xuICAgICAgICAgIHYxID0gdHJhbnNsYXRlWzBdICogKG1haW5ab29tLnNjYWxlKCkgKyAwLjEgLyAobWFpblpvb20uc2NhbGUoKSkpO1xuICAgICAgICAgIHYyID0gdHJhbnNsYXRlWzFdICogKG1haW5ab29tLnNjYWxlKCkgKyAwLjEgLyAobWFpblpvb20uc2NhbGUoKSkpO1xuICAgICAgICAgIG1haW5ab29tLnNjYWxlKG1haW5ab29tLnNjYWxlKCkgKyAwLjEpO1xuICAgICAgICAgIG1haW5ab29tLnRyYW5zbGF0ZShbdjEsIHYyXSk7XG4gICAgICAgICAgcmV0dXJuIGQzbWFpblN2Z0cuYXR0cihcInRyYW5zZm9ybVwiLCBcInRyYW5zbGF0ZShcIiArIHYxICsgXCIsXCIgKyB2MiArIFwiKSBzY2FsZShcIiArIG1haW5ab29tLnNjYWxlKCkgKyBcIilcIik7XG4gICAgICAgIH1cbiAgICAgIH07XG4gICAgICBzY29wZS56b29tT3V0ID0gZnVuY3Rpb24oKSB7XG4gICAgICAgIHZhciB0cmFuc2xhdGUsIHYxLCB2MjtcbiAgICAgICAgaWYgKG1haW5ab29tLnNjYWxlKCkgPiAwLjMxKSB7XG4gICAgICAgICAgbWFpblpvb20uc2NhbGUobWFpblpvb20uc2NhbGUoKSAtIDAuMSk7XG4gICAgICAgICAgdHJhbnNsYXRlID0gbWFpblpvb20udHJhbnNsYXRlKCk7XG4gICAgICAgICAgdjEgPSB0cmFuc2xhdGVbMF0gKiAobWFpblpvb20uc2NhbGUoKSAtIDAuMSAvIChtYWluWm9vbS5zY2FsZSgpKSk7XG4gICAgICAgICAgdjIgPSB0cmFuc2xhdGVbMV0gKiAobWFpblpvb20uc2NhbGUoKSAtIDAuMSAvIChtYWluWm9vbS5zY2FsZSgpKSk7XG4gICAgICAgICAgbWFpblpvb20udHJhbnNsYXRlKFt2MSwgdjJdKTtcbiAgICAgICAgICByZXR1cm4gZDNtYWluU3ZnRy5hdHRyKFwidHJhbnNmb3JtXCIsIFwidHJhbnNsYXRlKFwiICsgdjEgKyBcIixcIiArIHYyICsgXCIpIHNjYWxlKFwiICsgbWFpblpvb20uc2NhbGUoKSArIFwiKVwiKTtcbiAgICAgICAgfVxuICAgICAgfTtcbiAgICAgIGNyZWF0ZUxhYmVsRWRnZSA9IGZ1bmN0aW9uKGVsKSB7XG4gICAgICAgIHZhciBsYWJlbFZhbHVlO1xuICAgICAgICBsYWJlbFZhbHVlID0gXCJcIjtcbiAgICAgICAgaWYgKChlbC5zaGlwX3N0cmF0ZWd5ICE9IG51bGwpIHx8IChlbC5sb2NhbF9zdHJhdGVneSAhPSBudWxsKSkge1xuICAgICAgICAgIGxhYmVsVmFsdWUgKz0gXCI8ZGl2IGNsYXNzPSdlZGdlLWxhYmVsJz5cIjtcbiAgICAgICAgICBpZiAoZWwuc2hpcF9zdHJhdGVneSAhPSBudWxsKSB7XG4gICAgICAgICAgICBsYWJlbFZhbHVlICs9IGVsLnNoaXBfc3RyYXRlZ3k7XG4gICAgICAgICAgfVxuICAgICAgICAgIGlmIChlbC50ZW1wX21vZGUgIT09IHVuZGVmaW5lZCkge1xuICAgICAgICAgICAgbGFiZWxWYWx1ZSArPSBcIiAoXCIgKyBlbC50ZW1wX21vZGUgKyBcIilcIjtcbiAgICAgICAgICB9XG4gICAgICAgICAgaWYgKGVsLmxvY2FsX3N0cmF0ZWd5ICE9PSB1bmRlZmluZWQpIHtcbiAgICAgICAgICAgIGxhYmVsVmFsdWUgKz0gXCIsPGJyPlwiICsgZWwubG9jYWxfc3RyYXRlZ3k7XG4gICAgICAgICAgfVxuICAgICAgICAgIGxhYmVsVmFsdWUgKz0gXCI8L2Rpdj5cIjtcbiAgICAgICAgfVxuICAgICAgICByZXR1cm4gbGFiZWxWYWx1ZTtcbiAgICAgIH07XG4gICAgICBpc1NwZWNpYWxJdGVyYXRpb25Ob2RlID0gZnVuY3Rpb24oaW5mbykge1xuICAgICAgICByZXR1cm4gaW5mbyA9PT0gXCJwYXJ0aWFsU29sdXRpb25cIiB8fCBpbmZvID09PSBcIm5leHRQYXJ0aWFsU29sdXRpb25cIiB8fCBpbmZvID09PSBcIndvcmtzZXRcIiB8fCBpbmZvID09PSBcIm5leHRXb3Jrc2V0XCIgfHwgaW5mbyA9PT0gXCJzb2x1dGlvblNldFwiIHx8IGluZm8gPT09IFwic29sdXRpb25EZWx0YVwiO1xuICAgICAgfTtcbiAgICAgIGdldE5vZGVUeXBlID0gZnVuY3Rpb24oZWwsIGluZm8pIHtcbiAgICAgICAgaWYgKGluZm8gPT09IFwibWlycm9yXCIpIHtcbiAgICAgICAgICByZXR1cm4gJ25vZGUtbWlycm9yJztcbiAgICAgICAgfSBlbHNlIGlmIChpc1NwZWNpYWxJdGVyYXRpb25Ob2RlKGluZm8pKSB7XG4gICAgICAgICAgcmV0dXJuICdub2RlLWl0ZXJhdGlvbic7XG4gICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgcmV0dXJuICdub2RlLW5vcm1hbCc7XG4gICAgICAgIH1cbiAgICAgIH07XG4gICAgICBjcmVhdGVMYWJlbE5vZGUgPSBmdW5jdGlvbihlbCwgaW5mbywgbWF4VywgbWF4SCkge1xuICAgICAgICB2YXIgbGFiZWxWYWx1ZSwgc3RlcE5hbWU7XG4gICAgICAgIGxhYmVsVmFsdWUgPSBcIjxkaXYgaHJlZj0nIy9qb2JzL1wiICsgam9iaWQgKyBcIi92ZXJ0ZXgvXCIgKyBlbC5pZCArIFwiJyBjbGFzcz0nbm9kZS1sYWJlbCBcIiArIGdldE5vZGVUeXBlKGVsLCBpbmZvKSArIFwiJz5cIjtcbiAgICAgICAgaWYgKGluZm8gPT09IFwibWlycm9yXCIpIHtcbiAgICAgICAgICBsYWJlbFZhbHVlICs9IFwiPGgzIGNsYXNzPSdub2RlLW5hbWUnPk1pcnJvciBvZiBcIiArIGVsLm9wZXJhdG9yICsgXCI8L2gzPlwiO1xuICAgICAgICB9IGVsc2Uge1xuICAgICAgICAgIGxhYmVsVmFsdWUgKz0gXCI8aDMgY2xhc3M9J25vZGUtbmFtZSc+XCIgKyBlbC5vcGVyYXRvciArIFwiPC9oMz5cIjtcbiAgICAgICAgfVxuICAgICAgICBpZiAoZWwuZGVzY3JpcHRpb24gPT09IFwiXCIpIHtcbiAgICAgICAgICBsYWJlbFZhbHVlICs9IFwiXCI7XG4gICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgc3RlcE5hbWUgPSBlbC5kZXNjcmlwdGlvbjtcbiAgICAgICAgICBzdGVwTmFtZSA9IHNob3J0ZW5TdHJpbmcoc3RlcE5hbWUpO1xuICAgICAgICAgIGxhYmVsVmFsdWUgKz0gXCI8aDQgY2xhc3M9J3N0ZXAtbmFtZSc+XCIgKyBzdGVwTmFtZSArIFwiPC9oND5cIjtcbiAgICAgICAgfVxuICAgICAgICBpZiAoZWwuc3RlcF9mdW5jdGlvbiAhPSBudWxsKSB7XG4gICAgICAgICAgbGFiZWxWYWx1ZSArPSBleHRlbmRMYWJlbE5vZGVGb3JJdGVyYXRpb24oZWwuaWQsIG1heFcsIG1heEgpO1xuICAgICAgICB9IGVsc2Uge1xuICAgICAgICAgIGlmIChpc1NwZWNpYWxJdGVyYXRpb25Ob2RlKGluZm8pKSB7XG4gICAgICAgICAgICBsYWJlbFZhbHVlICs9IFwiPGg1PlwiICsgaW5mbyArIFwiIE5vZGU8L2g1PlwiO1xuICAgICAgICAgIH1cbiAgICAgICAgICBpZiAoZWwucGFyYWxsZWxpc20gIT09IFwiXCIpIHtcbiAgICAgICAgICAgIGxhYmVsVmFsdWUgKz0gXCI8aDU+UGFyYWxsZWxpc206IFwiICsgZWwucGFyYWxsZWxpc20gKyBcIjwvaDU+XCI7XG4gICAgICAgICAgfVxuICAgICAgICAgIGlmIChlbC5vcGVyYXRvciAhPT0gdW5kZWZpbmVkKSB7XG4gICAgICAgICAgICBsYWJlbFZhbHVlICs9IFwiPGg1Pk9wZXJhdGlvbjogXCIgKyBzaG9ydGVuU3RyaW5nKGVsLm9wZXJhdG9yX3N0cmF0ZWd5KSArIFwiPC9oNT5cIjtcbiAgICAgICAgICB9XG4gICAgICAgIH1cbiAgICAgICAgbGFiZWxWYWx1ZSArPSBcIjwvZGl2PlwiO1xuICAgICAgICByZXR1cm4gbGFiZWxWYWx1ZTtcbiAgICAgIH07XG4gICAgICBleHRlbmRMYWJlbE5vZGVGb3JJdGVyYXRpb24gPSBmdW5jdGlvbihpZCwgbWF4VywgbWF4SCkge1xuICAgICAgICB2YXIgbGFiZWxWYWx1ZSwgc3ZnSUQ7XG4gICAgICAgIHN2Z0lEID0gXCJzdmctXCIgKyBpZDtcbiAgICAgICAgbGFiZWxWYWx1ZSA9IFwiPHN2ZyBjbGFzcz0nXCIgKyBzdmdJRCArIFwiJyB3aWR0aD1cIiArIG1heFcgKyBcIiBoZWlnaHQ9XCIgKyBtYXhIICsgXCI+PGcgLz48L3N2Zz5cIjtcbiAgICAgICAgcmV0dXJuIGxhYmVsVmFsdWU7XG4gICAgICB9O1xuICAgICAgc2hvcnRlblN0cmluZyA9IGZ1bmN0aW9uKHMpIHtcbiAgICAgICAgdmFyIHNicjtcbiAgICAgICAgaWYgKHMuY2hhckF0KDApID09PSBcIjxcIikge1xuICAgICAgICAgIHMgPSBzLnJlcGxhY2UoXCI8XCIsIFwiJmx0O1wiKTtcbiAgICAgICAgICBzID0gcy5yZXBsYWNlKFwiPlwiLCBcIiZndDtcIik7XG4gICAgICAgIH1cbiAgICAgICAgc2JyID0gXCJcIjtcbiAgICAgICAgd2hpbGUgKHMubGVuZ3RoID4gMzApIHtcbiAgICAgICAgICBzYnIgPSBzYnIgKyBzLnN1YnN0cmluZygwLCAzMCkgKyBcIjxicj5cIjtcbiAgICAgICAgICBzID0gcy5zdWJzdHJpbmcoMzAsIHMubGVuZ3RoKTtcbiAgICAgICAgfVxuICAgICAgICBzYnIgPSBzYnIgKyBzO1xuICAgICAgICByZXR1cm4gc2JyO1xuICAgICAgfTtcbiAgICAgIGNyZWF0ZU5vZGUgPSBmdW5jdGlvbihnLCBkYXRhLCBlbCwgaXNQYXJlbnQsIG1heFcsIG1heEgpIHtcbiAgICAgICAgaWYgKGlzUGFyZW50ID09IG51bGwpIHtcbiAgICAgICAgICBpc1BhcmVudCA9IGZhbHNlO1xuICAgICAgICB9XG4gICAgICAgIGlmIChlbC5pZCA9PT0gZGF0YS5wYXJ0aWFsX3NvbHV0aW9uKSB7XG4gICAgICAgICAgcmV0dXJuIGcuc2V0Tm9kZShlbC5pZCwge1xuICAgICAgICAgICAgbGFiZWw6IGNyZWF0ZUxhYmVsTm9kZShlbCwgXCJwYXJ0aWFsU29sdXRpb25cIiwgbWF4VywgbWF4SCksXG4gICAgICAgICAgICBsYWJlbFR5cGU6ICdodG1sJyxcbiAgICAgICAgICAgIFwiY2xhc3NcIjogZ2V0Tm9kZVR5cGUoZWwsIFwicGFydGlhbFNvbHV0aW9uXCIpXG4gICAgICAgICAgfSk7XG4gICAgICAgIH0gZWxzZSBpZiAoZWwuaWQgPT09IGRhdGEubmV4dF9wYXJ0aWFsX3NvbHV0aW9uKSB7XG4gICAgICAgICAgcmV0dXJuIGcuc2V0Tm9kZShlbC5pZCwge1xuICAgICAgICAgICAgbGFiZWw6IGNyZWF0ZUxhYmVsTm9kZShlbCwgXCJuZXh0UGFydGlhbFNvbHV0aW9uXCIsIG1heFcsIG1heEgpLFxuICAgICAgICAgICAgbGFiZWxUeXBlOiAnaHRtbCcsXG4gICAgICAgICAgICBcImNsYXNzXCI6IGdldE5vZGVUeXBlKGVsLCBcIm5leHRQYXJ0aWFsU29sdXRpb25cIilcbiAgICAgICAgICB9KTtcbiAgICAgICAgfSBlbHNlIGlmIChlbC5pZCA9PT0gZGF0YS53b3Jrc2V0KSB7XG4gICAgICAgICAgcmV0dXJuIGcuc2V0Tm9kZShlbC5pZCwge1xuICAgICAgICAgICAgbGFiZWw6IGNyZWF0ZUxhYmVsTm9kZShlbCwgXCJ3b3Jrc2V0XCIsIG1heFcsIG1heEgpLFxuICAgICAgICAgICAgbGFiZWxUeXBlOiAnaHRtbCcsXG4gICAgICAgICAgICBcImNsYXNzXCI6IGdldE5vZGVUeXBlKGVsLCBcIndvcmtzZXRcIilcbiAgICAgICAgICB9KTtcbiAgICAgICAgfSBlbHNlIGlmIChlbC5pZCA9PT0gZGF0YS5uZXh0X3dvcmtzZXQpIHtcbiAgICAgICAgICByZXR1cm4gZy5zZXROb2RlKGVsLmlkLCB7XG4gICAgICAgICAgICBsYWJlbDogY3JlYXRlTGFiZWxOb2RlKGVsLCBcIm5leHRXb3Jrc2V0XCIsIG1heFcsIG1heEgpLFxuICAgICAgICAgICAgbGFiZWxUeXBlOiAnaHRtbCcsXG4gICAgICAgICAgICBcImNsYXNzXCI6IGdldE5vZGVUeXBlKGVsLCBcIm5leHRXb3Jrc2V0XCIpXG4gICAgICAgICAgfSk7XG4gICAgICAgIH0gZWxzZSBpZiAoZWwuaWQgPT09IGRhdGEuc29sdXRpb25fc2V0KSB7XG4gICAgICAgICAgcmV0dXJuIGcuc2V0Tm9kZShlbC5pZCwge1xuICAgICAgICAgICAgbGFiZWw6IGNyZWF0ZUxhYmVsTm9kZShlbCwgXCJzb2x1dGlvblNldFwiLCBtYXhXLCBtYXhIKSxcbiAgICAgICAgICAgIGxhYmVsVHlwZTogJ2h0bWwnLFxuICAgICAgICAgICAgXCJjbGFzc1wiOiBnZXROb2RlVHlwZShlbCwgXCJzb2x1dGlvblNldFwiKVxuICAgICAgICAgIH0pO1xuICAgICAgICB9IGVsc2UgaWYgKGVsLmlkID09PSBkYXRhLnNvbHV0aW9uX2RlbHRhKSB7XG4gICAgICAgICAgcmV0dXJuIGcuc2V0Tm9kZShlbC5pZCwge1xuICAgICAgICAgICAgbGFiZWw6IGNyZWF0ZUxhYmVsTm9kZShlbCwgXCJzb2x1dGlvbkRlbHRhXCIsIG1heFcsIG1heEgpLFxuICAgICAgICAgICAgbGFiZWxUeXBlOiAnaHRtbCcsXG4gICAgICAgICAgICBcImNsYXNzXCI6IGdldE5vZGVUeXBlKGVsLCBcInNvbHV0aW9uRGVsdGFcIilcbiAgICAgICAgICB9KTtcbiAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICByZXR1cm4gZy5zZXROb2RlKGVsLmlkLCB7XG4gICAgICAgICAgICBsYWJlbDogY3JlYXRlTGFiZWxOb2RlKGVsLCBcIlwiLCBtYXhXLCBtYXhIKSxcbiAgICAgICAgICAgIGxhYmVsVHlwZTogJ2h0bWwnLFxuICAgICAgICAgICAgXCJjbGFzc1wiOiBnZXROb2RlVHlwZShlbCwgXCJcIilcbiAgICAgICAgICB9KTtcbiAgICAgICAgfVxuICAgICAgfTtcbiAgICAgIGNyZWF0ZUVkZ2UgPSBmdW5jdGlvbihnLCBkYXRhLCBlbCwgZXhpc3RpbmdOb2RlcywgcHJlZCkge1xuICAgICAgICB2YXIgbWlzc2luZ05vZGU7XG4gICAgICAgIGlmIChleGlzdGluZ05vZGVzLmluZGV4T2YocHJlZC5pZCkgIT09IC0xKSB7XG4gICAgICAgICAgcmV0dXJuIGcuc2V0RWRnZShwcmVkLmlkLCBlbC5pZCwge1xuICAgICAgICAgICAgbGFiZWw6IGNyZWF0ZUxhYmVsRWRnZShwcmVkKSxcbiAgICAgICAgICAgIGxhYmVsVHlwZTogJ2h0bWwnLFxuICAgICAgICAgICAgYXJyb3doZWFkOiAnbm9ybWFsJ1xuICAgICAgICAgIH0pO1xuICAgICAgICB9IGVsc2Uge1xuICAgICAgICAgIG1pc3NpbmdOb2RlID0gc2VhcmNoRm9yTm9kZShkYXRhLCBwcmVkLmlkKTtcbiAgICAgICAgICBpZiAoISFtaXNzaW5nTm9kZSkge1xuICAgICAgICAgICAgcmV0dXJuIGcuc2V0RWRnZShtaXNzaW5nTm9kZS5pZCwgZWwuaWQsIHtcbiAgICAgICAgICAgICAgbGFiZWw6IGNyZWF0ZUxhYmVsRWRnZShtaXNzaW5nTm9kZSksXG4gICAgICAgICAgICAgIGxhYmVsVHlwZTogJ2h0bWwnXG4gICAgICAgICAgICB9KTtcbiAgICAgICAgICB9XG4gICAgICAgIH1cbiAgICAgIH07XG4gICAgICBsb2FkSnNvblRvRGFncmUgPSBmdW5jdGlvbihnLCBkYXRhKSB7XG4gICAgICAgIHZhciBlbCwgZXhpc3RpbmdOb2RlcywgaXNQYXJlbnQsIGssIGwsIGxlbiwgbGVuMSwgbWF4SCwgbWF4VywgcHJlZCwgciwgcmVmLCBzZywgdG9JdGVyYXRlO1xuICAgICAgICBleGlzdGluZ05vZGVzID0gW107XG4gICAgICAgIGlmIChkYXRhLm5vZGVzICE9IG51bGwpIHtcbiAgICAgICAgICB0b0l0ZXJhdGUgPSBkYXRhLm5vZGVzO1xuICAgICAgICB9IGVsc2Uge1xuICAgICAgICAgIHRvSXRlcmF0ZSA9IGRhdGEuc3RlcF9mdW5jdGlvbjtcbiAgICAgICAgICBpc1BhcmVudCA9IHRydWU7XG4gICAgICAgIH1cbiAgICAgICAgZm9yIChrID0gMCwgbGVuID0gdG9JdGVyYXRlLmxlbmd0aDsgayA8IGxlbjsgaysrKSB7XG4gICAgICAgICAgZWwgPSB0b0l0ZXJhdGVba107XG4gICAgICAgICAgbWF4VyA9IDA7XG4gICAgICAgICAgbWF4SCA9IDA7XG4gICAgICAgICAgaWYgKGVsLnN0ZXBfZnVuY3Rpb24pIHtcbiAgICAgICAgICAgIHNnID0gbmV3IGRhZ3JlRDMuZ3JhcGhsaWIuR3JhcGgoe1xuICAgICAgICAgICAgICBtdWx0aWdyYXBoOiB0cnVlLFxuICAgICAgICAgICAgICBjb21wb3VuZDogdHJ1ZVxuICAgICAgICAgICAgfSkuc2V0R3JhcGgoe1xuICAgICAgICAgICAgICBub2Rlc2VwOiAyMCxcbiAgICAgICAgICAgICAgZWRnZXNlcDogMCxcbiAgICAgICAgICAgICAgcmFua3NlcDogMjAsXG4gICAgICAgICAgICAgIHJhbmtkaXI6IFwiTFJcIixcbiAgICAgICAgICAgICAgbWFyZ2lueDogMTAsXG4gICAgICAgICAgICAgIG1hcmdpbnk6IDEwXG4gICAgICAgICAgICB9KTtcbiAgICAgICAgICAgIHN1YmdyYXBoc1tlbC5pZF0gPSBzZztcbiAgICAgICAgICAgIGxvYWRKc29uVG9EYWdyZShzZywgZWwpO1xuICAgICAgICAgICAgciA9IG5ldyBkYWdyZUQzLnJlbmRlcigpO1xuICAgICAgICAgICAgZDN0bXBTdmcuc2VsZWN0KCdnJykuY2FsbChyLCBzZyk7XG4gICAgICAgICAgICBtYXhXID0gc2cuZ3JhcGgoKS53aWR0aDtcbiAgICAgICAgICAgIG1heEggPSBzZy5ncmFwaCgpLmhlaWdodDtcbiAgICAgICAgICAgIGFuZ3VsYXIuZWxlbWVudChtYWluVG1wRWxlbWVudCkuZW1wdHkoKTtcbiAgICAgICAgICB9XG4gICAgICAgICAgY3JlYXRlTm9kZShnLCBkYXRhLCBlbCwgaXNQYXJlbnQsIG1heFcsIG1heEgpO1xuICAgICAgICAgIGV4aXN0aW5nTm9kZXMucHVzaChlbC5pZCk7XG4gICAgICAgICAgaWYgKGVsLmlucHV0cyAhPSBudWxsKSB7XG4gICAgICAgICAgICByZWYgPSBlbC5pbnB1dHM7XG4gICAgICAgICAgICBmb3IgKGwgPSAwLCBsZW4xID0gcmVmLmxlbmd0aDsgbCA8IGxlbjE7IGwrKykge1xuICAgICAgICAgICAgICBwcmVkID0gcmVmW2xdO1xuICAgICAgICAgICAgICBjcmVhdGVFZGdlKGcsIGRhdGEsIGVsLCBleGlzdGluZ05vZGVzLCBwcmVkKTtcbiAgICAgICAgICAgIH1cbiAgICAgICAgICB9XG4gICAgICAgIH1cbiAgICAgICAgcmV0dXJuIGc7XG4gICAgICB9O1xuICAgICAgc2VhcmNoRm9yTm9kZSA9IGZ1bmN0aW9uKGRhdGEsIG5vZGVJRCkge1xuICAgICAgICB2YXIgZWwsIGksIGo7XG4gICAgICAgIGZvciAoaSBpbiBkYXRhLm5vZGVzKSB7XG4gICAgICAgICAgZWwgPSBkYXRhLm5vZGVzW2ldO1xuICAgICAgICAgIGlmIChlbC5pZCA9PT0gbm9kZUlEKSB7XG4gICAgICAgICAgICByZXR1cm4gZWw7XG4gICAgICAgICAgfVxuICAgICAgICAgIGlmIChlbC5zdGVwX2Z1bmN0aW9uICE9IG51bGwpIHtcbiAgICAgICAgICAgIGZvciAoaiBpbiBlbC5zdGVwX2Z1bmN0aW9uKSB7XG4gICAgICAgICAgICAgIGlmIChlbC5zdGVwX2Z1bmN0aW9uW2pdLmlkID09PSBub2RlSUQpIHtcbiAgICAgICAgICAgICAgICByZXR1cm4gZWwuc3RlcF9mdW5jdGlvbltqXTtcbiAgICAgICAgICAgICAgfVxuICAgICAgICAgICAgfVxuICAgICAgICAgIH1cbiAgICAgICAgfVxuICAgICAgfTtcbiAgICAgIGRyYXdHcmFwaCA9IGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgICAgdmFyIGksIG5ld1NjYWxlLCByZW5kZXJlciwgc2csIHhDZW50ZXJPZmZzZXQsIHlDZW50ZXJPZmZzZXQ7XG4gICAgICAgIGcgPSBuZXcgZGFncmVEMy5ncmFwaGxpYi5HcmFwaCh7XG4gICAgICAgICAgbXVsdGlncmFwaDogdHJ1ZSxcbiAgICAgICAgICBjb21wb3VuZDogdHJ1ZVxuICAgICAgICB9KS5zZXRHcmFwaCh7XG4gICAgICAgICAgbm9kZXNlcDogNzAsXG4gICAgICAgICAgZWRnZXNlcDogMCxcbiAgICAgICAgICByYW5rc2VwOiA1MCxcbiAgICAgICAgICByYW5rZGlyOiBcIkxSXCIsXG4gICAgICAgICAgbWFyZ2lueDogNDAsXG4gICAgICAgICAgbWFyZ2lueTogNDBcbiAgICAgICAgfSk7XG4gICAgICAgIGxvYWRKc29uVG9EYWdyZShnLCBkYXRhKTtcbiAgICAgICAgcmVuZGVyZXIgPSBuZXcgZGFncmVEMy5yZW5kZXIoKTtcbiAgICAgICAgZDNtYWluU3ZnRy5jYWxsKHJlbmRlcmVyLCBnKTtcbiAgICAgICAgZm9yIChpIGluIHN1YmdyYXBocykge1xuICAgICAgICAgIHNnID0gc3ViZ3JhcGhzW2ldO1xuICAgICAgICAgIGQzbWFpblN2Zy5zZWxlY3QoJ3N2Zy5zdmctJyArIGkgKyAnIGcnKS5jYWxsKHJlbmRlcmVyLCBzZyk7XG4gICAgICAgIH1cbiAgICAgICAgbmV3U2NhbGUgPSAwLjU7XG4gICAgICAgIHhDZW50ZXJPZmZzZXQgPSBNYXRoLmZsb29yKChhbmd1bGFyLmVsZW1lbnQobWFpblN2Z0VsZW1lbnQpLndpZHRoKCkgLSBnLmdyYXBoKCkud2lkdGggKiBuZXdTY2FsZSkgLyAyKTtcbiAgICAgICAgeUNlbnRlck9mZnNldCA9IE1hdGguZmxvb3IoKGFuZ3VsYXIuZWxlbWVudChtYWluU3ZnRWxlbWVudCkuaGVpZ2h0KCkgLSBnLmdyYXBoKCkuaGVpZ2h0ICogbmV3U2NhbGUpIC8gMik7XG4gICAgICAgIG1haW5ab29tLnNjYWxlKG5ld1NjYWxlKS50cmFuc2xhdGUoW3hDZW50ZXJPZmZzZXQsIHlDZW50ZXJPZmZzZXRdKTtcbiAgICAgICAgZDNtYWluU3ZnRy5hdHRyKFwidHJhbnNmb3JtXCIsIFwidHJhbnNsYXRlKFwiICsgeENlbnRlck9mZnNldCArIFwiLCBcIiArIHlDZW50ZXJPZmZzZXQgKyBcIikgc2NhbGUoXCIgKyBtYWluWm9vbS5zY2FsZSgpICsgXCIpXCIpO1xuICAgICAgICBtYWluWm9vbS5vbihcInpvb21cIiwgZnVuY3Rpb24oKSB7XG4gICAgICAgICAgdmFyIGV2O1xuICAgICAgICAgIGV2ID0gZDMuZXZlbnQ7XG4gICAgICAgICAgcmV0dXJuIGQzbWFpblN2Z0cuYXR0cihcInRyYW5zZm9ybVwiLCBcInRyYW5zbGF0ZShcIiArIGV2LnRyYW5zbGF0ZSArIFwiKSBzY2FsZShcIiArIGV2LnNjYWxlICsgXCIpXCIpO1xuICAgICAgICB9KTtcbiAgICAgICAgbWFpblpvb20oZDNtYWluU3ZnKTtcbiAgICAgICAgcmV0dXJuIGQzbWFpblN2Z0cuc2VsZWN0QWxsKCcubm9kZScpLm9uKCdjbGljaycsIGZ1bmN0aW9uKGQpIHtcbiAgICAgICAgICByZXR1cm4gc2NvcGUuc2V0Tm9kZSh7XG4gICAgICAgICAgICBub2RlaWQ6IGRcbiAgICAgICAgICB9KTtcbiAgICAgICAgfSk7XG4gICAgICB9O1xuICAgICAgc2NvcGUuJHdhdGNoKGF0dHJzLnBsYW4sIGZ1bmN0aW9uKG5ld1BsYW4pIHtcbiAgICAgICAgaWYgKG5ld1BsYW4pIHtcbiAgICAgICAgICByZXR1cm4gZHJhd0dyYXBoKG5ld1BsYW4pO1xuICAgICAgICB9XG4gICAgICB9KTtcbiAgICB9XG4gIH07XG59KTtcbiIsIiNcbiMgTGljZW5zZWQgdG8gdGhlIEFwYWNoZSBTb2Z0d2FyZSBGb3VuZGF0aW9uIChBU0YpIHVuZGVyIG9uZVxuIyBvciBtb3JlIGNvbnRyaWJ1dG9yIGxpY2Vuc2UgYWdyZWVtZW50cy4gIFNlZSB0aGUgTk9USUNFIGZpbGVcbiMgZGlzdHJpYnV0ZWQgd2l0aCB0aGlzIHdvcmsgZm9yIGFkZGl0aW9uYWwgaW5mb3JtYXRpb25cbiMgcmVnYXJkaW5nIGNvcHlyaWdodCBvd25lcnNoaXAuICBUaGUgQVNGIGxpY2Vuc2VzIHRoaXMgZmlsZVxuIyB0byB5b3UgdW5kZXIgdGhlIEFwYWNoZSBMaWNlbnNlLCBWZXJzaW9uIDIuMCAodGhlXG4jIFwiTGljZW5zZVwiKTsgeW91IG1heSBub3QgdXNlIHRoaXMgZmlsZSBleGNlcHQgaW4gY29tcGxpYW5jZVxuIyB3aXRoIHRoZSBMaWNlbnNlLiAgWW91IG1heSBvYnRhaW4gYSBjb3B5IG9mIHRoZSBMaWNlbnNlIGF0XG4jXG4jICAgICBodHRwOi8vd3d3LmFwYWNoZS5vcmcvbGljZW5zZXMvTElDRU5TRS0yLjBcbiNcbiMgVW5sZXNzIHJlcXVpcmVkIGJ5IGFwcGxpY2FibGUgbGF3IG9yIGFncmVlZCB0byBpbiB3cml0aW5nLCBzb2Z0d2FyZVxuIyBkaXN0cmlidXRlZCB1bmRlciB0aGUgTGljZW5zZSBpcyBkaXN0cmlidXRlZCBvbiBhbiBcIkFTIElTXCIgQkFTSVMsXG4jIFdJVEhPVVQgV0FSUkFOVElFUyBPUiBDT05ESVRJT05TIE9GIEFOWSBLSU5ELCBlaXRoZXIgZXhwcmVzcyBvciBpbXBsaWVkLlxuIyBTZWUgdGhlIExpY2Vuc2UgZm9yIHRoZSBzcGVjaWZpYyBsYW5ndWFnZSBnb3Zlcm5pbmcgcGVybWlzc2lvbnMgYW5kXG4jIGxpbWl0YXRpb25zIHVuZGVyIHRoZSBMaWNlbnNlLlxuI1xuXG5hbmd1bGFyLm1vZHVsZSgnZmxpbmtBcHAnKVxuXG4uc2VydmljZSAnSm9ic1NlcnZpY2UnLCAoJGh0dHAsIGZsaW5rQ29uZmlnLCAkbG9nLCBhbU1vbWVudCwgJHEsICR0aW1lb3V0KSAtPlxuICBjdXJyZW50Sm9iID0gbnVsbFxuICBjdXJyZW50UGxhbiA9IG51bGxcblxuICBkZWZlcnJlZHMgPSB7fVxuICBqb2JzID0ge1xuICAgIHJ1bm5pbmc6IFtdXG4gICAgZmluaXNoZWQ6IFtdXG4gICAgY2FuY2VsbGVkOiBbXVxuICAgIGZhaWxlZDogW11cbiAgfVxuXG4gIGpvYk9ic2VydmVycyA9IFtdXG5cbiAgbm90aWZ5T2JzZXJ2ZXJzID0gLT5cbiAgICBhbmd1bGFyLmZvckVhY2ggam9iT2JzZXJ2ZXJzLCAoY2FsbGJhY2spIC0+XG4gICAgICBjYWxsYmFjaygpXG5cbiAgQHJlZ2lzdGVyT2JzZXJ2ZXIgPSAoY2FsbGJhY2spIC0+XG4gICAgam9iT2JzZXJ2ZXJzLnB1c2goY2FsbGJhY2spXG5cbiAgQHVuUmVnaXN0ZXJPYnNlcnZlciA9IChjYWxsYmFjaykgLT5cbiAgICBpbmRleCA9IGpvYk9ic2VydmVycy5pbmRleE9mKGNhbGxiYWNrKVxuICAgIGpvYk9ic2VydmVycy5zcGxpY2UoaW5kZXgsIDEpXG5cbiAgQHN0YXRlTGlzdCA9IC0+XG4gICAgWyBcbiAgICAgICMgJ0NSRUFURUQnXG4gICAgICAnU0NIRURVTEVEJ1xuICAgICAgJ0RFUExPWUlORydcbiAgICAgICdSVU5OSU5HJ1xuICAgICAgJ0ZJTklTSEVEJ1xuICAgICAgJ0ZBSUxFRCdcbiAgICAgICdDQU5DRUxJTkcnXG4gICAgICAnQ0FOQ0VMRUQnXG4gICAgXVxuXG4gIEB0cmFuc2xhdGVMYWJlbFN0YXRlID0gKHN0YXRlKSAtPlxuICAgIHN3aXRjaCBzdGF0ZS50b0xvd2VyQ2FzZSgpXG4gICAgICB3aGVuICdmaW5pc2hlZCcgdGhlbiAnc3VjY2VzcydcbiAgICAgIHdoZW4gJ2ZhaWxlZCcgdGhlbiAnZGFuZ2VyJ1xuICAgICAgd2hlbiAnc2NoZWR1bGVkJyB0aGVuICdkZWZhdWx0J1xuICAgICAgd2hlbiAnZGVwbG95aW5nJyB0aGVuICdpbmZvJ1xuICAgICAgd2hlbiAncnVubmluZycgdGhlbiAncHJpbWFyeSdcbiAgICAgIHdoZW4gJ2NhbmNlbGluZycgdGhlbiAnd2FybmluZydcbiAgICAgIHdoZW4gJ3BlbmRpbmcnIHRoZW4gJ2luZm8nXG4gICAgICB3aGVuICd0b3RhbCcgdGhlbiAnYmxhY2snXG4gICAgICBlbHNlICdkZWZhdWx0J1xuXG4gIEBzZXRFbmRUaW1lcyA9IChsaXN0KSAtPlxuICAgIGFuZ3VsYXIuZm9yRWFjaCBsaXN0LCAoaXRlbSwgam9iS2V5KSAtPlxuICAgICAgdW5sZXNzIGl0ZW1bJ2VuZC10aW1lJ10gPiAtMVxuICAgICAgICBpdGVtWydlbmQtdGltZSddID0gaXRlbVsnc3RhcnQtdGltZSddICsgaXRlbVsnZHVyYXRpb24nXVxuXG4gIEBwcm9jZXNzVmVydGljZXMgPSAoZGF0YSkgLT5cbiAgICBhbmd1bGFyLmZvckVhY2ggZGF0YS52ZXJ0aWNlcywgKHZlcnRleCwgaSkgLT5cbiAgICAgIHZlcnRleC50eXBlID0gJ3JlZ3VsYXInXG5cbiAgICBkYXRhLnZlcnRpY2VzLnVuc2hpZnQoe1xuICAgICAgbmFtZTogJ1NjaGVkdWxlZCdcbiAgICAgICdzdGFydC10aW1lJzogZGF0YS50aW1lc3RhbXBzWydDUkVBVEVEJ11cbiAgICAgICdlbmQtdGltZSc6IGRhdGEudGltZXN0YW1wc1snQ1JFQVRFRCddICsgMVxuICAgICAgdHlwZTogJ3NjaGVkdWxlZCdcbiAgICB9KVxuXG4gIEBsaXN0Sm9icyA9IC0+XG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpXG5cbiAgICAkaHR0cC5nZXQgZmxpbmtDb25maWcuam9iU2VydmVyICsgXCJqb2JvdmVydmlld1wiXG4gICAgLnN1Y2Nlc3MgKGRhdGEsIHN0YXR1cywgaGVhZGVycywgY29uZmlnKSA9PlxuICAgICAgYW5ndWxhci5mb3JFYWNoIGRhdGEsIChsaXN0LCBsaXN0S2V5KSA9PlxuICAgICAgICBzd2l0Y2ggbGlzdEtleVxuICAgICAgICAgIHdoZW4gJ3J1bm5pbmcnIHRoZW4gam9icy5ydW5uaW5nID0gQHNldEVuZFRpbWVzKGxpc3QpXG4gICAgICAgICAgd2hlbiAnZmluaXNoZWQnIHRoZW4gam9icy5maW5pc2hlZCA9IEBzZXRFbmRUaW1lcyhsaXN0KVxuICAgICAgICAgIHdoZW4gJ2NhbmNlbGxlZCcgdGhlbiBqb2JzLmNhbmNlbGxlZCA9IEBzZXRFbmRUaW1lcyhsaXN0KVxuICAgICAgICAgIHdoZW4gJ2ZhaWxlZCcgdGhlbiBqb2JzLmZhaWxlZCA9IEBzZXRFbmRUaW1lcyhsaXN0KVxuXG4gICAgICBkZWZlcnJlZC5yZXNvbHZlKGpvYnMpXG4gICAgICBub3RpZnlPYnNlcnZlcnMoKVxuXG4gICAgZGVmZXJyZWQucHJvbWlzZVxuXG4gIEBnZXRKb2JzID0gKHR5cGUpIC0+XG4gICAgam9ic1t0eXBlXVxuXG4gIEBnZXRBbGxKb2JzID0gLT5cbiAgICBqb2JzXG5cbiAgQGxvYWRKb2IgPSAoam9iaWQpIC0+XG4gICAgY3VycmVudEpvYiA9IG51bGxcbiAgICBkZWZlcnJlZHMuam9iID0gJHEuZGVmZXIoKVxuXG4gICAgJGh0dHAuZ2V0IGZsaW5rQ29uZmlnLmpvYlNlcnZlciArIFwiam9icy9cIiArIGpvYmlkXG4gICAgLnN1Y2Nlc3MgKGRhdGEsIHN0YXR1cywgaGVhZGVycywgY29uZmlnKSA9PlxuICAgICAgQHNldEVuZFRpbWVzKGRhdGEudmVydGljZXMpXG4gICAgICBAcHJvY2Vzc1ZlcnRpY2VzKGRhdGEpXG5cbiAgICAgICRodHRwLmdldCBmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcImpvYnMvXCIgKyBqb2JpZCArIFwiL2NvbmZpZ1wiXG4gICAgICAuc3VjY2VzcyAoam9iQ29uZmlnKSAtPlxuICAgICAgICBkYXRhID0gYW5ndWxhci5leHRlbmQoZGF0YSwgam9iQ29uZmlnKVxuXG4gICAgICAgIGN1cnJlbnRKb2IgPSBkYXRhXG5cbiAgICAgICAgZGVmZXJyZWRzLmpvYi5yZXNvbHZlKGN1cnJlbnRKb2IpXG5cbiAgICBkZWZlcnJlZHMuam9iLnByb21pc2VcblxuICBAZ2V0Tm9kZSA9IChub2RlaWQpIC0+XG4gICAgc2Vla05vZGUgPSAobm9kZWlkLCBkYXRhKSAtPlxuICAgICAgZm9yIG5vZGUgaW4gZGF0YVxuICAgICAgICByZXR1cm4gbm9kZSBpZiBub2RlLmlkIGlzIG5vZGVpZFxuICAgICAgICBzdWIgPSBzZWVrTm9kZShub2RlaWQsIG5vZGUuc3RlcF9mdW5jdGlvbikgaWYgbm9kZS5zdGVwX2Z1bmN0aW9uXG4gICAgICAgIHJldHVybiBzdWIgaWYgc3ViXG5cbiAgICAgIG51bGxcblxuICAgIGRlZmVycmVkID0gJHEuZGVmZXIoKVxuXG4gICAgZGVmZXJyZWRzLmpvYi5wcm9taXNlLnRoZW4gKGRhdGEpID0+XG4gICAgICBmb3VuZE5vZGUgPSBzZWVrTm9kZShub2RlaWQsIGN1cnJlbnRKb2IucGxhbi5ub2RlcylcblxuICAgICAgZm91bmROb2RlLnZlcnRleCA9IEBzZWVrVmVydGV4KG5vZGVpZClcblxuICAgICAgZGVmZXJyZWQucmVzb2x2ZShmb3VuZE5vZGUpXG5cbiAgICBkZWZlcnJlZC5wcm9taXNlXG5cbiAgQHNlZWtWZXJ0ZXggPSAobm9kZWlkKSAtPlxuICAgIGZvciB2ZXJ0ZXggaW4gY3VycmVudEpvYi52ZXJ0aWNlc1xuICAgICAgcmV0dXJuIHZlcnRleCBpZiB2ZXJ0ZXguaWQgaXMgbm9kZWlkXG5cbiAgICByZXR1cm4gbnVsbFxuXG4gIEBnZXRWZXJ0ZXggPSAodmVydGV4aWQpIC0+XG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpXG5cbiAgICBkZWZlcnJlZHMuam9iLnByb21pc2UudGhlbiAoZGF0YSkgPT5cbiAgICAgIHZlcnRleCA9IEBzZWVrVmVydGV4KHZlcnRleGlkKVxuXG4gICAgICAkaHR0cC5nZXQgZmxpbmtDb25maWcuam9iU2VydmVyICsgXCJqb2JzL1wiICsgY3VycmVudEpvYi5qaWQgKyBcIi92ZXJ0aWNlcy9cIiArIHZlcnRleGlkICsgXCIvc3VidGFza3RpbWVzXCJcbiAgICAgIC5zdWNjZXNzIChkYXRhKSA9PlxuICAgICAgICAjIFRPRE86IGNoYW5nZSB0byBzdWJ0YXNrdGltZXNcbiAgICAgICAgdmVydGV4LnN1YnRhc2tzID0gZGF0YS5zdWJ0YXNrc1xuXG4gICAgICAgIGRlZmVycmVkLnJlc29sdmUodmVydGV4KVxuXG4gICAgZGVmZXJyZWQucHJvbWlzZVxuXG4gIEBnZXRTdWJ0YXNrcyA9ICh2ZXJ0ZXhpZCkgLT5cbiAgICBkZWZlcnJlZCA9ICRxLmRlZmVyKClcblxuICAgIGRlZmVycmVkcy5qb2IucHJvbWlzZS50aGVuIChkYXRhKSA9PlxuICAgICAgIyB2ZXJ0ZXggPSBAc2Vla1ZlcnRleCh2ZXJ0ZXhpZClcblxuICAgICAgJGh0dHAuZ2V0IGZsaW5rQ29uZmlnLmpvYlNlcnZlciArIFwiam9icy9cIiArIGN1cnJlbnRKb2IuamlkICsgXCIvdmVydGljZXMvXCIgKyB2ZXJ0ZXhpZFxuICAgICAgLnN1Y2Nlc3MgKGRhdGEpIC0+XG4gICAgICAgIHN1YnRhc2tzID0gZGF0YS5zdWJ0YXNrc1xuXG4gICAgICAgIGRlZmVycmVkLnJlc29sdmUoc3VidGFza3MpXG5cbiAgICBkZWZlcnJlZC5wcm9taXNlXG5cbiAgQGdldFRhc2tNYW5hZ2VycyA9ICh2ZXJ0ZXhpZCkgLT5cbiAgICBkZWZlcnJlZCA9ICRxLmRlZmVyKClcblxuICAgIGRlZmVycmVkcy5qb2IucHJvbWlzZS50aGVuIChkYXRhKSA9PlxuICAgICAgIyB2ZXJ0ZXggPSBAc2Vla1ZlcnRleCh2ZXJ0ZXhpZClcblxuICAgICAgJGh0dHAuZ2V0IGZsaW5rQ29uZmlnLmpvYlNlcnZlciArIFwiam9icy9cIiArIGN1cnJlbnRKb2IuamlkICsgXCIvdmVydGljZXMvXCIgKyB2ZXJ0ZXhpZCArIFwiL3Rhc2ttYW5hZ2Vyc1wiXG4gICAgICAuc3VjY2VzcyAoZGF0YSkgLT5cbiAgICAgICAgdGFza21hbmFnZXJzID0gZGF0YS50YXNrbWFuYWdlcnNcblxuICAgICAgICBkZWZlcnJlZC5yZXNvbHZlKHRhc2ttYW5hZ2VycylcblxuICAgIGRlZmVycmVkLnByb21pc2VcblxuICBAZ2V0QWNjdW11bGF0b3JzID0gKHZlcnRleGlkKSAtPlxuICAgIGRlZmVycmVkID0gJHEuZGVmZXIoKVxuXG4gICAgZGVmZXJyZWRzLmpvYi5wcm9taXNlLnRoZW4gKGRhdGEpID0+XG4gICAgICAjIHZlcnRleCA9IEBzZWVrVmVydGV4KHZlcnRleGlkKVxuXG4gICAgICAkaHR0cC5nZXQgZmxpbmtDb25maWcuam9iU2VydmVyICsgXCJqb2JzL1wiICsgY3VycmVudEpvYi5qaWQgKyBcIi92ZXJ0aWNlcy9cIiArIHZlcnRleGlkICsgXCIvYWNjdW11bGF0b3JzXCJcbiAgICAgIC5zdWNjZXNzIChkYXRhKSAtPlxuICAgICAgICBhY2N1bXVsYXRvcnMgPSBkYXRhWyd1c2VyLWFjY3VtdWxhdG9ycyddXG5cbiAgICAgICAgJGh0dHAuZ2V0IGZsaW5rQ29uZmlnLmpvYlNlcnZlciArIFwiam9icy9cIiArIGN1cnJlbnRKb2IuamlkICsgXCIvdmVydGljZXMvXCIgKyB2ZXJ0ZXhpZCArIFwiL3N1YnRhc2tzL2FjY3VtdWxhdG9yc1wiXG4gICAgICAgIC5zdWNjZXNzIChkYXRhKSAtPlxuICAgICAgICAgIHN1YnRhc2tBY2N1bXVsYXRvcnMgPSBkYXRhLnN1YnRhc2tzXG5cbiAgICAgICAgICBkZWZlcnJlZC5yZXNvbHZlKHsgbWFpbjogYWNjdW11bGF0b3JzLCBzdWJ0YXNrczogc3VidGFza0FjY3VtdWxhdG9ycyB9KVxuXG4gICAgZGVmZXJyZWQucHJvbWlzZVxuXG4gICMgSm9iLWxldmVsIGNoZWNrcG9pbnQgc3RhdHNcbiAgQGdldEpvYkNoZWNrcG9pbnRTdGF0cyA9IChqb2JpZCkgLT5cbiAgICBkZWZlcnJlZCA9ICRxLmRlZmVyKClcblxuICAgICRodHRwLmdldCBmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcImpvYnMvXCIgKyBqb2JpZCArIFwiL2NoZWNrcG9pbnRzXCJcbiAgICAuc3VjY2VzcyAoZGF0YSwgc3RhdHVzLCBoZWFkZXJzLCBjb25maWcpID0+XG4gICAgICBpZiAoYW5ndWxhci5lcXVhbHMoe30sIGRhdGEpKVxuICAgICAgICBkZWZlcnJlZC5yZXNvbHZlKGRlZmVycmVkLnJlc29sdmUobnVsbCkpXG4gICAgICBlbHNlXG4gICAgICAgIGRlZmVycmVkLnJlc29sdmUoZGF0YSlcblxuICAgIGRlZmVycmVkLnByb21pc2VcblxuICAjIE9wZXJhdG9yLWxldmVsIGNoZWNrcG9pbnQgc3RhdHNcbiAgQGdldE9wZXJhdG9yQ2hlY2twb2ludFN0YXRzID0gKHZlcnRleGlkKSAtPlxuICAgIGRlZmVycmVkID0gJHEuZGVmZXIoKVxuXG4gICAgZGVmZXJyZWRzLmpvYi5wcm9taXNlLnRoZW4gKGRhdGEpID0+XG4gICAgICAkaHR0cC5nZXQgZmxpbmtDb25maWcuam9iU2VydmVyICsgXCJqb2JzL1wiICsgY3VycmVudEpvYi5qaWQgKyBcIi92ZXJ0aWNlcy9cIiArIHZlcnRleGlkICsgXCIvY2hlY2twb2ludHNcIlxuICAgICAgLnN1Y2Nlc3MgKGRhdGEpIC0+XG4gICAgICAgICMgSWYgbm8gZGF0YSBhdmFpbGFibGUsIHdlIGFyZSBkb25lLlxuICAgICAgICBpZiAoYW5ndWxhci5lcXVhbHMoe30sIGRhdGEpKVxuICAgICAgICAgIGRlZmVycmVkLnJlc29sdmUoeyBvcGVyYXRvclN0YXRzOiBudWxsLCBzdWJ0YXNrc1N0YXRzOiBudWxsIH0pXG4gICAgICAgIGVsc2VcbiAgICAgICAgICBvcGVyYXRvclN0YXRzID0geyBpZDogZGF0YVsnaWQnXSwgdGltZXN0YW1wOiBkYXRhWyd0aW1lc3RhbXAnXSwgZHVyYXRpb246IGRhdGFbJ2R1cmF0aW9uJ10sIHNpemU6IGRhdGFbJ3NpemUnXSB9XG5cbiAgICAgICAgICBpZiAoYW5ndWxhci5lcXVhbHMoe30sIGRhdGFbJ3N1YnRhc2tzJ10pKVxuICAgICAgICAgICAgZGVmZXJyZWQucmVzb2x2ZSh7IG9wZXJhdG9yU3RhdHM6IG9wZXJhdG9yU3RhdHMsIHN1YnRhc2tzU3RhdHM6IG51bGwgfSlcbiAgICAgICAgICBlbHNlXG4gICAgICAgICAgICBzdWJ0YXNrU3RhdHMgPSBkYXRhWydzdWJ0YXNrcyddXG4gICAgICAgICAgICBkZWZlcnJlZC5yZXNvbHZlKHsgb3BlcmF0b3JTdGF0czogb3BlcmF0b3JTdGF0cywgc3VidGFza3NTdGF0czogc3VidGFza1N0YXRzIH0pXG5cbiAgICBkZWZlcnJlZC5wcm9taXNlXG5cbiAgIyBPcGVyYXRvci1sZXZlbCBiYWNrIHByZXNzdXJlIHN0YXRzXG4gIEBnZXRPcGVyYXRvckJhY2tQcmVzc3VyZSA9ICh2ZXJ0ZXhpZCkgLT5cbiAgICBkZWZlcnJlZCA9ICRxLmRlZmVyKClcblxuICAgICRodHRwLmdldCBmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcImpvYnMvXCIgKyBjdXJyZW50Sm9iLmppZCArIFwiL3ZlcnRpY2VzL1wiICsgdmVydGV4aWQgKyBcIi9iYWNrcHJlc3N1cmVcIlxuICAgIC5zdWNjZXNzIChkYXRhKSA9PlxuICAgICAgZGVmZXJyZWQucmVzb2x2ZShkYXRhKVxuXG4gICAgZGVmZXJyZWQucHJvbWlzZVxuXG4gIEB0cmFuc2xhdGVCYWNrUHJlc3N1cmVMYWJlbFN0YXRlID0gKHN0YXRlKSAtPlxuICAgIHN3aXRjaCBzdGF0ZS50b0xvd2VyQ2FzZSgpXG4gICAgICB3aGVuICdpbi1wcm9ncmVzcycgdGhlbiAnZGFuZ2VyJ1xuICAgICAgd2hlbiAnb2snIHRoZW4gJ3N1Y2Nlc3MnXG4gICAgICB3aGVuICdsb3cnIHRoZW4gJ3dhcm5pbmcnXG4gICAgICB3aGVuICdoaWdoJyB0aGVuICdkYW5nZXInXG4gICAgICBlbHNlICdkZWZhdWx0J1xuXG4gIEBsb2FkRXhjZXB0aW9ucyA9IC0+XG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpXG5cbiAgICBkZWZlcnJlZHMuam9iLnByb21pc2UudGhlbiAoZGF0YSkgPT5cblxuICAgICAgJGh0dHAuZ2V0IGZsaW5rQ29uZmlnLmpvYlNlcnZlciArIFwiam9icy9cIiArIGN1cnJlbnRKb2IuamlkICsgXCIvZXhjZXB0aW9uc1wiXG4gICAgICAuc3VjY2VzcyAoZXhjZXB0aW9ucykgLT5cbiAgICAgICAgY3VycmVudEpvYi5leGNlcHRpb25zID0gZXhjZXB0aW9uc1xuXG4gICAgICAgIGRlZmVycmVkLnJlc29sdmUoZXhjZXB0aW9ucylcblxuICAgIGRlZmVycmVkLnByb21pc2VcblxuICBAY2FuY2VsSm9iID0gKGpvYmlkKSAtPlxuICAgICMgdXNlcyB0aGUgbm9uIFJFU1QtY29tcGxpYW50IEdFVCB5YXJuLWNhbmNlbCBoYW5kbGVyIHdoaWNoIGlzIGF2YWlsYWJsZSBpbiBhZGRpdGlvbiB0byB0aGVcbiAgICAjIHByb3BlciBcIkRFTEVURSBqb2JzLzxqb2JpZD4vXCJcbiAgICAkaHR0cC5nZXQgZmxpbmtDb25maWcuam9iU2VydmVyICsgXCJqb2JzL1wiICsgam9iaWQgKyBcIi95YXJuLWNhbmNlbFwiXG5cbiAgQHN0b3BKb2IgPSAoam9iaWQpIC0+XG4gICAgIyB1c2VzIHRoZSBub24gUkVTVC1jb21wbGlhbnQgR0VUIHlhcm4tY2FuY2VsIGhhbmRsZXIgd2hpY2ggaXMgYXZhaWxhYmxlIGluIGFkZGl0aW9uIHRvIHRoZVxuICAgICMgcHJvcGVyIFwiREVMRVRFIGpvYnMvPGpvYmlkPi9cIlxuICAgICRodHRwLmdldCBcImpvYnMvXCIgKyBqb2JpZCArIFwiL3lhcm4tc3RvcFwiXG5cbiAgQFxuIiwiYW5ndWxhci5tb2R1bGUoJ2ZsaW5rQXBwJykuc2VydmljZSgnSm9ic1NlcnZpY2UnLCBmdW5jdGlvbigkaHR0cCwgZmxpbmtDb25maWcsICRsb2csIGFtTW9tZW50LCAkcSwgJHRpbWVvdXQpIHtcbiAgdmFyIGN1cnJlbnRKb2IsIGN1cnJlbnRQbGFuLCBkZWZlcnJlZHMsIGpvYk9ic2VydmVycywgam9icywgbm90aWZ5T2JzZXJ2ZXJzO1xuICBjdXJyZW50Sm9iID0gbnVsbDtcbiAgY3VycmVudFBsYW4gPSBudWxsO1xuICBkZWZlcnJlZHMgPSB7fTtcbiAgam9icyA9IHtcbiAgICBydW5uaW5nOiBbXSxcbiAgICBmaW5pc2hlZDogW10sXG4gICAgY2FuY2VsbGVkOiBbXSxcbiAgICBmYWlsZWQ6IFtdXG4gIH07XG4gIGpvYk9ic2VydmVycyA9IFtdO1xuICBub3RpZnlPYnNlcnZlcnMgPSBmdW5jdGlvbigpIHtcbiAgICByZXR1cm4gYW5ndWxhci5mb3JFYWNoKGpvYk9ic2VydmVycywgZnVuY3Rpb24oY2FsbGJhY2spIHtcbiAgICAgIHJldHVybiBjYWxsYmFjaygpO1xuICAgIH0pO1xuICB9O1xuICB0aGlzLnJlZ2lzdGVyT2JzZXJ2ZXIgPSBmdW5jdGlvbihjYWxsYmFjaykge1xuICAgIHJldHVybiBqb2JPYnNlcnZlcnMucHVzaChjYWxsYmFjayk7XG4gIH07XG4gIHRoaXMudW5SZWdpc3Rlck9ic2VydmVyID0gZnVuY3Rpb24oY2FsbGJhY2spIHtcbiAgICB2YXIgaW5kZXg7XG4gICAgaW5kZXggPSBqb2JPYnNlcnZlcnMuaW5kZXhPZihjYWxsYmFjayk7XG4gICAgcmV0dXJuIGpvYk9ic2VydmVycy5zcGxpY2UoaW5kZXgsIDEpO1xuICB9O1xuICB0aGlzLnN0YXRlTGlzdCA9IGZ1bmN0aW9uKCkge1xuICAgIHJldHVybiBbJ1NDSEVEVUxFRCcsICdERVBMT1lJTkcnLCAnUlVOTklORycsICdGSU5JU0hFRCcsICdGQUlMRUQnLCAnQ0FOQ0VMSU5HJywgJ0NBTkNFTEVEJ107XG4gIH07XG4gIHRoaXMudHJhbnNsYXRlTGFiZWxTdGF0ZSA9IGZ1bmN0aW9uKHN0YXRlKSB7XG4gICAgc3dpdGNoIChzdGF0ZS50b0xvd2VyQ2FzZSgpKSB7XG4gICAgICBjYXNlICdmaW5pc2hlZCc6XG4gICAgICAgIHJldHVybiAnc3VjY2Vzcyc7XG4gICAgICBjYXNlICdmYWlsZWQnOlxuICAgICAgICByZXR1cm4gJ2Rhbmdlcic7XG4gICAgICBjYXNlICdzY2hlZHVsZWQnOlxuICAgICAgICByZXR1cm4gJ2RlZmF1bHQnO1xuICAgICAgY2FzZSAnZGVwbG95aW5nJzpcbiAgICAgICAgcmV0dXJuICdpbmZvJztcbiAgICAgIGNhc2UgJ3J1bm5pbmcnOlxuICAgICAgICByZXR1cm4gJ3ByaW1hcnknO1xuICAgICAgY2FzZSAnY2FuY2VsaW5nJzpcbiAgICAgICAgcmV0dXJuICd3YXJuaW5nJztcbiAgICAgIGNhc2UgJ3BlbmRpbmcnOlxuICAgICAgICByZXR1cm4gJ2luZm8nO1xuICAgICAgY2FzZSAndG90YWwnOlxuICAgICAgICByZXR1cm4gJ2JsYWNrJztcbiAgICAgIGRlZmF1bHQ6XG4gICAgICAgIHJldHVybiAnZGVmYXVsdCc7XG4gICAgfVxuICB9O1xuICB0aGlzLnNldEVuZFRpbWVzID0gZnVuY3Rpb24obGlzdCkge1xuICAgIHJldHVybiBhbmd1bGFyLmZvckVhY2gobGlzdCwgZnVuY3Rpb24oaXRlbSwgam9iS2V5KSB7XG4gICAgICBpZiAoIShpdGVtWydlbmQtdGltZSddID4gLTEpKSB7XG4gICAgICAgIHJldHVybiBpdGVtWydlbmQtdGltZSddID0gaXRlbVsnc3RhcnQtdGltZSddICsgaXRlbVsnZHVyYXRpb24nXTtcbiAgICAgIH1cbiAgICB9KTtcbiAgfTtcbiAgdGhpcy5wcm9jZXNzVmVydGljZXMgPSBmdW5jdGlvbihkYXRhKSB7XG4gICAgYW5ndWxhci5mb3JFYWNoKGRhdGEudmVydGljZXMsIGZ1bmN0aW9uKHZlcnRleCwgaSkge1xuICAgICAgcmV0dXJuIHZlcnRleC50eXBlID0gJ3JlZ3VsYXInO1xuICAgIH0pO1xuICAgIHJldHVybiBkYXRhLnZlcnRpY2VzLnVuc2hpZnQoe1xuICAgICAgbmFtZTogJ1NjaGVkdWxlZCcsXG4gICAgICAnc3RhcnQtdGltZSc6IGRhdGEudGltZXN0YW1wc1snQ1JFQVRFRCddLFxuICAgICAgJ2VuZC10aW1lJzogZGF0YS50aW1lc3RhbXBzWydDUkVBVEVEJ10gKyAxLFxuICAgICAgdHlwZTogJ3NjaGVkdWxlZCdcbiAgICB9KTtcbiAgfTtcbiAgdGhpcy5saXN0Sm9icyA9IGZ1bmN0aW9uKCkge1xuICAgIHZhciBkZWZlcnJlZDtcbiAgICBkZWZlcnJlZCA9ICRxLmRlZmVyKCk7XG4gICAgJGh0dHAuZ2V0KGZsaW5rQ29uZmlnLmpvYlNlcnZlciArIFwiam9ib3ZlcnZpZXdcIikuc3VjY2VzcygoZnVuY3Rpb24oX3RoaXMpIHtcbiAgICAgIHJldHVybiBmdW5jdGlvbihkYXRhLCBzdGF0dXMsIGhlYWRlcnMsIGNvbmZpZykge1xuICAgICAgICBhbmd1bGFyLmZvckVhY2goZGF0YSwgZnVuY3Rpb24obGlzdCwgbGlzdEtleSkge1xuICAgICAgICAgIHN3aXRjaCAobGlzdEtleSkge1xuICAgICAgICAgICAgY2FzZSAncnVubmluZyc6XG4gICAgICAgICAgICAgIHJldHVybiBqb2JzLnJ1bm5pbmcgPSBfdGhpcy5zZXRFbmRUaW1lcyhsaXN0KTtcbiAgICAgICAgICAgIGNhc2UgJ2ZpbmlzaGVkJzpcbiAgICAgICAgICAgICAgcmV0dXJuIGpvYnMuZmluaXNoZWQgPSBfdGhpcy5zZXRFbmRUaW1lcyhsaXN0KTtcbiAgICAgICAgICAgIGNhc2UgJ2NhbmNlbGxlZCc6XG4gICAgICAgICAgICAgIHJldHVybiBqb2JzLmNhbmNlbGxlZCA9IF90aGlzLnNldEVuZFRpbWVzKGxpc3QpO1xuICAgICAgICAgICAgY2FzZSAnZmFpbGVkJzpcbiAgICAgICAgICAgICAgcmV0dXJuIGpvYnMuZmFpbGVkID0gX3RoaXMuc2V0RW5kVGltZXMobGlzdCk7XG4gICAgICAgICAgfVxuICAgICAgICB9KTtcbiAgICAgICAgZGVmZXJyZWQucmVzb2x2ZShqb2JzKTtcbiAgICAgICAgcmV0dXJuIG5vdGlmeU9ic2VydmVycygpO1xuICAgICAgfTtcbiAgICB9KSh0aGlzKSk7XG4gICAgcmV0dXJuIGRlZmVycmVkLnByb21pc2U7XG4gIH07XG4gIHRoaXMuZ2V0Sm9icyA9IGZ1bmN0aW9uKHR5cGUpIHtcbiAgICByZXR1cm4gam9ic1t0eXBlXTtcbiAgfTtcbiAgdGhpcy5nZXRBbGxKb2JzID0gZnVuY3Rpb24oKSB7XG4gICAgcmV0dXJuIGpvYnM7XG4gIH07XG4gIHRoaXMubG9hZEpvYiA9IGZ1bmN0aW9uKGpvYmlkKSB7XG4gICAgY3VycmVudEpvYiA9IG51bGw7XG4gICAgZGVmZXJyZWRzLmpvYiA9ICRxLmRlZmVyKCk7XG4gICAgJGh0dHAuZ2V0KGZsaW5rQ29uZmlnLmpvYlNlcnZlciArIFwiam9icy9cIiArIGpvYmlkKS5zdWNjZXNzKChmdW5jdGlvbihfdGhpcykge1xuICAgICAgcmV0dXJuIGZ1bmN0aW9uKGRhdGEsIHN0YXR1cywgaGVhZGVycywgY29uZmlnKSB7XG4gICAgICAgIF90aGlzLnNldEVuZFRpbWVzKGRhdGEudmVydGljZXMpO1xuICAgICAgICBfdGhpcy5wcm9jZXNzVmVydGljZXMoZGF0YSk7XG4gICAgICAgIHJldHVybiAkaHR0cC5nZXQoZmxpbmtDb25maWcuam9iU2VydmVyICsgXCJqb2JzL1wiICsgam9iaWQgKyBcIi9jb25maWdcIikuc3VjY2VzcyhmdW5jdGlvbihqb2JDb25maWcpIHtcbiAgICAgICAgICBkYXRhID0gYW5ndWxhci5leHRlbmQoZGF0YSwgam9iQ29uZmlnKTtcbiAgICAgICAgICBjdXJyZW50Sm9iID0gZGF0YTtcbiAgICAgICAgICByZXR1cm4gZGVmZXJyZWRzLmpvYi5yZXNvbHZlKGN1cnJlbnRKb2IpO1xuICAgICAgICB9KTtcbiAgICAgIH07XG4gICAgfSkodGhpcykpO1xuICAgIHJldHVybiBkZWZlcnJlZHMuam9iLnByb21pc2U7XG4gIH07XG4gIHRoaXMuZ2V0Tm9kZSA9IGZ1bmN0aW9uKG5vZGVpZCkge1xuICAgIHZhciBkZWZlcnJlZCwgc2Vla05vZGU7XG4gICAgc2Vla05vZGUgPSBmdW5jdGlvbihub2RlaWQsIGRhdGEpIHtcbiAgICAgIHZhciBqLCBsZW4sIG5vZGUsIHN1YjtcbiAgICAgIGZvciAoaiA9IDAsIGxlbiA9IGRhdGEubGVuZ3RoOyBqIDwgbGVuOyBqKyspIHtcbiAgICAgICAgbm9kZSA9IGRhdGFbal07XG4gICAgICAgIGlmIChub2RlLmlkID09PSBub2RlaWQpIHtcbiAgICAgICAgICByZXR1cm4gbm9kZTtcbiAgICAgICAgfVxuICAgICAgICBpZiAobm9kZS5zdGVwX2Z1bmN0aW9uKSB7XG4gICAgICAgICAgc3ViID0gc2Vla05vZGUobm9kZWlkLCBub2RlLnN0ZXBfZnVuY3Rpb24pO1xuICAgICAgICB9XG4gICAgICAgIGlmIChzdWIpIHtcbiAgICAgICAgICByZXR1cm4gc3ViO1xuICAgICAgICB9XG4gICAgICB9XG4gICAgICByZXR1cm4gbnVsbDtcbiAgICB9O1xuICAgIGRlZmVycmVkID0gJHEuZGVmZXIoKTtcbiAgICBkZWZlcnJlZHMuam9iLnByb21pc2UudGhlbigoZnVuY3Rpb24oX3RoaXMpIHtcbiAgICAgIHJldHVybiBmdW5jdGlvbihkYXRhKSB7XG4gICAgICAgIHZhciBmb3VuZE5vZGU7XG4gICAgICAgIGZvdW5kTm9kZSA9IHNlZWtOb2RlKG5vZGVpZCwgY3VycmVudEpvYi5wbGFuLm5vZGVzKTtcbiAgICAgICAgZm91bmROb2RlLnZlcnRleCA9IF90aGlzLnNlZWtWZXJ0ZXgobm9kZWlkKTtcbiAgICAgICAgcmV0dXJuIGRlZmVycmVkLnJlc29sdmUoZm91bmROb2RlKTtcbiAgICAgIH07XG4gICAgfSkodGhpcykpO1xuICAgIHJldHVybiBkZWZlcnJlZC5wcm9taXNlO1xuICB9O1xuICB0aGlzLnNlZWtWZXJ0ZXggPSBmdW5jdGlvbihub2RlaWQpIHtcbiAgICB2YXIgaiwgbGVuLCByZWYsIHZlcnRleDtcbiAgICByZWYgPSBjdXJyZW50Sm9iLnZlcnRpY2VzO1xuICAgIGZvciAoaiA9IDAsIGxlbiA9IHJlZi5sZW5ndGg7IGogPCBsZW47IGorKykge1xuICAgICAgdmVydGV4ID0gcmVmW2pdO1xuICAgICAgaWYgKHZlcnRleC5pZCA9PT0gbm9kZWlkKSB7XG4gICAgICAgIHJldHVybiB2ZXJ0ZXg7XG4gICAgICB9XG4gICAgfVxuICAgIHJldHVybiBudWxsO1xuICB9O1xuICB0aGlzLmdldFZlcnRleCA9IGZ1bmN0aW9uKHZlcnRleGlkKSB7XG4gICAgdmFyIGRlZmVycmVkO1xuICAgIGRlZmVycmVkID0gJHEuZGVmZXIoKTtcbiAgICBkZWZlcnJlZHMuam9iLnByb21pc2UudGhlbigoZnVuY3Rpb24oX3RoaXMpIHtcbiAgICAgIHJldHVybiBmdW5jdGlvbihkYXRhKSB7XG4gICAgICAgIHZhciB2ZXJ0ZXg7XG4gICAgICAgIHZlcnRleCA9IF90aGlzLnNlZWtWZXJ0ZXgodmVydGV4aWQpO1xuICAgICAgICByZXR1cm4gJGh0dHAuZ2V0KGZsaW5rQ29uZmlnLmpvYlNlcnZlciArIFwiam9icy9cIiArIGN1cnJlbnRKb2IuamlkICsgXCIvdmVydGljZXMvXCIgKyB2ZXJ0ZXhpZCArIFwiL3N1YnRhc2t0aW1lc1wiKS5zdWNjZXNzKGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgICAgICB2ZXJ0ZXguc3VidGFza3MgPSBkYXRhLnN1YnRhc2tzO1xuICAgICAgICAgIHJldHVybiBkZWZlcnJlZC5yZXNvbHZlKHZlcnRleCk7XG4gICAgICAgIH0pO1xuICAgICAgfTtcbiAgICB9KSh0aGlzKSk7XG4gICAgcmV0dXJuIGRlZmVycmVkLnByb21pc2U7XG4gIH07XG4gIHRoaXMuZ2V0U3VidGFza3MgPSBmdW5jdGlvbih2ZXJ0ZXhpZCkge1xuICAgIHZhciBkZWZlcnJlZDtcbiAgICBkZWZlcnJlZCA9ICRxLmRlZmVyKCk7XG4gICAgZGVmZXJyZWRzLmpvYi5wcm9taXNlLnRoZW4oKGZ1bmN0aW9uKF90aGlzKSB7XG4gICAgICByZXR1cm4gZnVuY3Rpb24oZGF0YSkge1xuICAgICAgICByZXR1cm4gJGh0dHAuZ2V0KGZsaW5rQ29uZmlnLmpvYlNlcnZlciArIFwiam9icy9cIiArIGN1cnJlbnRKb2IuamlkICsgXCIvdmVydGljZXMvXCIgKyB2ZXJ0ZXhpZCkuc3VjY2VzcyhmdW5jdGlvbihkYXRhKSB7XG4gICAgICAgICAgdmFyIHN1YnRhc2tzO1xuICAgICAgICAgIHN1YnRhc2tzID0gZGF0YS5zdWJ0YXNrcztcbiAgICAgICAgICByZXR1cm4gZGVmZXJyZWQucmVzb2x2ZShzdWJ0YXNrcyk7XG4gICAgICAgIH0pO1xuICAgICAgfTtcbiAgICB9KSh0aGlzKSk7XG4gICAgcmV0dXJuIGRlZmVycmVkLnByb21pc2U7XG4gIH07XG4gIHRoaXMuZ2V0VGFza01hbmFnZXJzID0gZnVuY3Rpb24odmVydGV4aWQpIHtcbiAgICB2YXIgZGVmZXJyZWQ7XG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpO1xuICAgIGRlZmVycmVkcy5qb2IucHJvbWlzZS50aGVuKChmdW5jdGlvbihfdGhpcykge1xuICAgICAgcmV0dXJuIGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgICAgcmV0dXJuICRodHRwLmdldChmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcImpvYnMvXCIgKyBjdXJyZW50Sm9iLmppZCArIFwiL3ZlcnRpY2VzL1wiICsgdmVydGV4aWQgKyBcIi90YXNrbWFuYWdlcnNcIikuc3VjY2VzcyhmdW5jdGlvbihkYXRhKSB7XG4gICAgICAgICAgdmFyIHRhc2ttYW5hZ2VycztcbiAgICAgICAgICB0YXNrbWFuYWdlcnMgPSBkYXRhLnRhc2ttYW5hZ2VycztcbiAgICAgICAgICByZXR1cm4gZGVmZXJyZWQucmVzb2x2ZSh0YXNrbWFuYWdlcnMpO1xuICAgICAgICB9KTtcbiAgICAgIH07XG4gICAgfSkodGhpcykpO1xuICAgIHJldHVybiBkZWZlcnJlZC5wcm9taXNlO1xuICB9O1xuICB0aGlzLmdldEFjY3VtdWxhdG9ycyA9IGZ1bmN0aW9uKHZlcnRleGlkKSB7XG4gICAgdmFyIGRlZmVycmVkO1xuICAgIGRlZmVycmVkID0gJHEuZGVmZXIoKTtcbiAgICBkZWZlcnJlZHMuam9iLnByb21pc2UudGhlbigoZnVuY3Rpb24oX3RoaXMpIHtcbiAgICAgIHJldHVybiBmdW5jdGlvbihkYXRhKSB7XG4gICAgICAgIHJldHVybiAkaHR0cC5nZXQoZmxpbmtDb25maWcuam9iU2VydmVyICsgXCJqb2JzL1wiICsgY3VycmVudEpvYi5qaWQgKyBcIi92ZXJ0aWNlcy9cIiArIHZlcnRleGlkICsgXCIvYWNjdW11bGF0b3JzXCIpLnN1Y2Nlc3MoZnVuY3Rpb24oZGF0YSkge1xuICAgICAgICAgIHZhciBhY2N1bXVsYXRvcnM7XG4gICAgICAgICAgYWNjdW11bGF0b3JzID0gZGF0YVsndXNlci1hY2N1bXVsYXRvcnMnXTtcbiAgICAgICAgICByZXR1cm4gJGh0dHAuZ2V0KGZsaW5rQ29uZmlnLmpvYlNlcnZlciArIFwiam9icy9cIiArIGN1cnJlbnRKb2IuamlkICsgXCIvdmVydGljZXMvXCIgKyB2ZXJ0ZXhpZCArIFwiL3N1YnRhc2tzL2FjY3VtdWxhdG9yc1wiKS5zdWNjZXNzKGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgICAgICAgIHZhciBzdWJ0YXNrQWNjdW11bGF0b3JzO1xuICAgICAgICAgICAgc3VidGFza0FjY3VtdWxhdG9ycyA9IGRhdGEuc3VidGFza3M7XG4gICAgICAgICAgICByZXR1cm4gZGVmZXJyZWQucmVzb2x2ZSh7XG4gICAgICAgICAgICAgIG1haW46IGFjY3VtdWxhdG9ycyxcbiAgICAgICAgICAgICAgc3VidGFza3M6IHN1YnRhc2tBY2N1bXVsYXRvcnNcbiAgICAgICAgICAgIH0pO1xuICAgICAgICAgIH0pO1xuICAgICAgICB9KTtcbiAgICAgIH07XG4gICAgfSkodGhpcykpO1xuICAgIHJldHVybiBkZWZlcnJlZC5wcm9taXNlO1xuICB9O1xuICB0aGlzLmdldEpvYkNoZWNrcG9pbnRTdGF0cyA9IGZ1bmN0aW9uKGpvYmlkKSB7XG4gICAgdmFyIGRlZmVycmVkO1xuICAgIGRlZmVycmVkID0gJHEuZGVmZXIoKTtcbiAgICAkaHR0cC5nZXQoZmxpbmtDb25maWcuam9iU2VydmVyICsgXCJqb2JzL1wiICsgam9iaWQgKyBcIi9jaGVja3BvaW50c1wiKS5zdWNjZXNzKChmdW5jdGlvbihfdGhpcykge1xuICAgICAgcmV0dXJuIGZ1bmN0aW9uKGRhdGEsIHN0YXR1cywgaGVhZGVycywgY29uZmlnKSB7XG4gICAgICAgIGlmIChhbmd1bGFyLmVxdWFscyh7fSwgZGF0YSkpIHtcbiAgICAgICAgICByZXR1cm4gZGVmZXJyZWQucmVzb2x2ZShkZWZlcnJlZC5yZXNvbHZlKG51bGwpKTtcbiAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICByZXR1cm4gZGVmZXJyZWQucmVzb2x2ZShkYXRhKTtcbiAgICAgICAgfVxuICAgICAgfTtcbiAgICB9KSh0aGlzKSk7XG4gICAgcmV0dXJuIGRlZmVycmVkLnByb21pc2U7XG4gIH07XG4gIHRoaXMuZ2V0T3BlcmF0b3JDaGVja3BvaW50U3RhdHMgPSBmdW5jdGlvbih2ZXJ0ZXhpZCkge1xuICAgIHZhciBkZWZlcnJlZDtcbiAgICBkZWZlcnJlZCA9ICRxLmRlZmVyKCk7XG4gICAgZGVmZXJyZWRzLmpvYi5wcm9taXNlLnRoZW4oKGZ1bmN0aW9uKF90aGlzKSB7XG4gICAgICByZXR1cm4gZnVuY3Rpb24oZGF0YSkge1xuICAgICAgICByZXR1cm4gJGh0dHAuZ2V0KGZsaW5rQ29uZmlnLmpvYlNlcnZlciArIFwiam9icy9cIiArIGN1cnJlbnRKb2IuamlkICsgXCIvdmVydGljZXMvXCIgKyB2ZXJ0ZXhpZCArIFwiL2NoZWNrcG9pbnRzXCIpLnN1Y2Nlc3MoZnVuY3Rpb24oZGF0YSkge1xuICAgICAgICAgIHZhciBvcGVyYXRvclN0YXRzLCBzdWJ0YXNrU3RhdHM7XG4gICAgICAgICAgaWYgKGFuZ3VsYXIuZXF1YWxzKHt9LCBkYXRhKSkge1xuICAgICAgICAgICAgcmV0dXJuIGRlZmVycmVkLnJlc29sdmUoe1xuICAgICAgICAgICAgICBvcGVyYXRvclN0YXRzOiBudWxsLFxuICAgICAgICAgICAgICBzdWJ0YXNrc1N0YXRzOiBudWxsXG4gICAgICAgICAgICB9KTtcbiAgICAgICAgICB9IGVsc2Uge1xuICAgICAgICAgICAgb3BlcmF0b3JTdGF0cyA9IHtcbiAgICAgICAgICAgICAgaWQ6IGRhdGFbJ2lkJ10sXG4gICAgICAgICAgICAgIHRpbWVzdGFtcDogZGF0YVsndGltZXN0YW1wJ10sXG4gICAgICAgICAgICAgIGR1cmF0aW9uOiBkYXRhWydkdXJhdGlvbiddLFxuICAgICAgICAgICAgICBzaXplOiBkYXRhWydzaXplJ11cbiAgICAgICAgICAgIH07XG4gICAgICAgICAgICBpZiAoYW5ndWxhci5lcXVhbHMoe30sIGRhdGFbJ3N1YnRhc2tzJ10pKSB7XG4gICAgICAgICAgICAgIHJldHVybiBkZWZlcnJlZC5yZXNvbHZlKHtcbiAgICAgICAgICAgICAgICBvcGVyYXRvclN0YXRzOiBvcGVyYXRvclN0YXRzLFxuICAgICAgICAgICAgICAgIHN1YnRhc2tzU3RhdHM6IG51bGxcbiAgICAgICAgICAgICAgfSk7XG4gICAgICAgICAgICB9IGVsc2Uge1xuICAgICAgICAgICAgICBzdWJ0YXNrU3RhdHMgPSBkYXRhWydzdWJ0YXNrcyddO1xuICAgICAgICAgICAgICByZXR1cm4gZGVmZXJyZWQucmVzb2x2ZSh7XG4gICAgICAgICAgICAgICAgb3BlcmF0b3JTdGF0czogb3BlcmF0b3JTdGF0cyxcbiAgICAgICAgICAgICAgICBzdWJ0YXNrc1N0YXRzOiBzdWJ0YXNrU3RhdHNcbiAgICAgICAgICAgICAgfSk7XG4gICAgICAgICAgICB9XG4gICAgICAgICAgfVxuICAgICAgICB9KTtcbiAgICAgIH07XG4gICAgfSkodGhpcykpO1xuICAgIHJldHVybiBkZWZlcnJlZC5wcm9taXNlO1xuICB9O1xuICB0aGlzLmdldE9wZXJhdG9yQmFja1ByZXNzdXJlID0gZnVuY3Rpb24odmVydGV4aWQpIHtcbiAgICB2YXIgZGVmZXJyZWQ7XG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpO1xuICAgICRodHRwLmdldChmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcImpvYnMvXCIgKyBjdXJyZW50Sm9iLmppZCArIFwiL3ZlcnRpY2VzL1wiICsgdmVydGV4aWQgKyBcIi9iYWNrcHJlc3N1cmVcIikuc3VjY2VzcygoZnVuY3Rpb24oX3RoaXMpIHtcbiAgICAgIHJldHVybiBmdW5jdGlvbihkYXRhKSB7XG4gICAgICAgIHJldHVybiBkZWZlcnJlZC5yZXNvbHZlKGRhdGEpO1xuICAgICAgfTtcbiAgICB9KSh0aGlzKSk7XG4gICAgcmV0dXJuIGRlZmVycmVkLnByb21pc2U7XG4gIH07XG4gIHRoaXMudHJhbnNsYXRlQmFja1ByZXNzdXJlTGFiZWxTdGF0ZSA9IGZ1bmN0aW9uKHN0YXRlKSB7XG4gICAgc3dpdGNoIChzdGF0ZS50b0xvd2VyQ2FzZSgpKSB7XG4gICAgICBjYXNlICdpbi1wcm9ncmVzcyc6XG4gICAgICAgIHJldHVybiAnZGFuZ2VyJztcbiAgICAgIGNhc2UgJ29rJzpcbiAgICAgICAgcmV0dXJuICdzdWNjZXNzJztcbiAgICAgIGNhc2UgJ2xvdyc6XG4gICAgICAgIHJldHVybiAnd2FybmluZyc7XG4gICAgICBjYXNlICdoaWdoJzpcbiAgICAgICAgcmV0dXJuICdkYW5nZXInO1xuICAgICAgZGVmYXVsdDpcbiAgICAgICAgcmV0dXJuICdkZWZhdWx0JztcbiAgICB9XG4gIH07XG4gIHRoaXMubG9hZEV4Y2VwdGlvbnMgPSBmdW5jdGlvbigpIHtcbiAgICB2YXIgZGVmZXJyZWQ7XG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpO1xuICAgIGRlZmVycmVkcy5qb2IucHJvbWlzZS50aGVuKChmdW5jdGlvbihfdGhpcykge1xuICAgICAgcmV0dXJuIGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgICAgcmV0dXJuICRodHRwLmdldChmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcImpvYnMvXCIgKyBjdXJyZW50Sm9iLmppZCArIFwiL2V4Y2VwdGlvbnNcIikuc3VjY2VzcyhmdW5jdGlvbihleGNlcHRpb25zKSB7XG4gICAgICAgICAgY3VycmVudEpvYi5leGNlcHRpb25zID0gZXhjZXB0aW9ucztcbiAgICAgICAgICByZXR1cm4gZGVmZXJyZWQucmVzb2x2ZShleGNlcHRpb25zKTtcbiAgICAgICAgfSk7XG4gICAgICB9O1xuICAgIH0pKHRoaXMpKTtcbiAgICByZXR1cm4gZGVmZXJyZWQucHJvbWlzZTtcbiAgfTtcbiAgdGhpcy5jYW5jZWxKb2IgPSBmdW5jdGlvbihqb2JpZCkge1xuICAgIHJldHVybiAkaHR0cC5nZXQoZmxpbmtDb25maWcuam9iU2VydmVyICsgXCJqb2JzL1wiICsgam9iaWQgKyBcIi95YXJuLWNhbmNlbFwiKTtcbiAgfTtcbiAgdGhpcy5zdG9wSm9iID0gZnVuY3Rpb24oam9iaWQpIHtcbiAgICByZXR1cm4gJGh0dHAuZ2V0KFwiam9icy9cIiArIGpvYmlkICsgXCIveWFybi1zdG9wXCIpO1xuICB9O1xuICByZXR1cm4gdGhpcztcbn0pO1xuIiwiI1xuIyBMaWNlbnNlZCB0byB0aGUgQXBhY2hlIFNvZnR3YXJlIEZvdW5kYXRpb24gKEFTRikgdW5kZXIgb25lXG4jIG9yIG1vcmUgY29udHJpYnV0b3IgbGljZW5zZSBhZ3JlZW1lbnRzLiAgU2VlIHRoZSBOT1RJQ0UgZmlsZVxuIyBkaXN0cmlidXRlZCB3aXRoIHRoaXMgd29yayBmb3IgYWRkaXRpb25hbCBpbmZvcm1hdGlvblxuIyByZWdhcmRpbmcgY29weXJpZ2h0IG93bmVyc2hpcC4gIFRoZSBBU0YgbGljZW5zZXMgdGhpcyBmaWxlXG4jIHRvIHlvdSB1bmRlciB0aGUgQXBhY2hlIExpY2Vuc2UsIFZlcnNpb24gMi4wICh0aGVcbiMgXCJMaWNlbnNlXCIpOyB5b3UgbWF5IG5vdCB1c2UgdGhpcyBmaWxlIGV4Y2VwdCBpbiBjb21wbGlhbmNlXG4jIHdpdGggdGhlIExpY2Vuc2UuICBZb3UgbWF5IG9idGFpbiBhIGNvcHkgb2YgdGhlIExpY2Vuc2UgYXRcbiNcbiMgICAgIGh0dHA6Ly93d3cuYXBhY2hlLm9yZy9saWNlbnNlcy9MSUNFTlNFLTIuMFxuI1xuIyBVbmxlc3MgcmVxdWlyZWQgYnkgYXBwbGljYWJsZSBsYXcgb3IgYWdyZWVkIHRvIGluIHdyaXRpbmcsIHNvZnR3YXJlXG4jIGRpc3RyaWJ1dGVkIHVuZGVyIHRoZSBMaWNlbnNlIGlzIGRpc3RyaWJ1dGVkIG9uIGFuIFwiQVMgSVNcIiBCQVNJUyxcbiMgV0lUSE9VVCBXQVJSQU5USUVTIE9SIENPTkRJVElPTlMgT0YgQU5ZIEtJTkQsIGVpdGhlciBleHByZXNzIG9yIGltcGxpZWQuXG4jIFNlZSB0aGUgTGljZW5zZSBmb3IgdGhlIHNwZWNpZmljIGxhbmd1YWdlIGdvdmVybmluZyBwZXJtaXNzaW9ucyBhbmRcbiMgbGltaXRhdGlvbnMgdW5kZXIgdGhlIExpY2Vuc2UuXG4jXG5cbmFuZ3VsYXIubW9kdWxlKCdmbGlua0FwcCcpXG5cbi5jb250cm9sbGVyICdPdmVydmlld0NvbnRyb2xsZXInLCAoJHNjb3BlLCBPdmVydmlld1NlcnZpY2UsIEpvYnNTZXJ2aWNlLCAkaW50ZXJ2YWwsIGZsaW5rQ29uZmlnKSAtPlxuICAkc2NvcGUuam9iT2JzZXJ2ZXIgPSAtPlxuICAgICRzY29wZS5ydW5uaW5nSm9icyA9IEpvYnNTZXJ2aWNlLmdldEpvYnMoJ3J1bm5pbmcnKVxuICAgICRzY29wZS5maW5pc2hlZEpvYnMgPSBKb2JzU2VydmljZS5nZXRKb2JzKCdmaW5pc2hlZCcpXG5cbiAgSm9ic1NlcnZpY2UucmVnaXN0ZXJPYnNlcnZlcigkc2NvcGUuam9iT2JzZXJ2ZXIpXG4gICRzY29wZS4kb24gJyRkZXN0cm95JywgLT5cbiAgICBKb2JzU2VydmljZS51blJlZ2lzdGVyT2JzZXJ2ZXIoJHNjb3BlLmpvYk9ic2VydmVyKVxuXG4gICRzY29wZS5qb2JPYnNlcnZlcigpXG5cbiAgT3ZlcnZpZXdTZXJ2aWNlLmxvYWRPdmVydmlldygpLnRoZW4gKGRhdGEpIC0+XG4gICAgJHNjb3BlLm92ZXJ2aWV3ID0gZGF0YVxuXG4gIHJlZnJlc2ggPSAkaW50ZXJ2YWwgLT5cbiAgICBPdmVydmlld1NlcnZpY2UubG9hZE92ZXJ2aWV3KCkudGhlbiAoZGF0YSkgLT5cbiAgICAgICRzY29wZS5vdmVydmlldyA9IGRhdGFcbiAgLCBmbGlua0NvbmZpZ1tcInJlZnJlc2gtaW50ZXJ2YWxcIl1cblxuICAkc2NvcGUuJG9uICckZGVzdHJveScsIC0+XG4gICAgJGludGVydmFsLmNhbmNlbChyZWZyZXNoKVxuIiwiYW5ndWxhci5tb2R1bGUoJ2ZsaW5rQXBwJykuY29udHJvbGxlcignT3ZlcnZpZXdDb250cm9sbGVyJywgZnVuY3Rpb24oJHNjb3BlLCBPdmVydmlld1NlcnZpY2UsIEpvYnNTZXJ2aWNlLCAkaW50ZXJ2YWwsIGZsaW5rQ29uZmlnKSB7XG4gIHZhciByZWZyZXNoO1xuICAkc2NvcGUuam9iT2JzZXJ2ZXIgPSBmdW5jdGlvbigpIHtcbiAgICAkc2NvcGUucnVubmluZ0pvYnMgPSBKb2JzU2VydmljZS5nZXRKb2JzKCdydW5uaW5nJyk7XG4gICAgcmV0dXJuICRzY29wZS5maW5pc2hlZEpvYnMgPSBKb2JzU2VydmljZS5nZXRKb2JzKCdmaW5pc2hlZCcpO1xuICB9O1xuICBKb2JzU2VydmljZS5yZWdpc3Rlck9ic2VydmVyKCRzY29wZS5qb2JPYnNlcnZlcik7XG4gICRzY29wZS4kb24oJyRkZXN0cm95JywgZnVuY3Rpb24oKSB7XG4gICAgcmV0dXJuIEpvYnNTZXJ2aWNlLnVuUmVnaXN0ZXJPYnNlcnZlcigkc2NvcGUuam9iT2JzZXJ2ZXIpO1xuICB9KTtcbiAgJHNjb3BlLmpvYk9ic2VydmVyKCk7XG4gIE92ZXJ2aWV3U2VydmljZS5sb2FkT3ZlcnZpZXcoKS50aGVuKGZ1bmN0aW9uKGRhdGEpIHtcbiAgICByZXR1cm4gJHNjb3BlLm92ZXJ2aWV3ID0gZGF0YTtcbiAgfSk7XG4gIHJlZnJlc2ggPSAkaW50ZXJ2YWwoZnVuY3Rpb24oKSB7XG4gICAgcmV0dXJuIE92ZXJ2aWV3U2VydmljZS5sb2FkT3ZlcnZpZXcoKS50aGVuKGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgIHJldHVybiAkc2NvcGUub3ZlcnZpZXcgPSBkYXRhO1xuICAgIH0pO1xuICB9LCBmbGlua0NvbmZpZ1tcInJlZnJlc2gtaW50ZXJ2YWxcIl0pO1xuICByZXR1cm4gJHNjb3BlLiRvbignJGRlc3Ryb3knLCBmdW5jdGlvbigpIHtcbiAgICByZXR1cm4gJGludGVydmFsLmNhbmNlbChyZWZyZXNoKTtcbiAgfSk7XG59KTtcbiIsIiNcbiMgTGljZW5zZWQgdG8gdGhlIEFwYWNoZSBTb2Z0d2FyZSBGb3VuZGF0aW9uIChBU0YpIHVuZGVyIG9uZVxuIyBvciBtb3JlIGNvbnRyaWJ1dG9yIGxpY2Vuc2UgYWdyZWVtZW50cy4gIFNlZSB0aGUgTk9USUNFIGZpbGVcbiMgZGlzdHJpYnV0ZWQgd2l0aCB0aGlzIHdvcmsgZm9yIGFkZGl0aW9uYWwgaW5mb3JtYXRpb25cbiMgcmVnYXJkaW5nIGNvcHlyaWdodCBvd25lcnNoaXAuICBUaGUgQVNGIGxpY2Vuc2VzIHRoaXMgZmlsZVxuIyB0byB5b3UgdW5kZXIgdGhlIEFwYWNoZSBMaWNlbnNlLCBWZXJzaW9uIDIuMCAodGhlXG4jIFwiTGljZW5zZVwiKTsgeW91IG1heSBub3QgdXNlIHRoaXMgZmlsZSBleGNlcHQgaW4gY29tcGxpYW5jZVxuIyB3aXRoIHRoZSBMaWNlbnNlLiAgWW91IG1heSBvYnRhaW4gYSBjb3B5IG9mIHRoZSBMaWNlbnNlIGF0XG4jXG4jICAgICBodHRwOi8vd3d3LmFwYWNoZS5vcmcvbGljZW5zZXMvTElDRU5TRS0yLjBcbiNcbiMgVW5sZXNzIHJlcXVpcmVkIGJ5IGFwcGxpY2FibGUgbGF3IG9yIGFncmVlZCB0byBpbiB3cml0aW5nLCBzb2Z0d2FyZVxuIyBkaXN0cmlidXRlZCB1bmRlciB0aGUgTGljZW5zZSBpcyBkaXN0cmlidXRlZCBvbiBhbiBcIkFTIElTXCIgQkFTSVMsXG4jIFdJVEhPVVQgV0FSUkFOVElFUyBPUiBDT05ESVRJT05TIE9GIEFOWSBLSU5ELCBlaXRoZXIgZXhwcmVzcyBvciBpbXBsaWVkLlxuIyBTZWUgdGhlIExpY2Vuc2UgZm9yIHRoZSBzcGVjaWZpYyBsYW5ndWFnZSBnb3Zlcm5pbmcgcGVybWlzc2lvbnMgYW5kXG4jIGxpbWl0YXRpb25zIHVuZGVyIHRoZSBMaWNlbnNlLlxuI1xuXG5hbmd1bGFyLm1vZHVsZSgnZmxpbmtBcHAnKVxuXG4uc2VydmljZSAnT3ZlcnZpZXdTZXJ2aWNlJywgKCRodHRwLCBmbGlua0NvbmZpZywgJHEpIC0+XG4gIG92ZXJ2aWV3ID0ge31cblxuICBAbG9hZE92ZXJ2aWV3ID0gLT5cbiAgICBkZWZlcnJlZCA9ICRxLmRlZmVyKClcblxuICAgICRodHRwLmdldChmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcIm92ZXJ2aWV3XCIpXG4gICAgLnN1Y2Nlc3MgKGRhdGEsIHN0YXR1cywgaGVhZGVycywgY29uZmlnKSAtPlxuICAgICAgb3ZlcnZpZXcgPSBkYXRhXG4gICAgICBkZWZlcnJlZC5yZXNvbHZlKGRhdGEpXG5cbiAgICBkZWZlcnJlZC5wcm9taXNlXG5cbiAgQFxuIiwiYW5ndWxhci5tb2R1bGUoJ2ZsaW5rQXBwJykuc2VydmljZSgnT3ZlcnZpZXdTZXJ2aWNlJywgZnVuY3Rpb24oJGh0dHAsIGZsaW5rQ29uZmlnLCAkcSkge1xuICB2YXIgb3ZlcnZpZXc7XG4gIG92ZXJ2aWV3ID0ge307XG4gIHRoaXMubG9hZE92ZXJ2aWV3ID0gZnVuY3Rpb24oKSB7XG4gICAgdmFyIGRlZmVycmVkO1xuICAgIGRlZmVycmVkID0gJHEuZGVmZXIoKTtcbiAgICAkaHR0cC5nZXQoZmxpbmtDb25maWcuam9iU2VydmVyICsgXCJvdmVydmlld1wiKS5zdWNjZXNzKGZ1bmN0aW9uKGRhdGEsIHN0YXR1cywgaGVhZGVycywgY29uZmlnKSB7XG4gICAgICBvdmVydmlldyA9IGRhdGE7XG4gICAgICByZXR1cm4gZGVmZXJyZWQucmVzb2x2ZShkYXRhKTtcbiAgICB9KTtcbiAgICByZXR1cm4gZGVmZXJyZWQucHJvbWlzZTtcbiAgfTtcbiAgcmV0dXJuIHRoaXM7XG59KTtcbiIsIiNcbiMgTGljZW5zZWQgdG8gdGhlIEFwYWNoZSBTb2Z0d2FyZSBGb3VuZGF0aW9uIChBU0YpIHVuZGVyIG9uZVxuIyBvciBtb3JlIGNvbnRyaWJ1dG9yIGxpY2Vuc2UgYWdyZWVtZW50cy4gIFNlZSB0aGUgTk9USUNFIGZpbGVcbiMgZGlzdHJpYnV0ZWQgd2l0aCB0aGlzIHdvcmsgZm9yIGFkZGl0aW9uYWwgaW5mb3JtYXRpb25cbiMgcmVnYXJkaW5nIGNvcHlyaWdodCBvd25lcnNoaXAuICBUaGUgQVNGIGxpY2Vuc2VzIHRoaXMgZmlsZVxuIyB0byB5b3UgdW5kZXIgdGhlIEFwYWNoZSBMaWNlbnNlLCBWZXJzaW9uIDIuMCAodGhlXG4jIFwiTGljZW5zZVwiKTsgeW91IG1heSBub3QgdXNlIHRoaXMgZmlsZSBleGNlcHQgaW4gY29tcGxpYW5jZVxuIyB3aXRoIHRoZSBMaWNlbnNlLiAgWW91IG1heSBvYnRhaW4gYSBjb3B5IG9mIHRoZSBMaWNlbnNlIGF0XG4jXG4jICAgICBodHRwOi8vd3d3LmFwYWNoZS5vcmcvbGljZW5zZXMvTElDRU5TRS0yLjBcbiNcbiMgVW5sZXNzIHJlcXVpcmVkIGJ5IGFwcGxpY2FibGUgbGF3IG9yIGFncmVlZCB0byBpbiB3cml0aW5nLCBzb2Z0d2FyZVxuIyBkaXN0cmlidXRlZCB1bmRlciB0aGUgTGljZW5zZSBpcyBkaXN0cmlidXRlZCBvbiBhbiBcIkFTIElTXCIgQkFTSVMsXG4jIFdJVEhPVVQgV0FSUkFOVElFUyBPUiBDT05ESVRJT05TIE9GIEFOWSBLSU5ELCBlaXRoZXIgZXhwcmVzcyBvciBpbXBsaWVkLlxuIyBTZWUgdGhlIExpY2Vuc2UgZm9yIHRoZSBzcGVjaWZpYyBsYW5ndWFnZSBnb3Zlcm5pbmcgcGVybWlzc2lvbnMgYW5kXG4jIGxpbWl0YXRpb25zIHVuZGVyIHRoZSBMaWNlbnNlLlxuI1xuXG5hbmd1bGFyLm1vZHVsZSgnZmxpbmtBcHAnKVxuXG4uY29udHJvbGxlciAnSm9iU3VibWl0Q29udHJvbGxlcicsICgkc2NvcGUsIEpvYlN1Ym1pdFNlcnZpY2UsICRpbnRlcnZhbCwgZmxpbmtDb25maWcsICRzdGF0ZSwgJGxvY2F0aW9uKSAtPlxuICAkc2NvcGUueWFybiA9ICRsb2NhdGlvbi5hYnNVcmwoKS5pbmRleE9mKFwiL3Byb3h5L2FwcGxpY2F0aW9uX1wiKSAhPSAtMVxuICAkc2NvcGUubG9hZExpc3QgPSAoKSAtPlxuICAgIEpvYlN1Ym1pdFNlcnZpY2UubG9hZEphckxpc3QoKS50aGVuIChkYXRhKSAtPlxuICAgICAgJHNjb3BlLmFkZHJlc3MgPSBkYXRhLmFkZHJlc3NcbiAgICAgICRzY29wZS5ub2FjY2VzcyA9IGRhdGEuZXJyb3JcbiAgICAgICRzY29wZS5qYXJzID0gZGF0YS5maWxlc1xuXG4gICRzY29wZS5kZWZhdWx0U3RhdGUgPSAoKSAtPlxuICAgICRzY29wZS5wbGFuID0gbnVsbFxuICAgICRzY29wZS5lcnJvciA9IG51bGxcbiAgICAkc2NvcGUuc3RhdGUgPSB7XG4gICAgICBzZWxlY3RlZDogbnVsbCxcbiAgICAgIHBhcmFsbGVsaXNtOiBcIlwiLFxuICAgICAgJ2VudHJ5LWNsYXNzJzogXCJcIixcbiAgICAgICdwcm9ncmFtLWFyZ3MnOiBcIlwiLFxuICAgICAgJ3BsYW4tYnV0dG9uJzogXCJTaG93IFBsYW5cIixcbiAgICAgICdzdWJtaXQtYnV0dG9uJzogXCJTdWJtaXRcIixcbiAgICAgICdhY3Rpb24tdGltZSc6IDBcbiAgICB9XG5cbiAgJHNjb3BlLmRlZmF1bHRTdGF0ZSgpXG4gICRzY29wZS51cGxvYWRlciA9IHt9XG4gICRzY29wZS5sb2FkTGlzdCgpXG5cbiAgcmVmcmVzaCA9ICRpbnRlcnZhbCAtPlxuICAgICRzY29wZS5sb2FkTGlzdCgpXG4gICwgZmxpbmtDb25maWdbXCJyZWZyZXNoLWludGVydmFsXCJdXG5cbiAgJHNjb3BlLiRvbiAnJGRlc3Ryb3knLCAtPlxuICAgICRpbnRlcnZhbC5jYW5jZWwocmVmcmVzaClcblxuICAkc2NvcGUuc2VsZWN0SmFyID0gKGlkKSAtPlxuICAgIGlmICRzY29wZS5zdGF0ZS5zZWxlY3RlZCA9PSBpZFxuICAgICAgJHNjb3BlLmRlZmF1bHRTdGF0ZSgpXG4gICAgZWxzZVxuICAgICAgJHNjb3BlLmRlZmF1bHRTdGF0ZSgpXG4gICAgICAkc2NvcGUuc3RhdGUuc2VsZWN0ZWQgPSBpZFxuXG4gICRzY29wZS5kZWxldGVKYXIgPSAoZXZlbnQsIGlkKSAtPlxuICAgIGlmICRzY29wZS5zdGF0ZS5zZWxlY3RlZCA9PSBpZFxuICAgICAgJHNjb3BlLmRlZmF1bHRTdGF0ZSgpXG4gICAgYW5ndWxhci5lbGVtZW50KGV2ZW50LmN1cnJlbnRUYXJnZXQpLnJlbW92ZUNsYXNzKFwiZmEtcmVtb3ZlXCIpLmFkZENsYXNzKFwiZmEtc3BpbiBmYS1zcGlubmVyXCIpXG4gICAgSm9iU3VibWl0U2VydmljZS5kZWxldGVKYXIoaWQpLnRoZW4gKGRhdGEpIC0+XG4gICAgICBhbmd1bGFyLmVsZW1lbnQoZXZlbnQuY3VycmVudFRhcmdldCkucmVtb3ZlQ2xhc3MoXCJmYS1zcGluIGZhLXNwaW5uZXJcIikuYWRkQ2xhc3MoXCJmYS1yZW1vdmVcIilcbiAgICAgIGlmIGRhdGEuZXJyb3I/XG4gICAgICAgIGFsZXJ0KGRhdGEuZXJyb3IpXG5cbiAgJHNjb3BlLmxvYWRFbnRyeUNsYXNzID0gKG5hbWUpIC0+XG4gICAgJHNjb3BlLnN0YXRlWydlbnRyeS1jbGFzcyddID0gbmFtZVxuXG4gICRzY29wZS5nZXRQbGFuID0gKCkgLT5cbiAgICBpZiAkc2NvcGUuc3RhdGVbJ3BsYW4tYnV0dG9uJ10gPT0gXCJTaG93IFBsYW5cIlxuICAgICAgYWN0aW9uID0gbmV3IERhdGUoKS5nZXRUaW1lKClcbiAgICAgICRzY29wZS5zdGF0ZVsnYWN0aW9uLXRpbWUnXSA9IGFjdGlvblxuICAgICAgJHNjb3BlLnN0YXRlWydzdWJtaXQtYnV0dG9uJ10gPSBcIlN1Ym1pdFwiXG4gICAgICAkc2NvcGUuc3RhdGVbJ3BsYW4tYnV0dG9uJ10gPSBcIkdldHRpbmcgUGxhblwiXG4gICAgICAkc2NvcGUuZXJyb3IgPSBudWxsXG4gICAgICAkc2NvcGUucGxhbiA9IG51bGxcbiAgICAgIEpvYlN1Ym1pdFNlcnZpY2UuZ2V0UGxhbihcbiAgICAgICAgJHNjb3BlLnN0YXRlLnNlbGVjdGVkLCB7XG4gICAgICAgICAgJ2VudHJ5LWNsYXNzJzogJHNjb3BlLnN0YXRlWydlbnRyeS1jbGFzcyddLFxuICAgICAgICAgIHBhcmFsbGVsaXNtOiAkc2NvcGUuc3RhdGUucGFyYWxsZWxpc20sXG4gICAgICAgICAgJ3Byb2dyYW0tYXJncyc6ICRzY29wZS5zdGF0ZVsncHJvZ3JhbS1hcmdzJ11cbiAgICAgICAgfVxuICAgICAgKS50aGVuIChkYXRhKSAtPlxuICAgICAgICBpZiBhY3Rpb24gPT0gJHNjb3BlLnN0YXRlWydhY3Rpb24tdGltZSddXG4gICAgICAgICAgJHNjb3BlLnN0YXRlWydwbGFuLWJ1dHRvbiddID0gXCJTaG93IFBsYW5cIlxuICAgICAgICAgICRzY29wZS5lcnJvciA9IGRhdGEuZXJyb3JcbiAgICAgICAgICAkc2NvcGUucGxhbiA9IGRhdGEucGxhblxuXG4gICRzY29wZS5ydW5Kb2IgPSAoKSAtPlxuICAgIGlmICRzY29wZS5zdGF0ZVsnc3VibWl0LWJ1dHRvbiddID09IFwiU3VibWl0XCJcbiAgICAgIGFjdGlvbiA9IG5ldyBEYXRlKCkuZ2V0VGltZSgpXG4gICAgICAkc2NvcGUuc3RhdGVbJ2FjdGlvbi10aW1lJ10gPSBhY3Rpb25cbiAgICAgICRzY29wZS5zdGF0ZVsnc3VibWl0LWJ1dHRvbiddID0gXCJTdWJtaXR0aW5nXCJcbiAgICAgICRzY29wZS5zdGF0ZVsncGxhbi1idXR0b24nXSA9IFwiU2hvdyBQbGFuXCJcbiAgICAgICRzY29wZS5lcnJvciA9IG51bGxcbiAgICAgIEpvYlN1Ym1pdFNlcnZpY2UucnVuSm9iKFxuICAgICAgICAkc2NvcGUuc3RhdGUuc2VsZWN0ZWQsIHtcbiAgICAgICAgICAnZW50cnktY2xhc3MnOiAkc2NvcGUuc3RhdGVbJ2VudHJ5LWNsYXNzJ10sXG4gICAgICAgICAgcGFyYWxsZWxpc206ICRzY29wZS5zdGF0ZS5wYXJhbGxlbGlzbSxcbiAgICAgICAgICAncHJvZ3JhbS1hcmdzJzogJHNjb3BlLnN0YXRlWydwcm9ncmFtLWFyZ3MnXVxuICAgICAgICB9XG4gICAgICApLnRoZW4gKGRhdGEpIC0+XG4gICAgICAgIGlmIGFjdGlvbiA9PSAkc2NvcGUuc3RhdGVbJ2FjdGlvbi10aW1lJ11cbiAgICAgICAgICAkc2NvcGUuc3RhdGVbJ3N1Ym1pdC1idXR0b24nXSA9IFwiU3VibWl0XCJcbiAgICAgICAgICAkc2NvcGUuZXJyb3IgPSBkYXRhLmVycm9yXG4gICAgICAgICAgaWYgZGF0YS5qb2JpZD9cbiAgICAgICAgICAgICRzdGF0ZS5nbyhcInNpbmdsZS1qb2IucGxhbi5zdWJ0YXNrc1wiLCB7am9iaWQ6IGRhdGEuam9iaWR9KVxuXG4gICMgam9iIHBsYW4gZGlzcGxheSByZWxhdGVkIHN0dWZmXG4gICRzY29wZS5ub2RlaWQgPSBudWxsXG4gICRzY29wZS5jaGFuZ2VOb2RlID0gKG5vZGVpZCkgLT5cbiAgICBpZiBub2RlaWQgIT0gJHNjb3BlLm5vZGVpZFxuICAgICAgJHNjb3BlLm5vZGVpZCA9IG5vZGVpZFxuICAgICAgJHNjb3BlLnZlcnRleCA9IG51bGxcbiAgICAgICRzY29wZS5zdWJ0YXNrcyA9IG51bGxcbiAgICAgICRzY29wZS5hY2N1bXVsYXRvcnMgPSBudWxsXG5cbiAgICAgICRzY29wZS4kYnJvYWRjYXN0ICdyZWxvYWQnXG5cbiAgICBlbHNlXG4gICAgICAkc2NvcGUubm9kZWlkID0gbnVsbFxuICAgICAgJHNjb3BlLm5vZGVVbmZvbGRlZCA9IGZhbHNlXG4gICAgICAkc2NvcGUudmVydGV4ID0gbnVsbFxuICAgICAgJHNjb3BlLnN1YnRhc2tzID0gbnVsbFxuICAgICAgJHNjb3BlLmFjY3VtdWxhdG9ycyA9IG51bGxcblxuICAkc2NvcGUuY2xlYXJGaWxlcyA9ICgpIC0+XG4gICAgJHNjb3BlLnVwbG9hZGVyID0ge31cblxuICAkc2NvcGUudXBsb2FkRmlsZXMgPSAoZmlsZXMpIC0+XG4gICAgIyBtYWtlIHN1cmUgZXZlcnl0aGluZyBpcyBjbGVhciBhZ2Fpbi5cbiAgICAkc2NvcGUudXBsb2FkZXIgPSB7fVxuICAgIGlmIGZpbGVzLmxlbmd0aCA9PSAxXG4gICAgICAkc2NvcGUudXBsb2FkZXJbJ2ZpbGUnXSA9IGZpbGVzWzBdXG4gICAgICAkc2NvcGUudXBsb2FkZXJbJ3VwbG9hZCddID0gdHJ1ZVxuICAgIGVsc2VcbiAgICAgICRzY29wZS51cGxvYWRlclsnZXJyb3InXSA9IFwiRGlkIHlhIGZvcmdldCB0byBzZWxlY3QgYSBmaWxlP1wiXG5cbiAgJHNjb3BlLnN0YXJ0VXBsb2FkID0gKCkgLT5cbiAgICBpZiAkc2NvcGUudXBsb2FkZXJbJ2ZpbGUnXT9cbiAgICAgIGZvcm1kYXRhID0gbmV3IEZvcm1EYXRhKClcbiAgICAgIGZvcm1kYXRhLmFwcGVuZChcImphcmZpbGVcIiwgJHNjb3BlLnVwbG9hZGVyWydmaWxlJ10pXG4gICAgICAkc2NvcGUudXBsb2FkZXJbJ3VwbG9hZCddID0gZmFsc2VcbiAgICAgICRzY29wZS51cGxvYWRlclsnc3VjY2VzcyddID0gXCJJbml0aWFsaXppbmcgdXBsb2FkLi4uXCJcbiAgICAgIHhociA9IG5ldyBYTUxIdHRwUmVxdWVzdCgpXG4gICAgICB4aHIudXBsb2FkLm9ucHJvZ3Jlc3MgPSAoZXZlbnQpIC0+XG4gICAgICAgICRzY29wZS51cGxvYWRlclsnc3VjY2VzcyddID0gbnVsbFxuICAgICAgICAkc2NvcGUudXBsb2FkZXJbJ3Byb2dyZXNzJ10gPSBwYXJzZUludCgxMDAgKiBldmVudC5sb2FkZWQgLyBldmVudC50b3RhbClcbiAgICAgIHhoci51cGxvYWQub25lcnJvciA9IChldmVudCkgLT5cbiAgICAgICAgJHNjb3BlLnVwbG9hZGVyWydwcm9ncmVzcyddID0gbnVsbFxuICAgICAgICAkc2NvcGUudXBsb2FkZXJbJ2Vycm9yJ10gPSBcIkFuIGVycm9yIG9jY3VycmVkIHdoaWxlIHVwbG9hZGluZyB5b3VyIGZpbGVcIlxuICAgICAgeGhyLnVwbG9hZC5vbmxvYWQgPSAoZXZlbnQpIC0+XG4gICAgICAgICRzY29wZS51cGxvYWRlclsncHJvZ3Jlc3MnXSA9IG51bGxcbiAgICAgICAgJHNjb3BlLnVwbG9hZGVyWydzdWNjZXNzJ10gPSBcIlNhdmluZy4uLlwiXG4gICAgICB4aHIub25yZWFkeXN0YXRlY2hhbmdlID0gKCkgLT5cbiAgICAgICAgaWYgeGhyLnJlYWR5U3RhdGUgPT0gNFxuICAgICAgICAgIHJlc3BvbnNlID0gSlNPTi5wYXJzZSh4aHIucmVzcG9uc2VUZXh0KVxuICAgICAgICAgIGlmIHJlc3BvbnNlLmVycm9yP1xuICAgICAgICAgICAgJHNjb3BlLnVwbG9hZGVyWydlcnJvciddID0gcmVzcG9uc2UuZXJyb3JcbiAgICAgICAgICAgICRzY29wZS51cGxvYWRlclsnc3VjY2VzcyddID0gbnVsbFxuICAgICAgICAgIGVsc2VcbiAgICAgICAgICAgICRzY29wZS51cGxvYWRlclsnc3VjY2VzcyddID0gXCJVcGxvYWRlZCFcIlxuICAgICAgeGhyLm9wZW4oXCJQT1NUXCIsIFwiL2phcnMvdXBsb2FkXCIpXG4gICAgICB4aHIuc2VuZChmb3JtZGF0YSlcbiAgICBlbHNlXG4gICAgICBjb25zb2xlLmxvZyhcIlVuZXhwZWN0ZWQgRXJyb3IuIFRoaXMgc2hvdWxkIG5vdCBoYXBwZW5cIilcblxuLmZpbHRlciAnZ2V0SmFyU2VsZWN0Q2xhc3MnLCAtPlxuICAoc2VsZWN0ZWQsIGFjdHVhbCkgLT5cbiAgICBpZiBzZWxlY3RlZCA9PSBhY3R1YWxcbiAgICAgIFwiZmEtY2hlY2stc3F1YXJlXCJcbiAgICBlbHNlXG4gICAgICBcImZhLXNxdWFyZS1vXCJcbiIsImFuZ3VsYXIubW9kdWxlKCdmbGlua0FwcCcpLmNvbnRyb2xsZXIoJ0pvYlN1Ym1pdENvbnRyb2xsZXInLCBmdW5jdGlvbigkc2NvcGUsIEpvYlN1Ym1pdFNlcnZpY2UsICRpbnRlcnZhbCwgZmxpbmtDb25maWcsICRzdGF0ZSwgJGxvY2F0aW9uKSB7XG4gIHZhciByZWZyZXNoO1xuICAkc2NvcGUueWFybiA9ICRsb2NhdGlvbi5hYnNVcmwoKS5pbmRleE9mKFwiL3Byb3h5L2FwcGxpY2F0aW9uX1wiKSAhPT0gLTE7XG4gICRzY29wZS5sb2FkTGlzdCA9IGZ1bmN0aW9uKCkge1xuICAgIHJldHVybiBKb2JTdWJtaXRTZXJ2aWNlLmxvYWRKYXJMaXN0KCkudGhlbihmdW5jdGlvbihkYXRhKSB7XG4gICAgICAkc2NvcGUuYWRkcmVzcyA9IGRhdGEuYWRkcmVzcztcbiAgICAgICRzY29wZS5ub2FjY2VzcyA9IGRhdGEuZXJyb3I7XG4gICAgICByZXR1cm4gJHNjb3BlLmphcnMgPSBkYXRhLmZpbGVzO1xuICAgIH0pO1xuICB9O1xuICAkc2NvcGUuZGVmYXVsdFN0YXRlID0gZnVuY3Rpb24oKSB7XG4gICAgJHNjb3BlLnBsYW4gPSBudWxsO1xuICAgICRzY29wZS5lcnJvciA9IG51bGw7XG4gICAgcmV0dXJuICRzY29wZS5zdGF0ZSA9IHtcbiAgICAgIHNlbGVjdGVkOiBudWxsLFxuICAgICAgcGFyYWxsZWxpc206IFwiXCIsXG4gICAgICAnZW50cnktY2xhc3MnOiBcIlwiLFxuICAgICAgJ3Byb2dyYW0tYXJncyc6IFwiXCIsXG4gICAgICAncGxhbi1idXR0b24nOiBcIlNob3cgUGxhblwiLFxuICAgICAgJ3N1Ym1pdC1idXR0b24nOiBcIlN1Ym1pdFwiLFxuICAgICAgJ2FjdGlvbi10aW1lJzogMFxuICAgIH07XG4gIH07XG4gICRzY29wZS5kZWZhdWx0U3RhdGUoKTtcbiAgJHNjb3BlLnVwbG9hZGVyID0ge307XG4gICRzY29wZS5sb2FkTGlzdCgpO1xuICByZWZyZXNoID0gJGludGVydmFsKGZ1bmN0aW9uKCkge1xuICAgIHJldHVybiAkc2NvcGUubG9hZExpc3QoKTtcbiAgfSwgZmxpbmtDb25maWdbXCJyZWZyZXNoLWludGVydmFsXCJdKTtcbiAgJHNjb3BlLiRvbignJGRlc3Ryb3knLCBmdW5jdGlvbigpIHtcbiAgICByZXR1cm4gJGludGVydmFsLmNhbmNlbChyZWZyZXNoKTtcbiAgfSk7XG4gICRzY29wZS5zZWxlY3RKYXIgPSBmdW5jdGlvbihpZCkge1xuICAgIGlmICgkc2NvcGUuc3RhdGUuc2VsZWN0ZWQgPT09IGlkKSB7XG4gICAgICByZXR1cm4gJHNjb3BlLmRlZmF1bHRTdGF0ZSgpO1xuICAgIH0gZWxzZSB7XG4gICAgICAkc2NvcGUuZGVmYXVsdFN0YXRlKCk7XG4gICAgICByZXR1cm4gJHNjb3BlLnN0YXRlLnNlbGVjdGVkID0gaWQ7XG4gICAgfVxuICB9O1xuICAkc2NvcGUuZGVsZXRlSmFyID0gZnVuY3Rpb24oZXZlbnQsIGlkKSB7XG4gICAgaWYgKCRzY29wZS5zdGF0ZS5zZWxlY3RlZCA9PT0gaWQpIHtcbiAgICAgICRzY29wZS5kZWZhdWx0U3RhdGUoKTtcbiAgICB9XG4gICAgYW5ndWxhci5lbGVtZW50KGV2ZW50LmN1cnJlbnRUYXJnZXQpLnJlbW92ZUNsYXNzKFwiZmEtcmVtb3ZlXCIpLmFkZENsYXNzKFwiZmEtc3BpbiBmYS1zcGlubmVyXCIpO1xuICAgIHJldHVybiBKb2JTdWJtaXRTZXJ2aWNlLmRlbGV0ZUphcihpZCkudGhlbihmdW5jdGlvbihkYXRhKSB7XG4gICAgICBhbmd1bGFyLmVsZW1lbnQoZXZlbnQuY3VycmVudFRhcmdldCkucmVtb3ZlQ2xhc3MoXCJmYS1zcGluIGZhLXNwaW5uZXJcIikuYWRkQ2xhc3MoXCJmYS1yZW1vdmVcIik7XG4gICAgICBpZiAoZGF0YS5lcnJvciAhPSBudWxsKSB7XG4gICAgICAgIHJldHVybiBhbGVydChkYXRhLmVycm9yKTtcbiAgICAgIH1cbiAgICB9KTtcbiAgfTtcbiAgJHNjb3BlLmxvYWRFbnRyeUNsYXNzID0gZnVuY3Rpb24obmFtZSkge1xuICAgIHJldHVybiAkc2NvcGUuc3RhdGVbJ2VudHJ5LWNsYXNzJ10gPSBuYW1lO1xuICB9O1xuICAkc2NvcGUuZ2V0UGxhbiA9IGZ1bmN0aW9uKCkge1xuICAgIHZhciBhY3Rpb247XG4gICAgaWYgKCRzY29wZS5zdGF0ZVsncGxhbi1idXR0b24nXSA9PT0gXCJTaG93IFBsYW5cIikge1xuICAgICAgYWN0aW9uID0gbmV3IERhdGUoKS5nZXRUaW1lKCk7XG4gICAgICAkc2NvcGUuc3RhdGVbJ2FjdGlvbi10aW1lJ10gPSBhY3Rpb247XG4gICAgICAkc2NvcGUuc3RhdGVbJ3N1Ym1pdC1idXR0b24nXSA9IFwiU3VibWl0XCI7XG4gICAgICAkc2NvcGUuc3RhdGVbJ3BsYW4tYnV0dG9uJ10gPSBcIkdldHRpbmcgUGxhblwiO1xuICAgICAgJHNjb3BlLmVycm9yID0gbnVsbDtcbiAgICAgICRzY29wZS5wbGFuID0gbnVsbDtcbiAgICAgIHJldHVybiBKb2JTdWJtaXRTZXJ2aWNlLmdldFBsYW4oJHNjb3BlLnN0YXRlLnNlbGVjdGVkLCB7XG4gICAgICAgICdlbnRyeS1jbGFzcyc6ICRzY29wZS5zdGF0ZVsnZW50cnktY2xhc3MnXSxcbiAgICAgICAgcGFyYWxsZWxpc206ICRzY29wZS5zdGF0ZS5wYXJhbGxlbGlzbSxcbiAgICAgICAgJ3Byb2dyYW0tYXJncyc6ICRzY29wZS5zdGF0ZVsncHJvZ3JhbS1hcmdzJ11cbiAgICAgIH0pLnRoZW4oZnVuY3Rpb24oZGF0YSkge1xuICAgICAgICBpZiAoYWN0aW9uID09PSAkc2NvcGUuc3RhdGVbJ2FjdGlvbi10aW1lJ10pIHtcbiAgICAgICAgICAkc2NvcGUuc3RhdGVbJ3BsYW4tYnV0dG9uJ10gPSBcIlNob3cgUGxhblwiO1xuICAgICAgICAgICRzY29wZS5lcnJvciA9IGRhdGEuZXJyb3I7XG4gICAgICAgICAgcmV0dXJuICRzY29wZS5wbGFuID0gZGF0YS5wbGFuO1xuICAgICAgICB9XG4gICAgICB9KTtcbiAgICB9XG4gIH07XG4gICRzY29wZS5ydW5Kb2IgPSBmdW5jdGlvbigpIHtcbiAgICB2YXIgYWN0aW9uO1xuICAgIGlmICgkc2NvcGUuc3RhdGVbJ3N1Ym1pdC1idXR0b24nXSA9PT0gXCJTdWJtaXRcIikge1xuICAgICAgYWN0aW9uID0gbmV3IERhdGUoKS5nZXRUaW1lKCk7XG4gICAgICAkc2NvcGUuc3RhdGVbJ2FjdGlvbi10aW1lJ10gPSBhY3Rpb247XG4gICAgICAkc2NvcGUuc3RhdGVbJ3N1Ym1pdC1idXR0b24nXSA9IFwiU3VibWl0dGluZ1wiO1xuICAgICAgJHNjb3BlLnN0YXRlWydwbGFuLWJ1dHRvbiddID0gXCJTaG93IFBsYW5cIjtcbiAgICAgICRzY29wZS5lcnJvciA9IG51bGw7XG4gICAgICByZXR1cm4gSm9iU3VibWl0U2VydmljZS5ydW5Kb2IoJHNjb3BlLnN0YXRlLnNlbGVjdGVkLCB7XG4gICAgICAgICdlbnRyeS1jbGFzcyc6ICRzY29wZS5zdGF0ZVsnZW50cnktY2xhc3MnXSxcbiAgICAgICAgcGFyYWxsZWxpc206ICRzY29wZS5zdGF0ZS5wYXJhbGxlbGlzbSxcbiAgICAgICAgJ3Byb2dyYW0tYXJncyc6ICRzY29wZS5zdGF0ZVsncHJvZ3JhbS1hcmdzJ11cbiAgICAgIH0pLnRoZW4oZnVuY3Rpb24oZGF0YSkge1xuICAgICAgICBpZiAoYWN0aW9uID09PSAkc2NvcGUuc3RhdGVbJ2FjdGlvbi10aW1lJ10pIHtcbiAgICAgICAgICAkc2NvcGUuc3RhdGVbJ3N1Ym1pdC1idXR0b24nXSA9IFwiU3VibWl0XCI7XG4gICAgICAgICAgJHNjb3BlLmVycm9yID0gZGF0YS5lcnJvcjtcbiAgICAgICAgICBpZiAoZGF0YS5qb2JpZCAhPSBudWxsKSB7XG4gICAgICAgICAgICByZXR1cm4gJHN0YXRlLmdvKFwic2luZ2xlLWpvYi5wbGFuLnN1YnRhc2tzXCIsIHtcbiAgICAgICAgICAgICAgam9iaWQ6IGRhdGEuam9iaWRcbiAgICAgICAgICAgIH0pO1xuICAgICAgICAgIH1cbiAgICAgICAgfVxuICAgICAgfSk7XG4gICAgfVxuICB9O1xuICAkc2NvcGUubm9kZWlkID0gbnVsbDtcbiAgJHNjb3BlLmNoYW5nZU5vZGUgPSBmdW5jdGlvbihub2RlaWQpIHtcbiAgICBpZiAobm9kZWlkICE9PSAkc2NvcGUubm9kZWlkKSB7XG4gICAgICAkc2NvcGUubm9kZWlkID0gbm9kZWlkO1xuICAgICAgJHNjb3BlLnZlcnRleCA9IG51bGw7XG4gICAgICAkc2NvcGUuc3VidGFza3MgPSBudWxsO1xuICAgICAgJHNjb3BlLmFjY3VtdWxhdG9ycyA9IG51bGw7XG4gICAgICByZXR1cm4gJHNjb3BlLiRicm9hZGNhc3QoJ3JlbG9hZCcpO1xuICAgIH0gZWxzZSB7XG4gICAgICAkc2NvcGUubm9kZWlkID0gbnVsbDtcbiAgICAgICRzY29wZS5ub2RlVW5mb2xkZWQgPSBmYWxzZTtcbiAgICAgICRzY29wZS52ZXJ0ZXggPSBudWxsO1xuICAgICAgJHNjb3BlLnN1YnRhc2tzID0gbnVsbDtcbiAgICAgIHJldHVybiAkc2NvcGUuYWNjdW11bGF0b3JzID0gbnVsbDtcbiAgICB9XG4gIH07XG4gICRzY29wZS5jbGVhckZpbGVzID0gZnVuY3Rpb24oKSB7XG4gICAgcmV0dXJuICRzY29wZS51cGxvYWRlciA9IHt9O1xuICB9O1xuICAkc2NvcGUudXBsb2FkRmlsZXMgPSBmdW5jdGlvbihmaWxlcykge1xuICAgICRzY29wZS51cGxvYWRlciA9IHt9O1xuICAgIGlmIChmaWxlcy5sZW5ndGggPT09IDEpIHtcbiAgICAgICRzY29wZS51cGxvYWRlclsnZmlsZSddID0gZmlsZXNbMF07XG4gICAgICByZXR1cm4gJHNjb3BlLnVwbG9hZGVyWyd1cGxvYWQnXSA9IHRydWU7XG4gICAgfSBlbHNlIHtcbiAgICAgIHJldHVybiAkc2NvcGUudXBsb2FkZXJbJ2Vycm9yJ10gPSBcIkRpZCB5YSBmb3JnZXQgdG8gc2VsZWN0IGEgZmlsZT9cIjtcbiAgICB9XG4gIH07XG4gIHJldHVybiAkc2NvcGUuc3RhcnRVcGxvYWQgPSBmdW5jdGlvbigpIHtcbiAgICB2YXIgZm9ybWRhdGEsIHhocjtcbiAgICBpZiAoJHNjb3BlLnVwbG9hZGVyWydmaWxlJ10gIT0gbnVsbCkge1xuICAgICAgZm9ybWRhdGEgPSBuZXcgRm9ybURhdGEoKTtcbiAgICAgIGZvcm1kYXRhLmFwcGVuZChcImphcmZpbGVcIiwgJHNjb3BlLnVwbG9hZGVyWydmaWxlJ10pO1xuICAgICAgJHNjb3BlLnVwbG9hZGVyWyd1cGxvYWQnXSA9IGZhbHNlO1xuICAgICAgJHNjb3BlLnVwbG9hZGVyWydzdWNjZXNzJ10gPSBcIkluaXRpYWxpemluZyB1cGxvYWQuLi5cIjtcbiAgICAgIHhociA9IG5ldyBYTUxIdHRwUmVxdWVzdCgpO1xuICAgICAgeGhyLnVwbG9hZC5vbnByb2dyZXNzID0gZnVuY3Rpb24oZXZlbnQpIHtcbiAgICAgICAgJHNjb3BlLnVwbG9hZGVyWydzdWNjZXNzJ10gPSBudWxsO1xuICAgICAgICByZXR1cm4gJHNjb3BlLnVwbG9hZGVyWydwcm9ncmVzcyddID0gcGFyc2VJbnQoMTAwICogZXZlbnQubG9hZGVkIC8gZXZlbnQudG90YWwpO1xuICAgICAgfTtcbiAgICAgIHhoci51cGxvYWQub25lcnJvciA9IGZ1bmN0aW9uKGV2ZW50KSB7XG4gICAgICAgICRzY29wZS51cGxvYWRlclsncHJvZ3Jlc3MnXSA9IG51bGw7XG4gICAgICAgIHJldHVybiAkc2NvcGUudXBsb2FkZXJbJ2Vycm9yJ10gPSBcIkFuIGVycm9yIG9jY3VycmVkIHdoaWxlIHVwbG9hZGluZyB5b3VyIGZpbGVcIjtcbiAgICAgIH07XG4gICAgICB4aHIudXBsb2FkLm9ubG9hZCA9IGZ1bmN0aW9uKGV2ZW50KSB7XG4gICAgICAgICRzY29wZS51cGxvYWRlclsncHJvZ3Jlc3MnXSA9IG51bGw7XG4gICAgICAgIHJldHVybiAkc2NvcGUudXBsb2FkZXJbJ3N1Y2Nlc3MnXSA9IFwiU2F2aW5nLi4uXCI7XG4gICAgICB9O1xuICAgICAgeGhyLm9ucmVhZHlzdGF0ZWNoYW5nZSA9IGZ1bmN0aW9uKCkge1xuICAgICAgICB2YXIgcmVzcG9uc2U7XG4gICAgICAgIGlmICh4aHIucmVhZHlTdGF0ZSA9PT0gNCkge1xuICAgICAgICAgIHJlc3BvbnNlID0gSlNPTi5wYXJzZSh4aHIucmVzcG9uc2VUZXh0KTtcbiAgICAgICAgICBpZiAocmVzcG9uc2UuZXJyb3IgIT0gbnVsbCkge1xuICAgICAgICAgICAgJHNjb3BlLnVwbG9hZGVyWydlcnJvciddID0gcmVzcG9uc2UuZXJyb3I7XG4gICAgICAgICAgICByZXR1cm4gJHNjb3BlLnVwbG9hZGVyWydzdWNjZXNzJ10gPSBudWxsO1xuICAgICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgICByZXR1cm4gJHNjb3BlLnVwbG9hZGVyWydzdWNjZXNzJ10gPSBcIlVwbG9hZGVkIVwiO1xuICAgICAgICAgIH1cbiAgICAgICAgfVxuICAgICAgfTtcbiAgICAgIHhoci5vcGVuKFwiUE9TVFwiLCBcIi9qYXJzL3VwbG9hZFwiKTtcbiAgICAgIHJldHVybiB4aHIuc2VuZChmb3JtZGF0YSk7XG4gICAgfSBlbHNlIHtcbiAgICAgIHJldHVybiBjb25zb2xlLmxvZyhcIlVuZXhwZWN0ZWQgRXJyb3IuIFRoaXMgc2hvdWxkIG5vdCBoYXBwZW5cIik7XG4gICAgfVxuICB9O1xufSkuZmlsdGVyKCdnZXRKYXJTZWxlY3RDbGFzcycsIGZ1bmN0aW9uKCkge1xuICByZXR1cm4gZnVuY3Rpb24oc2VsZWN0ZWQsIGFjdHVhbCkge1xuICAgIGlmIChzZWxlY3RlZCA9PT0gYWN0dWFsKSB7XG4gICAgICByZXR1cm4gXCJmYS1jaGVjay1zcXVhcmVcIjtcbiAgICB9IGVsc2Uge1xuICAgICAgcmV0dXJuIFwiZmEtc3F1YXJlLW9cIjtcbiAgICB9XG4gIH07XG59KTtcbiIsIiNcbiMgTGljZW5zZWQgdG8gdGhlIEFwYWNoZSBTb2Z0d2FyZSBGb3VuZGF0aW9uIChBU0YpIHVuZGVyIG9uZVxuIyBvciBtb3JlIGNvbnRyaWJ1dG9yIGxpY2Vuc2UgYWdyZWVtZW50cy4gIFNlZSB0aGUgTk9USUNFIGZpbGVcbiMgZGlzdHJpYnV0ZWQgd2l0aCB0aGlzIHdvcmsgZm9yIGFkZGl0aW9uYWwgaW5mb3JtYXRpb25cbiMgcmVnYXJkaW5nIGNvcHlyaWdodCBvd25lcnNoaXAuICBUaGUgQVNGIGxpY2Vuc2VzIHRoaXMgZmlsZVxuIyB0byB5b3UgdW5kZXIgdGhlIEFwYWNoZSBMaWNlbnNlLCBWZXJzaW9uIDIuMCAodGhlXG4jIFwiTGljZW5zZVwiKTsgeW91IG1heSBub3QgdXNlIHRoaXMgZmlsZSBleGNlcHQgaW4gY29tcGxpYW5jZVxuIyB3aXRoIHRoZSBMaWNlbnNlLiAgWW91IG1heSBvYnRhaW4gYSBjb3B5IG9mIHRoZSBMaWNlbnNlIGF0XG4jXG4jICAgICBodHRwOi8vd3d3LmFwYWNoZS5vcmcvbGljZW5zZXMvTElDRU5TRS0yLjBcbiNcbiMgVW5sZXNzIHJlcXVpcmVkIGJ5IGFwcGxpY2FibGUgbGF3IG9yIGFncmVlZCB0byBpbiB3cml0aW5nLCBzb2Z0d2FyZVxuIyBkaXN0cmlidXRlZCB1bmRlciB0aGUgTGljZW5zZSBpcyBkaXN0cmlidXRlZCBvbiBhbiBcIkFTIElTXCIgQkFTSVMsXG4jIFdJVEhPVVQgV0FSUkFOVElFUyBPUiBDT05ESVRJT05TIE9GIEFOWSBLSU5ELCBlaXRoZXIgZXhwcmVzcyBvciBpbXBsaWVkLlxuIyBTZWUgdGhlIExpY2Vuc2UgZm9yIHRoZSBzcGVjaWZpYyBsYW5ndWFnZSBnb3Zlcm5pbmcgcGVybWlzc2lvbnMgYW5kXG4jIGxpbWl0YXRpb25zIHVuZGVyIHRoZSBMaWNlbnNlLlxuI1xuXG5hbmd1bGFyLm1vZHVsZSgnZmxpbmtBcHAnKVxuXG4uc2VydmljZSAnSm9iU3VibWl0U2VydmljZScsICgkaHR0cCwgZmxpbmtDb25maWcsICRxKSAtPlxuXG4gIEBsb2FkSmFyTGlzdCA9ICgpIC0+XG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpXG5cbiAgICAkaHR0cC5nZXQoXCJqYXJzL1wiKVxuICAgIC5zdWNjZXNzIChkYXRhLCBzdGF0dXMsIGhlYWRlcnMsIGNvbmZpZykgLT5cbiAgICAgIGRlZmVycmVkLnJlc29sdmUoZGF0YSlcblxuICAgIGRlZmVycmVkLnByb21pc2VcblxuICBAZGVsZXRlSmFyID0gKGlkKSAtPlxuICAgIGRlZmVycmVkID0gJHEuZGVmZXIoKVxuXG4gICAgJGh0dHAuZGVsZXRlKFwiamFycy9cIiArIGlkKVxuICAgIC5zdWNjZXNzIChkYXRhLCBzdGF0dXMsIGhlYWRlcnMsIGNvbmZpZykgLT5cbiAgICAgICBkZWZlcnJlZC5yZXNvbHZlKGRhdGEpXG5cbiAgICBkZWZlcnJlZC5wcm9taXNlXG5cbiAgQGdldFBsYW4gPSAoaWQsIGFyZ3MpIC0+XG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpXG5cbiAgICAkaHR0cC5nZXQoXCJqYXJzL1wiICsgaWQgKyBcIi9wbGFuXCIsIHtwYXJhbXM6IGFyZ3N9KVxuICAgIC5zdWNjZXNzIChkYXRhLCBzdGF0dXMsIGhlYWRlcnMsIGNvbmZpZykgLT5cbiAgICAgIGRlZmVycmVkLnJlc29sdmUoZGF0YSlcblxuICAgIGRlZmVycmVkLnByb21pc2VcblxuICBAcnVuSm9iID0gKGlkLCBhcmdzKSAtPlxuICAgIGRlZmVycmVkID0gJHEuZGVmZXIoKVxuXG4gICAgJGh0dHAucG9zdChcImphcnMvXCIgKyBpZCArIFwiL3J1blwiLCB7fSwge3BhcmFtczogYXJnc30pXG4gICAgLnN1Y2Nlc3MgKGRhdGEsIHN0YXR1cywgaGVhZGVycywgY29uZmlnKSAtPlxuICAgICAgZGVmZXJyZWQucmVzb2x2ZShkYXRhKVxuXG4gICAgZGVmZXJyZWQucHJvbWlzZVxuXG4gIEBcbiIsImFuZ3VsYXIubW9kdWxlKCdmbGlua0FwcCcpLnNlcnZpY2UoJ0pvYlN1Ym1pdFNlcnZpY2UnLCBmdW5jdGlvbigkaHR0cCwgZmxpbmtDb25maWcsICRxKSB7XG4gIHRoaXMubG9hZEphckxpc3QgPSBmdW5jdGlvbigpIHtcbiAgICB2YXIgZGVmZXJyZWQ7XG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpO1xuICAgICRodHRwLmdldChcImphcnMvXCIpLnN1Y2Nlc3MoZnVuY3Rpb24oZGF0YSwgc3RhdHVzLCBoZWFkZXJzLCBjb25maWcpIHtcbiAgICAgIHJldHVybiBkZWZlcnJlZC5yZXNvbHZlKGRhdGEpO1xuICAgIH0pO1xuICAgIHJldHVybiBkZWZlcnJlZC5wcm9taXNlO1xuICB9O1xuICB0aGlzLmRlbGV0ZUphciA9IGZ1bmN0aW9uKGlkKSB7XG4gICAgdmFyIGRlZmVycmVkO1xuICAgIGRlZmVycmVkID0gJHEuZGVmZXIoKTtcbiAgICAkaHR0cFtcImRlbGV0ZVwiXShcImphcnMvXCIgKyBpZCkuc3VjY2VzcyhmdW5jdGlvbihkYXRhLCBzdGF0dXMsIGhlYWRlcnMsIGNvbmZpZykge1xuICAgICAgcmV0dXJuIGRlZmVycmVkLnJlc29sdmUoZGF0YSk7XG4gICAgfSk7XG4gICAgcmV0dXJuIGRlZmVycmVkLnByb21pc2U7XG4gIH07XG4gIHRoaXMuZ2V0UGxhbiA9IGZ1bmN0aW9uKGlkLCBhcmdzKSB7XG4gICAgdmFyIGRlZmVycmVkO1xuICAgIGRlZmVycmVkID0gJHEuZGVmZXIoKTtcbiAgICAkaHR0cC5nZXQoXCJqYXJzL1wiICsgaWQgKyBcIi9wbGFuXCIsIHtcbiAgICAgIHBhcmFtczogYXJnc1xuICAgIH0pLnN1Y2Nlc3MoZnVuY3Rpb24oZGF0YSwgc3RhdHVzLCBoZWFkZXJzLCBjb25maWcpIHtcbiAgICAgIHJldHVybiBkZWZlcnJlZC5yZXNvbHZlKGRhdGEpO1xuICAgIH0pO1xuICAgIHJldHVybiBkZWZlcnJlZC5wcm9taXNlO1xuICB9O1xuICB0aGlzLnJ1bkpvYiA9IGZ1bmN0aW9uKGlkLCBhcmdzKSB7XG4gICAgdmFyIGRlZmVycmVkO1xuICAgIGRlZmVycmVkID0gJHEuZGVmZXIoKTtcbiAgICAkaHR0cC5wb3N0KFwiamFycy9cIiArIGlkICsgXCIvcnVuXCIsIHt9LCB7XG4gICAgICBwYXJhbXM6IGFyZ3NcbiAgICB9KS5zdWNjZXNzKGZ1bmN0aW9uKGRhdGEsIHN0YXR1cywgaGVhZGVycywgY29uZmlnKSB7XG4gICAgICByZXR1cm4gZGVmZXJyZWQucmVzb2x2ZShkYXRhKTtcbiAgICB9KTtcbiAgICByZXR1cm4gZGVmZXJyZWQucHJvbWlzZTtcbiAgfTtcbiAgcmV0dXJuIHRoaXM7XG59KTtcbiIsIiNcbiMgTGljZW5zZWQgdG8gdGhlIEFwYWNoZSBTb2Z0d2FyZSBGb3VuZGF0aW9uIChBU0YpIHVuZGVyIG9uZVxuIyBvciBtb3JlIGNvbnRyaWJ1dG9yIGxpY2Vuc2UgYWdyZWVtZW50cy4gIFNlZSB0aGUgTk9USUNFIGZpbGVcbiMgZGlzdHJpYnV0ZWQgd2l0aCB0aGlzIHdvcmsgZm9yIGFkZGl0aW9uYWwgaW5mb3JtYXRpb25cbiMgcmVnYXJkaW5nIGNvcHlyaWdodCBvd25lcnNoaXAuICBUaGUgQVNGIGxpY2Vuc2VzIHRoaXMgZmlsZVxuIyB0byB5b3UgdW5kZXIgdGhlIEFwYWNoZSBMaWNlbnNlLCBWZXJzaW9uIDIuMCAodGhlXG4jIFwiTGljZW5zZVwiKTsgeW91IG1heSBub3QgdXNlIHRoaXMgZmlsZSBleGNlcHQgaW4gY29tcGxpYW5jZVxuIyB3aXRoIHRoZSBMaWNlbnNlLiAgWW91IG1heSBvYnRhaW4gYSBjb3B5IG9mIHRoZSBMaWNlbnNlIGF0XG4jXG4jICAgICBodHRwOi8vd3d3LmFwYWNoZS5vcmcvbGljZW5zZXMvTElDRU5TRS0yLjBcbiNcbiMgVW5sZXNzIHJlcXVpcmVkIGJ5IGFwcGxpY2FibGUgbGF3IG9yIGFncmVlZCB0byBpbiB3cml0aW5nLCBzb2Z0d2FyZVxuIyBkaXN0cmlidXRlZCB1bmRlciB0aGUgTGljZW5zZSBpcyBkaXN0cmlidXRlZCBvbiBhbiBcIkFTIElTXCIgQkFTSVMsXG4jIFdJVEhPVVQgV0FSUkFOVElFUyBPUiBDT05ESVRJT05TIE9GIEFOWSBLSU5ELCBlaXRoZXIgZXhwcmVzcyBvciBpbXBsaWVkLlxuIyBTZWUgdGhlIExpY2Vuc2UgZm9yIHRoZSBzcGVjaWZpYyBsYW5ndWFnZSBnb3Zlcm5pbmcgcGVybWlzc2lvbnMgYW5kXG4jIGxpbWl0YXRpb25zIHVuZGVyIHRoZSBMaWNlbnNlLlxuI1xuXG5hbmd1bGFyLm1vZHVsZSgnZmxpbmtBcHAnKVxuXG4uY29udHJvbGxlciAnQWxsVGFza01hbmFnZXJzQ29udHJvbGxlcicsICgkc2NvcGUsIFRhc2tNYW5hZ2Vyc1NlcnZpY2UsICRpbnRlcnZhbCwgZmxpbmtDb25maWcpIC0+XG4gIFRhc2tNYW5hZ2Vyc1NlcnZpY2UubG9hZE1hbmFnZXJzKCkudGhlbiAoZGF0YSkgLT5cbiAgICAkc2NvcGUubWFuYWdlcnMgPSBkYXRhXG5cbiAgcmVmcmVzaCA9ICRpbnRlcnZhbCAtPlxuICAgIFRhc2tNYW5hZ2Vyc1NlcnZpY2UubG9hZE1hbmFnZXJzKCkudGhlbiAoZGF0YSkgLT5cbiAgICAgICRzY29wZS5tYW5hZ2VycyA9IGRhdGFcbiAgLCBmbGlua0NvbmZpZ1tcInJlZnJlc2gtaW50ZXJ2YWxcIl1cblxuICAkc2NvcGUuJG9uICckZGVzdHJveScsIC0+XG4gICAgJGludGVydmFsLmNhbmNlbChyZWZyZXNoKVxuXG4uY29udHJvbGxlciAnU2luZ2xlVGFza01hbmFnZXJDb250cm9sbGVyJywgKCRzY29wZSwgJHN0YXRlUGFyYW1zLCBTaW5nbGVUYXNrTWFuYWdlclNlcnZpY2UsICRpbnRlcnZhbCwgZmxpbmtDb25maWcpIC0+XG4gICRzY29wZS5tZXRyaWNzID0ge31cbiAgU2luZ2xlVGFza01hbmFnZXJTZXJ2aWNlLmxvYWRNZXRyaWNzKCRzdGF0ZVBhcmFtcy50YXNrbWFuYWdlcmlkKS50aGVuIChkYXRhKSAtPlxuICAgICAgJHNjb3BlLm1ldHJpY3MgPSBkYXRhWzBdXG5cbiAgICByZWZyZXNoID0gJGludGVydmFsIC0+XG4gICAgICBTaW5nbGVUYXNrTWFuYWdlclNlcnZpY2UubG9hZE1ldHJpY3MoJHN0YXRlUGFyYW1zLnRhc2ttYW5hZ2VyaWQpLnRoZW4gKGRhdGEpIC0+XG4gICAgICAgICRzY29wZS5tZXRyaWNzID0gZGF0YVswXVxuICAgICwgZmxpbmtDb25maWdbXCJyZWZyZXNoLWludGVydmFsXCJdXG5cbiAgICAkc2NvcGUuJG9uICckZGVzdHJveScsIC0+XG4gICAgICAkaW50ZXJ2YWwuY2FuY2VsKHJlZnJlc2gpXG5cbiIsImFuZ3VsYXIubW9kdWxlKCdmbGlua0FwcCcpLmNvbnRyb2xsZXIoJ0FsbFRhc2tNYW5hZ2Vyc0NvbnRyb2xsZXInLCBmdW5jdGlvbigkc2NvcGUsIFRhc2tNYW5hZ2Vyc1NlcnZpY2UsICRpbnRlcnZhbCwgZmxpbmtDb25maWcpIHtcbiAgdmFyIHJlZnJlc2g7XG4gIFRhc2tNYW5hZ2Vyc1NlcnZpY2UubG9hZE1hbmFnZXJzKCkudGhlbihmdW5jdGlvbihkYXRhKSB7XG4gICAgcmV0dXJuICRzY29wZS5tYW5hZ2VycyA9IGRhdGE7XG4gIH0pO1xuICByZWZyZXNoID0gJGludGVydmFsKGZ1bmN0aW9uKCkge1xuICAgIHJldHVybiBUYXNrTWFuYWdlcnNTZXJ2aWNlLmxvYWRNYW5hZ2VycygpLnRoZW4oZnVuY3Rpb24oZGF0YSkge1xuICAgICAgcmV0dXJuICRzY29wZS5tYW5hZ2VycyA9IGRhdGE7XG4gICAgfSk7XG4gIH0sIGZsaW5rQ29uZmlnW1wicmVmcmVzaC1pbnRlcnZhbFwiXSk7XG4gIHJldHVybiAkc2NvcGUuJG9uKCckZGVzdHJveScsIGZ1bmN0aW9uKCkge1xuICAgIHJldHVybiAkaW50ZXJ2YWwuY2FuY2VsKHJlZnJlc2gpO1xuICB9KTtcbn0pLmNvbnRyb2xsZXIoJ1NpbmdsZVRhc2tNYW5hZ2VyQ29udHJvbGxlcicsIGZ1bmN0aW9uKCRzY29wZSwgJHN0YXRlUGFyYW1zLCBTaW5nbGVUYXNrTWFuYWdlclNlcnZpY2UsICRpbnRlcnZhbCwgZmxpbmtDb25maWcpIHtcbiAgdmFyIHJlZnJlc2g7XG4gICRzY29wZS5tZXRyaWNzID0ge307XG4gIFNpbmdsZVRhc2tNYW5hZ2VyU2VydmljZS5sb2FkTWV0cmljcygkc3RhdGVQYXJhbXMudGFza21hbmFnZXJpZCkudGhlbihmdW5jdGlvbihkYXRhKSB7XG4gICAgcmV0dXJuICRzY29wZS5tZXRyaWNzID0gZGF0YVswXTtcbiAgfSk7XG4gIHJlZnJlc2ggPSAkaW50ZXJ2YWwoZnVuY3Rpb24oKSB7XG4gICAgcmV0dXJuIFNpbmdsZVRhc2tNYW5hZ2VyU2VydmljZS5sb2FkTWV0cmljcygkc3RhdGVQYXJhbXMudGFza21hbmFnZXJpZCkudGhlbihmdW5jdGlvbihkYXRhKSB7XG4gICAgICByZXR1cm4gJHNjb3BlLm1ldHJpY3MgPSBkYXRhWzBdO1xuICAgIH0pO1xuICB9LCBmbGlua0NvbmZpZ1tcInJlZnJlc2gtaW50ZXJ2YWxcIl0pO1xuICByZXR1cm4gJHNjb3BlLiRvbignJGRlc3Ryb3knLCBmdW5jdGlvbigpIHtcbiAgICByZXR1cm4gJGludGVydmFsLmNhbmNlbChyZWZyZXNoKTtcbiAgfSk7XG59KTtcbiIsIiNcbiMgTGljZW5zZWQgdG8gdGhlIEFwYWNoZSBTb2Z0d2FyZSBGb3VuZGF0aW9uIChBU0YpIHVuZGVyIG9uZVxuIyBvciBtb3JlIGNvbnRyaWJ1dG9yIGxpY2Vuc2UgYWdyZWVtZW50cy4gIFNlZSB0aGUgTk9USUNFIGZpbGVcbiMgZGlzdHJpYnV0ZWQgd2l0aCB0aGlzIHdvcmsgZm9yIGFkZGl0aW9uYWwgaW5mb3JtYXRpb25cbiMgcmVnYXJkaW5nIGNvcHlyaWdodCBvd25lcnNoaXAuICBUaGUgQVNGIGxpY2Vuc2VzIHRoaXMgZmlsZVxuIyB0byB5b3UgdW5kZXIgdGhlIEFwYWNoZSBMaWNlbnNlLCBWZXJzaW9uIDIuMCAodGhlXG4jIFwiTGljZW5zZVwiKTsgeW91IG1heSBub3QgdXNlIHRoaXMgZmlsZSBleGNlcHQgaW4gY29tcGxpYW5jZVxuIyB3aXRoIHRoZSBMaWNlbnNlLiAgWW91IG1heSBvYnRhaW4gYSBjb3B5IG9mIHRoZSBMaWNlbnNlIGF0XG4jXG4jICAgICBodHRwOi8vd3d3LmFwYWNoZS5vcmcvbGljZW5zZXMvTElDRU5TRS0yLjBcbiNcbiMgVW5sZXNzIHJlcXVpcmVkIGJ5IGFwcGxpY2FibGUgbGF3IG9yIGFncmVlZCB0byBpbiB3cml0aW5nLCBzb2Z0d2FyZVxuIyBkaXN0cmlidXRlZCB1bmRlciB0aGUgTGljZW5zZSBpcyBkaXN0cmlidXRlZCBvbiBhbiBcIkFTIElTXCIgQkFTSVMsXG4jIFdJVEhPVVQgV0FSUkFOVElFUyBPUiBDT05ESVRJT05TIE9GIEFOWSBLSU5ELCBlaXRoZXIgZXhwcmVzcyBvciBpbXBsaWVkLlxuIyBTZWUgdGhlIExpY2Vuc2UgZm9yIHRoZSBzcGVjaWZpYyBsYW5ndWFnZSBnb3Zlcm5pbmcgcGVybWlzc2lvbnMgYW5kXG4jIGxpbWl0YXRpb25zIHVuZGVyIHRoZSBMaWNlbnNlLlxuI1xuXG5hbmd1bGFyLm1vZHVsZSgnZmxpbmtBcHAnKVxuXG4uc2VydmljZSAnVGFza01hbmFnZXJzU2VydmljZScsICgkaHR0cCwgZmxpbmtDb25maWcsICRxKSAtPlxuICBAbG9hZE1hbmFnZXJzID0gKCkgLT5cbiAgICBkZWZlcnJlZCA9ICRxLmRlZmVyKClcblxuICAgICRodHRwLmdldChmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcInRhc2ttYW5hZ2Vyc1wiKVxuICAgIC5zdWNjZXNzIChkYXRhLCBzdGF0dXMsIGhlYWRlcnMsIGNvbmZpZykgLT5cbiAgICAgIGRlZmVycmVkLnJlc29sdmUoZGF0YVsndGFza21hbmFnZXJzJ10pXG5cbiAgICBkZWZlcnJlZC5wcm9taXNlXG5cbiAgQFxuXG4uc2VydmljZSAnU2luZ2xlVGFza01hbmFnZXJTZXJ2aWNlJywgKCRodHRwLCBmbGlua0NvbmZpZywgJHEpIC0+XG4gIEBsb2FkTWV0cmljcyA9ICh0YXNrbWFuYWdlcmlkKSAtPlxuICAgIGRlZmVycmVkID0gJHEuZGVmZXIoKVxuXG4gICAgJGh0dHAuZ2V0KGZsaW5rQ29uZmlnLmpvYlNlcnZlciArIFwidGFza21hbmFnZXJzL1wiICsgdGFza21hbmFnZXJpZClcbiAgICAuc3VjY2VzcyAoZGF0YSwgc3RhdHVzLCBoZWFkZXJzLCBjb25maWcpIC0+XG4gICAgICBkZWZlcnJlZC5yZXNvbHZlKGRhdGFbJ3Rhc2ttYW5hZ2VycyddKVxuXG4gICAgZGVmZXJyZWQucHJvbWlzZVxuXG4gIEBcblxuIiwiYW5ndWxhci5tb2R1bGUoJ2ZsaW5rQXBwJykuc2VydmljZSgnVGFza01hbmFnZXJzU2VydmljZScsIGZ1bmN0aW9uKCRodHRwLCBmbGlua0NvbmZpZywgJHEpIHtcbiAgdGhpcy5sb2FkTWFuYWdlcnMgPSBmdW5jdGlvbigpIHtcbiAgICB2YXIgZGVmZXJyZWQ7XG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpO1xuICAgICRodHRwLmdldChmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcInRhc2ttYW5hZ2Vyc1wiKS5zdWNjZXNzKGZ1bmN0aW9uKGRhdGEsIHN0YXR1cywgaGVhZGVycywgY29uZmlnKSB7XG4gICAgICByZXR1cm4gZGVmZXJyZWQucmVzb2x2ZShkYXRhWyd0YXNrbWFuYWdlcnMnXSk7XG4gICAgfSk7XG4gICAgcmV0dXJuIGRlZmVycmVkLnByb21pc2U7XG4gIH07XG4gIHJldHVybiB0aGlzO1xufSkuc2VydmljZSgnU2luZ2xlVGFza01hbmFnZXJTZXJ2aWNlJywgZnVuY3Rpb24oJGh0dHAsIGZsaW5rQ29uZmlnLCAkcSkge1xuICB0aGlzLmxvYWRNZXRyaWNzID0gZnVuY3Rpb24odGFza21hbmFnZXJpZCkge1xuICAgIHZhciBkZWZlcnJlZDtcbiAgICBkZWZlcnJlZCA9ICRxLmRlZmVyKCk7XG4gICAgJGh0dHAuZ2V0KGZsaW5rQ29uZmlnLmpvYlNlcnZlciArIFwidGFza21hbmFnZXJzL1wiICsgdGFza21hbmFnZXJpZCkuc3VjY2VzcyhmdW5jdGlvbihkYXRhLCBzdGF0dXMsIGhlYWRlcnMsIGNvbmZpZykge1xuICAgICAgcmV0dXJuIGRlZmVycmVkLnJlc29sdmUoZGF0YVsndGFza21hbmFnZXJzJ10pO1xuICAgIH0pO1xuICAgIHJldHVybiBkZWZlcnJlZC5wcm9taXNlO1xuICB9O1xuICByZXR1cm4gdGhpcztcbn0pO1xuIl0sInNvdXJjZVJvb3QiOiIvc291cmNlLyJ9
