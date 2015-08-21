angular.module('flinkApp', ['ui.router', 'angularMoment']).run(["$rootScope", function($rootScope) {
  $rootScope.sidebarVisible = false;
  return $rootScope.showSidebar = function() {
    $rootScope.sidebarVisible = !$rootScope.sidebarVisible;
    return $rootScope.sidebarClass = 'force-show';
  };
}]).value('flinkConfig', {
  jobServer: 'http://localhost:8081',
  "refresh-interval": 10000
}).run(["JobsService", "MainService", "flinkConfig", "$interval", function(JobsService, MainService, flinkConfig, $interval) {
  return MainService.loadConfig().then(function(config) {
    angular.extend(flinkConfig, config);
    JobsService.listJobs();
    return $interval(function() {
      return JobsService.listJobs();
    }, flinkConfig["refresh-interval"]);
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
    views: {
      details: {
        templateUrl: "partials/jobs/job.plan.html",
        controller: 'JobPlanController'
      }
    }
  }).state("single-job.plan.node", {
    url: "/vertex/{nodeid}",
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
        templateUrl: "partials/jobs/job.exceptions.html",
        controller: 'JobExceptionsController'
      }
    }
  }).state("single-job.config", {
    url: "/config",
    views: {
      details: {
        templateUrl: "partials/jobs/job.config.html"
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
}]).filter("humanizeTaskName", function() {
  return function(text) {
    if (text) {
      return text.replace("&gt;", ">").replace("<br/>", "");
    } else {
      return '';
    }
  };
});

angular.module('flinkApp').service('MainService', ["$http", "flinkConfig", "$q", function($http, flinkConfig, $q) {
  var jobObservers;
  jobObservers = [];
  this.loadConfig = function() {
    var deferred;
    deferred = $q.defer();
    $http.get(flinkConfig.jobServer + "/config").success(function(data, status, headers, config) {
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
  return JobsService.getVertex($stateParams.vertexId).then(function(data) {
    return $scope.vertex = data;
  });
}]).controller('JobExceptionsController', ["$scope", "$state", "$stateParams", "JobsService", function($scope, $state, $stateParams, JobsService) {
  return JobsService.loadExceptions().then(function(data) {
    return $scope.exceptions = data;
  });
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
        testData = [];
        angular.forEach(data.subtasks, function(subtask, i) {
          return testData.push({
            label: subtask.host + " (" + subtask.subtask + ")",
            times: [
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
              }, {
                label: "Running",
                color: "#ddd",
                borderColor: "#555",
                starting_time: subtask.timestamps["RUNNING"],
                ending_time: subtask.timestamps["FINISHED"],
                type: 'regular'
              }
            ]
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
      job: "="
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
        testData = [];
        testData.push({
          times: [
            {
              label: "Scheduled",
              color: "#cccccc",
              borderColor: "#555",
              starting_time: data.timestamps["CREATED"],
              ending_time: data.timestamps["CREATED"] + 1,
              type: 'scheduled'
            }
          ]
        });
        angular.forEach(data.vertices, function(vertex) {
          return testData.push({
            times: [
              {
                label: translateLabel(vertex.name),
                color: "#d9f1f7",
                borderColor: "#62cdea",
                starting_time: vertex['start-time'],
                ending_time: vertex['end-time'],
                link: vertex.id,
                type: 'regular'
              }
            ]
          });
        });
        chart = d3.timeline().stack().click(function(d, i, datum) {
          if (d.link) {
            return $state.go("single-job.timeline.vertex", {
              jobid: data.jid,
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
          return 'node-normal';
        }
      };
      createLabelNode = function(el, info, maxW, maxH) {
        var labelValue, stepName;
        labelValue = "<a href='#/jobs/" + jobid + "/vertex/" + el.id + "' class='node-label " + getNodeType(el, info) + "'>";
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
    $http.get(flinkConfig.jobServer + "/joboverview").success(function(data, status, headers, config) {
      angular.forEach(data, function(list, listKey) {
        switch (listKey) {
          case 'running':
            return jobs.running = list;
          case 'finished':
            return jobs.finished = list;
          case 'cancelled':
            return jobs.cancelled = list;
          case 'failed':
            return jobs.failed = list;
        }
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
    $http.get(flinkConfig.jobServer + "/jobs/" + jobid).success(function(data, status, headers, config) {
      return $http.get(flinkConfig.jobServer + "/jobs/" + jobid + "/vertices").success(function(vertices) {
        data = angular.extend(data, vertices);
        return $http.get(flinkConfig.jobServer + "/jobs/" + jobid + "/config").success(function(jobConfig) {
          data = angular.extend(data, jobConfig);
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
    $http.get(flinkConfig.jobServer + "/jobs/" + jobid + "/plan").success(function(data) {
      currentPlan = data;
      return deferreds.plan.resolve(data);
    });
    return deferreds.plan.promise;
  };
  this.getNode = function(nodeid) {
    var deferred, seekNode;
    seekNode = function(nodeid, data) {
      var i, len, node, sub;
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
        foundNode.vertex = _this.seekVertex(nodeid);
        return deferred.resolve(foundNode);
      };
    })(this));
    return deferred.promise;
  };
  this.seekVertex = function(nodeid) {
    var i, len, ref, vertex;
    ref = currentJob.vertices;
    for (i = 0, len = ref.length; i < len; i++) {
      vertex = ref[i];
      if (vertex.id === nodeid) {
        return vertex;
      }
    }
    return null;
  };
  this.getVertex = function(vertexid) {
    var deferred;
    deferred = $q.defer();
    $q.all([deferreds.job.promise]).then((function(_this) {
      return function(data) {
        var vertex;
        vertex = _this.seekVertex(vertexid);
        return $http.get(flinkConfig.jobServer + "/jobs/" + currentJob.jid + "/vertices/" + vertexid + "/subtasktimes").success(function(data) {
          vertex.subtasks = data.subtasks;
          return deferred.resolve(vertex);
        });
      };
    })(this));
    return deferred.promise;
  };
  this.loadExceptions = function() {
    var deferred;
    deferred = $q.defer();
    $q.all([deferreds.job.promise]).then((function(_this) {
      return function(data) {
        return $http.get(flinkConfig.jobServer + "/jobs/" + currentJob.jid + "/exceptions").success(function(exceptions) {
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
    $http.get(flinkConfig.jobServer + "/overview").success(function(data, status, headers, config) {
      overview = data;
      return deferred.resolve(data);
    });
    return deferred.promise;
  };
  return this;
}]);

//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJzb3VyY2VzIjpbImluZGV4LmNvZmZlZSIsImluZGV4LmpzIiwiY29tbW9uL2RpcmVjdGl2ZXMuY29mZmVlIiwiY29tbW9uL2RpcmVjdGl2ZXMuanMiLCJjb21tb24vZmlsdGVycy5jb2ZmZWUiLCJjb21tb24vZmlsdGVycy5qcyIsImNvbW1vbi9zZXJ2aWNlcy5jb2ZmZWUiLCJjb21tb24vc2VydmljZXMuanMiLCJtb2R1bGVzL2pvYnMvam9icy5jdHJsLmNvZmZlZSIsIm1vZHVsZXMvam9icy9qb2JzLmN0cmwuanMiLCJtb2R1bGVzL2pvYnMvam9icy5kaXIuY29mZmVlIiwibW9kdWxlcy9qb2JzL2pvYnMuZGlyLmpzIiwibW9kdWxlcy9qb2JzL2pvYnMuc3ZjLmNvZmZlZSIsIm1vZHVsZXMvam9icy9qb2JzLnN2Yy5qcyIsIm1vZHVsZXMvb3ZlcnZpZXcvb3ZlcnZpZXcuY3RybC5jb2ZmZWUiLCJtb2R1bGVzL292ZXJ2aWV3L292ZXJ2aWV3LmN0cmwuanMiLCJtb2R1bGVzL292ZXJ2aWV3L292ZXJ2aWV3LnN2Yy5jb2ZmZWUiLCJtb2R1bGVzL292ZXJ2aWV3L292ZXJ2aWV3LnN2Yy5qcyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiQUFrQkEsUUFBUSxPQUFPLFlBQVksQ0FBQyxhQUFhLGtCQUl4QyxtQkFBSSxTQUFDLFlBQUQ7RUFDSCxXQUFXLGlCQUFpQjtFQ3JCNUIsT0RzQkEsV0FBVyxjQUFjLFdBQUE7SUFDdkIsV0FBVyxpQkFBaUIsQ0FBQyxXQUFXO0lDckJ4QyxPRHNCQSxXQUFXLGVBQWU7O0lBSTdCLE1BQU0sZUFBZTtFQUNwQixXQUFXO0VBRVgsb0JBQW9CO0dBS3JCLCtEQUFJLFNBQUMsYUFBYSxhQUFhLGFBQWEsV0FBeEM7RUM1QkgsT0Q2QkEsWUFBWSxhQUFhLEtBQUssU0FBQyxRQUFEO0lBQzVCLFFBQVEsT0FBTyxhQUFhO0lBRTVCLFlBQVk7SUM3QlosT0QrQkEsVUFBVSxXQUFBO01DOUJSLE9EK0JBLFlBQVk7T0FDWixZQUFZOztJQUtqQixnREFBTyxT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O0VBQ2hDLElBQUE7RUFBQSxPQUFPLGNBQWMsV0FBQTtJQUNuQixPQUFPLGNBQWMsWUFBWSxRQUFRO0lDbEJ6QyxPRG1CQSxPQUFPLGVBQWUsWUFBWSxRQUFROztFQUU1QyxZQUFZLGlCQUFpQixPQUFPO0VBQ3BDLE9BQU8sSUFBSSxZQUFZLFdBQUE7SUNsQnJCLE9EbUJBLFlBQVksbUJBQW1CLE9BQU87O0VBRXhDLE9BQU87RUFFUCxnQkFBZ0IsZUFBZSxLQUFLLFNBQUMsTUFBRDtJQ25CbEMsT0RvQkEsT0FBTyxXQUFXOztFQUVwQixVQUFVLFVBQVUsV0FBQTtJQ25CbEIsT0RvQkEsZ0JBQWdCLGVBQWUsS0FBSyxTQUFDLE1BQUQ7TUNuQmxDLE9Eb0JBLE9BQU8sV0FBVzs7S0FDcEIsWUFBWTtFQ2xCZCxPRG9CQSxPQUFPLElBQUksWUFBWSxXQUFBO0lDbkJyQixPRG9CQSxVQUFVLE9BQU87OztBQ2pCckI7QUNMQSxRQUFRLE9BQU8sWUFFZCxRQUFRLGtEQUFtQixTQUFDLE9BQU8sYUFBYSxJQUFyQjtFQUMxQixJQUFBO0VBQUEsV0FBVztFQUVYLEtBQUMsZUFBZSxXQUFBO0lBQ2QsSUFBQTtJQUFBLFdBQVcsR0FBRztJQUVkLE1BQU0sSUFBSSxZQUFZLFlBQVksYUFDakMsUUFBUSxTQUFDLE1BQU0sUUFBUSxTQUFTLFFBQXhCO01BQ1AsV0FBVztNQ3BCWCxPRHFCQSxTQUFTLFFBQVE7O0lDbkJuQixPRHFCQSxTQUFTOztFQ25CWCxPRHFCQTs7QUNuQkYiLCJmaWxlIjoiaW5kZXguanMiLCJzb3VyY2VzQ29udGVudCI6WyIjXG4jIExpY2Vuc2VkIHRvIHRoZSBBcGFjaGUgU29mdHdhcmUgRm91bmRhdGlvbiAoQVNGKSB1bmRlciBvbmVcbiMgb3IgbW9yZSBjb250cmlidXRvciBsaWNlbnNlIGFncmVlbWVudHMuICBTZWUgdGhlIE5PVElDRSBmaWxlXG4jIGRpc3RyaWJ1dGVkIHdpdGggdGhpcyB3b3JrIGZvciBhZGRpdGlvbmFsIGluZm9ybWF0aW9uXG4jIHJlZ2FyZGluZyBjb3B5cmlnaHQgb3duZXJzaGlwLiAgVGhlIEFTRiBsaWNlbnNlcyB0aGlzIGZpbGVcbiMgdG8geW91IHVuZGVyIHRoZSBBcGFjaGUgTGljZW5zZSwgVmVyc2lvbiAyLjAgKHRoZVxuIyBcIkxpY2Vuc2VcIik7IHlvdSBtYXkgbm90IHVzZSB0aGlzIGZpbGUgZXhjZXB0IGluIGNvbXBsaWFuY2VcbiMgd2l0aCB0aGUgTGljZW5zZS4gIFlvdSBtYXkgb2J0YWluIGEgY29weSBvZiB0aGUgTGljZW5zZSBhdFxuI1xuIyAgICAgaHR0cDovL3d3dy5hcGFjaGUub3JnL2xpY2Vuc2VzL0xJQ0VOU0UtMi4wXG4jXG4jIFVubGVzcyByZXF1aXJlZCBieSBhcHBsaWNhYmxlIGxhdyBvciBhZ3JlZWQgdG8gaW4gd3JpdGluZywgc29mdHdhcmVcbiMgZGlzdHJpYnV0ZWQgdW5kZXIgdGhlIExpY2Vuc2UgaXMgZGlzdHJpYnV0ZWQgb24gYW4gXCJBUyBJU1wiIEJBU0lTLFxuIyBXSVRIT1VUIFdBUlJBTlRJRVMgT1IgQ09ORElUSU9OUyBPRiBBTlkgS0lORCwgZWl0aGVyIGV4cHJlc3Mgb3IgaW1wbGllZC5cbiMgU2VlIHRoZSBMaWNlbnNlIGZvciB0aGUgc3BlY2lmaWMgbGFuZ3VhZ2UgZ292ZXJuaW5nIHBlcm1pc3Npb25zIGFuZFxuIyBsaW1pdGF0aW9ucyB1bmRlciB0aGUgTGljZW5zZS5cbiNcblxuYW5ndWxhci5tb2R1bGUoJ2ZsaW5rQXBwJywgWyd1aS5yb3V0ZXInLCAnYW5ndWxhck1vbWVudCddKVxuXG4jIC0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tXG5cbi5ydW4gKCRyb290U2NvcGUpIC0+XG4gICRyb290U2NvcGUuc2lkZWJhclZpc2libGUgPSBmYWxzZVxuICAkcm9vdFNjb3BlLnNob3dTaWRlYmFyID0gLT5cbiAgICAkcm9vdFNjb3BlLnNpZGViYXJWaXNpYmxlID0gISRyb290U2NvcGUuc2lkZWJhclZpc2libGVcbiAgICAkcm9vdFNjb3BlLnNpZGViYXJDbGFzcyA9ICdmb3JjZS1zaG93J1xuXG4jIC0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tXG5cbi52YWx1ZSAnZmxpbmtDb25maWcnLCB7XG4gIGpvYlNlcnZlcjogJ2h0dHA6Ly9sb2NhbGhvc3Q6ODA4MSdcbiMgIGpvYlNlcnZlcjogJ2h0dHA6Ly9sb2NhbGhvc3Q6MzAwMC9uZXctc2VydmVyJ1xuICBcInJlZnJlc2gtaW50ZXJ2YWxcIjogMTAwMDBcbn1cblxuIyAtLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLVxuXG4ucnVuIChKb2JzU2VydmljZSwgTWFpblNlcnZpY2UsIGZsaW5rQ29uZmlnLCAkaW50ZXJ2YWwpIC0+XG4gIE1haW5TZXJ2aWNlLmxvYWRDb25maWcoKS50aGVuIChjb25maWcpIC0+XG4gICAgYW5ndWxhci5leHRlbmQgZmxpbmtDb25maWcsIGNvbmZpZ1xuXG4gICAgSm9ic1NlcnZpY2UubGlzdEpvYnMoKVxuXG4gICAgJGludGVydmFsIC0+XG4gICAgICBKb2JzU2VydmljZS5saXN0Sm9icygpXG4gICAgLCBmbGlua0NvbmZpZ1tcInJlZnJlc2gtaW50ZXJ2YWxcIl1cblxuXG4jIC0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tXG5cbi5jb25maWcgKCRzdGF0ZVByb3ZpZGVyLCAkdXJsUm91dGVyUHJvdmlkZXIpIC0+XG4gICRzdGF0ZVByb3ZpZGVyLnN0YXRlIFwib3ZlcnZpZXdcIixcbiAgICB1cmw6IFwiL292ZXJ2aWV3XCJcbiAgICB2aWV3czpcbiAgICAgIG1haW46XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL292ZXJ2aWV3Lmh0bWxcIlxuICAgICAgICBjb250cm9sbGVyOiAnT3ZlcnZpZXdDb250cm9sbGVyJ1xuXG4gIC5zdGF0ZSBcInJ1bm5pbmctam9ic1wiLFxuICAgIHVybDogXCIvcnVubmluZy1qb2JzXCJcbiAgICB2aWV3czpcbiAgICAgIG1haW46XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYnMvcnVubmluZy1qb2JzLmh0bWxcIlxuICAgICAgICBjb250cm9sbGVyOiAnUnVubmluZ0pvYnNDb250cm9sbGVyJ1xuICBcbiAgLnN0YXRlIFwiY29tcGxldGVkLWpvYnNcIixcbiAgICB1cmw6IFwiL2NvbXBsZXRlZC1qb2JzXCJcbiAgICB2aWV3czpcbiAgICAgIG1haW46XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYnMvY29tcGxldGVkLWpvYnMuaHRtbFwiXG4gICAgICAgIGNvbnRyb2xsZXI6ICdDb21wbGV0ZWRKb2JzQ29udHJvbGxlcidcblxuICAuc3RhdGUgXCJzaW5nbGUtam9iXCIsXG4gICAgdXJsOiBcIi9qb2JzL3tqb2JpZH1cIlxuICAgIGFic3RyYWN0OiB0cnVlXG4gICAgdmlld3M6XG4gICAgICBtYWluOlxuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JzL2pvYi5odG1sXCJcbiAgICAgICAgY29udHJvbGxlcjogJ1NpbmdsZUpvYkNvbnRyb2xsZXInXG5cbiAgLnN0YXRlIFwic2luZ2xlLWpvYi5wbGFuXCIsXG4gICAgdXJsOiBcIlwiXG4gICAgdmlld3M6XG4gICAgICBkZXRhaWxzOlxuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JzL2pvYi5wbGFuLmh0bWxcIlxuICAgICAgICBjb250cm9sbGVyOiAnSm9iUGxhbkNvbnRyb2xsZXInXG5cbiAgLnN0YXRlIFwic2luZ2xlLWpvYi5wbGFuLm5vZGVcIixcbiAgICB1cmw6IFwiL3ZlcnRleC97bm9kZWlkfVwiXG4gICAgdmlld3M6XG4gICAgICBub2RlOlxuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JzL2pvYi5wbGFuLm5vZGUuaHRtbFwiXG4gICAgICAgIGNvbnRyb2xsZXI6ICdKb2JQbGFuTm9kZUNvbnRyb2xsZXInXG5cbiAgLnN0YXRlIFwic2luZ2xlLWpvYi50aW1lbGluZVwiLFxuICAgIHVybDogXCIvdGltZWxpbmVcIlxuICAgIHZpZXdzOlxuICAgICAgZGV0YWlsczpcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvam9icy9qb2IudGltZWxpbmUuaHRtbFwiXG5cbiAgLnN0YXRlIFwic2luZ2xlLWpvYi50aW1lbGluZS52ZXJ0ZXhcIixcbiAgICB1cmw6IFwiL3t2ZXJ0ZXhJZH1cIlxuICAgIHZpZXdzOlxuICAgICAgdmVydGV4OlxuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JzL2pvYi50aW1lbGluZS52ZXJ0ZXguaHRtbFwiXG4gICAgICAgIGNvbnRyb2xsZXI6ICdKb2JUaW1lbGluZVZlcnRleENvbnRyb2xsZXInXG5cbiAgLnN0YXRlIFwic2luZ2xlLWpvYi5zdGF0aXN0aWNzXCIsXG4gICAgdXJsOiBcIi9zdGF0aXN0aWNzXCJcbiAgICB2aWV3czpcbiAgICAgIGRldGFpbHM6XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYnMvam9iLnN0YXRpc3RpY3MuaHRtbFwiXG5cbiAgLnN0YXRlIFwic2luZ2xlLWpvYi5leGNlcHRpb25zXCIsXG4gICAgdXJsOiBcIi9leGNlcHRpb25zXCJcbiAgICB2aWV3czpcbiAgICAgIGRldGFpbHM6XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYnMvam9iLmV4Y2VwdGlvbnMuaHRtbFwiXG4gICAgICAgIGNvbnRyb2xsZXI6ICdKb2JFeGNlcHRpb25zQ29udHJvbGxlcidcblxuICAuc3RhdGUgXCJzaW5nbGUtam9iLmNvbmZpZ1wiLFxuICAgIHVybDogXCIvY29uZmlnXCJcbiAgICB2aWV3czpcbiAgICAgIGRldGFpbHM6XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYnMvam9iLmNvbmZpZy5odG1sXCJcblxuICAkdXJsUm91dGVyUHJvdmlkZXIub3RoZXJ3aXNlIFwiL292ZXJ2aWV3XCJcbiIsImFuZ3VsYXIubW9kdWxlKCdmbGlua0FwcCcsIFsndWkucm91dGVyJywgJ2FuZ3VsYXJNb21lbnQnXSkucnVuKGZ1bmN0aW9uKCRyb290U2NvcGUpIHtcbiAgJHJvb3RTY29wZS5zaWRlYmFyVmlzaWJsZSA9IGZhbHNlO1xuICByZXR1cm4gJHJvb3RTY29wZS5zaG93U2lkZWJhciA9IGZ1bmN0aW9uKCkge1xuICAgICRyb290U2NvcGUuc2lkZWJhclZpc2libGUgPSAhJHJvb3RTY29wZS5zaWRlYmFyVmlzaWJsZTtcbiAgICByZXR1cm4gJHJvb3RTY29wZS5zaWRlYmFyQ2xhc3MgPSAnZm9yY2Utc2hvdyc7XG4gIH07XG59KS52YWx1ZSgnZmxpbmtDb25maWcnLCB7XG4gIGpvYlNlcnZlcjogJ2h0dHA6Ly9sb2NhbGhvc3Q6ODA4MScsXG4gIFwicmVmcmVzaC1pbnRlcnZhbFwiOiAxMDAwMFxufSkucnVuKGZ1bmN0aW9uKEpvYnNTZXJ2aWNlLCBNYWluU2VydmljZSwgZmxpbmtDb25maWcsICRpbnRlcnZhbCkge1xuICByZXR1cm4gTWFpblNlcnZpY2UubG9hZENvbmZpZygpLnRoZW4oZnVuY3Rpb24oY29uZmlnKSB7XG4gICAgYW5ndWxhci5leHRlbmQoZmxpbmtDb25maWcsIGNvbmZpZyk7XG4gICAgSm9ic1NlcnZpY2UubGlzdEpvYnMoKTtcbiAgICByZXR1cm4gJGludGVydmFsKGZ1bmN0aW9uKCkge1xuICAgICAgcmV0dXJuIEpvYnNTZXJ2aWNlLmxpc3RKb2JzKCk7XG4gICAgfSwgZmxpbmtDb25maWdbXCJyZWZyZXNoLWludGVydmFsXCJdKTtcbiAgfSk7XG59KS5jb25maWcoZnVuY3Rpb24oJHN0YXRlUHJvdmlkZXIsICR1cmxSb3V0ZXJQcm92aWRlcikge1xuICAkc3RhdGVQcm92aWRlci5zdGF0ZShcIm92ZXJ2aWV3XCIsIHtcbiAgICB1cmw6IFwiL292ZXJ2aWV3XCIsXG4gICAgdmlld3M6IHtcbiAgICAgIG1haW46IHtcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvb3ZlcnZpZXcuaHRtbFwiLFxuICAgICAgICBjb250cm9sbGVyOiAnT3ZlcnZpZXdDb250cm9sbGVyJ1xuICAgICAgfVxuICAgIH1cbiAgfSkuc3RhdGUoXCJydW5uaW5nLWpvYnNcIiwge1xuICAgIHVybDogXCIvcnVubmluZy1qb2JzXCIsXG4gICAgdmlld3M6IHtcbiAgICAgIG1haW46IHtcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvam9icy9ydW5uaW5nLWpvYnMuaHRtbFwiLFxuICAgICAgICBjb250cm9sbGVyOiAnUnVubmluZ0pvYnNDb250cm9sbGVyJ1xuICAgICAgfVxuICAgIH1cbiAgfSkuc3RhdGUoXCJjb21wbGV0ZWQtam9ic1wiLCB7XG4gICAgdXJsOiBcIi9jb21wbGV0ZWQtam9ic1wiLFxuICAgIHZpZXdzOiB7XG4gICAgICBtYWluOiB7XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYnMvY29tcGxldGVkLWpvYnMuaHRtbFwiLFxuICAgICAgICBjb250cm9sbGVyOiAnQ29tcGxldGVkSm9ic0NvbnRyb2xsZXInXG4gICAgICB9XG4gICAgfVxuICB9KS5zdGF0ZShcInNpbmdsZS1qb2JcIiwge1xuICAgIHVybDogXCIvam9icy97am9iaWR9XCIsXG4gICAgYWJzdHJhY3Q6IHRydWUsXG4gICAgdmlld3M6IHtcbiAgICAgIG1haW46IHtcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvam9icy9qb2IuaHRtbFwiLFxuICAgICAgICBjb250cm9sbGVyOiAnU2luZ2xlSm9iQ29udHJvbGxlcidcbiAgICAgIH1cbiAgICB9XG4gIH0pLnN0YXRlKFwic2luZ2xlLWpvYi5wbGFuXCIsIHtcbiAgICB1cmw6IFwiXCIsXG4gICAgdmlld3M6IHtcbiAgICAgIGRldGFpbHM6IHtcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvam9icy9qb2IucGxhbi5odG1sXCIsXG4gICAgICAgIGNvbnRyb2xsZXI6ICdKb2JQbGFuQ29udHJvbGxlcidcbiAgICAgIH1cbiAgICB9XG4gIH0pLnN0YXRlKFwic2luZ2xlLWpvYi5wbGFuLm5vZGVcIiwge1xuICAgIHVybDogXCIvdmVydGV4L3tub2RlaWR9XCIsXG4gICAgdmlld3M6IHtcbiAgICAgIG5vZGU6IHtcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvam9icy9qb2IucGxhbi5ub2RlLmh0bWxcIixcbiAgICAgICAgY29udHJvbGxlcjogJ0pvYlBsYW5Ob2RlQ29udHJvbGxlcidcbiAgICAgIH1cbiAgICB9XG4gIH0pLnN0YXRlKFwic2luZ2xlLWpvYi50aW1lbGluZVwiLCB7XG4gICAgdXJsOiBcIi90aW1lbGluZVwiLFxuICAgIHZpZXdzOiB7XG4gICAgICBkZXRhaWxzOiB7XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYnMvam9iLnRpbWVsaW5lLmh0bWxcIlxuICAgICAgfVxuICAgIH1cbiAgfSkuc3RhdGUoXCJzaW5nbGUtam9iLnRpbWVsaW5lLnZlcnRleFwiLCB7XG4gICAgdXJsOiBcIi97dmVydGV4SWR9XCIsXG4gICAgdmlld3M6IHtcbiAgICAgIHZlcnRleDoge1xuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JzL2pvYi50aW1lbGluZS52ZXJ0ZXguaHRtbFwiLFxuICAgICAgICBjb250cm9sbGVyOiAnSm9iVGltZWxpbmVWZXJ0ZXhDb250cm9sbGVyJ1xuICAgICAgfVxuICAgIH1cbiAgfSkuc3RhdGUoXCJzaW5nbGUtam9iLnN0YXRpc3RpY3NcIiwge1xuICAgIHVybDogXCIvc3RhdGlzdGljc1wiLFxuICAgIHZpZXdzOiB7XG4gICAgICBkZXRhaWxzOiB7XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYnMvam9iLnN0YXRpc3RpY3MuaHRtbFwiXG4gICAgICB9XG4gICAgfVxuICB9KS5zdGF0ZShcInNpbmdsZS1qb2IuZXhjZXB0aW9uc1wiLCB7XG4gICAgdXJsOiBcIi9leGNlcHRpb25zXCIsXG4gICAgdmlld3M6IHtcbiAgICAgIGRldGFpbHM6IHtcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvam9icy9qb2IuZXhjZXB0aW9ucy5odG1sXCIsXG4gICAgICAgIGNvbnRyb2xsZXI6ICdKb2JFeGNlcHRpb25zQ29udHJvbGxlcidcbiAgICAgIH1cbiAgICB9XG4gIH0pLnN0YXRlKFwic2luZ2xlLWpvYi5jb25maWdcIiwge1xuICAgIHVybDogXCIvY29uZmlnXCIsXG4gICAgdmlld3M6IHtcbiAgICAgIGRldGFpbHM6IHtcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvam9icy9qb2IuY29uZmlnLmh0bWxcIlxuICAgICAgfVxuICAgIH1cbiAgfSk7XG4gIHJldHVybiAkdXJsUm91dGVyUHJvdmlkZXIub3RoZXJ3aXNlKFwiL292ZXJ2aWV3XCIpO1xufSk7XG4iLCIjXG4jIExpY2Vuc2VkIHRvIHRoZSBBcGFjaGUgU29mdHdhcmUgRm91bmRhdGlvbiAoQVNGKSB1bmRlciBvbmVcbiMgb3IgbW9yZSBjb250cmlidXRvciBsaWNlbnNlIGFncmVlbWVudHMuICBTZWUgdGhlIE5PVElDRSBmaWxlXG4jIGRpc3RyaWJ1dGVkIHdpdGggdGhpcyB3b3JrIGZvciBhZGRpdGlvbmFsIGluZm9ybWF0aW9uXG4jIHJlZ2FyZGluZyBjb3B5cmlnaHQgb3duZXJzaGlwLiAgVGhlIEFTRiBsaWNlbnNlcyB0aGlzIGZpbGVcbiMgdG8geW91IHVuZGVyIHRoZSBBcGFjaGUgTGljZW5zZSwgVmVyc2lvbiAyLjAgKHRoZVxuIyBcIkxpY2Vuc2VcIik7IHlvdSBtYXkgbm90IHVzZSB0aGlzIGZpbGUgZXhjZXB0IGluIGNvbXBsaWFuY2VcbiMgd2l0aCB0aGUgTGljZW5zZS4gIFlvdSBtYXkgb2J0YWluIGEgY29weSBvZiB0aGUgTGljZW5zZSBhdFxuI1xuIyAgICAgaHR0cDovL3d3dy5hcGFjaGUub3JnL2xpY2Vuc2VzL0xJQ0VOU0UtMi4wXG4jXG4jIFVubGVzcyByZXF1aXJlZCBieSBhcHBsaWNhYmxlIGxhdyBvciBhZ3JlZWQgdG8gaW4gd3JpdGluZywgc29mdHdhcmVcbiMgZGlzdHJpYnV0ZWQgdW5kZXIgdGhlIExpY2Vuc2UgaXMgZGlzdHJpYnV0ZWQgb24gYW4gXCJBUyBJU1wiIEJBU0lTLFxuIyBXSVRIT1VUIFdBUlJBTlRJRVMgT1IgQ09ORElUSU9OUyBPRiBBTlkgS0lORCwgZWl0aGVyIGV4cHJlc3Mgb3IgaW1wbGllZC5cbiMgU2VlIHRoZSBMaWNlbnNlIGZvciB0aGUgc3BlY2lmaWMgbGFuZ3VhZ2UgZ292ZXJuaW5nIHBlcm1pc3Npb25zIGFuZFxuIyBsaW1pdGF0aW9ucyB1bmRlciB0aGUgTGljZW5zZS5cbiNcblxuYW5ndWxhci5tb2R1bGUoJ2ZsaW5rQXBwJylcblxuIyAtLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tXG5cbi5kaXJlY3RpdmUgJ2JzTGFiZWwnLCAoSm9ic1NlcnZpY2UpIC0+XG4gIHRyYW5zY2x1ZGU6IHRydWVcbiAgcmVwbGFjZTogdHJ1ZVxuICBzY29wZTogXG4gICAgZ2V0TGFiZWxDbGFzczogXCImXCJcbiAgICBzdGF0dXM6IFwiQFwiXG5cbiAgdGVtcGxhdGU6IFwiPHNwYW4gdGl0bGU9J3t7c3RhdHVzfX0nIG5nLWNsYXNzPSdnZXRMYWJlbENsYXNzKCknPjxuZy10cmFuc2NsdWRlPjwvbmctdHJhbnNjbHVkZT48L3NwYW4+XCJcbiAgXG4gIGxpbms6IChzY29wZSwgZWxlbWVudCwgYXR0cnMpIC0+XG4gICAgc2NvcGUuZ2V0TGFiZWxDbGFzcyA9IC0+XG4gICAgICAnbGFiZWwgbGFiZWwtJyArIEpvYnNTZXJ2aWNlLnRyYW5zbGF0ZUxhYmVsU3RhdGUoYXR0cnMuc3RhdHVzKVxuXG4jIC0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS1cblxuLmRpcmVjdGl2ZSAnaW5kaWNhdG9yUHJpbWFyeScsIChKb2JzU2VydmljZSkgLT5cbiAgcmVwbGFjZTogdHJ1ZVxuICBzY29wZTogXG4gICAgZ2V0TGFiZWxDbGFzczogXCImXCJcbiAgICBzdGF0dXM6ICdAJ1xuXG4gIHRlbXBsYXRlOiBcIjxpIHRpdGxlPSd7e3N0YXR1c319JyBuZy1jbGFzcz0nZ2V0TGFiZWxDbGFzcygpJyAvPlwiXG4gIFxuICBsaW5rOiAoc2NvcGUsIGVsZW1lbnQsIGF0dHJzKSAtPlxuICAgIHNjb3BlLmdldExhYmVsQ2xhc3MgPSAtPlxuICAgICAgJ2ZhIGZhLWNpcmNsZSBpbmRpY2F0b3IgaW5kaWNhdG9yLScgKyBKb2JzU2VydmljZS50cmFuc2xhdGVMYWJlbFN0YXRlKGF0dHJzLnN0YXR1cylcblxuIyAtLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tXG5cbi5kaXJlY3RpdmUgJ3RhYmxlUHJvcGVydHknLCAtPlxuICByZXBsYWNlOiB0cnVlXG4gIHNjb3BlOlxuICAgIHZhbHVlOiAnPSdcblxuICB0ZW1wbGF0ZTogXCI8dGQgdGl0bGU9XFxcInt7dmFsdWUgfHwgJ05vbmUnfX1cXFwiPnt7dmFsdWUgfHwgJ05vbmUnfX08L3RkPlwiXG4iLCJhbmd1bGFyLm1vZHVsZSgnZmxpbmtBcHAnKS5kaXJlY3RpdmUoJ2JzTGFiZWwnLCBmdW5jdGlvbihKb2JzU2VydmljZSkge1xuICByZXR1cm4ge1xuICAgIHRyYW5zY2x1ZGU6IHRydWUsXG4gICAgcmVwbGFjZTogdHJ1ZSxcbiAgICBzY29wZToge1xuICAgICAgZ2V0TGFiZWxDbGFzczogXCImXCIsXG4gICAgICBzdGF0dXM6IFwiQFwiXG4gICAgfSxcbiAgICB0ZW1wbGF0ZTogXCI8c3BhbiB0aXRsZT0ne3tzdGF0dXN9fScgbmctY2xhc3M9J2dldExhYmVsQ2xhc3MoKSc+PG5nLXRyYW5zY2x1ZGU+PC9uZy10cmFuc2NsdWRlPjwvc3Bhbj5cIixcbiAgICBsaW5rOiBmdW5jdGlvbihzY29wZSwgZWxlbWVudCwgYXR0cnMpIHtcbiAgICAgIHJldHVybiBzY29wZS5nZXRMYWJlbENsYXNzID0gZnVuY3Rpb24oKSB7XG4gICAgICAgIHJldHVybiAnbGFiZWwgbGFiZWwtJyArIEpvYnNTZXJ2aWNlLnRyYW5zbGF0ZUxhYmVsU3RhdGUoYXR0cnMuc3RhdHVzKTtcbiAgICAgIH07XG4gICAgfVxuICB9O1xufSkuZGlyZWN0aXZlKCdpbmRpY2F0b3JQcmltYXJ5JywgZnVuY3Rpb24oSm9ic1NlcnZpY2UpIHtcbiAgcmV0dXJuIHtcbiAgICByZXBsYWNlOiB0cnVlLFxuICAgIHNjb3BlOiB7XG4gICAgICBnZXRMYWJlbENsYXNzOiBcIiZcIixcbiAgICAgIHN0YXR1czogJ0AnXG4gICAgfSxcbiAgICB0ZW1wbGF0ZTogXCI8aSB0aXRsZT0ne3tzdGF0dXN9fScgbmctY2xhc3M9J2dldExhYmVsQ2xhc3MoKScgLz5cIixcbiAgICBsaW5rOiBmdW5jdGlvbihzY29wZSwgZWxlbWVudCwgYXR0cnMpIHtcbiAgICAgIHJldHVybiBzY29wZS5nZXRMYWJlbENsYXNzID0gZnVuY3Rpb24oKSB7XG4gICAgICAgIHJldHVybiAnZmEgZmEtY2lyY2xlIGluZGljYXRvciBpbmRpY2F0b3ItJyArIEpvYnNTZXJ2aWNlLnRyYW5zbGF0ZUxhYmVsU3RhdGUoYXR0cnMuc3RhdHVzKTtcbiAgICAgIH07XG4gICAgfVxuICB9O1xufSkuZGlyZWN0aXZlKCd0YWJsZVByb3BlcnR5JywgZnVuY3Rpb24oKSB7XG4gIHJldHVybiB7XG4gICAgcmVwbGFjZTogdHJ1ZSxcbiAgICBzY29wZToge1xuICAgICAgdmFsdWU6ICc9J1xuICAgIH0sXG4gICAgdGVtcGxhdGU6IFwiPHRkIHRpdGxlPVxcXCJ7e3ZhbHVlIHx8ICdOb25lJ319XFxcIj57e3ZhbHVlIHx8ICdOb25lJ319PC90ZD5cIlxuICB9O1xufSk7XG4iLCIjXG4jIExpY2Vuc2VkIHRvIHRoZSBBcGFjaGUgU29mdHdhcmUgRm91bmRhdGlvbiAoQVNGKSB1bmRlciBvbmVcbiMgb3IgbW9yZSBjb250cmlidXRvciBsaWNlbnNlIGFncmVlbWVudHMuICBTZWUgdGhlIE5PVElDRSBmaWxlXG4jIGRpc3RyaWJ1dGVkIHdpdGggdGhpcyB3b3JrIGZvciBhZGRpdGlvbmFsIGluZm9ybWF0aW9uXG4jIHJlZ2FyZGluZyBjb3B5cmlnaHQgb3duZXJzaGlwLiAgVGhlIEFTRiBsaWNlbnNlcyB0aGlzIGZpbGVcbiMgdG8geW91IHVuZGVyIHRoZSBBcGFjaGUgTGljZW5zZSwgVmVyc2lvbiAyLjAgKHRoZVxuIyBcIkxpY2Vuc2VcIik7IHlvdSBtYXkgbm90IHVzZSB0aGlzIGZpbGUgZXhjZXB0IGluIGNvbXBsaWFuY2VcbiMgd2l0aCB0aGUgTGljZW5zZS4gIFlvdSBtYXkgb2J0YWluIGEgY29weSBvZiB0aGUgTGljZW5zZSBhdFxuI1xuIyAgICAgaHR0cDovL3d3dy5hcGFjaGUub3JnL2xpY2Vuc2VzL0xJQ0VOU0UtMi4wXG4jXG4jIFVubGVzcyByZXF1aXJlZCBieSBhcHBsaWNhYmxlIGxhdyBvciBhZ3JlZWQgdG8gaW4gd3JpdGluZywgc29mdHdhcmVcbiMgZGlzdHJpYnV0ZWQgdW5kZXIgdGhlIExpY2Vuc2UgaXMgZGlzdHJpYnV0ZWQgb24gYW4gXCJBUyBJU1wiIEJBU0lTLFxuIyBXSVRIT1VUIFdBUlJBTlRJRVMgT1IgQ09ORElUSU9OUyBPRiBBTlkgS0lORCwgZWl0aGVyIGV4cHJlc3Mgb3IgaW1wbGllZC5cbiMgU2VlIHRoZSBMaWNlbnNlIGZvciB0aGUgc3BlY2lmaWMgbGFuZ3VhZ2UgZ292ZXJuaW5nIHBlcm1pc3Npb25zIGFuZFxuIyBsaW1pdGF0aW9ucyB1bmRlciB0aGUgTGljZW5zZS5cbiNcblxuYW5ndWxhci5tb2R1bGUoJ2ZsaW5rQXBwJylcblxuLmZpbHRlciBcImFtRHVyYXRpb25Gb3JtYXRFeHRlbmRlZFwiLCAoYW5ndWxhck1vbWVudENvbmZpZykgLT5cbiAgYW1EdXJhdGlvbkZvcm1hdEV4dGVuZGVkRmlsdGVyID0gKHZhbHVlLCBmb3JtYXQsIGR1cmF0aW9uRm9ybWF0KSAtPlxuICAgIHJldHVybiBcIlwiICBpZiB0eXBlb2YgdmFsdWUgaXMgXCJ1bmRlZmluZWRcIiBvciB2YWx1ZSBpcyBudWxsXG5cbiAgICBtb21lbnQuZHVyYXRpb24odmFsdWUsIGZvcm1hdCkuZm9ybWF0KGR1cmF0aW9uRm9ybWF0LCB7IHRyaW06IGZhbHNlIH0pXG5cbiAgYW1EdXJhdGlvbkZvcm1hdEV4dGVuZGVkRmlsdGVyLiRzdGF0ZWZ1bCA9IGFuZ3VsYXJNb21lbnRDb25maWcuc3RhdGVmdWxGaWx0ZXJzXG5cbiAgYW1EdXJhdGlvbkZvcm1hdEV4dGVuZGVkRmlsdGVyXG5cbi5maWx0ZXIgXCJodW1hbml6ZVRhc2tOYW1lXCIsIC0+XG4gICh0ZXh0KSAtPlxuICAgICMgVE9ETzogZXh0ZW5kLi4uIGEgbG90XG4gICAgaWYgdGV4dCB0aGVuIHRleHQucmVwbGFjZShcIiZndDtcIiwgXCI+XCIpLnJlcGxhY2UoXCI8YnIvPlwiLFwiXCIpIGVsc2UgJydcbiIsImFuZ3VsYXIubW9kdWxlKCdmbGlua0FwcCcpLmZpbHRlcihcImFtRHVyYXRpb25Gb3JtYXRFeHRlbmRlZFwiLCBmdW5jdGlvbihhbmd1bGFyTW9tZW50Q29uZmlnKSB7XG4gIHZhciBhbUR1cmF0aW9uRm9ybWF0RXh0ZW5kZWRGaWx0ZXI7XG4gIGFtRHVyYXRpb25Gb3JtYXRFeHRlbmRlZEZpbHRlciA9IGZ1bmN0aW9uKHZhbHVlLCBmb3JtYXQsIGR1cmF0aW9uRm9ybWF0KSB7XG4gICAgaWYgKHR5cGVvZiB2YWx1ZSA9PT0gXCJ1bmRlZmluZWRcIiB8fCB2YWx1ZSA9PT0gbnVsbCkge1xuICAgICAgcmV0dXJuIFwiXCI7XG4gICAgfVxuICAgIHJldHVybiBtb21lbnQuZHVyYXRpb24odmFsdWUsIGZvcm1hdCkuZm9ybWF0KGR1cmF0aW9uRm9ybWF0LCB7XG4gICAgICB0cmltOiBmYWxzZVxuICAgIH0pO1xuICB9O1xuICBhbUR1cmF0aW9uRm9ybWF0RXh0ZW5kZWRGaWx0ZXIuJHN0YXRlZnVsID0gYW5ndWxhck1vbWVudENvbmZpZy5zdGF0ZWZ1bEZpbHRlcnM7XG4gIHJldHVybiBhbUR1cmF0aW9uRm9ybWF0RXh0ZW5kZWRGaWx0ZXI7XG59KS5maWx0ZXIoXCJodW1hbml6ZVRhc2tOYW1lXCIsIGZ1bmN0aW9uKCkge1xuICByZXR1cm4gZnVuY3Rpb24odGV4dCkge1xuICAgIGlmICh0ZXh0KSB7XG4gICAgICByZXR1cm4gdGV4dC5yZXBsYWNlKFwiJmd0O1wiLCBcIj5cIikucmVwbGFjZShcIjxici8+XCIsIFwiXCIpO1xuICAgIH0gZWxzZSB7XG4gICAgICByZXR1cm4gJyc7XG4gICAgfVxuICB9O1xufSk7XG4iLCIjXG4jIExpY2Vuc2VkIHRvIHRoZSBBcGFjaGUgU29mdHdhcmUgRm91bmRhdGlvbiAoQVNGKSB1bmRlciBvbmVcbiMgb3IgbW9yZSBjb250cmlidXRvciBsaWNlbnNlIGFncmVlbWVudHMuICBTZWUgdGhlIE5PVElDRSBmaWxlXG4jIGRpc3RyaWJ1dGVkIHdpdGggdGhpcyB3b3JrIGZvciBhZGRpdGlvbmFsIGluZm9ybWF0aW9uXG4jIHJlZ2FyZGluZyBjb3B5cmlnaHQgb3duZXJzaGlwLiAgVGhlIEFTRiBsaWNlbnNlcyB0aGlzIGZpbGVcbiMgdG8geW91IHVuZGVyIHRoZSBBcGFjaGUgTGljZW5zZSwgVmVyc2lvbiAyLjAgKHRoZVxuIyBcIkxpY2Vuc2VcIik7IHlvdSBtYXkgbm90IHVzZSB0aGlzIGZpbGUgZXhjZXB0IGluIGNvbXBsaWFuY2VcbiMgd2l0aCB0aGUgTGljZW5zZS4gIFlvdSBtYXkgb2J0YWluIGEgY29weSBvZiB0aGUgTGljZW5zZSBhdFxuI1xuIyAgICAgaHR0cDovL3d3dy5hcGFjaGUub3JnL2xpY2Vuc2VzL0xJQ0VOU0UtMi4wXG4jXG4jIFVubGVzcyByZXF1aXJlZCBieSBhcHBsaWNhYmxlIGxhdyBvciBhZ3JlZWQgdG8gaW4gd3JpdGluZywgc29mdHdhcmVcbiMgZGlzdHJpYnV0ZWQgdW5kZXIgdGhlIExpY2Vuc2UgaXMgZGlzdHJpYnV0ZWQgb24gYW4gXCJBUyBJU1wiIEJBU0lTLFxuIyBXSVRIT1VUIFdBUlJBTlRJRVMgT1IgQ09ORElUSU9OUyBPRiBBTlkgS0lORCwgZWl0aGVyIGV4cHJlc3Mgb3IgaW1wbGllZC5cbiMgU2VlIHRoZSBMaWNlbnNlIGZvciB0aGUgc3BlY2lmaWMgbGFuZ3VhZ2UgZ292ZXJuaW5nIHBlcm1pc3Npb25zIGFuZFxuIyBsaW1pdGF0aW9ucyB1bmRlciB0aGUgTGljZW5zZS5cbiNcblxuYW5ndWxhci5tb2R1bGUoJ2ZsaW5rQXBwJylcblxuLnNlcnZpY2UgJ01haW5TZXJ2aWNlJywgKCRodHRwLCBmbGlua0NvbmZpZywgJHEpIC0+XG4gIGpvYk9ic2VydmVycyA9IFtdXG5cbiAgQGxvYWRDb25maWcgPSAtPlxuICAgIGRlZmVycmVkID0gJHEuZGVmZXIoKVxuXG4gICAgJGh0dHAuZ2V0IGZsaW5rQ29uZmlnLmpvYlNlcnZlciArIFwiL2NvbmZpZ1wiXG4gICAgLnN1Y2Nlc3MgKGRhdGEsIHN0YXR1cywgaGVhZGVycywgY29uZmlnKSAtPlxuICAgICAgZGVmZXJyZWQucmVzb2x2ZShkYXRhKVxuXG4gICAgZGVmZXJyZWQucHJvbWlzZVxuXG5cbiAgQFxuIiwiYW5ndWxhci5tb2R1bGUoJ2ZsaW5rQXBwJykuc2VydmljZSgnTWFpblNlcnZpY2UnLCBmdW5jdGlvbigkaHR0cCwgZmxpbmtDb25maWcsICRxKSB7XG4gIHZhciBqb2JPYnNlcnZlcnM7XG4gIGpvYk9ic2VydmVycyA9IFtdO1xuICB0aGlzLmxvYWRDb25maWcgPSBmdW5jdGlvbigpIHtcbiAgICB2YXIgZGVmZXJyZWQ7XG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpO1xuICAgICRodHRwLmdldChmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcIi9jb25maWdcIikuc3VjY2VzcyhmdW5jdGlvbihkYXRhLCBzdGF0dXMsIGhlYWRlcnMsIGNvbmZpZykge1xuICAgICAgcmV0dXJuIGRlZmVycmVkLnJlc29sdmUoZGF0YSk7XG4gICAgfSk7XG4gICAgcmV0dXJuIGRlZmVycmVkLnByb21pc2U7XG4gIH07XG4gIHJldHVybiB0aGlzO1xufSk7XG4iLCIjXG4jIExpY2Vuc2VkIHRvIHRoZSBBcGFjaGUgU29mdHdhcmUgRm91bmRhdGlvbiAoQVNGKSB1bmRlciBvbmVcbiMgb3IgbW9yZSBjb250cmlidXRvciBsaWNlbnNlIGFncmVlbWVudHMuICBTZWUgdGhlIE5PVElDRSBmaWxlXG4jIGRpc3RyaWJ1dGVkIHdpdGggdGhpcyB3b3JrIGZvciBhZGRpdGlvbmFsIGluZm9ybWF0aW9uXG4jIHJlZ2FyZGluZyBjb3B5cmlnaHQgb3duZXJzaGlwLiAgVGhlIEFTRiBsaWNlbnNlcyB0aGlzIGZpbGVcbiMgdG8geW91IHVuZGVyIHRoZSBBcGFjaGUgTGljZW5zZSwgVmVyc2lvbiAyLjAgKHRoZVxuIyBcIkxpY2Vuc2VcIik7IHlvdSBtYXkgbm90IHVzZSB0aGlzIGZpbGUgZXhjZXB0IGluIGNvbXBsaWFuY2VcbiMgd2l0aCB0aGUgTGljZW5zZS4gIFlvdSBtYXkgb2J0YWluIGEgY29weSBvZiB0aGUgTGljZW5zZSBhdFxuI1xuIyAgICAgaHR0cDovL3d3dy5hcGFjaGUub3JnL2xpY2Vuc2VzL0xJQ0VOU0UtMi4wXG4jXG4jIFVubGVzcyByZXF1aXJlZCBieSBhcHBsaWNhYmxlIGxhdyBvciBhZ3JlZWQgdG8gaW4gd3JpdGluZywgc29mdHdhcmVcbiMgZGlzdHJpYnV0ZWQgdW5kZXIgdGhlIExpY2Vuc2UgaXMgZGlzdHJpYnV0ZWQgb24gYW4gXCJBUyBJU1wiIEJBU0lTLFxuIyBXSVRIT1VUIFdBUlJBTlRJRVMgT1IgQ09ORElUSU9OUyBPRiBBTlkgS0lORCwgZWl0aGVyIGV4cHJlc3Mgb3IgaW1wbGllZC5cbiMgU2VlIHRoZSBMaWNlbnNlIGZvciB0aGUgc3BlY2lmaWMgbGFuZ3VhZ2UgZ292ZXJuaW5nIHBlcm1pc3Npb25zIGFuZFxuIyBsaW1pdGF0aW9ucyB1bmRlciB0aGUgTGljZW5zZS5cbiNcblxuYW5ndWxhci5tb2R1bGUoJ2ZsaW5rQXBwJylcblxuLmNvbnRyb2xsZXIgJ1J1bm5pbmdKb2JzQ29udHJvbGxlcicsICgkc2NvcGUsICRzdGF0ZSwgJHN0YXRlUGFyYW1zLCBKb2JzU2VydmljZSkgLT5cbiAgJHNjb3BlLmpvYk9ic2VydmVyID0gLT5cbiAgICAkc2NvcGUuam9icyA9IEpvYnNTZXJ2aWNlLmdldEpvYnMoJ3J1bm5pbmcnKVxuXG4gIEpvYnNTZXJ2aWNlLnJlZ2lzdGVyT2JzZXJ2ZXIoJHNjb3BlLmpvYk9ic2VydmVyKVxuICAkc2NvcGUuJG9uICckZGVzdHJveScsIC0+XG4gICAgSm9ic1NlcnZpY2UudW5SZWdpc3Rlck9ic2VydmVyKCRzY29wZS5qb2JPYnNlcnZlcilcblxuICAkc2NvcGUuam9iT2JzZXJ2ZXIoKVxuXG4jIC0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tXG5cbi5jb250cm9sbGVyICdDb21wbGV0ZWRKb2JzQ29udHJvbGxlcicsICgkc2NvcGUsICRzdGF0ZSwgJHN0YXRlUGFyYW1zLCBKb2JzU2VydmljZSkgLT5cbiAgJHNjb3BlLmpvYk9ic2VydmVyID0gLT5cbiAgICAkc2NvcGUuam9icyA9IEpvYnNTZXJ2aWNlLmdldEpvYnMoJ2ZpbmlzaGVkJylcblxuICBKb2JzU2VydmljZS5yZWdpc3Rlck9ic2VydmVyKCRzY29wZS5qb2JPYnNlcnZlcilcbiAgJHNjb3BlLiRvbiAnJGRlc3Ryb3knLCAtPlxuICAgIEpvYnNTZXJ2aWNlLnVuUmVnaXN0ZXJPYnNlcnZlcigkc2NvcGUuam9iT2JzZXJ2ZXIpXG5cbiAgJHNjb3BlLmpvYk9ic2VydmVyKClcblxuIyAtLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLVxuXG4uY29udHJvbGxlciAnU2luZ2xlSm9iQ29udHJvbGxlcicsICgkc2NvcGUsICRzdGF0ZSwgJHN0YXRlUGFyYW1zLCBKb2JzU2VydmljZSwgJHJvb3RTY29wZSkgLT5cbiAgJHNjb3BlLmpvYmlkID0gJHN0YXRlUGFyYW1zLmpvYmlkXG4gICRyb290U2NvcGUuam9iID0gbnVsbFxuXG4gIEpvYnNTZXJ2aWNlLmxvYWRKb2IoJHN0YXRlUGFyYW1zLmpvYmlkKS50aGVuIChkYXRhKSAtPlxuICAgICRyb290U2NvcGUuam9iID0gZGF0YVxuXG4gICRzY29wZS4kb24gJyRkZXN0cm95JywgLT5cbiAgICAkcm9vdFNjb3BlLmpvYiA9IG51bGxcblxuIyAtLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLVxuXG4uY29udHJvbGxlciAnSm9iUGxhbkNvbnRyb2xsZXInLCAoJHNjb3BlLCAkc3RhdGUsICRzdGF0ZVBhcmFtcywgSm9ic1NlcnZpY2UpIC0+XG4gIEpvYnNTZXJ2aWNlLmxvYWRQbGFuKCRzdGF0ZVBhcmFtcy5qb2JpZCkudGhlbiAoZGF0YSkgLT5cbiAgICAkc2NvcGUucGxhbiA9IGRhdGFcblxuIyAtLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLVxuXG4uY29udHJvbGxlciAnSm9iUGxhbk5vZGVDb250cm9sbGVyJywgKCRzY29wZSwgJHN0YXRlLCAkc3RhdGVQYXJhbXMsIEpvYnNTZXJ2aWNlKSAtPlxuICAkc2NvcGUubm9kZWlkID0gJHN0YXRlUGFyYW1zLm5vZGVpZFxuICAkc2NvcGUuc3RhdGVMaXN0ID0gSm9ic1NlcnZpY2Uuc3RhdGVMaXN0KClcblxuICBKb2JzU2VydmljZS5nZXROb2RlKCRzY29wZS5ub2RlaWQpLnRoZW4gKGRhdGEpIC0+XG4gICAgJHNjb3BlLm5vZGUgPSBkYXRhXG5cbiMgLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS1cblxuLmNvbnRyb2xsZXIgJ0pvYlRpbWVsaW5lVmVydGV4Q29udHJvbGxlcicsICgkc2NvcGUsICRzdGF0ZSwgJHN0YXRlUGFyYW1zLCBKb2JzU2VydmljZSkgLT5cbiAgSm9ic1NlcnZpY2UuZ2V0VmVydGV4KCRzdGF0ZVBhcmFtcy52ZXJ0ZXhJZCkudGhlbiAoZGF0YSkgLT5cbiAgICAkc2NvcGUudmVydGV4ID0gZGF0YVxuXG4jIC0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tXG5cbi5jb250cm9sbGVyICdKb2JFeGNlcHRpb25zQ29udHJvbGxlcicsICgkc2NvcGUsICRzdGF0ZSwgJHN0YXRlUGFyYW1zLCBKb2JzU2VydmljZSkgLT5cbiAgSm9ic1NlcnZpY2UubG9hZEV4Y2VwdGlvbnMoKS50aGVuIChkYXRhKSAtPlxuICAgICRzY29wZS5leGNlcHRpb25zID0gZGF0YVxuXG4iLCJhbmd1bGFyLm1vZHVsZSgnZmxpbmtBcHAnKS5jb250cm9sbGVyKCdSdW5uaW5nSm9ic0NvbnRyb2xsZXInLCBmdW5jdGlvbigkc2NvcGUsICRzdGF0ZSwgJHN0YXRlUGFyYW1zLCBKb2JzU2VydmljZSkge1xuICAkc2NvcGUuam9iT2JzZXJ2ZXIgPSBmdW5jdGlvbigpIHtcbiAgICByZXR1cm4gJHNjb3BlLmpvYnMgPSBKb2JzU2VydmljZS5nZXRKb2JzKCdydW5uaW5nJyk7XG4gIH07XG4gIEpvYnNTZXJ2aWNlLnJlZ2lzdGVyT2JzZXJ2ZXIoJHNjb3BlLmpvYk9ic2VydmVyKTtcbiAgJHNjb3BlLiRvbignJGRlc3Ryb3knLCBmdW5jdGlvbigpIHtcbiAgICByZXR1cm4gSm9ic1NlcnZpY2UudW5SZWdpc3Rlck9ic2VydmVyKCRzY29wZS5qb2JPYnNlcnZlcik7XG4gIH0pO1xuICByZXR1cm4gJHNjb3BlLmpvYk9ic2VydmVyKCk7XG59KS5jb250cm9sbGVyKCdDb21wbGV0ZWRKb2JzQ29udHJvbGxlcicsIGZ1bmN0aW9uKCRzY29wZSwgJHN0YXRlLCAkc3RhdGVQYXJhbXMsIEpvYnNTZXJ2aWNlKSB7XG4gICRzY29wZS5qb2JPYnNlcnZlciA9IGZ1bmN0aW9uKCkge1xuICAgIHJldHVybiAkc2NvcGUuam9icyA9IEpvYnNTZXJ2aWNlLmdldEpvYnMoJ2ZpbmlzaGVkJyk7XG4gIH07XG4gIEpvYnNTZXJ2aWNlLnJlZ2lzdGVyT2JzZXJ2ZXIoJHNjb3BlLmpvYk9ic2VydmVyKTtcbiAgJHNjb3BlLiRvbignJGRlc3Ryb3knLCBmdW5jdGlvbigpIHtcbiAgICByZXR1cm4gSm9ic1NlcnZpY2UudW5SZWdpc3Rlck9ic2VydmVyKCRzY29wZS5qb2JPYnNlcnZlcik7XG4gIH0pO1xuICByZXR1cm4gJHNjb3BlLmpvYk9ic2VydmVyKCk7XG59KS5jb250cm9sbGVyKCdTaW5nbGVKb2JDb250cm9sbGVyJywgZnVuY3Rpb24oJHNjb3BlLCAkc3RhdGUsICRzdGF0ZVBhcmFtcywgSm9ic1NlcnZpY2UsICRyb290U2NvcGUpIHtcbiAgJHNjb3BlLmpvYmlkID0gJHN0YXRlUGFyYW1zLmpvYmlkO1xuICAkcm9vdFNjb3BlLmpvYiA9IG51bGw7XG4gIEpvYnNTZXJ2aWNlLmxvYWRKb2IoJHN0YXRlUGFyYW1zLmpvYmlkKS50aGVuKGZ1bmN0aW9uKGRhdGEpIHtcbiAgICByZXR1cm4gJHJvb3RTY29wZS5qb2IgPSBkYXRhO1xuICB9KTtcbiAgcmV0dXJuICRzY29wZS4kb24oJyRkZXN0cm95JywgZnVuY3Rpb24oKSB7XG4gICAgcmV0dXJuICRyb290U2NvcGUuam9iID0gbnVsbDtcbiAgfSk7XG59KS5jb250cm9sbGVyKCdKb2JQbGFuQ29udHJvbGxlcicsIGZ1bmN0aW9uKCRzY29wZSwgJHN0YXRlLCAkc3RhdGVQYXJhbXMsIEpvYnNTZXJ2aWNlKSB7XG4gIHJldHVybiBKb2JzU2VydmljZS5sb2FkUGxhbigkc3RhdGVQYXJhbXMuam9iaWQpLnRoZW4oZnVuY3Rpb24oZGF0YSkge1xuICAgIHJldHVybiAkc2NvcGUucGxhbiA9IGRhdGE7XG4gIH0pO1xufSkuY29udHJvbGxlcignSm9iUGxhbk5vZGVDb250cm9sbGVyJywgZnVuY3Rpb24oJHNjb3BlLCAkc3RhdGUsICRzdGF0ZVBhcmFtcywgSm9ic1NlcnZpY2UpIHtcbiAgJHNjb3BlLm5vZGVpZCA9ICRzdGF0ZVBhcmFtcy5ub2RlaWQ7XG4gICRzY29wZS5zdGF0ZUxpc3QgPSBKb2JzU2VydmljZS5zdGF0ZUxpc3QoKTtcbiAgcmV0dXJuIEpvYnNTZXJ2aWNlLmdldE5vZGUoJHNjb3BlLm5vZGVpZCkudGhlbihmdW5jdGlvbihkYXRhKSB7XG4gICAgcmV0dXJuICRzY29wZS5ub2RlID0gZGF0YTtcbiAgfSk7XG59KS5jb250cm9sbGVyKCdKb2JUaW1lbGluZVZlcnRleENvbnRyb2xsZXInLCBmdW5jdGlvbigkc2NvcGUsICRzdGF0ZSwgJHN0YXRlUGFyYW1zLCBKb2JzU2VydmljZSkge1xuICByZXR1cm4gSm9ic1NlcnZpY2UuZ2V0VmVydGV4KCRzdGF0ZVBhcmFtcy52ZXJ0ZXhJZCkudGhlbihmdW5jdGlvbihkYXRhKSB7XG4gICAgcmV0dXJuICRzY29wZS52ZXJ0ZXggPSBkYXRhO1xuICB9KTtcbn0pLmNvbnRyb2xsZXIoJ0pvYkV4Y2VwdGlvbnNDb250cm9sbGVyJywgZnVuY3Rpb24oJHNjb3BlLCAkc3RhdGUsICRzdGF0ZVBhcmFtcywgSm9ic1NlcnZpY2UpIHtcbiAgcmV0dXJuIEpvYnNTZXJ2aWNlLmxvYWRFeGNlcHRpb25zKCkudGhlbihmdW5jdGlvbihkYXRhKSB7XG4gICAgcmV0dXJuICRzY29wZS5leGNlcHRpb25zID0gZGF0YTtcbiAgfSk7XG59KTtcbiIsIiNcbiMgTGljZW5zZWQgdG8gdGhlIEFwYWNoZSBTb2Z0d2FyZSBGb3VuZGF0aW9uIChBU0YpIHVuZGVyIG9uZVxuIyBvciBtb3JlIGNvbnRyaWJ1dG9yIGxpY2Vuc2UgYWdyZWVtZW50cy4gIFNlZSB0aGUgTk9USUNFIGZpbGVcbiMgZGlzdHJpYnV0ZWQgd2l0aCB0aGlzIHdvcmsgZm9yIGFkZGl0aW9uYWwgaW5mb3JtYXRpb25cbiMgcmVnYXJkaW5nIGNvcHlyaWdodCBvd25lcnNoaXAuICBUaGUgQVNGIGxpY2Vuc2VzIHRoaXMgZmlsZVxuIyB0byB5b3UgdW5kZXIgdGhlIEFwYWNoZSBMaWNlbnNlLCBWZXJzaW9uIDIuMCAodGhlXG4jIFwiTGljZW5zZVwiKTsgeW91IG1heSBub3QgdXNlIHRoaXMgZmlsZSBleGNlcHQgaW4gY29tcGxpYW5jZVxuIyB3aXRoIHRoZSBMaWNlbnNlLiAgWW91IG1heSBvYnRhaW4gYSBjb3B5IG9mIHRoZSBMaWNlbnNlIGF0XG4jXG4jICAgICBodHRwOi8vd3d3LmFwYWNoZS5vcmcvbGljZW5zZXMvTElDRU5TRS0yLjBcbiNcbiMgVW5sZXNzIHJlcXVpcmVkIGJ5IGFwcGxpY2FibGUgbGF3IG9yIGFncmVlZCB0byBpbiB3cml0aW5nLCBzb2Z0d2FyZVxuIyBkaXN0cmlidXRlZCB1bmRlciB0aGUgTGljZW5zZSBpcyBkaXN0cmlidXRlZCBvbiBhbiBcIkFTIElTXCIgQkFTSVMsXG4jIFdJVEhPVVQgV0FSUkFOVElFUyBPUiBDT05ESVRJT05TIE9GIEFOWSBLSU5ELCBlaXRoZXIgZXhwcmVzcyBvciBpbXBsaWVkLlxuIyBTZWUgdGhlIExpY2Vuc2UgZm9yIHRoZSBzcGVjaWZpYyBsYW5ndWFnZSBnb3Zlcm5pbmcgcGVybWlzc2lvbnMgYW5kXG4jIGxpbWl0YXRpb25zIHVuZGVyIHRoZSBMaWNlbnNlLlxuI1xuXG5hbmd1bGFyLm1vZHVsZSgnZmxpbmtBcHAnKVxuXG4jIC0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS1cblxuLmRpcmVjdGl2ZSAndmVydGV4JywgKCRzdGF0ZSkgLT5cbiAgdGVtcGxhdGU6IFwiPHN2ZyBjbGFzcz0ndGltZWxpbmUgc2Vjb25kYXJ5JyB3aWR0aD0nMCcgaGVpZ2h0PScwJz48L3N2Zz5cIlxuXG4gIHNjb3BlOlxuICAgIGRhdGE6IFwiPVwiXG5cbiAgbGluazogKHNjb3BlLCBlbGVtLCBhdHRycykgLT5cbiAgICBzdmdFbCA9IGVsZW0uY2hpbGRyZW4oKVswXVxuXG4gICAgY29udGFpbmVyVyA9IGVsZW0ud2lkdGgoKVxuICAgIGFuZ3VsYXIuZWxlbWVudChzdmdFbCkuYXR0cignd2lkdGgnLCBjb250YWluZXJXKVxuXG4gICAgYW5hbHl6ZVRpbWUgPSAoZGF0YSkgLT5cbiAgICAgIHRlc3REYXRhID0gW11cblxuICAgICAgYW5ndWxhci5mb3JFYWNoIGRhdGEuc3VidGFza3MsIChzdWJ0YXNrLCBpKSAtPlxuICAgICAgICB0ZXN0RGF0YS5wdXNoIHtcbiAgICAgICAgICBsYWJlbDogXCIje3N1YnRhc2suaG9zdH0gKCN7c3VidGFzay5zdWJ0YXNrfSlcIlxuICAgICAgICAgIHRpbWVzOiBbXG4gICAgICAgICAgICB7XG4gICAgICAgICAgICAgIGxhYmVsOiBcIlNjaGVkdWxlZFwiXG4gICAgICAgICAgICAgIGNvbG9yOiBcIiM2NjZcIlxuICAgICAgICAgICAgICBib3JkZXJDb2xvcjogXCIjNTU1XCJcbiAgICAgICAgICAgICAgc3RhcnRpbmdfdGltZTogc3VidGFzay50aW1lc3RhbXBzW1wiU0NIRURVTEVEXCJdXG4gICAgICAgICAgICAgIGVuZGluZ190aW1lOiBzdWJ0YXNrLnRpbWVzdGFtcHNbXCJERVBMT1lJTkdcIl1cbiAgICAgICAgICAgICAgdHlwZTogJ3JlZ3VsYXInXG4gICAgICAgICAgICB9XG4gICAgICAgICAgICB7XG4gICAgICAgICAgICAgIGxhYmVsOiBcIkRlcGxveWluZ1wiXG4gICAgICAgICAgICAgIGNvbG9yOiBcIiNhYWFcIlxuICAgICAgICAgICAgICBib3JkZXJDb2xvcjogXCIjNTU1XCJcbiAgICAgICAgICAgICAgc3RhcnRpbmdfdGltZTogc3VidGFzay50aW1lc3RhbXBzW1wiREVQTE9ZSU5HXCJdXG4gICAgICAgICAgICAgIGVuZGluZ190aW1lOiBzdWJ0YXNrLnRpbWVzdGFtcHNbXCJSVU5OSU5HXCJdXG4gICAgICAgICAgICAgIHR5cGU6ICdyZWd1bGFyJ1xuICAgICAgICAgICAgfVxuICAgICAgICAgICAge1xuICAgICAgICAgICAgICBsYWJlbDogXCJSdW5uaW5nXCJcbiAgICAgICAgICAgICAgY29sb3I6IFwiI2RkZFwiXG4gICAgICAgICAgICAgIGJvcmRlckNvbG9yOiBcIiM1NTVcIlxuICAgICAgICAgICAgICBzdGFydGluZ190aW1lOiBzdWJ0YXNrLnRpbWVzdGFtcHNbXCJSVU5OSU5HXCJdXG4gICAgICAgICAgICAgIGVuZGluZ190aW1lOiBzdWJ0YXNrLnRpbWVzdGFtcHNbXCJGSU5JU0hFRFwiXVxuICAgICAgICAgICAgICB0eXBlOiAncmVndWxhcidcbiAgICAgICAgICAgIH1cbiAgICAgICAgICBdXG4gICAgICAgIH1cblxuICAgICAgY2hhcnQgPSBkMy50aW1lbGluZSgpLnN0YWNrKClcbiAgICAgIC50aWNrRm9ybWF0KHtcbiAgICAgICAgZm9ybWF0OiBkMy50aW1lLmZvcm1hdChcIiVMXCIpXG4gICAgICAgICMgdGlja0ludGVydmFsOiAxXG4gICAgICAgIHRpY2tTaXplOiAxXG4gICAgICB9KVxuICAgICAgLnByZWZpeChcInNpbmdsZVwiKVxuICAgICAgLmxhYmVsRm9ybWF0KChsYWJlbCkgLT5cbiAgICAgICAgbGFiZWxcbiAgICAgIClcbiAgICAgIC5tYXJnaW4oeyBsZWZ0OiAxMDAsIHJpZ2h0OiAwLCB0b3A6IDAsIGJvdHRvbTogMCB9KVxuICAgICAgLml0ZW1IZWlnaHQoMzApXG4gICAgICAucmVsYXRpdmVUaW1lKClcblxuICAgICAgc3ZnID0gZDMuc2VsZWN0KHN2Z0VsKVxuICAgICAgLmRhdHVtKHRlc3REYXRhKVxuICAgICAgLmNhbGwoY2hhcnQpXG5cbiAgICBhbmFseXplVGltZShzY29wZS5kYXRhKVxuXG4gICAgcmV0dXJuXG5cbiMgLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLVxuXG4uZGlyZWN0aXZlICd0aW1lbGluZScsICgkc3RhdGUpIC0+XG4gIHRlbXBsYXRlOiBcIjxzdmcgY2xhc3M9J3RpbWVsaW5lJyB3aWR0aD0nMCcgaGVpZ2h0PScwJz48L3N2Zz5cIlxuXG4gIHNjb3BlOlxuICAgIGpvYjogXCI9XCJcblxuICBsaW5rOiAoc2NvcGUsIGVsZW0sIGF0dHJzKSAtPlxuICAgIHN2Z0VsID0gZWxlbS5jaGlsZHJlbigpWzBdXG5cbiAgICBjb250YWluZXJXID0gZWxlbS53aWR0aCgpXG4gICAgYW5ndWxhci5lbGVtZW50KHN2Z0VsKS5hdHRyKCd3aWR0aCcsIGNvbnRhaW5lclcpXG5cbiAgICB0cmFuc2xhdGVMYWJlbCA9IChsYWJlbCkgLT5cbiAgICAgIGxhYmVsLnJlcGxhY2UoXCImZ3Q7XCIsIFwiPlwiKVxuXG4gICAgYW5hbHl6ZVRpbWUgPSAoZGF0YSkgLT5cbiAgICAgIHRlc3REYXRhID0gW11cblxuICAgICAgdGVzdERhdGEucHVzaCBcbiAgICAgICAgdGltZXM6IFtcbiAgICAgICAgICBsYWJlbDogXCJTY2hlZHVsZWRcIlxuICAgICAgICAgIGNvbG9yOiBcIiNjY2NjY2NcIlxuICAgICAgICAgIGJvcmRlckNvbG9yOiBcIiM1NTVcIlxuICAgICAgICAgIHN0YXJ0aW5nX3RpbWU6IGRhdGEudGltZXN0YW1wc1tcIkNSRUFURURcIl1cbiAgICAgICAgICBlbmRpbmdfdGltZTogZGF0YS50aW1lc3RhbXBzW1wiQ1JFQVRFRFwiXSArIDFcbiAgICAgICAgICB0eXBlOiAnc2NoZWR1bGVkJ1xuICAgICAgICBdXG5cblxuICAgICAgYW5ndWxhci5mb3JFYWNoIGRhdGEudmVydGljZXMsICh2ZXJ0ZXgpIC0+XG4gICAgICAgIHRlc3REYXRhLnB1c2ggXG4gICAgICAgICAgdGltZXM6IFtcbiAgICAgICAgICAgIGxhYmVsOiB0cmFuc2xhdGVMYWJlbCh2ZXJ0ZXgubmFtZSlcbiAgICAgICAgICAgIGNvbG9yOiBcIiNkOWYxZjdcIlxuICAgICAgICAgICAgYm9yZGVyQ29sb3I6IFwiIzYyY2RlYVwiXG4gICAgICAgICAgICBzdGFydGluZ190aW1lOiB2ZXJ0ZXhbJ3N0YXJ0LXRpbWUnXVxuICAgICAgICAgICAgZW5kaW5nX3RpbWU6IHZlcnRleFsnZW5kLXRpbWUnXVxuICAgICAgICAgICAgbGluazogdmVydGV4LmlkXG4gICAgICAgICAgICB0eXBlOiAncmVndWxhcidcbiAgICAgICAgICBdXG5cbiAgICAgIGNoYXJ0ID0gZDMudGltZWxpbmUoKS5zdGFjaygpLmNsaWNrKChkLCBpLCBkYXR1bSkgLT5cbiAgICAgICAgaWYgZC5saW5rXG4gICAgICAgICAgJHN0YXRlLmdvIFwic2luZ2xlLWpvYi50aW1lbGluZS52ZXJ0ZXhcIiwgeyBqb2JpZDogZGF0YS5qaWQsIHZlcnRleElkOiBkLmxpbmsgfVxuXG4gICAgICApXG4gICAgICAudGlja0Zvcm1hdCh7XG4gICAgICAgIGZvcm1hdDogZDMudGltZS5mb3JtYXQoXCIlTFwiKVxuICAgICAgICAjIHRpY2tUaW1lOiBkMy50aW1lLnNlY29uZFxuICAgICAgICAjIHRpY2tJbnRlcnZhbDogMC41XG4gICAgICAgIHRpY2tTaXplOiAxXG4gICAgICB9KVxuICAgICAgLnByZWZpeChcIm1haW5cIilcbiAgICAgIC5tYXJnaW4oeyBsZWZ0OiAwLCByaWdodDogMCwgdG9wOiAwLCBib3R0b206IDAgfSlcbiAgICAgIC5pdGVtSGVpZ2h0KDMwKVxuICAgICAgLnNob3dCb3JkZXJMaW5lKClcbiAgICAgIC5zaG93SG91clRpbWVsaW5lKClcblxuICAgICAgc3ZnID0gZDMuc2VsZWN0KHN2Z0VsKVxuICAgICAgLmRhdHVtKHRlc3REYXRhKVxuICAgICAgLmNhbGwoY2hhcnQpXG5cbiAgICBzY29wZS4kd2F0Y2ggYXR0cnMuam9iLCAoZGF0YSkgLT5cbiAgICAgIGFuYWx5emVUaW1lKGRhdGEpIGlmIGRhdGFcblxuICAgIHJldHVyblxuXG4jIC0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS1cblxuLmRpcmVjdGl2ZSAnam9iUGxhbicsICgkdGltZW91dCkgLT5cbiAgdGVtcGxhdGU6IFwiXG4gICAgPHN2ZyBjbGFzcz0nZ3JhcGgnIHdpZHRoPSc1MDAnIGhlaWdodD0nNDAwJz48ZyAvPjwvc3ZnPlxuICAgIDxzdmcgY2xhc3M9J3RtcCcgd2lkdGg9JzEnIGhlaWdodD0nMSc+PGcgLz48L3N2Zz5cbiAgICA8ZGl2IGNsYXNzPSdidG4tZ3JvdXAgem9vbS1idXR0b25zJz5cbiAgICAgIDxhIGNsYXNzPSdidG4gYnRuLWRlZmF1bHQgem9vbS1pbicgbmctY2xpY2s9J3pvb21JbigpJz48aSBjbGFzcz0nZmEgZmEtcGx1cycgLz48L2E+XG4gICAgICA8YSBjbGFzcz0nYnRuIGJ0bi1kZWZhdWx0IHpvb20tb3V0JyBuZy1jbGljaz0nem9vbU91dCgpJz48aSBjbGFzcz0nZmEgZmEtbWludXMnIC8+PC9hPlxuICAgIDwvZGl2PlwiXG5cbiAgc2NvcGU6XG4gICAgcGxhbjogJz0nXG5cbiAgbGluazogKHNjb3BlLCBlbGVtLCBhdHRycykgLT5cbiAgICBtYWluWm9vbSA9IGQzLmJlaGF2aW9yLnpvb20oKVxuICAgIHN1YmdyYXBocyA9IFtdXG4gICAgam9iaWQgPSBhdHRycy5qb2JpZFxuXG4gICAgbWFpblN2Z0VsZW1lbnQgPSBlbGVtLmNoaWxkcmVuKClbMF1cbiAgICBtYWluRyA9IGVsZW0uY2hpbGRyZW4oKS5jaGlsZHJlbigpWzBdXG4gICAgbWFpblRtcEVsZW1lbnQgPSBlbGVtLmNoaWxkcmVuKClbMV1cblxuICAgIGQzbWFpblN2ZyA9IGQzLnNlbGVjdChtYWluU3ZnRWxlbWVudClcbiAgICBkM21haW5TdmdHID0gZDMuc2VsZWN0KG1haW5HKVxuICAgIGQzdG1wU3ZnID0gZDMuc2VsZWN0KG1haW5UbXBFbGVtZW50KVxuXG4gICAgIyBhbmd1bGFyLmVsZW1lbnQobWFpbkcpLmVtcHR5KClcblxuICAgIGNvbnRhaW5lclcgPSBlbGVtLndpZHRoKClcbiAgICBhbmd1bGFyLmVsZW1lbnQoZWxlbS5jaGlsZHJlbigpWzBdKS53aWR0aChjb250YWluZXJXKVxuXG4gICAgc2NvcGUuem9vbUluID0gLT5cbiAgICAgIGlmIG1haW5ab29tLnNjYWxlKCkgPCAyLjk5XG4gICAgICAgIFxuICAgICAgICAjIENhbGN1bGF0ZSBhbmQgc3RvcmUgbmV3IHZhbHVlcyBpbiB6b29tIG9iamVjdFxuICAgICAgICB0cmFuc2xhdGUgPSBtYWluWm9vbS50cmFuc2xhdGUoKVxuICAgICAgICB2MSA9IHRyYW5zbGF0ZVswXSAqIChtYWluWm9vbS5zY2FsZSgpICsgMC4xIC8gKG1haW5ab29tLnNjYWxlKCkpKVxuICAgICAgICB2MiA9IHRyYW5zbGF0ZVsxXSAqIChtYWluWm9vbS5zY2FsZSgpICsgMC4xIC8gKG1haW5ab29tLnNjYWxlKCkpKVxuICAgICAgICBtYWluWm9vbS5zY2FsZSBtYWluWm9vbS5zY2FsZSgpICsgMC4xXG4gICAgICAgIG1haW5ab29tLnRyYW5zbGF0ZSBbIHYxLCB2MiBdXG4gICAgICAgIFxuICAgICAgICAjIFRyYW5zZm9ybSBzdmdcbiAgICAgICAgZDNtYWluU3ZnRy5hdHRyIFwidHJhbnNmb3JtXCIsIFwidHJhbnNsYXRlKFwiICsgdjEgKyBcIixcIiArIHYyICsgXCIpIHNjYWxlKFwiICsgbWFpblpvb20uc2NhbGUoKSArIFwiKVwiXG5cbiAgICBzY29wZS56b29tT3V0ID0gLT5cbiAgICAgIGlmIG1haW5ab29tLnNjYWxlKCkgPiAwLjMxXG4gICAgICAgIFxuICAgICAgICAjIENhbGN1bGF0ZSBhbmQgc3RvcmUgbmV3IHZhbHVlcyBpbiBtYWluWm9vbSBvYmplY3RcbiAgICAgICAgbWFpblpvb20uc2NhbGUgbWFpblpvb20uc2NhbGUoKSAtIDAuMVxuICAgICAgICB0cmFuc2xhdGUgPSBtYWluWm9vbS50cmFuc2xhdGUoKVxuICAgICAgICB2MSA9IHRyYW5zbGF0ZVswXSAqIChtYWluWm9vbS5zY2FsZSgpIC0gMC4xIC8gKG1haW5ab29tLnNjYWxlKCkpKVxuICAgICAgICB2MiA9IHRyYW5zbGF0ZVsxXSAqIChtYWluWm9vbS5zY2FsZSgpIC0gMC4xIC8gKG1haW5ab29tLnNjYWxlKCkpKVxuICAgICAgICBtYWluWm9vbS50cmFuc2xhdGUgWyB2MSwgdjIgXVxuICAgICAgICBcbiAgICAgICAgIyBUcmFuc2Zvcm0gc3ZnXG4gICAgICAgIGQzbWFpblN2Z0cuYXR0ciBcInRyYW5zZm9ybVwiLCBcInRyYW5zbGF0ZShcIiArIHYxICsgXCIsXCIgKyB2MiArIFwiKSBzY2FsZShcIiArIG1haW5ab29tLnNjYWxlKCkgKyBcIilcIlxuXG4gICAgI2NyZWF0ZSBhIGxhYmVsIG9mIGFuIGVkZ2VcbiAgICBjcmVhdGVMYWJlbEVkZ2UgPSAoZWwpIC0+XG4gICAgICBsYWJlbFZhbHVlID0gXCJcIlxuICAgICAgaWYgZWwuc2hpcF9zdHJhdGVneT8gb3IgZWwubG9jYWxfc3RyYXRlZ3k/XG4gICAgICAgIGxhYmVsVmFsdWUgKz0gXCI8ZGl2IGNsYXNzPSdlZGdlLWxhYmVsJz5cIlxuICAgICAgICBsYWJlbFZhbHVlICs9IGVsLnNoaXBfc3RyYXRlZ3kgIGlmIGVsLnNoaXBfc3RyYXRlZ3k/XG4gICAgICAgIGxhYmVsVmFsdWUgKz0gXCIgKFwiICsgZWwudGVtcF9tb2RlICsgXCIpXCIgIHVubGVzcyBlbC50ZW1wX21vZGUgaXMgYHVuZGVmaW5lZGBcbiAgICAgICAgbGFiZWxWYWx1ZSArPSBcIiw8YnI+XCIgKyBlbC5sb2NhbF9zdHJhdGVneSAgdW5sZXNzIGVsLmxvY2FsX3N0cmF0ZWd5IGlzIGB1bmRlZmluZWRgXG4gICAgICAgIGxhYmVsVmFsdWUgKz0gXCI8L2Rpdj5cIlxuICAgICAgbGFiZWxWYWx1ZVxuXG5cbiAgICAjIHRydWUsIGlmIHRoZSBub2RlIGlzIGEgc3BlY2lhbCBub2RlIGZyb20gYW4gaXRlcmF0aW9uXG4gICAgaXNTcGVjaWFsSXRlcmF0aW9uTm9kZSA9IChpbmZvKSAtPlxuICAgICAgKGluZm8gaXMgXCJwYXJ0aWFsU29sdXRpb25cIiBvciBpbmZvIGlzIFwibmV4dFBhcnRpYWxTb2x1dGlvblwiIG9yIGluZm8gaXMgXCJ3b3Jrc2V0XCIgb3IgaW5mbyBpcyBcIm5leHRXb3Jrc2V0XCIgb3IgaW5mbyBpcyBcInNvbHV0aW9uU2V0XCIgb3IgaW5mbyBpcyBcInNvbHV0aW9uRGVsdGFcIilcblxuICAgIGdldE5vZGVUeXBlID0gKGVsLCBpbmZvKSAtPlxuICAgICAgaWYgaW5mbyBpcyBcIm1pcnJvclwiXG4gICAgICAgICdub2RlLW1pcnJvcidcblxuICAgICAgZWxzZSBpZiBpc1NwZWNpYWxJdGVyYXRpb25Ob2RlKGluZm8pXG4gICAgICAgICdub2RlLWl0ZXJhdGlvbidcblxuICAgICAgZWxzZVxuICAgICAgICAgICdub2RlLW5vcm1hbCdcbiAgICAgIFxuICAgICMgY3JlYXRlcyB0aGUgbGFiZWwgb2YgYSBub2RlLCBpbiBpbmZvIGlzIHN0b3JlZCwgd2hldGhlciBpdCBpcyBhIHNwZWNpYWwgbm9kZSAobGlrZSBhIG1pcnJvciBpbiBhbiBpdGVyYXRpb24pXG4gICAgY3JlYXRlTGFiZWxOb2RlID0gKGVsLCBpbmZvLCBtYXhXLCBtYXhIKSAtPlxuICAgICAgbGFiZWxWYWx1ZSA9IFwiPGEgaHJlZj0nIy9qb2JzL1wiICsgam9iaWQgKyBcIi92ZXJ0ZXgvXCIgKyBlbC5pZCArIFwiJyBjbGFzcz0nbm9kZS1sYWJlbCBcIiArIGdldE5vZGVUeXBlKGVsLCBpbmZvKSArIFwiJz5cIlxuXG4gICAgICAjIE5vZGVuYW1lXG4gICAgICBpZiBpbmZvIGlzIFwibWlycm9yXCJcbiAgICAgICAgbGFiZWxWYWx1ZSArPSBcIjxoMyBjbGFzcz0nbm9kZS1uYW1lJz5NaXJyb3Igb2YgXCIgKyBlbC5vcGVyYXRvciArIFwiPC9oMz5cIlxuICAgICAgZWxzZVxuICAgICAgICBsYWJlbFZhbHVlICs9IFwiPGgzIGNsYXNzPSdub2RlLW5hbWUnPlwiICsgZWwub3BlcmF0b3IgKyBcIjwvaDM+XCJcbiAgICAgIGlmIGVsLmRlc2NyaXB0aW9uIGlzIFwiXCJcbiAgICAgICAgbGFiZWxWYWx1ZSArPSBcIlwiXG4gICAgICBlbHNlXG4gICAgICAgIHN0ZXBOYW1lID0gZWwuZGVzY3JpcHRpb25cbiAgICAgICAgXG4gICAgICAgICMgY2xlYW4gc3RlcE5hbWVcbiAgICAgICAgc3RlcE5hbWUgPSBzaG9ydGVuU3RyaW5nKHN0ZXBOYW1lKVxuICAgICAgICBsYWJlbFZhbHVlICs9IFwiPGg0IGNsYXNzPSdzdGVwLW5hbWUnPlwiICsgc3RlcE5hbWUgKyBcIjwvaDQ+XCJcbiAgICAgIFxuICAgICAgIyBJZiB0aGlzIG5vZGUgaXMgYW4gXCJpdGVyYXRpb25cIiB3ZSBuZWVkIGEgZGlmZmVyZW50IHBhbmVsLWJvZHlcbiAgICAgIGlmIGVsLnN0ZXBfZnVuY3Rpb24/XG4gICAgICAgIGxhYmVsVmFsdWUgKz0gZXh0ZW5kTGFiZWxOb2RlRm9ySXRlcmF0aW9uKGVsLmlkLCBtYXhXLCBtYXhIKVxuICAgICAgZWxzZVxuICAgICAgICBcbiAgICAgICAgIyBPdGhlcndpc2UgYWRkIGluZm9zICAgIFxuICAgICAgICBsYWJlbFZhbHVlICs9IFwiPGg1PlwiICsgaW5mbyArIFwiIE5vZGU8L2g1PlwiICBpZiBpc1NwZWNpYWxJdGVyYXRpb25Ob2RlKGluZm8pXG4gICAgICAgIGxhYmVsVmFsdWUgKz0gXCI8aDU+UGFyYWxsZWxpc206IFwiICsgZWwucGFyYWxsZWxpc20gKyBcIjwvaDU+XCIgIHVubGVzcyBlbC5wYXJhbGxlbGlzbSBpcyBcIlwiXG4gICAgICAgIGxhYmVsVmFsdWUgKz0gXCI8aDU+T3BlcmF0aW9uOiBcIiArIHNob3J0ZW5TdHJpbmcoZWwub3BlcmF0b3Jfc3RyYXRlZ3kpICsgXCI8L2g1PlwiICB1bmxlc3MgZWwub3BlcmF0b3IgaXMgYHVuZGVmaW5lZGBcbiAgICAgIFxuICAgICAgbGFiZWxWYWx1ZSArPSBcIjwvYT5cIlxuICAgICAgbGFiZWxWYWx1ZVxuXG4gICAgIyBFeHRlbmRzIHRoZSBsYWJlbCBvZiBhIG5vZGUgd2l0aCBhbiBhZGRpdGlvbmFsIHN2ZyBFbGVtZW50IHRvIHByZXNlbnQgdGhlIGl0ZXJhdGlvbi5cbiAgICBleHRlbmRMYWJlbE5vZGVGb3JJdGVyYXRpb24gPSAoaWQsIG1heFcsIG1heEgpIC0+XG4gICAgICBzdmdJRCA9IFwic3ZnLVwiICsgaWRcblxuICAgICAgbGFiZWxWYWx1ZSA9IFwiPHN2ZyBjbGFzcz0nXCIgKyBzdmdJRCArIFwiJyB3aWR0aD1cIiArIG1heFcgKyBcIiBoZWlnaHQ9XCIgKyBtYXhIICsgXCI+PGcgLz48L3N2Zz5cIlxuICAgICAgbGFiZWxWYWx1ZVxuXG4gICAgIyBTcGxpdCBhIHN0cmluZyBpbnRvIG11bHRpcGxlIGxpbmVzIHNvIHRoYXQgZWFjaCBsaW5lIGhhcyBsZXNzIHRoYW4gMzAgbGV0dGVycy5cbiAgICBzaG9ydGVuU3RyaW5nID0gKHMpIC0+XG4gICAgICAjIG1ha2Ugc3VyZSB0aGF0IG5hbWUgZG9lcyBub3QgY29udGFpbiBhIDwgKGJlY2F1c2Ugb2YgaHRtbClcbiAgICAgIGlmIHMuY2hhckF0KDApIGlzIFwiPFwiXG4gICAgICAgIHMgPSBzLnJlcGxhY2UoXCI8XCIsIFwiJmx0O1wiKVxuICAgICAgICBzID0gcy5yZXBsYWNlKFwiPlwiLCBcIiZndDtcIilcbiAgICAgIHNiciA9IFwiXCJcbiAgICAgIHdoaWxlIHMubGVuZ3RoID4gMzBcbiAgICAgICAgc2JyID0gc2JyICsgcy5zdWJzdHJpbmcoMCwgMzApICsgXCI8YnI+XCJcbiAgICAgICAgcyA9IHMuc3Vic3RyaW5nKDMwLCBzLmxlbmd0aClcbiAgICAgIHNiciA9IHNiciArIHNcbiAgICAgIHNiclxuXG4gICAgY3JlYXRlTm9kZSA9IChnLCBkYXRhLCBlbCwgaXNQYXJlbnQgPSBmYWxzZSwgbWF4VywgbWF4SCkgLT5cbiAgICAgICMgY3JlYXRlIG5vZGUsIHNlbmQgYWRkaXRpb25hbCBpbmZvcm1hdGlvbnMgYWJvdXQgdGhlIG5vZGUgaWYgaXQgaXMgYSBzcGVjaWFsIG9uZVxuICAgICAgaWYgZWwuaWQgaXMgZGF0YS5wYXJ0aWFsX3NvbHV0aW9uXG4gICAgICAgIGcuc2V0Tm9kZSBlbC5pZCxcbiAgICAgICAgICBsYWJlbDogY3JlYXRlTGFiZWxOb2RlKGVsLCBcInBhcnRpYWxTb2x1dGlvblwiLCBtYXhXLCBtYXhIKVxuICAgICAgICAgIGxhYmVsVHlwZTogJ2h0bWwnXG4gICAgICAgICAgY2xhc3M6IGdldE5vZGVUeXBlKGVsLCBcInBhcnRpYWxTb2x1dGlvblwiKVxuXG4gICAgICBlbHNlIGlmIGVsLmlkIGlzIGRhdGEubmV4dF9wYXJ0aWFsX3NvbHV0aW9uXG4gICAgICAgIGcuc2V0Tm9kZSBlbC5pZCxcbiAgICAgICAgICBsYWJlbDogY3JlYXRlTGFiZWxOb2RlKGVsLCBcIm5leHRQYXJ0aWFsU29sdXRpb25cIiwgbWF4VywgbWF4SClcbiAgICAgICAgICBsYWJlbFR5cGU6ICdodG1sJ1xuICAgICAgICAgIGNsYXNzOiBnZXROb2RlVHlwZShlbCwgXCJuZXh0UGFydGlhbFNvbHV0aW9uXCIpXG5cbiAgICAgIGVsc2UgaWYgZWwuaWQgaXMgZGF0YS53b3Jrc2V0XG4gICAgICAgIGcuc2V0Tm9kZSBlbC5pZCxcbiAgICAgICAgICBsYWJlbDogY3JlYXRlTGFiZWxOb2RlKGVsLCBcIndvcmtzZXRcIiwgbWF4VywgbWF4SClcbiAgICAgICAgICBsYWJlbFR5cGU6ICdodG1sJ1xuICAgICAgICAgIGNsYXNzOiBnZXROb2RlVHlwZShlbCwgXCJ3b3Jrc2V0XCIpXG5cbiAgICAgIGVsc2UgaWYgZWwuaWQgaXMgZGF0YS5uZXh0X3dvcmtzZXRcbiAgICAgICAgZy5zZXROb2RlIGVsLmlkLFxuICAgICAgICAgIGxhYmVsOiBjcmVhdGVMYWJlbE5vZGUoZWwsIFwibmV4dFdvcmtzZXRcIiwgbWF4VywgbWF4SClcbiAgICAgICAgICBsYWJlbFR5cGU6ICdodG1sJ1xuICAgICAgICAgIGNsYXNzOiBnZXROb2RlVHlwZShlbCwgXCJuZXh0V29ya3NldFwiKVxuXG4gICAgICBlbHNlIGlmIGVsLmlkIGlzIGRhdGEuc29sdXRpb25fc2V0XG4gICAgICAgIGcuc2V0Tm9kZSBlbC5pZCxcbiAgICAgICAgICBsYWJlbDogY3JlYXRlTGFiZWxOb2RlKGVsLCBcInNvbHV0aW9uU2V0XCIsIG1heFcsIG1heEgpXG4gICAgICAgICAgbGFiZWxUeXBlOiAnaHRtbCdcbiAgICAgICAgICBjbGFzczogZ2V0Tm9kZVR5cGUoZWwsIFwic29sdXRpb25TZXRcIilcblxuICAgICAgZWxzZSBpZiBlbC5pZCBpcyBkYXRhLnNvbHV0aW9uX2RlbHRhXG4gICAgICAgIGcuc2V0Tm9kZSBlbC5pZCxcbiAgICAgICAgICBsYWJlbDogY3JlYXRlTGFiZWxOb2RlKGVsLCBcInNvbHV0aW9uRGVsdGFcIiwgbWF4VywgbWF4SClcbiAgICAgICAgICBsYWJlbFR5cGU6ICdodG1sJ1xuICAgICAgICAgIGNsYXNzOiBnZXROb2RlVHlwZShlbCwgXCJzb2x1dGlvbkRlbHRhXCIpXG5cbiAgICAgIGVsc2VcbiAgICAgICAgZy5zZXROb2RlIGVsLmlkLFxuICAgICAgICAgIGxhYmVsOiBjcmVhdGVMYWJlbE5vZGUoZWwsIFwiXCIsIG1heFcsIG1heEgpXG4gICAgICAgICAgbGFiZWxUeXBlOiAnaHRtbCdcbiAgICAgICAgICBjbGFzczogZ2V0Tm9kZVR5cGUoZWwsIFwiXCIpXG5cbiAgICBjcmVhdGVFZGdlID0gKGcsIGRhdGEsIGVsLCBleGlzdGluZ05vZGVzLCBwcmVkKSAtPlxuICAgICAgdW5sZXNzIGV4aXN0aW5nTm9kZXMuaW5kZXhPZihwcmVkLmlkKSBpcyAtMVxuICAgICAgICBnLnNldEVkZ2UgcHJlZC5pZCwgZWwuaWQsXG4gICAgICAgICAgbGFiZWw6IGNyZWF0ZUxhYmVsRWRnZShwcmVkKVxuICAgICAgICAgIGxhYmVsVHlwZTogJ2h0bWwnXG4gICAgICAgICAgYXJyb3doZWFkOiAnbm9ybWFsJ1xuXG4gICAgICBlbHNlXG4gICAgICAgIG1pc3NpbmdOb2RlID0gc2VhcmNoRm9yTm9kZShkYXRhLCBwcmVkLmlkKVxuICAgICAgICB1bmxlc3MgIW1pc3NpbmdOb2RlIG9yIG1pc3NpbmdOb2RlLmFscmVhZHlBZGRlZCBpcyB0cnVlXG4gICAgICAgICAgbWlzc2luZ05vZGUuYWxyZWFkeUFkZGVkID0gdHJ1ZVxuICAgICAgICAgIGcuc2V0Tm9kZSBtaXNzaW5nTm9kZS5pZCxcbiAgICAgICAgICAgIGxhYmVsOiBjcmVhdGVMYWJlbE5vZGUobWlzc2luZ05vZGUsIFwibWlycm9yXCIpXG4gICAgICAgICAgICBsYWJlbFR5cGU6ICdodG1sJ1xuICAgICAgICAgICAgY2xhc3M6IGdldE5vZGVUeXBlKG1pc3NpbmdOb2RlLCAnbWlycm9yJylcblxuICAgICAgICAgIGcuc2V0RWRnZSBtaXNzaW5nTm9kZS5pZCwgZWwuaWQsXG4gICAgICAgICAgICBsYWJlbDogY3JlYXRlTGFiZWxFZGdlKG1pc3NpbmdOb2RlKVxuICAgICAgICAgICAgbGFiZWxUeXBlOiAnaHRtbCdcblxuICAgIGxvYWRKc29uVG9EYWdyZSA9IChnLCBkYXRhKSAtPlxuICAgICAgZXhpc3RpbmdOb2RlcyA9IFtdXG5cbiAgICAgIGlmIGRhdGEubm9kZXM/XG4gICAgICAgICMgVGhpcyBpcyB0aGUgbm9ybWFsIGpzb24gZGF0YVxuICAgICAgICB0b0l0ZXJhdGUgPSBkYXRhLm5vZGVzXG5cbiAgICAgIGVsc2VcbiAgICAgICAgIyBUaGlzIGlzIGFuIGl0ZXJhdGlvbiwgd2Ugbm93IHN0b3JlIHNwZWNpYWwgaXRlcmF0aW9uIG5vZGVzIGlmIHBvc3NpYmxlXG4gICAgICAgIHRvSXRlcmF0ZSA9IGRhdGEuc3RlcF9mdW5jdGlvblxuICAgICAgICBpc1BhcmVudCA9IHRydWVcblxuICAgICAgZm9yIGVsIGluIHRvSXRlcmF0ZVxuICAgICAgICBtYXhXID0gMFxuICAgICAgICBtYXhIID0gMFxuXG4gICAgICAgIGlmIGVsLnN0ZXBfZnVuY3Rpb25cbiAgICAgICAgICBzZyA9IG5ldyBkYWdyZUQzLmdyYXBobGliLkdyYXBoKHsgbXVsdGlncmFwaDogdHJ1ZSwgY29tcG91bmQ6IHRydWUgfSkuc2V0R3JhcGgoe1xuICAgICAgICAgICAgbm9kZXNlcDogMjBcbiAgICAgICAgICAgIGVkZ2VzZXA6IDBcbiAgICAgICAgICAgIHJhbmtzZXA6IDIwXG4gICAgICAgICAgICByYW5rZGlyOiBcIkxSXCJcbiAgICAgICAgICAgIG1hcmdpbng6IDEwXG4gICAgICAgICAgICBtYXJnaW55OiAxMFxuICAgICAgICAgICAgfSlcblxuICAgICAgICAgIHN1YmdyYXBoc1tlbC5pZF0gPSBzZ1xuXG4gICAgICAgICAgbG9hZEpzb25Ub0RhZ3JlKHNnLCBlbClcblxuICAgICAgICAgIHIgPSBuZXcgZGFncmVEMy5yZW5kZXIoKVxuICAgICAgICAgIGQzdG1wU3ZnLnNlbGVjdCgnZycpLmNhbGwociwgc2cpXG4gICAgICAgICAgbWF4VyA9IHNnLmdyYXBoKCkud2lkdGhcbiAgICAgICAgICBtYXhIID0gc2cuZ3JhcGgoKS5oZWlnaHRcblxuICAgICAgICAgIGFuZ3VsYXIuZWxlbWVudChtYWluVG1wRWxlbWVudCkuZW1wdHkoKVxuXG4gICAgICAgIGNyZWF0ZU5vZGUoZywgZGF0YSwgZWwsIGlzUGFyZW50LCBtYXhXLCBtYXhIKVxuXG4gICAgICAgIGV4aXN0aW5nTm9kZXMucHVzaCBlbC5pZFxuICAgICAgICBcbiAgICAgICAgIyBjcmVhdGUgZWRnZXMgZnJvbSBpbnB1dHMgdG8gY3VycmVudCBub2RlXG4gICAgICAgIGlmIGVsLmlucHV0cz9cbiAgICAgICAgICBmb3IgcHJlZCBpbiBlbC5pbnB1dHNcbiAgICAgICAgICAgIGNyZWF0ZUVkZ2UoZywgZGF0YSwgZWwsIGV4aXN0aW5nTm9kZXMsIHByZWQpXG5cbiAgICAgIGdcblxuICAgICMgc2VhcmNoZXMgaW4gdGhlIGdsb2JhbCBKU09ORGF0YSBmb3IgdGhlIG5vZGUgd2l0aCB0aGUgZ2l2ZW4gaWRcbiAgICBzZWFyY2hGb3JOb2RlID0gKGRhdGEsIG5vZGVJRCkgLT5cbiAgICAgIGZvciBpIG9mIGRhdGEubm9kZXNcbiAgICAgICAgZWwgPSBkYXRhLm5vZGVzW2ldXG4gICAgICAgIHJldHVybiBlbCAgaWYgZWwuaWQgaXMgbm9kZUlEXG4gICAgICAgIFxuICAgICAgICAjIGxvb2sgZm9yIG5vZGVzIHRoYXQgYXJlIGluIGl0ZXJhdGlvbnNcbiAgICAgICAgaWYgZWwuc3RlcF9mdW5jdGlvbj9cbiAgICAgICAgICBmb3IgaiBvZiBlbC5zdGVwX2Z1bmN0aW9uXG4gICAgICAgICAgICByZXR1cm4gZWwuc3RlcF9mdW5jdGlvbltqXSAgaWYgZWwuc3RlcF9mdW5jdGlvbltqXS5pZCBpcyBub2RlSURcblxuICAgIGRyYXdHcmFwaCA9IChkYXRhKSAtPlxuICAgICAgZyA9IG5ldyBkYWdyZUQzLmdyYXBobGliLkdyYXBoKHsgbXVsdGlncmFwaDogdHJ1ZSwgY29tcG91bmQ6IHRydWUgfSkuc2V0R3JhcGgoe1xuICAgICAgICBub2Rlc2VwOiA3MFxuICAgICAgICBlZGdlc2VwOiAwXG4gICAgICAgIHJhbmtzZXA6IDUwXG4gICAgICAgIHJhbmtkaXI6IFwiTFJcIlxuICAgICAgICBtYXJnaW54OiA0MFxuICAgICAgICBtYXJnaW55OiA0MFxuICAgICAgICB9KVxuXG4gICAgICBsb2FkSnNvblRvRGFncmUoZywgZGF0YSlcblxuICAgICAgcmVuZGVyZXIgPSBuZXcgZGFncmVEMy5yZW5kZXIoKVxuICAgICAgZDNtYWluU3ZnRy5jYWxsKHJlbmRlcmVyLCBnKVxuXG4gICAgICBmb3IgaSwgc2cgb2Ygc3ViZ3JhcGhzXG4gICAgICAgIGQzbWFpblN2Zy5zZWxlY3QoJ3N2Zy5zdmctJyArIGkgKyAnIGcnKS5jYWxsKHJlbmRlcmVyLCBzZylcblxuICAgICAgbmV3U2NhbGUgPSAwLjVcblxuICAgICAgeENlbnRlck9mZnNldCA9IE1hdGguZmxvb3IoKGFuZ3VsYXIuZWxlbWVudChtYWluU3ZnRWxlbWVudCkud2lkdGgoKSAtIGcuZ3JhcGgoKS53aWR0aCAqIG5ld1NjYWxlKSAvIDIpXG4gICAgICB5Q2VudGVyT2Zmc2V0ID0gTWF0aC5mbG9vcigoYW5ndWxhci5lbGVtZW50KG1haW5TdmdFbGVtZW50KS5oZWlnaHQoKSAtIGcuZ3JhcGgoKS5oZWlnaHQgKiBuZXdTY2FsZSkgLyAyKVxuXG4gICAgICBtYWluWm9vbS5zY2FsZShuZXdTY2FsZSkudHJhbnNsYXRlKFt4Q2VudGVyT2Zmc2V0LCB5Q2VudGVyT2Zmc2V0XSlcblxuICAgICAgZDNtYWluU3ZnRy5hdHRyKFwidHJhbnNmb3JtXCIsIFwidHJhbnNsYXRlKFwiICsgeENlbnRlck9mZnNldCArIFwiLCBcIiArIHlDZW50ZXJPZmZzZXQgKyBcIikgc2NhbGUoXCIgKyBtYWluWm9vbS5zY2FsZSgpICsgXCIpXCIpXG5cbiAgICAgIG1haW5ab29tLm9uKFwiem9vbVwiLCAtPlxuICAgICAgICBldiA9IGQzLmV2ZW50XG4gICAgICAgIGQzbWFpblN2Z0cuYXR0ciBcInRyYW5zZm9ybVwiLCBcInRyYW5zbGF0ZShcIiArIGV2LnRyYW5zbGF0ZSArIFwiKSBzY2FsZShcIiArIGV2LnNjYWxlICsgXCIpXCJcbiAgICAgIClcbiAgICAgIG1haW5ab29tKGQzbWFpblN2ZylcblxuICAgIHNjb3BlLiR3YXRjaCBhdHRycy5wbGFuLCAobmV3UGxhbikgLT5cbiAgICAgIGRyYXdHcmFwaChuZXdQbGFuKSBpZiBuZXdQbGFuXG5cbiAgICByZXR1cm5cbiIsImFuZ3VsYXIubW9kdWxlKCdmbGlua0FwcCcpLmRpcmVjdGl2ZSgndmVydGV4JywgZnVuY3Rpb24oJHN0YXRlKSB7XG4gIHJldHVybiB7XG4gICAgdGVtcGxhdGU6IFwiPHN2ZyBjbGFzcz0ndGltZWxpbmUgc2Vjb25kYXJ5JyB3aWR0aD0nMCcgaGVpZ2h0PScwJz48L3N2Zz5cIixcbiAgICBzY29wZToge1xuICAgICAgZGF0YTogXCI9XCJcbiAgICB9LFxuICAgIGxpbms6IGZ1bmN0aW9uKHNjb3BlLCBlbGVtLCBhdHRycykge1xuICAgICAgdmFyIGFuYWx5emVUaW1lLCBjb250YWluZXJXLCBzdmdFbDtcbiAgICAgIHN2Z0VsID0gZWxlbS5jaGlsZHJlbigpWzBdO1xuICAgICAgY29udGFpbmVyVyA9IGVsZW0ud2lkdGgoKTtcbiAgICAgIGFuZ3VsYXIuZWxlbWVudChzdmdFbCkuYXR0cignd2lkdGgnLCBjb250YWluZXJXKTtcbiAgICAgIGFuYWx5emVUaW1lID0gZnVuY3Rpb24oZGF0YSkge1xuICAgICAgICB2YXIgY2hhcnQsIHN2ZywgdGVzdERhdGE7XG4gICAgICAgIHRlc3REYXRhID0gW107XG4gICAgICAgIGFuZ3VsYXIuZm9yRWFjaChkYXRhLnN1YnRhc2tzLCBmdW5jdGlvbihzdWJ0YXNrLCBpKSB7XG4gICAgICAgICAgcmV0dXJuIHRlc3REYXRhLnB1c2goe1xuICAgICAgICAgICAgbGFiZWw6IHN1YnRhc2suaG9zdCArIFwiIChcIiArIHN1YnRhc2suc3VidGFzayArIFwiKVwiLFxuICAgICAgICAgICAgdGltZXM6IFtcbiAgICAgICAgICAgICAge1xuICAgICAgICAgICAgICAgIGxhYmVsOiBcIlNjaGVkdWxlZFwiLFxuICAgICAgICAgICAgICAgIGNvbG9yOiBcIiM2NjZcIixcbiAgICAgICAgICAgICAgICBib3JkZXJDb2xvcjogXCIjNTU1XCIsXG4gICAgICAgICAgICAgICAgc3RhcnRpbmdfdGltZTogc3VidGFzay50aW1lc3RhbXBzW1wiU0NIRURVTEVEXCJdLFxuICAgICAgICAgICAgICAgIGVuZGluZ190aW1lOiBzdWJ0YXNrLnRpbWVzdGFtcHNbXCJERVBMT1lJTkdcIl0sXG4gICAgICAgICAgICAgICAgdHlwZTogJ3JlZ3VsYXInXG4gICAgICAgICAgICAgIH0sIHtcbiAgICAgICAgICAgICAgICBsYWJlbDogXCJEZXBsb3lpbmdcIixcbiAgICAgICAgICAgICAgICBjb2xvcjogXCIjYWFhXCIsXG4gICAgICAgICAgICAgICAgYm9yZGVyQ29sb3I6IFwiIzU1NVwiLFxuICAgICAgICAgICAgICAgIHN0YXJ0aW5nX3RpbWU6IHN1YnRhc2sudGltZXN0YW1wc1tcIkRFUExPWUlOR1wiXSxcbiAgICAgICAgICAgICAgICBlbmRpbmdfdGltZTogc3VidGFzay50aW1lc3RhbXBzW1wiUlVOTklOR1wiXSxcbiAgICAgICAgICAgICAgICB0eXBlOiAncmVndWxhcidcbiAgICAgICAgICAgICAgfSwge1xuICAgICAgICAgICAgICAgIGxhYmVsOiBcIlJ1bm5pbmdcIixcbiAgICAgICAgICAgICAgICBjb2xvcjogXCIjZGRkXCIsXG4gICAgICAgICAgICAgICAgYm9yZGVyQ29sb3I6IFwiIzU1NVwiLFxuICAgICAgICAgICAgICAgIHN0YXJ0aW5nX3RpbWU6IHN1YnRhc2sudGltZXN0YW1wc1tcIlJVTk5JTkdcIl0sXG4gICAgICAgICAgICAgICAgZW5kaW5nX3RpbWU6IHN1YnRhc2sudGltZXN0YW1wc1tcIkZJTklTSEVEXCJdLFxuICAgICAgICAgICAgICAgIHR5cGU6ICdyZWd1bGFyJ1xuICAgICAgICAgICAgICB9XG4gICAgICAgICAgICBdXG4gICAgICAgICAgfSk7XG4gICAgICAgIH0pO1xuICAgICAgICBjaGFydCA9IGQzLnRpbWVsaW5lKCkuc3RhY2soKS50aWNrRm9ybWF0KHtcbiAgICAgICAgICBmb3JtYXQ6IGQzLnRpbWUuZm9ybWF0KFwiJUxcIiksXG4gICAgICAgICAgdGlja1NpemU6IDFcbiAgICAgICAgfSkucHJlZml4KFwic2luZ2xlXCIpLmxhYmVsRm9ybWF0KGZ1bmN0aW9uKGxhYmVsKSB7XG4gICAgICAgICAgcmV0dXJuIGxhYmVsO1xuICAgICAgICB9KS5tYXJnaW4oe1xuICAgICAgICAgIGxlZnQ6IDEwMCxcbiAgICAgICAgICByaWdodDogMCxcbiAgICAgICAgICB0b3A6IDAsXG4gICAgICAgICAgYm90dG9tOiAwXG4gICAgICAgIH0pLml0ZW1IZWlnaHQoMzApLnJlbGF0aXZlVGltZSgpO1xuICAgICAgICByZXR1cm4gc3ZnID0gZDMuc2VsZWN0KHN2Z0VsKS5kYXR1bSh0ZXN0RGF0YSkuY2FsbChjaGFydCk7XG4gICAgICB9O1xuICAgICAgYW5hbHl6ZVRpbWUoc2NvcGUuZGF0YSk7XG4gICAgfVxuICB9O1xufSkuZGlyZWN0aXZlKCd0aW1lbGluZScsIGZ1bmN0aW9uKCRzdGF0ZSkge1xuICByZXR1cm4ge1xuICAgIHRlbXBsYXRlOiBcIjxzdmcgY2xhc3M9J3RpbWVsaW5lJyB3aWR0aD0nMCcgaGVpZ2h0PScwJz48L3N2Zz5cIixcbiAgICBzY29wZToge1xuICAgICAgam9iOiBcIj1cIlxuICAgIH0sXG4gICAgbGluazogZnVuY3Rpb24oc2NvcGUsIGVsZW0sIGF0dHJzKSB7XG4gICAgICB2YXIgYW5hbHl6ZVRpbWUsIGNvbnRhaW5lclcsIHN2Z0VsLCB0cmFuc2xhdGVMYWJlbDtcbiAgICAgIHN2Z0VsID0gZWxlbS5jaGlsZHJlbigpWzBdO1xuICAgICAgY29udGFpbmVyVyA9IGVsZW0ud2lkdGgoKTtcbiAgICAgIGFuZ3VsYXIuZWxlbWVudChzdmdFbCkuYXR0cignd2lkdGgnLCBjb250YWluZXJXKTtcbiAgICAgIHRyYW5zbGF0ZUxhYmVsID0gZnVuY3Rpb24obGFiZWwpIHtcbiAgICAgICAgcmV0dXJuIGxhYmVsLnJlcGxhY2UoXCImZ3Q7XCIsIFwiPlwiKTtcbiAgICAgIH07XG4gICAgICBhbmFseXplVGltZSA9IGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgICAgdmFyIGNoYXJ0LCBzdmcsIHRlc3REYXRhO1xuICAgICAgICB0ZXN0RGF0YSA9IFtdO1xuICAgICAgICB0ZXN0RGF0YS5wdXNoKHtcbiAgICAgICAgICB0aW1lczogW1xuICAgICAgICAgICAge1xuICAgICAgICAgICAgICBsYWJlbDogXCJTY2hlZHVsZWRcIixcbiAgICAgICAgICAgICAgY29sb3I6IFwiI2NjY2NjY1wiLFxuICAgICAgICAgICAgICBib3JkZXJDb2xvcjogXCIjNTU1XCIsXG4gICAgICAgICAgICAgIHN0YXJ0aW5nX3RpbWU6IGRhdGEudGltZXN0YW1wc1tcIkNSRUFURURcIl0sXG4gICAgICAgICAgICAgIGVuZGluZ190aW1lOiBkYXRhLnRpbWVzdGFtcHNbXCJDUkVBVEVEXCJdICsgMSxcbiAgICAgICAgICAgICAgdHlwZTogJ3NjaGVkdWxlZCdcbiAgICAgICAgICAgIH1cbiAgICAgICAgICBdXG4gICAgICAgIH0pO1xuICAgICAgICBhbmd1bGFyLmZvckVhY2goZGF0YS52ZXJ0aWNlcywgZnVuY3Rpb24odmVydGV4KSB7XG4gICAgICAgICAgcmV0dXJuIHRlc3REYXRhLnB1c2goe1xuICAgICAgICAgICAgdGltZXM6IFtcbiAgICAgICAgICAgICAge1xuICAgICAgICAgICAgICAgIGxhYmVsOiB0cmFuc2xhdGVMYWJlbCh2ZXJ0ZXgubmFtZSksXG4gICAgICAgICAgICAgICAgY29sb3I6IFwiI2Q5ZjFmN1wiLFxuICAgICAgICAgICAgICAgIGJvcmRlckNvbG9yOiBcIiM2MmNkZWFcIixcbiAgICAgICAgICAgICAgICBzdGFydGluZ190aW1lOiB2ZXJ0ZXhbJ3N0YXJ0LXRpbWUnXSxcbiAgICAgICAgICAgICAgICBlbmRpbmdfdGltZTogdmVydGV4WydlbmQtdGltZSddLFxuICAgICAgICAgICAgICAgIGxpbms6IHZlcnRleC5pZCxcbiAgICAgICAgICAgICAgICB0eXBlOiAncmVndWxhcidcbiAgICAgICAgICAgICAgfVxuICAgICAgICAgICAgXVxuICAgICAgICAgIH0pO1xuICAgICAgICB9KTtcbiAgICAgICAgY2hhcnQgPSBkMy50aW1lbGluZSgpLnN0YWNrKCkuY2xpY2soZnVuY3Rpb24oZCwgaSwgZGF0dW0pIHtcbiAgICAgICAgICBpZiAoZC5saW5rKSB7XG4gICAgICAgICAgICByZXR1cm4gJHN0YXRlLmdvKFwic2luZ2xlLWpvYi50aW1lbGluZS52ZXJ0ZXhcIiwge1xuICAgICAgICAgICAgICBqb2JpZDogZGF0YS5qaWQsXG4gICAgICAgICAgICAgIHZlcnRleElkOiBkLmxpbmtcbiAgICAgICAgICAgIH0pO1xuICAgICAgICAgIH1cbiAgICAgICAgfSkudGlja0Zvcm1hdCh7XG4gICAgICAgICAgZm9ybWF0OiBkMy50aW1lLmZvcm1hdChcIiVMXCIpLFxuICAgICAgICAgIHRpY2tTaXplOiAxXG4gICAgICAgIH0pLnByZWZpeChcIm1haW5cIikubWFyZ2luKHtcbiAgICAgICAgICBsZWZ0OiAwLFxuICAgICAgICAgIHJpZ2h0OiAwLFxuICAgICAgICAgIHRvcDogMCxcbiAgICAgICAgICBib3R0b206IDBcbiAgICAgICAgfSkuaXRlbUhlaWdodCgzMCkuc2hvd0JvcmRlckxpbmUoKS5zaG93SG91clRpbWVsaW5lKCk7XG4gICAgICAgIHJldHVybiBzdmcgPSBkMy5zZWxlY3Qoc3ZnRWwpLmRhdHVtKHRlc3REYXRhKS5jYWxsKGNoYXJ0KTtcbiAgICAgIH07XG4gICAgICBzY29wZS4kd2F0Y2goYXR0cnMuam9iLCBmdW5jdGlvbihkYXRhKSB7XG4gICAgICAgIGlmIChkYXRhKSB7XG4gICAgICAgICAgcmV0dXJuIGFuYWx5emVUaW1lKGRhdGEpO1xuICAgICAgICB9XG4gICAgICB9KTtcbiAgICB9XG4gIH07XG59KS5kaXJlY3RpdmUoJ2pvYlBsYW4nLCBmdW5jdGlvbigkdGltZW91dCkge1xuICByZXR1cm4ge1xuICAgIHRlbXBsYXRlOiBcIjxzdmcgY2xhc3M9J2dyYXBoJyB3aWR0aD0nNTAwJyBoZWlnaHQ9JzQwMCc+PGcgLz48L3N2Zz4gPHN2ZyBjbGFzcz0ndG1wJyB3aWR0aD0nMScgaGVpZ2h0PScxJz48ZyAvPjwvc3ZnPiA8ZGl2IGNsYXNzPSdidG4tZ3JvdXAgem9vbS1idXR0b25zJz4gPGEgY2xhc3M9J2J0biBidG4tZGVmYXVsdCB6b29tLWluJyBuZy1jbGljaz0nem9vbUluKCknPjxpIGNsYXNzPSdmYSBmYS1wbHVzJyAvPjwvYT4gPGEgY2xhc3M9J2J0biBidG4tZGVmYXVsdCB6b29tLW91dCcgbmctY2xpY2s9J3pvb21PdXQoKSc+PGkgY2xhc3M9J2ZhIGZhLW1pbnVzJyAvPjwvYT4gPC9kaXY+XCIsXG4gICAgc2NvcGU6IHtcbiAgICAgIHBsYW46ICc9J1xuICAgIH0sXG4gICAgbGluazogZnVuY3Rpb24oc2NvcGUsIGVsZW0sIGF0dHJzKSB7XG4gICAgICB2YXIgY29udGFpbmVyVywgY3JlYXRlRWRnZSwgY3JlYXRlTGFiZWxFZGdlLCBjcmVhdGVMYWJlbE5vZGUsIGNyZWF0ZU5vZGUsIGQzbWFpblN2ZywgZDNtYWluU3ZnRywgZDN0bXBTdmcsIGRyYXdHcmFwaCwgZXh0ZW5kTGFiZWxOb2RlRm9ySXRlcmF0aW9uLCBnZXROb2RlVHlwZSwgaXNTcGVjaWFsSXRlcmF0aW9uTm9kZSwgam9iaWQsIGxvYWRKc29uVG9EYWdyZSwgbWFpbkcsIG1haW5TdmdFbGVtZW50LCBtYWluVG1wRWxlbWVudCwgbWFpblpvb20sIHNlYXJjaEZvck5vZGUsIHNob3J0ZW5TdHJpbmcsIHN1YmdyYXBocztcbiAgICAgIG1haW5ab29tID0gZDMuYmVoYXZpb3Iuem9vbSgpO1xuICAgICAgc3ViZ3JhcGhzID0gW107XG4gICAgICBqb2JpZCA9IGF0dHJzLmpvYmlkO1xuICAgICAgbWFpblN2Z0VsZW1lbnQgPSBlbGVtLmNoaWxkcmVuKClbMF07XG4gICAgICBtYWluRyA9IGVsZW0uY2hpbGRyZW4oKS5jaGlsZHJlbigpWzBdO1xuICAgICAgbWFpblRtcEVsZW1lbnQgPSBlbGVtLmNoaWxkcmVuKClbMV07XG4gICAgICBkM21haW5TdmcgPSBkMy5zZWxlY3QobWFpblN2Z0VsZW1lbnQpO1xuICAgICAgZDNtYWluU3ZnRyA9IGQzLnNlbGVjdChtYWluRyk7XG4gICAgICBkM3RtcFN2ZyA9IGQzLnNlbGVjdChtYWluVG1wRWxlbWVudCk7XG4gICAgICBjb250YWluZXJXID0gZWxlbS53aWR0aCgpO1xuICAgICAgYW5ndWxhci5lbGVtZW50KGVsZW0uY2hpbGRyZW4oKVswXSkud2lkdGgoY29udGFpbmVyVyk7XG4gICAgICBzY29wZS56b29tSW4gPSBmdW5jdGlvbigpIHtcbiAgICAgICAgdmFyIHRyYW5zbGF0ZSwgdjEsIHYyO1xuICAgICAgICBpZiAobWFpblpvb20uc2NhbGUoKSA8IDIuOTkpIHtcbiAgICAgICAgICB0cmFuc2xhdGUgPSBtYWluWm9vbS50cmFuc2xhdGUoKTtcbiAgICAgICAgICB2MSA9IHRyYW5zbGF0ZVswXSAqIChtYWluWm9vbS5zY2FsZSgpICsgMC4xIC8gKG1haW5ab29tLnNjYWxlKCkpKTtcbiAgICAgICAgICB2MiA9IHRyYW5zbGF0ZVsxXSAqIChtYWluWm9vbS5zY2FsZSgpICsgMC4xIC8gKG1haW5ab29tLnNjYWxlKCkpKTtcbiAgICAgICAgICBtYWluWm9vbS5zY2FsZShtYWluWm9vbS5zY2FsZSgpICsgMC4xKTtcbiAgICAgICAgICBtYWluWm9vbS50cmFuc2xhdGUoW3YxLCB2Ml0pO1xuICAgICAgICAgIHJldHVybiBkM21haW5TdmdHLmF0dHIoXCJ0cmFuc2Zvcm1cIiwgXCJ0cmFuc2xhdGUoXCIgKyB2MSArIFwiLFwiICsgdjIgKyBcIikgc2NhbGUoXCIgKyBtYWluWm9vbS5zY2FsZSgpICsgXCIpXCIpO1xuICAgICAgICB9XG4gICAgICB9O1xuICAgICAgc2NvcGUuem9vbU91dCA9IGZ1bmN0aW9uKCkge1xuICAgICAgICB2YXIgdHJhbnNsYXRlLCB2MSwgdjI7XG4gICAgICAgIGlmIChtYWluWm9vbS5zY2FsZSgpID4gMC4zMSkge1xuICAgICAgICAgIG1haW5ab29tLnNjYWxlKG1haW5ab29tLnNjYWxlKCkgLSAwLjEpO1xuICAgICAgICAgIHRyYW5zbGF0ZSA9IG1haW5ab29tLnRyYW5zbGF0ZSgpO1xuICAgICAgICAgIHYxID0gdHJhbnNsYXRlWzBdICogKG1haW5ab29tLnNjYWxlKCkgLSAwLjEgLyAobWFpblpvb20uc2NhbGUoKSkpO1xuICAgICAgICAgIHYyID0gdHJhbnNsYXRlWzFdICogKG1haW5ab29tLnNjYWxlKCkgLSAwLjEgLyAobWFpblpvb20uc2NhbGUoKSkpO1xuICAgICAgICAgIG1haW5ab29tLnRyYW5zbGF0ZShbdjEsIHYyXSk7XG4gICAgICAgICAgcmV0dXJuIGQzbWFpblN2Z0cuYXR0cihcInRyYW5zZm9ybVwiLCBcInRyYW5zbGF0ZShcIiArIHYxICsgXCIsXCIgKyB2MiArIFwiKSBzY2FsZShcIiArIG1haW5ab29tLnNjYWxlKCkgKyBcIilcIik7XG4gICAgICAgIH1cbiAgICAgIH07XG4gICAgICBjcmVhdGVMYWJlbEVkZ2UgPSBmdW5jdGlvbihlbCkge1xuICAgICAgICB2YXIgbGFiZWxWYWx1ZTtcbiAgICAgICAgbGFiZWxWYWx1ZSA9IFwiXCI7XG4gICAgICAgIGlmICgoZWwuc2hpcF9zdHJhdGVneSAhPSBudWxsKSB8fCAoZWwubG9jYWxfc3RyYXRlZ3kgIT0gbnVsbCkpIHtcbiAgICAgICAgICBsYWJlbFZhbHVlICs9IFwiPGRpdiBjbGFzcz0nZWRnZS1sYWJlbCc+XCI7XG4gICAgICAgICAgaWYgKGVsLnNoaXBfc3RyYXRlZ3kgIT0gbnVsbCkge1xuICAgICAgICAgICAgbGFiZWxWYWx1ZSArPSBlbC5zaGlwX3N0cmF0ZWd5O1xuICAgICAgICAgIH1cbiAgICAgICAgICBpZiAoZWwudGVtcF9tb2RlICE9PSB1bmRlZmluZWQpIHtcbiAgICAgICAgICAgIGxhYmVsVmFsdWUgKz0gXCIgKFwiICsgZWwudGVtcF9tb2RlICsgXCIpXCI7XG4gICAgICAgICAgfVxuICAgICAgICAgIGlmIChlbC5sb2NhbF9zdHJhdGVneSAhPT0gdW5kZWZpbmVkKSB7XG4gICAgICAgICAgICBsYWJlbFZhbHVlICs9IFwiLDxicj5cIiArIGVsLmxvY2FsX3N0cmF0ZWd5O1xuICAgICAgICAgIH1cbiAgICAgICAgICBsYWJlbFZhbHVlICs9IFwiPC9kaXY+XCI7XG4gICAgICAgIH1cbiAgICAgICAgcmV0dXJuIGxhYmVsVmFsdWU7XG4gICAgICB9O1xuICAgICAgaXNTcGVjaWFsSXRlcmF0aW9uTm9kZSA9IGZ1bmN0aW9uKGluZm8pIHtcbiAgICAgICAgcmV0dXJuIGluZm8gPT09IFwicGFydGlhbFNvbHV0aW9uXCIgfHwgaW5mbyA9PT0gXCJuZXh0UGFydGlhbFNvbHV0aW9uXCIgfHwgaW5mbyA9PT0gXCJ3b3Jrc2V0XCIgfHwgaW5mbyA9PT0gXCJuZXh0V29ya3NldFwiIHx8IGluZm8gPT09IFwic29sdXRpb25TZXRcIiB8fCBpbmZvID09PSBcInNvbHV0aW9uRGVsdGFcIjtcbiAgICAgIH07XG4gICAgICBnZXROb2RlVHlwZSA9IGZ1bmN0aW9uKGVsLCBpbmZvKSB7XG4gICAgICAgIGlmIChpbmZvID09PSBcIm1pcnJvclwiKSB7XG4gICAgICAgICAgcmV0dXJuICdub2RlLW1pcnJvcic7XG4gICAgICAgIH0gZWxzZSBpZiAoaXNTcGVjaWFsSXRlcmF0aW9uTm9kZShpbmZvKSkge1xuICAgICAgICAgIHJldHVybiAnbm9kZS1pdGVyYXRpb24nO1xuICAgICAgICB9IGVsc2Uge1xuICAgICAgICAgIHJldHVybiAnbm9kZS1ub3JtYWwnO1xuICAgICAgICB9XG4gICAgICB9O1xuICAgICAgY3JlYXRlTGFiZWxOb2RlID0gZnVuY3Rpb24oZWwsIGluZm8sIG1heFcsIG1heEgpIHtcbiAgICAgICAgdmFyIGxhYmVsVmFsdWUsIHN0ZXBOYW1lO1xuICAgICAgICBsYWJlbFZhbHVlID0gXCI8YSBocmVmPScjL2pvYnMvXCIgKyBqb2JpZCArIFwiL3ZlcnRleC9cIiArIGVsLmlkICsgXCInIGNsYXNzPSdub2RlLWxhYmVsIFwiICsgZ2V0Tm9kZVR5cGUoZWwsIGluZm8pICsgXCInPlwiO1xuICAgICAgICBpZiAoaW5mbyA9PT0gXCJtaXJyb3JcIikge1xuICAgICAgICAgIGxhYmVsVmFsdWUgKz0gXCI8aDMgY2xhc3M9J25vZGUtbmFtZSc+TWlycm9yIG9mIFwiICsgZWwub3BlcmF0b3IgKyBcIjwvaDM+XCI7XG4gICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgbGFiZWxWYWx1ZSArPSBcIjxoMyBjbGFzcz0nbm9kZS1uYW1lJz5cIiArIGVsLm9wZXJhdG9yICsgXCI8L2gzPlwiO1xuICAgICAgICB9XG4gICAgICAgIGlmIChlbC5kZXNjcmlwdGlvbiA9PT0gXCJcIikge1xuICAgICAgICAgIGxhYmVsVmFsdWUgKz0gXCJcIjtcbiAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICBzdGVwTmFtZSA9IGVsLmRlc2NyaXB0aW9uO1xuICAgICAgICAgIHN0ZXBOYW1lID0gc2hvcnRlblN0cmluZyhzdGVwTmFtZSk7XG4gICAgICAgICAgbGFiZWxWYWx1ZSArPSBcIjxoNCBjbGFzcz0nc3RlcC1uYW1lJz5cIiArIHN0ZXBOYW1lICsgXCI8L2g0PlwiO1xuICAgICAgICB9XG4gICAgICAgIGlmIChlbC5zdGVwX2Z1bmN0aW9uICE9IG51bGwpIHtcbiAgICAgICAgICBsYWJlbFZhbHVlICs9IGV4dGVuZExhYmVsTm9kZUZvckl0ZXJhdGlvbihlbC5pZCwgbWF4VywgbWF4SCk7XG4gICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgaWYgKGlzU3BlY2lhbEl0ZXJhdGlvbk5vZGUoaW5mbykpIHtcbiAgICAgICAgICAgIGxhYmVsVmFsdWUgKz0gXCI8aDU+XCIgKyBpbmZvICsgXCIgTm9kZTwvaDU+XCI7XG4gICAgICAgICAgfVxuICAgICAgICAgIGlmIChlbC5wYXJhbGxlbGlzbSAhPT0gXCJcIikge1xuICAgICAgICAgICAgbGFiZWxWYWx1ZSArPSBcIjxoNT5QYXJhbGxlbGlzbTogXCIgKyBlbC5wYXJhbGxlbGlzbSArIFwiPC9oNT5cIjtcbiAgICAgICAgICB9XG4gICAgICAgICAgaWYgKGVsLm9wZXJhdG9yICE9PSB1bmRlZmluZWQpIHtcbiAgICAgICAgICAgIGxhYmVsVmFsdWUgKz0gXCI8aDU+T3BlcmF0aW9uOiBcIiArIHNob3J0ZW5TdHJpbmcoZWwub3BlcmF0b3Jfc3RyYXRlZ3kpICsgXCI8L2g1PlwiO1xuICAgICAgICAgIH1cbiAgICAgICAgfVxuICAgICAgICBsYWJlbFZhbHVlICs9IFwiPC9hPlwiO1xuICAgICAgICByZXR1cm4gbGFiZWxWYWx1ZTtcbiAgICAgIH07XG4gICAgICBleHRlbmRMYWJlbE5vZGVGb3JJdGVyYXRpb24gPSBmdW5jdGlvbihpZCwgbWF4VywgbWF4SCkge1xuICAgICAgICB2YXIgbGFiZWxWYWx1ZSwgc3ZnSUQ7XG4gICAgICAgIHN2Z0lEID0gXCJzdmctXCIgKyBpZDtcbiAgICAgICAgbGFiZWxWYWx1ZSA9IFwiPHN2ZyBjbGFzcz0nXCIgKyBzdmdJRCArIFwiJyB3aWR0aD1cIiArIG1heFcgKyBcIiBoZWlnaHQ9XCIgKyBtYXhIICsgXCI+PGcgLz48L3N2Zz5cIjtcbiAgICAgICAgcmV0dXJuIGxhYmVsVmFsdWU7XG4gICAgICB9O1xuICAgICAgc2hvcnRlblN0cmluZyA9IGZ1bmN0aW9uKHMpIHtcbiAgICAgICAgdmFyIHNicjtcbiAgICAgICAgaWYgKHMuY2hhckF0KDApID09PSBcIjxcIikge1xuICAgICAgICAgIHMgPSBzLnJlcGxhY2UoXCI8XCIsIFwiJmx0O1wiKTtcbiAgICAgICAgICBzID0gcy5yZXBsYWNlKFwiPlwiLCBcIiZndDtcIik7XG4gICAgICAgIH1cbiAgICAgICAgc2JyID0gXCJcIjtcbiAgICAgICAgd2hpbGUgKHMubGVuZ3RoID4gMzApIHtcbiAgICAgICAgICBzYnIgPSBzYnIgKyBzLnN1YnN0cmluZygwLCAzMCkgKyBcIjxicj5cIjtcbiAgICAgICAgICBzID0gcy5zdWJzdHJpbmcoMzAsIHMubGVuZ3RoKTtcbiAgICAgICAgfVxuICAgICAgICBzYnIgPSBzYnIgKyBzO1xuICAgICAgICByZXR1cm4gc2JyO1xuICAgICAgfTtcbiAgICAgIGNyZWF0ZU5vZGUgPSBmdW5jdGlvbihnLCBkYXRhLCBlbCwgaXNQYXJlbnQsIG1heFcsIG1heEgpIHtcbiAgICAgICAgaWYgKGlzUGFyZW50ID09IG51bGwpIHtcbiAgICAgICAgICBpc1BhcmVudCA9IGZhbHNlO1xuICAgICAgICB9XG4gICAgICAgIGlmIChlbC5pZCA9PT0gZGF0YS5wYXJ0aWFsX3NvbHV0aW9uKSB7XG4gICAgICAgICAgcmV0dXJuIGcuc2V0Tm9kZShlbC5pZCwge1xuICAgICAgICAgICAgbGFiZWw6IGNyZWF0ZUxhYmVsTm9kZShlbCwgXCJwYXJ0aWFsU29sdXRpb25cIiwgbWF4VywgbWF4SCksXG4gICAgICAgICAgICBsYWJlbFR5cGU6ICdodG1sJyxcbiAgICAgICAgICAgIFwiY2xhc3NcIjogZ2V0Tm9kZVR5cGUoZWwsIFwicGFydGlhbFNvbHV0aW9uXCIpXG4gICAgICAgICAgfSk7XG4gICAgICAgIH0gZWxzZSBpZiAoZWwuaWQgPT09IGRhdGEubmV4dF9wYXJ0aWFsX3NvbHV0aW9uKSB7XG4gICAgICAgICAgcmV0dXJuIGcuc2V0Tm9kZShlbC5pZCwge1xuICAgICAgICAgICAgbGFiZWw6IGNyZWF0ZUxhYmVsTm9kZShlbCwgXCJuZXh0UGFydGlhbFNvbHV0aW9uXCIsIG1heFcsIG1heEgpLFxuICAgICAgICAgICAgbGFiZWxUeXBlOiAnaHRtbCcsXG4gICAgICAgICAgICBcImNsYXNzXCI6IGdldE5vZGVUeXBlKGVsLCBcIm5leHRQYXJ0aWFsU29sdXRpb25cIilcbiAgICAgICAgICB9KTtcbiAgICAgICAgfSBlbHNlIGlmIChlbC5pZCA9PT0gZGF0YS53b3Jrc2V0KSB7XG4gICAgICAgICAgcmV0dXJuIGcuc2V0Tm9kZShlbC5pZCwge1xuICAgICAgICAgICAgbGFiZWw6IGNyZWF0ZUxhYmVsTm9kZShlbCwgXCJ3b3Jrc2V0XCIsIG1heFcsIG1heEgpLFxuICAgICAgICAgICAgbGFiZWxUeXBlOiAnaHRtbCcsXG4gICAgICAgICAgICBcImNsYXNzXCI6IGdldE5vZGVUeXBlKGVsLCBcIndvcmtzZXRcIilcbiAgICAgICAgICB9KTtcbiAgICAgICAgfSBlbHNlIGlmIChlbC5pZCA9PT0gZGF0YS5uZXh0X3dvcmtzZXQpIHtcbiAgICAgICAgICByZXR1cm4gZy5zZXROb2RlKGVsLmlkLCB7XG4gICAgICAgICAgICBsYWJlbDogY3JlYXRlTGFiZWxOb2RlKGVsLCBcIm5leHRXb3Jrc2V0XCIsIG1heFcsIG1heEgpLFxuICAgICAgICAgICAgbGFiZWxUeXBlOiAnaHRtbCcsXG4gICAgICAgICAgICBcImNsYXNzXCI6IGdldE5vZGVUeXBlKGVsLCBcIm5leHRXb3Jrc2V0XCIpXG4gICAgICAgICAgfSk7XG4gICAgICAgIH0gZWxzZSBpZiAoZWwuaWQgPT09IGRhdGEuc29sdXRpb25fc2V0KSB7XG4gICAgICAgICAgcmV0dXJuIGcuc2V0Tm9kZShlbC5pZCwge1xuICAgICAgICAgICAgbGFiZWw6IGNyZWF0ZUxhYmVsTm9kZShlbCwgXCJzb2x1dGlvblNldFwiLCBtYXhXLCBtYXhIKSxcbiAgICAgICAgICAgIGxhYmVsVHlwZTogJ2h0bWwnLFxuICAgICAgICAgICAgXCJjbGFzc1wiOiBnZXROb2RlVHlwZShlbCwgXCJzb2x1dGlvblNldFwiKVxuICAgICAgICAgIH0pO1xuICAgICAgICB9IGVsc2UgaWYgKGVsLmlkID09PSBkYXRhLnNvbHV0aW9uX2RlbHRhKSB7XG4gICAgICAgICAgcmV0dXJuIGcuc2V0Tm9kZShlbC5pZCwge1xuICAgICAgICAgICAgbGFiZWw6IGNyZWF0ZUxhYmVsTm9kZShlbCwgXCJzb2x1dGlvbkRlbHRhXCIsIG1heFcsIG1heEgpLFxuICAgICAgICAgICAgbGFiZWxUeXBlOiAnaHRtbCcsXG4gICAgICAgICAgICBcImNsYXNzXCI6IGdldE5vZGVUeXBlKGVsLCBcInNvbHV0aW9uRGVsdGFcIilcbiAgICAgICAgICB9KTtcbiAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICByZXR1cm4gZy5zZXROb2RlKGVsLmlkLCB7XG4gICAgICAgICAgICBsYWJlbDogY3JlYXRlTGFiZWxOb2RlKGVsLCBcIlwiLCBtYXhXLCBtYXhIKSxcbiAgICAgICAgICAgIGxhYmVsVHlwZTogJ2h0bWwnLFxuICAgICAgICAgICAgXCJjbGFzc1wiOiBnZXROb2RlVHlwZShlbCwgXCJcIilcbiAgICAgICAgICB9KTtcbiAgICAgICAgfVxuICAgICAgfTtcbiAgICAgIGNyZWF0ZUVkZ2UgPSBmdW5jdGlvbihnLCBkYXRhLCBlbCwgZXhpc3RpbmdOb2RlcywgcHJlZCkge1xuICAgICAgICB2YXIgbWlzc2luZ05vZGU7XG4gICAgICAgIGlmIChleGlzdGluZ05vZGVzLmluZGV4T2YocHJlZC5pZCkgIT09IC0xKSB7XG4gICAgICAgICAgcmV0dXJuIGcuc2V0RWRnZShwcmVkLmlkLCBlbC5pZCwge1xuICAgICAgICAgICAgbGFiZWw6IGNyZWF0ZUxhYmVsRWRnZShwcmVkKSxcbiAgICAgICAgICAgIGxhYmVsVHlwZTogJ2h0bWwnLFxuICAgICAgICAgICAgYXJyb3doZWFkOiAnbm9ybWFsJ1xuICAgICAgICAgIH0pO1xuICAgICAgICB9IGVsc2Uge1xuICAgICAgICAgIG1pc3NpbmdOb2RlID0gc2VhcmNoRm9yTm9kZShkYXRhLCBwcmVkLmlkKTtcbiAgICAgICAgICBpZiAoISghbWlzc2luZ05vZGUgfHwgbWlzc2luZ05vZGUuYWxyZWFkeUFkZGVkID09PSB0cnVlKSkge1xuICAgICAgICAgICAgbWlzc2luZ05vZGUuYWxyZWFkeUFkZGVkID0gdHJ1ZTtcbiAgICAgICAgICAgIGcuc2V0Tm9kZShtaXNzaW5nTm9kZS5pZCwge1xuICAgICAgICAgICAgICBsYWJlbDogY3JlYXRlTGFiZWxOb2RlKG1pc3NpbmdOb2RlLCBcIm1pcnJvclwiKSxcbiAgICAgICAgICAgICAgbGFiZWxUeXBlOiAnaHRtbCcsXG4gICAgICAgICAgICAgIFwiY2xhc3NcIjogZ2V0Tm9kZVR5cGUobWlzc2luZ05vZGUsICdtaXJyb3InKVxuICAgICAgICAgICAgfSk7XG4gICAgICAgICAgICByZXR1cm4gZy5zZXRFZGdlKG1pc3NpbmdOb2RlLmlkLCBlbC5pZCwge1xuICAgICAgICAgICAgICBsYWJlbDogY3JlYXRlTGFiZWxFZGdlKG1pc3NpbmdOb2RlKSxcbiAgICAgICAgICAgICAgbGFiZWxUeXBlOiAnaHRtbCdcbiAgICAgICAgICAgIH0pO1xuICAgICAgICAgIH1cbiAgICAgICAgfVxuICAgICAgfTtcbiAgICAgIGxvYWRKc29uVG9EYWdyZSA9IGZ1bmN0aW9uKGcsIGRhdGEpIHtcbiAgICAgICAgdmFyIGVsLCBleGlzdGluZ05vZGVzLCBpc1BhcmVudCwgaywgbCwgbGVuLCBsZW4xLCBtYXhILCBtYXhXLCBwcmVkLCByLCByZWYsIHNnLCB0b0l0ZXJhdGU7XG4gICAgICAgIGV4aXN0aW5nTm9kZXMgPSBbXTtcbiAgICAgICAgaWYgKGRhdGEubm9kZXMgIT0gbnVsbCkge1xuICAgICAgICAgIHRvSXRlcmF0ZSA9IGRhdGEubm9kZXM7XG4gICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgdG9JdGVyYXRlID0gZGF0YS5zdGVwX2Z1bmN0aW9uO1xuICAgICAgICAgIGlzUGFyZW50ID0gdHJ1ZTtcbiAgICAgICAgfVxuICAgICAgICBmb3IgKGsgPSAwLCBsZW4gPSB0b0l0ZXJhdGUubGVuZ3RoOyBrIDwgbGVuOyBrKyspIHtcbiAgICAgICAgICBlbCA9IHRvSXRlcmF0ZVtrXTtcbiAgICAgICAgICBtYXhXID0gMDtcbiAgICAgICAgICBtYXhIID0gMDtcbiAgICAgICAgICBpZiAoZWwuc3RlcF9mdW5jdGlvbikge1xuICAgICAgICAgICAgc2cgPSBuZXcgZGFncmVEMy5ncmFwaGxpYi5HcmFwaCh7XG4gICAgICAgICAgICAgIG11bHRpZ3JhcGg6IHRydWUsXG4gICAgICAgICAgICAgIGNvbXBvdW5kOiB0cnVlXG4gICAgICAgICAgICB9KS5zZXRHcmFwaCh7XG4gICAgICAgICAgICAgIG5vZGVzZXA6IDIwLFxuICAgICAgICAgICAgICBlZGdlc2VwOiAwLFxuICAgICAgICAgICAgICByYW5rc2VwOiAyMCxcbiAgICAgICAgICAgICAgcmFua2RpcjogXCJMUlwiLFxuICAgICAgICAgICAgICBtYXJnaW54OiAxMCxcbiAgICAgICAgICAgICAgbWFyZ2lueTogMTBcbiAgICAgICAgICAgIH0pO1xuICAgICAgICAgICAgc3ViZ3JhcGhzW2VsLmlkXSA9IHNnO1xuICAgICAgICAgICAgbG9hZEpzb25Ub0RhZ3JlKHNnLCBlbCk7XG4gICAgICAgICAgICByID0gbmV3IGRhZ3JlRDMucmVuZGVyKCk7XG4gICAgICAgICAgICBkM3RtcFN2Zy5zZWxlY3QoJ2cnKS5jYWxsKHIsIHNnKTtcbiAgICAgICAgICAgIG1heFcgPSBzZy5ncmFwaCgpLndpZHRoO1xuICAgICAgICAgICAgbWF4SCA9IHNnLmdyYXBoKCkuaGVpZ2h0O1xuICAgICAgICAgICAgYW5ndWxhci5lbGVtZW50KG1haW5UbXBFbGVtZW50KS5lbXB0eSgpO1xuICAgICAgICAgIH1cbiAgICAgICAgICBjcmVhdGVOb2RlKGcsIGRhdGEsIGVsLCBpc1BhcmVudCwgbWF4VywgbWF4SCk7XG4gICAgICAgICAgZXhpc3RpbmdOb2Rlcy5wdXNoKGVsLmlkKTtcbiAgICAgICAgICBpZiAoZWwuaW5wdXRzICE9IG51bGwpIHtcbiAgICAgICAgICAgIHJlZiA9IGVsLmlucHV0cztcbiAgICAgICAgICAgIGZvciAobCA9IDAsIGxlbjEgPSByZWYubGVuZ3RoOyBsIDwgbGVuMTsgbCsrKSB7XG4gICAgICAgICAgICAgIHByZWQgPSByZWZbbF07XG4gICAgICAgICAgICAgIGNyZWF0ZUVkZ2UoZywgZGF0YSwgZWwsIGV4aXN0aW5nTm9kZXMsIHByZWQpO1xuICAgICAgICAgICAgfVxuICAgICAgICAgIH1cbiAgICAgICAgfVxuICAgICAgICByZXR1cm4gZztcbiAgICAgIH07XG4gICAgICBzZWFyY2hGb3JOb2RlID0gZnVuY3Rpb24oZGF0YSwgbm9kZUlEKSB7XG4gICAgICAgIHZhciBlbCwgaSwgajtcbiAgICAgICAgZm9yIChpIGluIGRhdGEubm9kZXMpIHtcbiAgICAgICAgICBlbCA9IGRhdGEubm9kZXNbaV07XG4gICAgICAgICAgaWYgKGVsLmlkID09PSBub2RlSUQpIHtcbiAgICAgICAgICAgIHJldHVybiBlbDtcbiAgICAgICAgICB9XG4gICAgICAgICAgaWYgKGVsLnN0ZXBfZnVuY3Rpb24gIT0gbnVsbCkge1xuICAgICAgICAgICAgZm9yIChqIGluIGVsLnN0ZXBfZnVuY3Rpb24pIHtcbiAgICAgICAgICAgICAgaWYgKGVsLnN0ZXBfZnVuY3Rpb25bal0uaWQgPT09IG5vZGVJRCkge1xuICAgICAgICAgICAgICAgIHJldHVybiBlbC5zdGVwX2Z1bmN0aW9uW2pdO1xuICAgICAgICAgICAgICB9XG4gICAgICAgICAgICB9XG4gICAgICAgICAgfVxuICAgICAgICB9XG4gICAgICB9O1xuICAgICAgZHJhd0dyYXBoID0gZnVuY3Rpb24oZGF0YSkge1xuICAgICAgICB2YXIgZywgaSwgbmV3U2NhbGUsIHJlbmRlcmVyLCBzZywgeENlbnRlck9mZnNldCwgeUNlbnRlck9mZnNldDtcbiAgICAgICAgZyA9IG5ldyBkYWdyZUQzLmdyYXBobGliLkdyYXBoKHtcbiAgICAgICAgICBtdWx0aWdyYXBoOiB0cnVlLFxuICAgICAgICAgIGNvbXBvdW5kOiB0cnVlXG4gICAgICAgIH0pLnNldEdyYXBoKHtcbiAgICAgICAgICBub2Rlc2VwOiA3MCxcbiAgICAgICAgICBlZGdlc2VwOiAwLFxuICAgICAgICAgIHJhbmtzZXA6IDUwLFxuICAgICAgICAgIHJhbmtkaXI6IFwiTFJcIixcbiAgICAgICAgICBtYXJnaW54OiA0MCxcbiAgICAgICAgICBtYXJnaW55OiA0MFxuICAgICAgICB9KTtcbiAgICAgICAgbG9hZEpzb25Ub0RhZ3JlKGcsIGRhdGEpO1xuICAgICAgICByZW5kZXJlciA9IG5ldyBkYWdyZUQzLnJlbmRlcigpO1xuICAgICAgICBkM21haW5TdmdHLmNhbGwocmVuZGVyZXIsIGcpO1xuICAgICAgICBmb3IgKGkgaW4gc3ViZ3JhcGhzKSB7XG4gICAgICAgICAgc2cgPSBzdWJncmFwaHNbaV07XG4gICAgICAgICAgZDNtYWluU3ZnLnNlbGVjdCgnc3ZnLnN2Zy0nICsgaSArICcgZycpLmNhbGwocmVuZGVyZXIsIHNnKTtcbiAgICAgICAgfVxuICAgICAgICBuZXdTY2FsZSA9IDAuNTtcbiAgICAgICAgeENlbnRlck9mZnNldCA9IE1hdGguZmxvb3IoKGFuZ3VsYXIuZWxlbWVudChtYWluU3ZnRWxlbWVudCkud2lkdGgoKSAtIGcuZ3JhcGgoKS53aWR0aCAqIG5ld1NjYWxlKSAvIDIpO1xuICAgICAgICB5Q2VudGVyT2Zmc2V0ID0gTWF0aC5mbG9vcigoYW5ndWxhci5lbGVtZW50KG1haW5TdmdFbGVtZW50KS5oZWlnaHQoKSAtIGcuZ3JhcGgoKS5oZWlnaHQgKiBuZXdTY2FsZSkgLyAyKTtcbiAgICAgICAgbWFpblpvb20uc2NhbGUobmV3U2NhbGUpLnRyYW5zbGF0ZShbeENlbnRlck9mZnNldCwgeUNlbnRlck9mZnNldF0pO1xuICAgICAgICBkM21haW5TdmdHLmF0dHIoXCJ0cmFuc2Zvcm1cIiwgXCJ0cmFuc2xhdGUoXCIgKyB4Q2VudGVyT2Zmc2V0ICsgXCIsIFwiICsgeUNlbnRlck9mZnNldCArIFwiKSBzY2FsZShcIiArIG1haW5ab29tLnNjYWxlKCkgKyBcIilcIik7XG4gICAgICAgIG1haW5ab29tLm9uKFwiem9vbVwiLCBmdW5jdGlvbigpIHtcbiAgICAgICAgICB2YXIgZXY7XG4gICAgICAgICAgZXYgPSBkMy5ldmVudDtcbiAgICAgICAgICByZXR1cm4gZDNtYWluU3ZnRy5hdHRyKFwidHJhbnNmb3JtXCIsIFwidHJhbnNsYXRlKFwiICsgZXYudHJhbnNsYXRlICsgXCIpIHNjYWxlKFwiICsgZXYuc2NhbGUgKyBcIilcIik7XG4gICAgICAgIH0pO1xuICAgICAgICByZXR1cm4gbWFpblpvb20oZDNtYWluU3ZnKTtcbiAgICAgIH07XG4gICAgICBzY29wZS4kd2F0Y2goYXR0cnMucGxhbiwgZnVuY3Rpb24obmV3UGxhbikge1xuICAgICAgICBpZiAobmV3UGxhbikge1xuICAgICAgICAgIHJldHVybiBkcmF3R3JhcGgobmV3UGxhbik7XG4gICAgICAgIH1cbiAgICAgIH0pO1xuICAgIH1cbiAgfTtcbn0pO1xuIiwiI1xuIyBMaWNlbnNlZCB0byB0aGUgQXBhY2hlIFNvZnR3YXJlIEZvdW5kYXRpb24gKEFTRikgdW5kZXIgb25lXG4jIG9yIG1vcmUgY29udHJpYnV0b3IgbGljZW5zZSBhZ3JlZW1lbnRzLiAgU2VlIHRoZSBOT1RJQ0UgZmlsZVxuIyBkaXN0cmlidXRlZCB3aXRoIHRoaXMgd29yayBmb3IgYWRkaXRpb25hbCBpbmZvcm1hdGlvblxuIyByZWdhcmRpbmcgY29weXJpZ2h0IG93bmVyc2hpcC4gIFRoZSBBU0YgbGljZW5zZXMgdGhpcyBmaWxlXG4jIHRvIHlvdSB1bmRlciB0aGUgQXBhY2hlIExpY2Vuc2UsIFZlcnNpb24gMi4wICh0aGVcbiMgXCJMaWNlbnNlXCIpOyB5b3UgbWF5IG5vdCB1c2UgdGhpcyBmaWxlIGV4Y2VwdCBpbiBjb21wbGlhbmNlXG4jIHdpdGggdGhlIExpY2Vuc2UuICBZb3UgbWF5IG9idGFpbiBhIGNvcHkgb2YgdGhlIExpY2Vuc2UgYXRcbiNcbiMgICAgIGh0dHA6Ly93d3cuYXBhY2hlLm9yZy9saWNlbnNlcy9MSUNFTlNFLTIuMFxuI1xuIyBVbmxlc3MgcmVxdWlyZWQgYnkgYXBwbGljYWJsZSBsYXcgb3IgYWdyZWVkIHRvIGluIHdyaXRpbmcsIHNvZnR3YXJlXG4jIGRpc3RyaWJ1dGVkIHVuZGVyIHRoZSBMaWNlbnNlIGlzIGRpc3RyaWJ1dGVkIG9uIGFuIFwiQVMgSVNcIiBCQVNJUyxcbiMgV0lUSE9VVCBXQVJSQU5USUVTIE9SIENPTkRJVElPTlMgT0YgQU5ZIEtJTkQsIGVpdGhlciBleHByZXNzIG9yIGltcGxpZWQuXG4jIFNlZSB0aGUgTGljZW5zZSBmb3IgdGhlIHNwZWNpZmljIGxhbmd1YWdlIGdvdmVybmluZyBwZXJtaXNzaW9ucyBhbmRcbiMgbGltaXRhdGlvbnMgdW5kZXIgdGhlIExpY2Vuc2UuXG4jXG5cbmFuZ3VsYXIubW9kdWxlKCdmbGlua0FwcCcpXG5cbi5zZXJ2aWNlICdKb2JzU2VydmljZScsICgkaHR0cCwgZmxpbmtDb25maWcsICRsb2csIGFtTW9tZW50LCAkcSwgJHRpbWVvdXQpIC0+XG4gIGN1cnJlbnRKb2IgPSBudWxsXG4gIGN1cnJlbnRQbGFuID0gbnVsbFxuICBkZWZlcnJlZHMgPSB7fVxuICBqb2JzID0ge1xuICAgIHJ1bm5pbmc6IFtdXG4gICAgZmluaXNoZWQ6IFtdXG4gICAgY2FuY2VsbGVkOiBbXVxuICAgIGZhaWxlZDogW11cbiAgfVxuXG4gIGpvYk9ic2VydmVycyA9IFtdXG5cbiAgbm90aWZ5T2JzZXJ2ZXJzID0gLT5cbiAgICBhbmd1bGFyLmZvckVhY2ggam9iT2JzZXJ2ZXJzLCAoY2FsbGJhY2spIC0+XG4gICAgICBjYWxsYmFjaygpXG5cbiAgQHJlZ2lzdGVyT2JzZXJ2ZXIgPSAoY2FsbGJhY2spIC0+XG4gICAgam9iT2JzZXJ2ZXJzLnB1c2goY2FsbGJhY2spXG5cbiAgQHVuUmVnaXN0ZXJPYnNlcnZlciA9IChjYWxsYmFjaykgLT5cbiAgICBpbmRleCA9IGpvYk9ic2VydmVycy5pbmRleE9mKGNhbGxiYWNrKVxuICAgIGpvYk9ic2VydmVycy5zcGxpY2UoaW5kZXgsIDEpXG5cbiAgQHN0YXRlTGlzdCA9IC0+XG4gICAgWyBcbiAgICAgICMgJ0NSRUFURUQnXG4gICAgICAnU0NIRURVTEVEJ1xuICAgICAgJ0RFUExPWUlORydcbiAgICAgICdSVU5OSU5HJ1xuICAgICAgJ0ZJTklTSEVEJ1xuICAgICAgJ0ZBSUxFRCdcbiAgICAgICdDQU5DRUxJTkcnXG4gICAgICAnQ0FOQ0VMRUQnXG4gICAgXVxuXG4gIEB0cmFuc2xhdGVMYWJlbFN0YXRlID0gKHN0YXRlKSAtPlxuICAgIHN3aXRjaCBzdGF0ZS50b0xvd2VyQ2FzZSgpXG4gICAgICB3aGVuICdmaW5pc2hlZCcgdGhlbiAnc3VjY2VzcydcbiAgICAgIHdoZW4gJ2ZhaWxlZCcgdGhlbiAnZGFuZ2VyJ1xuICAgICAgd2hlbiAnc2NoZWR1bGVkJyB0aGVuICdkZWZhdWx0J1xuICAgICAgd2hlbiAnZGVwbG95aW5nJyB0aGVuICdpbmZvJ1xuICAgICAgd2hlbiAncnVubmluZycgdGhlbiAncHJpbWFyeSdcbiAgICAgIHdoZW4gJ2NhbmNlbGluZycgdGhlbiAnd2FybmluZydcbiAgICAgIHdoZW4gJ3BlbmRpbmcnIHRoZW4gJ2luZm8nXG4gICAgICB3aGVuICd0b3RhbCcgdGhlbiAnYmxhY2snXG4gICAgICBlbHNlICdkZWZhdWx0J1xuXG4gIEBsaXN0Sm9icyA9IC0+XG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpXG5cbiAgICAkaHR0cC5nZXQgZmxpbmtDb25maWcuam9iU2VydmVyICsgXCIvam9ib3ZlcnZpZXdcIlxuICAgIC5zdWNjZXNzIChkYXRhLCBzdGF0dXMsIGhlYWRlcnMsIGNvbmZpZykgLT5cblxuICAgICAgYW5ndWxhci5mb3JFYWNoIGRhdGEsIChsaXN0LCBsaXN0S2V5KSAtPlxuXG4gICAgICAgIHN3aXRjaCBsaXN0S2V5XG4gICAgICAgICAgd2hlbiAncnVubmluZycgdGhlbiBqb2JzLnJ1bm5pbmcgPSBsaXN0XG4gICAgICAgICAgd2hlbiAnZmluaXNoZWQnIHRoZW4gam9icy5maW5pc2hlZCA9IGxpc3RcbiAgICAgICAgICB3aGVuICdjYW5jZWxsZWQnIHRoZW4gam9icy5jYW5jZWxsZWQgPSBsaXN0XG4gICAgICAgICAgd2hlbiAnZmFpbGVkJyB0aGVuIGpvYnMuZmFpbGVkID0gbGlzdFxuXG4gICAgICBkZWZlcnJlZC5yZXNvbHZlKGpvYnMpXG4gICAgICBub3RpZnlPYnNlcnZlcnMoKVxuXG4gICAgZGVmZXJyZWQucHJvbWlzZVxuXG4gIEBnZXRKb2JzID0gKHR5cGUpIC0+XG4gICAgam9ic1t0eXBlXVxuXG4gIEBnZXRBbGxKb2JzID0gLT5cbiAgICBqb2JzXG5cbiAgQGxvYWRKb2IgPSAoam9iaWQpIC0+XG4gICAgY3VycmVudEpvYiA9IG51bGxcbiAgICBkZWZlcnJlZHMuam9iID0gJHEuZGVmZXIoKVxuXG4gICAgJGh0dHAuZ2V0IGZsaW5rQ29uZmlnLmpvYlNlcnZlciArIFwiL2pvYnMvXCIgKyBqb2JpZFxuICAgIC5zdWNjZXNzIChkYXRhLCBzdGF0dXMsIGhlYWRlcnMsIGNvbmZpZykgLT5cbiAgICAgICMgZGF0YS50aW1lID0gRGF0ZS5ub3coKVxuXG4gICAgICAkaHR0cC5nZXQgZmxpbmtDb25maWcuam9iU2VydmVyICsgXCIvam9icy9cIiArIGpvYmlkICsgXCIvdmVydGljZXNcIlxuICAgICAgLnN1Y2Nlc3MgKHZlcnRpY2VzKSAtPlxuICAgICAgICBkYXRhID0gYW5ndWxhci5leHRlbmQoZGF0YSwgdmVydGljZXMpXG5cbiAgICAgICAgIyAkaHR0cC5nZXQgZmxpbmtDb25maWcuam9iU2VydmVyICsgXCIvam9ic0luZm8/Z2V0PWpvYiZqb2I9XCIgKyBqb2JpZFxuICAgICAgICAjIC5zdWNjZXNzIChvbGRWZXJ0aWNlcykgLT5cbiAgICAgICAgIyAgIGRhdGEub2xkViA9IG9sZFZlcnRpY2VzWzBdXG5cbiAgICAgICAgJGh0dHAuZ2V0IGZsaW5rQ29uZmlnLmpvYlNlcnZlciArIFwiL2pvYnMvXCIgKyBqb2JpZCArIFwiL2NvbmZpZ1wiXG4gICAgICAgIC5zdWNjZXNzIChqb2JDb25maWcpIC0+XG4gICAgICAgICAgZGF0YSA9IGFuZ3VsYXIuZXh0ZW5kKGRhdGEsIGpvYkNvbmZpZylcblxuICAgICAgICAgIGN1cnJlbnRKb2IgPSBkYXRhXG5cbiAgICAgICAgICBkZWZlcnJlZHMuam9iLnJlc29sdmUoZGF0YSlcblxuICAgIGRlZmVycmVkcy5qb2IucHJvbWlzZVxuXG4gIEBsb2FkUGxhbiA9IChqb2JpZCkgLT5cbiAgICBjdXJyZW50UGxhbiA9IG51bGxcbiAgICBkZWZlcnJlZHMucGxhbiA9ICRxLmRlZmVyKClcblxuICAgICRodHRwLmdldCBmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcIi9qb2JzL1wiICsgam9iaWQgKyBcIi9wbGFuXCJcbiAgICAuc3VjY2VzcyAoZGF0YSkgLT5cbiAgICAgIGN1cnJlbnRQbGFuID0gZGF0YVxuXG4gICAgICBkZWZlcnJlZHMucGxhbi5yZXNvbHZlKGRhdGEpXG5cbiAgICBkZWZlcnJlZHMucGxhbi5wcm9taXNlXG5cbiAgQGdldE5vZGUgPSAobm9kZWlkKSAtPlxuICAgIHNlZWtOb2RlID0gKG5vZGVpZCwgZGF0YSkgLT5cbiAgICAgIGZvciBub2RlIGluIGRhdGFcbiAgICAgICAgcmV0dXJuIG5vZGUgaWYgbm9kZS5pZCBpcyBub2RlaWRcbiAgICAgICAgc3ViID0gc2Vla05vZGUobm9kZWlkLCBub2RlLnN0ZXBfZnVuY3Rpb24pIGlmIG5vZGUuc3RlcF9mdW5jdGlvblxuICAgICAgICByZXR1cm4gc3ViIGlmIHN1YlxuXG4gICAgICBudWxsXG5cblxuICAgIGRlZmVycmVkID0gJHEuZGVmZXIoKVxuXG4gICAgJHEuYWxsKFtkZWZlcnJlZHMucGxhbi5wcm9taXNlLCBkZWZlcnJlZHMuam9iLnByb21pc2VdKS50aGVuIChkYXRhKSA9PlxuICAgICAgZm91bmROb2RlID0gc2Vla05vZGUobm9kZWlkLCBjdXJyZW50UGxhbi5ub2RlcylcblxuICAgICAgIyAkaHR0cC5nZXQgZmxpbmtDb25maWcuam9iU2VydmVyICsgXCIvam9ic0luZm8/Z2V0PWdyb3VwdmVydGV4JmpvYj1cIiArIGN1cnJlbnRKb2IuamlkICsgXCImZ3JvdXB2ZXJ0ZXg9XCIgKyBub2RlaWRcblxuICAgICAgZm91bmROb2RlLnZlcnRleCA9IEBzZWVrVmVydGV4KG5vZGVpZClcblxuICAgICAgZGVmZXJyZWQucmVzb2x2ZShmb3VuZE5vZGUpXG5cbiAgICBkZWZlcnJlZC5wcm9taXNlXG5cbiAgQHNlZWtWZXJ0ZXggPSAobm9kZWlkKSAtPlxuICAgIGZvciB2ZXJ0ZXggaW4gY3VycmVudEpvYi52ZXJ0aWNlc1xuICAgICAgcmV0dXJuIHZlcnRleCBpZiB2ZXJ0ZXguaWQgaXMgbm9kZWlkXG5cbiAgICByZXR1cm4gbnVsbFxuXG4gIEBnZXRWZXJ0ZXggPSAodmVydGV4aWQpIC0+XG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpXG5cbiAgICAkcS5hbGwoW2RlZmVycmVkcy5qb2IucHJvbWlzZV0pLnRoZW4gKGRhdGEpID0+XG4gICAgICB2ZXJ0ZXggPSBAc2Vla1ZlcnRleCh2ZXJ0ZXhpZClcblxuICAgICAgJGh0dHAuZ2V0IGZsaW5rQ29uZmlnLmpvYlNlcnZlciArIFwiL2pvYnMvXCIgKyBjdXJyZW50Sm9iLmppZCArIFwiL3ZlcnRpY2VzL1wiICsgdmVydGV4aWQgKyBcIi9zdWJ0YXNrdGltZXNcIlxuICAgICAgLnN1Y2Nlc3MgKGRhdGEpIC0+XG4gICAgICAgIHZlcnRleC5zdWJ0YXNrcyA9IGRhdGEuc3VidGFza3NcblxuICAgICAgICBkZWZlcnJlZC5yZXNvbHZlKHZlcnRleClcblxuICAgIGRlZmVycmVkLnByb21pc2VcblxuICBAbG9hZEV4Y2VwdGlvbnMgPSAtPlxuICAgIGRlZmVycmVkID0gJHEuZGVmZXIoKVxuXG4gICAgJHEuYWxsKFtkZWZlcnJlZHMuam9iLnByb21pc2VdKS50aGVuIChkYXRhKSA9PlxuXG4gICAgICAkaHR0cC5nZXQgZmxpbmtDb25maWcuam9iU2VydmVyICsgXCIvam9icy9cIiArIGN1cnJlbnRKb2IuamlkICsgXCIvZXhjZXB0aW9uc1wiXG4gICAgICAuc3VjY2VzcyAoZXhjZXB0aW9ucykgLT5cbiAgICAgICAgY3VycmVudEpvYi5leGNlcHRpb25zID0gZXhjZXB0aW9uc1xuXG4gICAgICAgIGRlZmVycmVkLnJlc29sdmUoZXhjZXB0aW9ucylcblxuICAgIGRlZmVycmVkLnByb21pc2VcblxuICBAXG4iLCJhbmd1bGFyLm1vZHVsZSgnZmxpbmtBcHAnKS5zZXJ2aWNlKCdKb2JzU2VydmljZScsIGZ1bmN0aW9uKCRodHRwLCBmbGlua0NvbmZpZywgJGxvZywgYW1Nb21lbnQsICRxLCAkdGltZW91dCkge1xuICB2YXIgY3VycmVudEpvYiwgY3VycmVudFBsYW4sIGRlZmVycmVkcywgam9iT2JzZXJ2ZXJzLCBqb2JzLCBub3RpZnlPYnNlcnZlcnM7XG4gIGN1cnJlbnRKb2IgPSBudWxsO1xuICBjdXJyZW50UGxhbiA9IG51bGw7XG4gIGRlZmVycmVkcyA9IHt9O1xuICBqb2JzID0ge1xuICAgIHJ1bm5pbmc6IFtdLFxuICAgIGZpbmlzaGVkOiBbXSxcbiAgICBjYW5jZWxsZWQ6IFtdLFxuICAgIGZhaWxlZDogW11cbiAgfTtcbiAgam9iT2JzZXJ2ZXJzID0gW107XG4gIG5vdGlmeU9ic2VydmVycyA9IGZ1bmN0aW9uKCkge1xuICAgIHJldHVybiBhbmd1bGFyLmZvckVhY2goam9iT2JzZXJ2ZXJzLCBmdW5jdGlvbihjYWxsYmFjaykge1xuICAgICAgcmV0dXJuIGNhbGxiYWNrKCk7XG4gICAgfSk7XG4gIH07XG4gIHRoaXMucmVnaXN0ZXJPYnNlcnZlciA9IGZ1bmN0aW9uKGNhbGxiYWNrKSB7XG4gICAgcmV0dXJuIGpvYk9ic2VydmVycy5wdXNoKGNhbGxiYWNrKTtcbiAgfTtcbiAgdGhpcy51blJlZ2lzdGVyT2JzZXJ2ZXIgPSBmdW5jdGlvbihjYWxsYmFjaykge1xuICAgIHZhciBpbmRleDtcbiAgICBpbmRleCA9IGpvYk9ic2VydmVycy5pbmRleE9mKGNhbGxiYWNrKTtcbiAgICByZXR1cm4gam9iT2JzZXJ2ZXJzLnNwbGljZShpbmRleCwgMSk7XG4gIH07XG4gIHRoaXMuc3RhdGVMaXN0ID0gZnVuY3Rpb24oKSB7XG4gICAgcmV0dXJuIFsnU0NIRURVTEVEJywgJ0RFUExPWUlORycsICdSVU5OSU5HJywgJ0ZJTklTSEVEJywgJ0ZBSUxFRCcsICdDQU5DRUxJTkcnLCAnQ0FOQ0VMRUQnXTtcbiAgfTtcbiAgdGhpcy50cmFuc2xhdGVMYWJlbFN0YXRlID0gZnVuY3Rpb24oc3RhdGUpIHtcbiAgICBzd2l0Y2ggKHN0YXRlLnRvTG93ZXJDYXNlKCkpIHtcbiAgICAgIGNhc2UgJ2ZpbmlzaGVkJzpcbiAgICAgICAgcmV0dXJuICdzdWNjZXNzJztcbiAgICAgIGNhc2UgJ2ZhaWxlZCc6XG4gICAgICAgIHJldHVybiAnZGFuZ2VyJztcbiAgICAgIGNhc2UgJ3NjaGVkdWxlZCc6XG4gICAgICAgIHJldHVybiAnZGVmYXVsdCc7XG4gICAgICBjYXNlICdkZXBsb3lpbmcnOlxuICAgICAgICByZXR1cm4gJ2luZm8nO1xuICAgICAgY2FzZSAncnVubmluZyc6XG4gICAgICAgIHJldHVybiAncHJpbWFyeSc7XG4gICAgICBjYXNlICdjYW5jZWxpbmcnOlxuICAgICAgICByZXR1cm4gJ3dhcm5pbmcnO1xuICAgICAgY2FzZSAncGVuZGluZyc6XG4gICAgICAgIHJldHVybiAnaW5mbyc7XG4gICAgICBjYXNlICd0b3RhbCc6XG4gICAgICAgIHJldHVybiAnYmxhY2snO1xuICAgICAgZGVmYXVsdDpcbiAgICAgICAgcmV0dXJuICdkZWZhdWx0JztcbiAgICB9XG4gIH07XG4gIHRoaXMubGlzdEpvYnMgPSBmdW5jdGlvbigpIHtcbiAgICB2YXIgZGVmZXJyZWQ7XG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpO1xuICAgICRodHRwLmdldChmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcIi9qb2JvdmVydmlld1wiKS5zdWNjZXNzKGZ1bmN0aW9uKGRhdGEsIHN0YXR1cywgaGVhZGVycywgY29uZmlnKSB7XG4gICAgICBhbmd1bGFyLmZvckVhY2goZGF0YSwgZnVuY3Rpb24obGlzdCwgbGlzdEtleSkge1xuICAgICAgICBzd2l0Y2ggKGxpc3RLZXkpIHtcbiAgICAgICAgICBjYXNlICdydW5uaW5nJzpcbiAgICAgICAgICAgIHJldHVybiBqb2JzLnJ1bm5pbmcgPSBsaXN0O1xuICAgICAgICAgIGNhc2UgJ2ZpbmlzaGVkJzpcbiAgICAgICAgICAgIHJldHVybiBqb2JzLmZpbmlzaGVkID0gbGlzdDtcbiAgICAgICAgICBjYXNlICdjYW5jZWxsZWQnOlxuICAgICAgICAgICAgcmV0dXJuIGpvYnMuY2FuY2VsbGVkID0gbGlzdDtcbiAgICAgICAgICBjYXNlICdmYWlsZWQnOlxuICAgICAgICAgICAgcmV0dXJuIGpvYnMuZmFpbGVkID0gbGlzdDtcbiAgICAgICAgfVxuICAgICAgfSk7XG4gICAgICBkZWZlcnJlZC5yZXNvbHZlKGpvYnMpO1xuICAgICAgcmV0dXJuIG5vdGlmeU9ic2VydmVycygpO1xuICAgIH0pO1xuICAgIHJldHVybiBkZWZlcnJlZC5wcm9taXNlO1xuICB9O1xuICB0aGlzLmdldEpvYnMgPSBmdW5jdGlvbih0eXBlKSB7XG4gICAgcmV0dXJuIGpvYnNbdHlwZV07XG4gIH07XG4gIHRoaXMuZ2V0QWxsSm9icyA9IGZ1bmN0aW9uKCkge1xuICAgIHJldHVybiBqb2JzO1xuICB9O1xuICB0aGlzLmxvYWRKb2IgPSBmdW5jdGlvbihqb2JpZCkge1xuICAgIGN1cnJlbnRKb2IgPSBudWxsO1xuICAgIGRlZmVycmVkcy5qb2IgPSAkcS5kZWZlcigpO1xuICAgICRodHRwLmdldChmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcIi9qb2JzL1wiICsgam9iaWQpLnN1Y2Nlc3MoZnVuY3Rpb24oZGF0YSwgc3RhdHVzLCBoZWFkZXJzLCBjb25maWcpIHtcbiAgICAgIHJldHVybiAkaHR0cC5nZXQoZmxpbmtDb25maWcuam9iU2VydmVyICsgXCIvam9icy9cIiArIGpvYmlkICsgXCIvdmVydGljZXNcIikuc3VjY2VzcyhmdW5jdGlvbih2ZXJ0aWNlcykge1xuICAgICAgICBkYXRhID0gYW5ndWxhci5leHRlbmQoZGF0YSwgdmVydGljZXMpO1xuICAgICAgICByZXR1cm4gJGh0dHAuZ2V0KGZsaW5rQ29uZmlnLmpvYlNlcnZlciArIFwiL2pvYnMvXCIgKyBqb2JpZCArIFwiL2NvbmZpZ1wiKS5zdWNjZXNzKGZ1bmN0aW9uKGpvYkNvbmZpZykge1xuICAgICAgICAgIGRhdGEgPSBhbmd1bGFyLmV4dGVuZChkYXRhLCBqb2JDb25maWcpO1xuICAgICAgICAgIGN1cnJlbnRKb2IgPSBkYXRhO1xuICAgICAgICAgIHJldHVybiBkZWZlcnJlZHMuam9iLnJlc29sdmUoZGF0YSk7XG4gICAgICAgIH0pO1xuICAgICAgfSk7XG4gICAgfSk7XG4gICAgcmV0dXJuIGRlZmVycmVkcy5qb2IucHJvbWlzZTtcbiAgfTtcbiAgdGhpcy5sb2FkUGxhbiA9IGZ1bmN0aW9uKGpvYmlkKSB7XG4gICAgY3VycmVudFBsYW4gPSBudWxsO1xuICAgIGRlZmVycmVkcy5wbGFuID0gJHEuZGVmZXIoKTtcbiAgICAkaHR0cC5nZXQoZmxpbmtDb25maWcuam9iU2VydmVyICsgXCIvam9icy9cIiArIGpvYmlkICsgXCIvcGxhblwiKS5zdWNjZXNzKGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgIGN1cnJlbnRQbGFuID0gZGF0YTtcbiAgICAgIHJldHVybiBkZWZlcnJlZHMucGxhbi5yZXNvbHZlKGRhdGEpO1xuICAgIH0pO1xuICAgIHJldHVybiBkZWZlcnJlZHMucGxhbi5wcm9taXNlO1xuICB9O1xuICB0aGlzLmdldE5vZGUgPSBmdW5jdGlvbihub2RlaWQpIHtcbiAgICB2YXIgZGVmZXJyZWQsIHNlZWtOb2RlO1xuICAgIHNlZWtOb2RlID0gZnVuY3Rpb24obm9kZWlkLCBkYXRhKSB7XG4gICAgICB2YXIgaSwgbGVuLCBub2RlLCBzdWI7XG4gICAgICBmb3IgKGkgPSAwLCBsZW4gPSBkYXRhLmxlbmd0aDsgaSA8IGxlbjsgaSsrKSB7XG4gICAgICAgIG5vZGUgPSBkYXRhW2ldO1xuICAgICAgICBpZiAobm9kZS5pZCA9PT0gbm9kZWlkKSB7XG4gICAgICAgICAgcmV0dXJuIG5vZGU7XG4gICAgICAgIH1cbiAgICAgICAgaWYgKG5vZGUuc3RlcF9mdW5jdGlvbikge1xuICAgICAgICAgIHN1YiA9IHNlZWtOb2RlKG5vZGVpZCwgbm9kZS5zdGVwX2Z1bmN0aW9uKTtcbiAgICAgICAgfVxuICAgICAgICBpZiAoc3ViKSB7XG4gICAgICAgICAgcmV0dXJuIHN1YjtcbiAgICAgICAgfVxuICAgICAgfVxuICAgICAgcmV0dXJuIG51bGw7XG4gICAgfTtcbiAgICBkZWZlcnJlZCA9ICRxLmRlZmVyKCk7XG4gICAgJHEuYWxsKFtkZWZlcnJlZHMucGxhbi5wcm9taXNlLCBkZWZlcnJlZHMuam9iLnByb21pc2VdKS50aGVuKChmdW5jdGlvbihfdGhpcykge1xuICAgICAgcmV0dXJuIGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgICAgdmFyIGZvdW5kTm9kZTtcbiAgICAgICAgZm91bmROb2RlID0gc2Vla05vZGUobm9kZWlkLCBjdXJyZW50UGxhbi5ub2Rlcyk7XG4gICAgICAgIGZvdW5kTm9kZS52ZXJ0ZXggPSBfdGhpcy5zZWVrVmVydGV4KG5vZGVpZCk7XG4gICAgICAgIHJldHVybiBkZWZlcnJlZC5yZXNvbHZlKGZvdW5kTm9kZSk7XG4gICAgICB9O1xuICAgIH0pKHRoaXMpKTtcbiAgICByZXR1cm4gZGVmZXJyZWQucHJvbWlzZTtcbiAgfTtcbiAgdGhpcy5zZWVrVmVydGV4ID0gZnVuY3Rpb24obm9kZWlkKSB7XG4gICAgdmFyIGksIGxlbiwgcmVmLCB2ZXJ0ZXg7XG4gICAgcmVmID0gY3VycmVudEpvYi52ZXJ0aWNlcztcbiAgICBmb3IgKGkgPSAwLCBsZW4gPSByZWYubGVuZ3RoOyBpIDwgbGVuOyBpKyspIHtcbiAgICAgIHZlcnRleCA9IHJlZltpXTtcbiAgICAgIGlmICh2ZXJ0ZXguaWQgPT09IG5vZGVpZCkge1xuICAgICAgICByZXR1cm4gdmVydGV4O1xuICAgICAgfVxuICAgIH1cbiAgICByZXR1cm4gbnVsbDtcbiAgfTtcbiAgdGhpcy5nZXRWZXJ0ZXggPSBmdW5jdGlvbih2ZXJ0ZXhpZCkge1xuICAgIHZhciBkZWZlcnJlZDtcbiAgICBkZWZlcnJlZCA9ICRxLmRlZmVyKCk7XG4gICAgJHEuYWxsKFtkZWZlcnJlZHMuam9iLnByb21pc2VdKS50aGVuKChmdW5jdGlvbihfdGhpcykge1xuICAgICAgcmV0dXJuIGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgICAgdmFyIHZlcnRleDtcbiAgICAgICAgdmVydGV4ID0gX3RoaXMuc2Vla1ZlcnRleCh2ZXJ0ZXhpZCk7XG4gICAgICAgIHJldHVybiAkaHR0cC5nZXQoZmxpbmtDb25maWcuam9iU2VydmVyICsgXCIvam9icy9cIiArIGN1cnJlbnRKb2IuamlkICsgXCIvdmVydGljZXMvXCIgKyB2ZXJ0ZXhpZCArIFwiL3N1YnRhc2t0aW1lc1wiKS5zdWNjZXNzKGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgICAgICB2ZXJ0ZXguc3VidGFza3MgPSBkYXRhLnN1YnRhc2tzO1xuICAgICAgICAgIHJldHVybiBkZWZlcnJlZC5yZXNvbHZlKHZlcnRleCk7XG4gICAgICAgIH0pO1xuICAgICAgfTtcbiAgICB9KSh0aGlzKSk7XG4gICAgcmV0dXJuIGRlZmVycmVkLnByb21pc2U7XG4gIH07XG4gIHRoaXMubG9hZEV4Y2VwdGlvbnMgPSBmdW5jdGlvbigpIHtcbiAgICB2YXIgZGVmZXJyZWQ7XG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpO1xuICAgICRxLmFsbChbZGVmZXJyZWRzLmpvYi5wcm9taXNlXSkudGhlbigoZnVuY3Rpb24oX3RoaXMpIHtcbiAgICAgIHJldHVybiBmdW5jdGlvbihkYXRhKSB7XG4gICAgICAgIHJldHVybiAkaHR0cC5nZXQoZmxpbmtDb25maWcuam9iU2VydmVyICsgXCIvam9icy9cIiArIGN1cnJlbnRKb2IuamlkICsgXCIvZXhjZXB0aW9uc1wiKS5zdWNjZXNzKGZ1bmN0aW9uKGV4Y2VwdGlvbnMpIHtcbiAgICAgICAgICBjdXJyZW50Sm9iLmV4Y2VwdGlvbnMgPSBleGNlcHRpb25zO1xuICAgICAgICAgIHJldHVybiBkZWZlcnJlZC5yZXNvbHZlKGV4Y2VwdGlvbnMpO1xuICAgICAgICB9KTtcbiAgICAgIH07XG4gICAgfSkodGhpcykpO1xuICAgIHJldHVybiBkZWZlcnJlZC5wcm9taXNlO1xuICB9O1xuICByZXR1cm4gdGhpcztcbn0pO1xuIiwiI1xuIyBMaWNlbnNlZCB0byB0aGUgQXBhY2hlIFNvZnR3YXJlIEZvdW5kYXRpb24gKEFTRikgdW5kZXIgb25lXG4jIG9yIG1vcmUgY29udHJpYnV0b3IgbGljZW5zZSBhZ3JlZW1lbnRzLiAgU2VlIHRoZSBOT1RJQ0UgZmlsZVxuIyBkaXN0cmlidXRlZCB3aXRoIHRoaXMgd29yayBmb3IgYWRkaXRpb25hbCBpbmZvcm1hdGlvblxuIyByZWdhcmRpbmcgY29weXJpZ2h0IG93bmVyc2hpcC4gIFRoZSBBU0YgbGljZW5zZXMgdGhpcyBmaWxlXG4jIHRvIHlvdSB1bmRlciB0aGUgQXBhY2hlIExpY2Vuc2UsIFZlcnNpb24gMi4wICh0aGVcbiMgXCJMaWNlbnNlXCIpOyB5b3UgbWF5IG5vdCB1c2UgdGhpcyBmaWxlIGV4Y2VwdCBpbiBjb21wbGlhbmNlXG4jIHdpdGggdGhlIExpY2Vuc2UuICBZb3UgbWF5IG9idGFpbiBhIGNvcHkgb2YgdGhlIExpY2Vuc2UgYXRcbiNcbiMgICAgIGh0dHA6Ly93d3cuYXBhY2hlLm9yZy9saWNlbnNlcy9MSUNFTlNFLTIuMFxuI1xuIyBVbmxlc3MgcmVxdWlyZWQgYnkgYXBwbGljYWJsZSBsYXcgb3IgYWdyZWVkIHRvIGluIHdyaXRpbmcsIHNvZnR3YXJlXG4jIGRpc3RyaWJ1dGVkIHVuZGVyIHRoZSBMaWNlbnNlIGlzIGRpc3RyaWJ1dGVkIG9uIGFuIFwiQVMgSVNcIiBCQVNJUyxcbiMgV0lUSE9VVCBXQVJSQU5USUVTIE9SIENPTkRJVElPTlMgT0YgQU5ZIEtJTkQsIGVpdGhlciBleHByZXNzIG9yIGltcGxpZWQuXG4jIFNlZSB0aGUgTGljZW5zZSBmb3IgdGhlIHNwZWNpZmljIGxhbmd1YWdlIGdvdmVybmluZyBwZXJtaXNzaW9ucyBhbmRcbiMgbGltaXRhdGlvbnMgdW5kZXIgdGhlIExpY2Vuc2UuXG4jXG5cbmFuZ3VsYXIubW9kdWxlKCdmbGlua0FwcCcpXG5cbi5jb250cm9sbGVyICdPdmVydmlld0NvbnRyb2xsZXInLCAoJHNjb3BlLCBPdmVydmlld1NlcnZpY2UsIEpvYnNTZXJ2aWNlLCAkaW50ZXJ2YWwsIGZsaW5rQ29uZmlnKSAtPlxuICAkc2NvcGUuam9iT2JzZXJ2ZXIgPSAtPlxuICAgICRzY29wZS5ydW5uaW5nSm9icyA9IEpvYnNTZXJ2aWNlLmdldEpvYnMoJ3J1bm5pbmcnKVxuICAgICRzY29wZS5maW5pc2hlZEpvYnMgPSBKb2JzU2VydmljZS5nZXRKb2JzKCdmaW5pc2hlZCcpXG5cbiAgSm9ic1NlcnZpY2UucmVnaXN0ZXJPYnNlcnZlcigkc2NvcGUuam9iT2JzZXJ2ZXIpXG4gICRzY29wZS4kb24gJyRkZXN0cm95JywgLT5cbiAgICBKb2JzU2VydmljZS51blJlZ2lzdGVyT2JzZXJ2ZXIoJHNjb3BlLmpvYk9ic2VydmVyKVxuXG4gICRzY29wZS5qb2JPYnNlcnZlcigpXG5cbiAgT3ZlcnZpZXdTZXJ2aWNlLmxvYWRPdmVydmlldygpLnRoZW4gKGRhdGEpIC0+XG4gICAgJHNjb3BlLm92ZXJ2aWV3ID0gZGF0YVxuXG4gIHJlZnJlc2ggPSAkaW50ZXJ2YWwgLT5cbiAgICBPdmVydmlld1NlcnZpY2UubG9hZE92ZXJ2aWV3KCkudGhlbiAoZGF0YSkgLT5cbiAgICAgICRzY29wZS5vdmVydmlldyA9IGRhdGFcbiAgLCBmbGlua0NvbmZpZ1tcInJlZnJlc2gtaW50ZXJ2YWxcIl1cblxuICAkc2NvcGUuJG9uICckZGVzdHJveScsIC0+XG4gICAgJGludGVydmFsLmNhbmNlbChyZWZyZXNoKVxuIiwiYW5ndWxhci5tb2R1bGUoJ2ZsaW5rQXBwJykuY29udHJvbGxlcignT3ZlcnZpZXdDb250cm9sbGVyJywgZnVuY3Rpb24oJHNjb3BlLCBPdmVydmlld1NlcnZpY2UsIEpvYnNTZXJ2aWNlLCAkaW50ZXJ2YWwsIGZsaW5rQ29uZmlnKSB7XG4gIHZhciByZWZyZXNoO1xuICAkc2NvcGUuam9iT2JzZXJ2ZXIgPSBmdW5jdGlvbigpIHtcbiAgICAkc2NvcGUucnVubmluZ0pvYnMgPSBKb2JzU2VydmljZS5nZXRKb2JzKCdydW5uaW5nJyk7XG4gICAgcmV0dXJuICRzY29wZS5maW5pc2hlZEpvYnMgPSBKb2JzU2VydmljZS5nZXRKb2JzKCdmaW5pc2hlZCcpO1xuICB9O1xuICBKb2JzU2VydmljZS5yZWdpc3Rlck9ic2VydmVyKCRzY29wZS5qb2JPYnNlcnZlcik7XG4gICRzY29wZS4kb24oJyRkZXN0cm95JywgZnVuY3Rpb24oKSB7XG4gICAgcmV0dXJuIEpvYnNTZXJ2aWNlLnVuUmVnaXN0ZXJPYnNlcnZlcigkc2NvcGUuam9iT2JzZXJ2ZXIpO1xuICB9KTtcbiAgJHNjb3BlLmpvYk9ic2VydmVyKCk7XG4gIE92ZXJ2aWV3U2VydmljZS5sb2FkT3ZlcnZpZXcoKS50aGVuKGZ1bmN0aW9uKGRhdGEpIHtcbiAgICByZXR1cm4gJHNjb3BlLm92ZXJ2aWV3ID0gZGF0YTtcbiAgfSk7XG4gIHJlZnJlc2ggPSAkaW50ZXJ2YWwoZnVuY3Rpb24oKSB7XG4gICAgcmV0dXJuIE92ZXJ2aWV3U2VydmljZS5sb2FkT3ZlcnZpZXcoKS50aGVuKGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgIHJldHVybiAkc2NvcGUub3ZlcnZpZXcgPSBkYXRhO1xuICAgIH0pO1xuICB9LCBmbGlua0NvbmZpZ1tcInJlZnJlc2gtaW50ZXJ2YWxcIl0pO1xuICByZXR1cm4gJHNjb3BlLiRvbignJGRlc3Ryb3knLCBmdW5jdGlvbigpIHtcbiAgICByZXR1cm4gJGludGVydmFsLmNhbmNlbChyZWZyZXNoKTtcbiAgfSk7XG59KTtcbiIsIiNcbiMgTGljZW5zZWQgdG8gdGhlIEFwYWNoZSBTb2Z0d2FyZSBGb3VuZGF0aW9uIChBU0YpIHVuZGVyIG9uZVxuIyBvciBtb3JlIGNvbnRyaWJ1dG9yIGxpY2Vuc2UgYWdyZWVtZW50cy4gIFNlZSB0aGUgTk9USUNFIGZpbGVcbiMgZGlzdHJpYnV0ZWQgd2l0aCB0aGlzIHdvcmsgZm9yIGFkZGl0aW9uYWwgaW5mb3JtYXRpb25cbiMgcmVnYXJkaW5nIGNvcHlyaWdodCBvd25lcnNoaXAuICBUaGUgQVNGIGxpY2Vuc2VzIHRoaXMgZmlsZVxuIyB0byB5b3UgdW5kZXIgdGhlIEFwYWNoZSBMaWNlbnNlLCBWZXJzaW9uIDIuMCAodGhlXG4jIFwiTGljZW5zZVwiKTsgeW91IG1heSBub3QgdXNlIHRoaXMgZmlsZSBleGNlcHQgaW4gY29tcGxpYW5jZVxuIyB3aXRoIHRoZSBMaWNlbnNlLiAgWW91IG1heSBvYnRhaW4gYSBjb3B5IG9mIHRoZSBMaWNlbnNlIGF0XG4jXG4jICAgICBodHRwOi8vd3d3LmFwYWNoZS5vcmcvbGljZW5zZXMvTElDRU5TRS0yLjBcbiNcbiMgVW5sZXNzIHJlcXVpcmVkIGJ5IGFwcGxpY2FibGUgbGF3IG9yIGFncmVlZCB0byBpbiB3cml0aW5nLCBzb2Z0d2FyZVxuIyBkaXN0cmlidXRlZCB1bmRlciB0aGUgTGljZW5zZSBpcyBkaXN0cmlidXRlZCBvbiBhbiBcIkFTIElTXCIgQkFTSVMsXG4jIFdJVEhPVVQgV0FSUkFOVElFUyBPUiBDT05ESVRJT05TIE9GIEFOWSBLSU5ELCBlaXRoZXIgZXhwcmVzcyBvciBpbXBsaWVkLlxuIyBTZWUgdGhlIExpY2Vuc2UgZm9yIHRoZSBzcGVjaWZpYyBsYW5ndWFnZSBnb3Zlcm5pbmcgcGVybWlzc2lvbnMgYW5kXG4jIGxpbWl0YXRpb25zIHVuZGVyIHRoZSBMaWNlbnNlLlxuI1xuXG5hbmd1bGFyLm1vZHVsZSgnZmxpbmtBcHAnKVxuXG4uc2VydmljZSAnT3ZlcnZpZXdTZXJ2aWNlJywgKCRodHRwLCBmbGlua0NvbmZpZywgJHEpIC0+XG4gIG92ZXJ2aWV3ID0ge31cblxuICBAbG9hZE92ZXJ2aWV3ID0gLT5cbiAgICBkZWZlcnJlZCA9ICRxLmRlZmVyKClcblxuICAgICRodHRwLmdldChmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcIi9vdmVydmlld1wiKVxuICAgIC5zdWNjZXNzIChkYXRhLCBzdGF0dXMsIGhlYWRlcnMsIGNvbmZpZykgLT5cbiAgICAgIG92ZXJ2aWV3ID0gZGF0YVxuICAgICAgZGVmZXJyZWQucmVzb2x2ZShkYXRhKVxuXG4gICAgZGVmZXJyZWQucHJvbWlzZVxuXG4gIEBcbiIsImFuZ3VsYXIubW9kdWxlKCdmbGlua0FwcCcpLnNlcnZpY2UoJ092ZXJ2aWV3U2VydmljZScsIGZ1bmN0aW9uKCRodHRwLCBmbGlua0NvbmZpZywgJHEpIHtcbiAgdmFyIG92ZXJ2aWV3O1xuICBvdmVydmlldyA9IHt9O1xuICB0aGlzLmxvYWRPdmVydmlldyA9IGZ1bmN0aW9uKCkge1xuICAgIHZhciBkZWZlcnJlZDtcbiAgICBkZWZlcnJlZCA9ICRxLmRlZmVyKCk7XG4gICAgJGh0dHAuZ2V0KGZsaW5rQ29uZmlnLmpvYlNlcnZlciArIFwiL292ZXJ2aWV3XCIpLnN1Y2Nlc3MoZnVuY3Rpb24oZGF0YSwgc3RhdHVzLCBoZWFkZXJzLCBjb25maWcpIHtcbiAgICAgIG92ZXJ2aWV3ID0gZGF0YTtcbiAgICAgIHJldHVybiBkZWZlcnJlZC5yZXNvbHZlKGRhdGEpO1xuICAgIH0pO1xuICAgIHJldHVybiBkZWZlcnJlZC5wcm9taXNlO1xuICB9O1xuICByZXR1cm4gdGhpcztcbn0pO1xuIl0sInNvdXJjZVJvb3QiOiIvc291cmNlLyJ9