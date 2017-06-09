/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// ----------------------------------------------------------------------------
//  Build file for the web dashboard
// ----------------------------------------------------------------------------

var gulp = require('gulp');
var gutil = require('gulp-util');
var jade = require('gulp-jade');
var livereload = require('gulp-livereload');
var plumber = require('gulp-plumber');
var stylus = require('gulp-stylus');
var browserify = require('gulp-browserify');
var concat = require('gulp-concat');
var uglify = require('gulp-uglify');
var ngAnnotate = require('gulp-ng-annotate');
var minify = require('gulp-minify-css');
var serve = require('gulp-serve');
var nib = require('nib');
var coffee = require('gulp-coffee');
var sourcemaps = require('gulp-sourcemaps');
var filter = require('gulp-filter');
var mainBowerFiles = require('main-bower-files');
var less = require('gulp-less');
var path = require('path');

var environment = 'production';
var paths = {
  src: './app/',
  dest: './web/',
  vendor: './bower_components/',
  vendorLocal: './vendor-local/',
  assets: './assets/',
  tmp: './tmp/'
}

gulp.task('set-development', function() {
  environment = 'development';
});

gulp.task('fonts', function() {
  return gulp.src(paths.vendor + "font-awesome/fonts/*")
      .pipe(plumber())
      .pipe(gulp.dest(paths.assets + 'fonts'));
});

gulp.task('images', function() {
  return gulp.src(paths.vendor + "Split.js/grips/*")
    .pipe(plumber())
    .pipe(gulp.dest(paths.assets + 'images/grips'));
});

gulp.task('assets', ['fonts', 'images'], function() {
  return gulp.src(paths.assets + "**")
    .pipe(plumber())
    .pipe(gulp.dest(paths.dest));
});

gulp.task('pre-process-vendor-styles', function () {
  return gulp.src(mainBowerFiles('**/*.less').concat(paths.src + 'styles/bootstrap_custom.less'))
    .pipe(less({
      paths: [ path.join(__dirname, 'less', 'includes') ]
    }))
    .pipe(gulp.dest(paths.tmp + 'css/'));
});

gulp.task('vendor-styles', [ 'pre-process-vendor-styles' ], function() {
  stream = gulp.src(mainBowerFiles().concat([paths.tmp + 'css/*.css']).concat([paths.vendor + 'qtip2/jquery.qtip.css']))
    .pipe(filter(['*.css', '!bootstrap.css']))
    .pipe(sourcemaps.init())
    .pipe(concat("vendor.css"))
    .pipe(sourcemaps.write());

  if (environment == 'production') {
    stream.pipe(minify())
  }

  stream.pipe(gulp.dest(paths.dest + 'css/'))
});

gulp.task('vendor-scripts', function() {
  stream = gulp.src(mainBowerFiles({
      env: 'development'
    }).concat([paths.vendorLocal + '*.js']))
    .pipe(filter('*.js'))
    .pipe(sourcemaps.init())
    .pipe(concat("vendor.js"))
    .pipe(sourcemaps.write());

  if (environment == 'production') {
    stream.pipe(uglify())
  }

  stream.pipe(gulp.dest(paths.dest + 'js/'))
 });

gulp.task('scripts', function() {
  stream = gulp.src([ paths.src + 'scripts/config.js', paths.src + 'scripts/**/*.coffee', '!' + paths.src + 'scripts/index_hs.coffee'] )
    .pipe(plumber())
    .pipe(sourcemaps.init())
    .pipe(coffee({ bare: true }))
    .pipe(ngAnnotate())
    .pipe(concat('index.js'))
    .pipe(sourcemaps.write());

  if (environment == 'production') {
    stream.pipe(uglify())
  }

  stream.pipe(gulp.dest(paths.dest + 'js/'))
});

gulp.task('scripts_hs', function() {
  stream = gulp.src([ paths.src + 'scripts/config.js', paths.src + 'scripts/**/*.coffee', '!' + paths.src + 'scripts/index.coffee'] )
    .pipe(plumber())
    .pipe(sourcemaps.init())
    .pipe(coffee({ bare: true }))
    .pipe(ngAnnotate())
    .pipe(concat('index.js'))
    .pipe(sourcemaps.write());

  if (environment == 'production') {
    stream.pipe(uglify())
  }

  stream.pipe(gulp.dest(paths.dest + 'js/hs'))
});

gulp.task('html', function() {
  gulp.src(paths.src + 'index.jade')
    .pipe(plumber())
    .pipe(jade({
      pretty: true
    }))
    .pipe(gulp.dest(paths.dest))
});

gulp.task('html_hs', function() {
  gulp.src(paths.src + 'index_hs.jade')
    .pipe(plumber())
    .pipe(jade({
      pretty: true
    }))
    .pipe(gulp.dest(paths.dest))
});

gulp.task('partials', function() {
  gulp.src(paths.src + 'partials/**/*.jade')
    .pipe(plumber())
    .pipe(jade({
      pretty: true
    }))
    .pipe(gulp.dest(paths.dest + 'partials/'))
});

gulp.task('styles', function () {
  stream = gulp.src(paths.src + 'styles/index.styl')
    .pipe(plumber())
    .pipe(stylus({ use: [nib()] }))
  
  if (environment == 'production') {
    stream.pipe(minify());
  }

  stream.pipe(gulp.dest(paths.dest + 'css/'))
});

gulp.task('watch', function () {
  environment = 'development';
  livereload.listen();

  gulp.watch(paths.vendorLocal + '**', ['vendor-scripts']);
  gulp.watch(paths.src + 'partials/**', ['partials']);
  gulp.watch(paths.src + 'scripts/**', ['scripts']);
  gulp.watch(paths.src + 'styles/**/*.styl', ['styles']);
  gulp.watch(paths.src + 'index.jade', ['html']);

  gulp.watch([
      paths.dest + 'js/*.js',
      paths.dest + 'css/*.css',
      paths.dest + '**/*.html'
    ], livereload.changed);
});

gulp.task('serve', serve({
  root: 'web',
  port: 3001
}));

gulp.task('vendor', ['vendor-styles', 'vendor-scripts']);
gulp.task('compile', ['html', 'html_hs', 'partials','styles', 'scripts', 'scripts_hs']);

gulp.task('default', ['fonts', 'assets', 'vendor', 'compile']);
gulp.task('dev', ['set-development', 'default']);

