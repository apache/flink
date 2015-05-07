var buildDir= 'build/';

module.exports = function(grunt) {

  grunt.initConfig({
    pkg: grunt.file.readJSON('package.json'),

    copy: {
      main: {
        files: [
          {expand: true, src: '*.html', dest: buildDir},
          {expand: true, src: 'css/**', dest: buildDir},
          {expand: true, src: 'font-awesome/**', dest: buildDir},
          {expand: true, src: 'img/**', dest: buildDir},
          {expand: true, src: 'js/**', dest: buildDir}
        ]
      }
    },

    clean: {
      build: [buildDir]
    }
  });

  grunt.loadNpmTasks('grunt-contrib-copy');
  grunt.loadNpmTasks('grunt-contrib-clean');

  grunt.registerTask('default', ['clean:build', 'copy:main']);

};
