The expected structure is as follows: the given plugins root folder,
containing the plugins folder. One plugin folder contains all
resources (jar files) belonging to a plugin. The name of the plugin
folder becomes the plugin id. For example:

plugins/ (root folder)
    |------------plugin-a/ (folder of plugin a)
    |                |-plugin-a-1.jar
    |                |-plugin-a-2.jar
    |                |-...
    |                |-(the jars containing the classes of plugin a)
    |
    |------------plugin-b/
    |                |-plugin-b-1.jar
    |                |-...
    ...

Read more in the documentation about how to create a plugin.
