.. ################################################################################
     Licensed to the Apache Software Foundation (ASF) under one
     or more contributor license agreements.  See the NOTICE file
     distributed with this work for additional information
     regarding copyright ownership.  The ASF licenses this file
     to you under the Apache License, Version 2.0 (the
     "License"); you may not use this file except in compliance
     with the License.  You may obtain a copy of the License at

         http://www.apache.org/licenses/LICENSE-2.0

     Unless required by applicable law or agreed to in writing, software
     distributed under the License is distributed on an "AS IS" BASIS,
     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
     See the License for the specific language governing permissions and
    limitations under the License.
   ################################################################################

Installation
============

Environment Requirements
------------------------

.. note::
   Python version (3.9, 3.10, 3.11 or 3.12) is required for PyFlink. Please run the following command
   to make sure that it meets the requirements:

.. code-block:: bash

   $ python --version
   # the version printed here must be 3.9, 3.10, 3.11 or 3.12

Environment Setup
-----------------

Your system may include multiple Python versions, and thus also include multiple Python binary executables.
You can run the following ``ls`` command to find out what Python binary executables are available in your system:

.. code-block:: bash

   $ ls /usr/bin/python*

To satisfy the PyFlink requirement regarding the Python environment version, you can choose to soft link ``python`` to point to your ``python3`` interpreter:

.. code-block:: bash

   ln -s /usr/bin/python3 python

In addition to creating a soft link, you can also choose to create a Python virtual environment (``venv``):

.. code-block:: bash

   python -m venv myenv
   source myenv/bin/activate  # On Windows: myenv\Scripts\activate

You can refer to the :flinkdoc:`Deployment Preparation <docs/deployment/resource-providers/standalone/overview/>` documentation page for details on how to achieve that setup.

If you don't want to use a soft link to change the system's ``python`` interpreter point to, you can use the configuration way to specify the Python interpreter.
For specifying the Python interpreter used to compile the jobs, you can refer to the configuration :doc:`../user_guide/configuration`.
For specifying the Python interpreter used to execute the Python UDF, you can refer to the configuration :doc:`../user_guide/configuration`.

Installation of PyFlink
-----------------------

PyFlink is available in `PyPi <https://pypi.org/project/apache-flink/>`_ and can be installed as follows:

.. code-block:: bash

   $ python -m pip install apache-flink

You can also build PyFlink from source by following the `development guide <https://flink.apache.org/docs/latest/flinkdev/building/#build-pyflink>`_.

.. note::
   Starting from Flink 1.11, it's also supported to run
   PyFlink jobs locally on Windows and so you could develop and debug PyFlink jobs on Windows.
