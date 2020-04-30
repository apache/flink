# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at

# http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# ---------------------------------------------------------
# Ensures that the documentation contains the Apache License
# headers once, not repeatedly for each include.
# ---------------------------------------------------------

module Jekyll

  module LicenseRemover
 
    AL2 = "<!--\n"+
          "Licensed to the Apache Software Foundation (ASF) under one\n"+
          "or more contributor license agreements.  See the NOTICE file\n"+
          "distributed with this work for additional information\n"+
          "regarding copyright ownership.  The ASF licenses this file\n"+
          "to you under the Apache License, Version 2.0 (the\n"+
          "\"License\"); you may not use this file except in compliance\n"+
          "with the License.  You may obtain a copy of the License at\n"+
          "\n"+
          "http://www.apache.org/licenses/LICENSE-2.0\n"+
          "\n"+
          "Unless required by applicable law or agreed to in writing,\n"+
          "software distributed under the License is distributed on an\n"+
          "\"AS IS\" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY\n"+
          "KIND, either express or implied.  See the License for the\n"+
          "specific language governing permissions and limitations\n"+
          "under the License.\n"+
          "-->\n"

    def writeFile(dest, content)
      path = self.destination(dest)
      FileUtils.mkdir_p(File.dirname(path))
      File.open(path, 'w') do |f|
        # remove all Apache Licenses
        withoutLicense = content.gsub(/<!--[^>]*LICENSE-2.0[^>]*-->/,'')
        # put single Apache License on top
        singleLicense = AL2+withoutLicense
        # write file out
        f.write(singleLicense)
      end
    end

  end

  class Post
    include LicenseRemover
    def write(dest)
      self.writeFile(dest, self.output)
    end
  end

  class Page
    include LicenseRemover
    def write(dest)
      self.writeFile(dest, self.output)
    end
  end

end
