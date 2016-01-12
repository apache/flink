# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
# 
# http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# This file was derived from https://github.com/navarroj/krampygs 
#   written by Juan Navarro and released under the Unlicense (http://unlicense.org).

require 'kramdown'
require 'pygments'

module Kramdown
  module Converter

    class Pygs < Html
    
      def convert_codeblock(el, indent)
        attr = el.attr.dup
        lang = extract_code_language!(attr) || @options[:kramdown_default_lang]
        code = pygmentize(el.value, lang)
        code_attr = {}
        code_attr['class'] = "language-#{lang}" if lang
        "#{' '*indent}<div class=\"highlight\"><pre#{html_attributes(attr)}><code#{html_attributes(code_attr)}>#{code}</code></pre></div>\n"
      end

      def convert_codespan(el, indent)
        attr = el.attr.dup
        lang = extract_code_language!(attr) || @options[:kramdown_default_lang]
        code = pygmentize(el.value, lang)
        if lang
          attr['class'] = "highlight"
          if attr.has_key?('class')
            attr['class'] += " language-#{lang}"
          else
            attr['class'] = "language-#{lang}"
          end
        end
        "<code#{html_attributes(attr)}>#{code}</code>"
      end
      
      def pygmentize(code, lang)
        if lang
          Pygments.highlight(code,
            :lexer => lang,
            :options => { :encoding => 'utf-8', :nowrap => true })
        else
          escape_html(code)
        end
      end

    end
  end
end

class Jekyll::Converters::Markdown::KramdownPygments

  def initialize(config)
    require 'kramdown'
    @config = config
  rescue LoadError
    STDERR.puts 'You are missing a library required for Markdown. Please run:'
    STDERR.puts '  $ [sudo] gem install kramdown'
    raise FatalException.new("Missing dependency: kramdown")
  end
  
  def matches(ext)
    ext =~ /^\.md$/i
  end

  def output_ext(ext)
    ".html"
  end

  def convert(content)
    html = Kramdown::Document.new(content, {
        :auto_ids             => @config['kramdown']['auto_ids'],
        :footnote_nr          => @config['kramdown']['footnote_nr'],
        :entity_output        => @config['kramdown']['entity_output'],
        :toc_levels           => @config['kramdown']['toc_levels'],
        :smart_quotes         => @config['kramdown']['smart_quotes'],
        :coderay_default_lang => @config['kramdown']['default_lang'],
        :input                => @config['kramdown']['input'],
        :hard_wrap            => @config['kramdown']['hard_wrap']
    }).to_pygs
    return html
  end

end