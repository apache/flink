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
package org.apache.flink.table.runtime.functions

import java.io.UnsupportedEncodingException
import java.lang.{Long => JLong}
import java.sql.Timestamp
import java.util.{Calendar, Date, TimeZone}
import java.text.{ParseException, SimpleDateFormat}
import java.util.regex.{MatchResult, Matcher, Pattern}

import org.apache.commons.lang3.StringUtils
import org.apache.flink.table.runtime.functions.utils.ParseUrlUtils
import org.slf4j.LoggerFactory

import scala.annotation.varargs
import java.lang.StringBuilder
import java.nio.charset.StandardCharsets
import java.security.MessageDigest

import org.apache.commons.codec.binary.Hex
import org.apache.flink.table.dataformat.{BinaryString, Decimal}

/**
  * Built-in scalar runtime functions.
  *
  * NOTE: Before you add functions here, check if Calcite provides it in
  * [[org.apache.calcite.runtime.SqlFunctions]]. Furthermore, make sure to implement the function
  * efficiently. Sometimes it makes sense to create a
  * [[org.apache.flink.table.codegen.calls.CallGenerator]] instead to avoid massive object
  * creation and reuse instances.
  */
class ScalarFunctions {}

object ScalarFunctions {
  val LOG = LoggerFactory.getLogger(ScalarFunctions.getClass)
  val regexpPatternCache: ThreadLocalCache[String, Pattern] =
    new ThreadLocalCache[String, Pattern](64) {
      def getNewInstance(regex: String): Pattern = {
        return Pattern.compile(regex)
      }
    }
  val dateFormatterCache: ThreadLocalCache[String, SimpleDateFormat] =
    new ThreadLocalCache[String, SimpleDateFormat](64) {
      def getNewInstance(format: String): SimpleDateFormat = {
        val dateFormat: SimpleDateFormat = new SimpleDateFormat(format)
        dateFormat.setLenient(false)
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"))
        return dateFormat
      }
    }
  val calendarCache: ThreadLocalCache[String, Calendar] =
    new ThreadLocalCache[String, Calendar](1) {
      def getNewInstance(format: String): Calendar = {
        val calendar: Calendar = Calendar.getInstance()
        calendar.setLenient(false)
        return calendar
      }
    }

  def exp(b: Decimal): Double = {
    Math.exp(b.doubleValue())
  }

  def power(a: Double, b: Decimal): Double = {
    Math.pow(a, b.doubleValue())
  }

  def power(a: Decimal, b: Decimal): Double = {
    Math.pow(a.doubleValue(), b.doubleValue())
  }

  def power(a: Decimal, b: Double): Double = {
    Math.pow(a.doubleValue(), b)
  }

  /**
    * Returns the hyperbolic cosine of a big decimal value.
    */
  def cosh(x: Decimal): Double = {
    Math.cosh(x.doubleValue())
  }

  def acos(b0: Decimal): Double = {
    Math.acos(b0.doubleValue())
  }

  def asin(b0: Decimal): Double = {
    Math.asin(b0.doubleValue())
  }

  def atan(b0: Decimal): Double = {
    Math.atan(b0.doubleValue())
  }

  def atan2(b0: Decimal, b1: Decimal): Double = {
    Math.atan2(b0.doubleValue(), b1.doubleValue())
  }

  def sin(b0: Decimal): Double = {
    Math.sin(b0.doubleValue())
  }

  def cos(b0: Decimal): Double = {
    Math.cos(b0.doubleValue())
  }

  def tan(b0: Decimal): Double = {
    Math.tan(b0.doubleValue())
  }

  def cot(b0: Decimal): Double = {
    1.0d / Math.tan(b0.doubleValue())
  }

  def degrees(b0: Decimal): Double = {
    Math.toDegrees(b0.doubleValue())
  }

  def radians(b0: Decimal): Double = {
    Math.toRadians(b0.doubleValue())
  }

  def abs(b0: Decimal): Decimal = {
    b0.abs()
  }

  def floor(b0: Decimal): Decimal = {
    b0.floor()
  }

  def ceil(b0: Decimal): Decimal = {
    b0.ceil()
  }


  /**
    * Returns the string that results from concatenating the arguments.
    */
  @varargs
  def concat(args: String*): String = {
    val sb = new StringBuilder
    var i = 0
    while (i < args.length) {
      if (args(i) != null) {
        sb.append(args(i))
      }
      i += 1
    }
    sb.toString
  }

  /**
    * Returns the string that results from concatenating the arguments and separator.
    **/
  @varargs
  def concat_ws(separator: String, args: String*): String = {
    if (null == separator || "".equals(separator)) {
      return concat(args: _*)
    }

    val sb = new StringBuilder

    var i = 0
    var hasValueAppended = false

    while (i < args.length) {
      if (null != args(i)) {
        if (hasValueAppended) {
          sb.append(separator)
        }
        sb.append(args(i))
        hasValueAppended = true
      }
      i = i + 1
    }
    sb.toString
  }

  /**
    * Returns the natural logarithm of "x".
    */
  def log(x: Double): Double = {
    Math.log(x)
  }

  def log(x: Decimal): Double = {
    log(x.doubleValue())
  }

  /**
    * Calculates the hyperbolic tangent of a big decimal number.
    */
  def tanh(x: Decimal): Double = {
    Math.tanh(x.doubleValue())
  }

  /**
    * Returns the logarithm of "x" with base "base".
    */
  def log(base: Double, x: Double): Double = {
    Math.log(x) / Math.log(base)
  }

  def log(base: Double, x: Decimal): Double = {
    log(base, x.doubleValue())
  }


  def log(base: Decimal, x: Double): Double = {
    log(base.doubleValue(), x)
  }

  def log(base: Decimal, x: Decimal): Double = {
    log(base.doubleValue(), x.doubleValue())
  }

  /**
    * Returns the logarithm of "a" with base 2.
    */
  def log2(x: Double): Double = {
    Math.log(x) / Math.log(2)
  }

  def log2(x: Decimal): Double = {
    log2(x.doubleValue())
  }

  def log10(x: Double): Double = {
    Math.log10(x)
  }

  def log10(x: Decimal): Double = {
    log10(x.doubleValue())
  }

  /**
    * Left padding the string until its length equals n.
    * If accept any null arguments, return null.
    *
    * @param s   target string.
    * @param n   target length.
    * @param pad the padding string.
    */
  def lpad(s: String, n: Int, pad: String): String = {
    if (s == "" || n < 0 || pad == "") {
      if (n < 0) {
        return null
      }
      if (pad == "" && s.length < n) {
        return null
      }
    }
    if (s.length > n) {
      return s.substring(0, n)
    }
    StringUtils.leftPad(s, n, pad)
  }

  /**
    * Right padding the string until its length equals n.
    * If accept any null arguments, return null.
    *
    * @param s   target string.
    * @param n   target length.
    * @param pad the padding string.
    */
  def rpad(s: String, n: Int, pad: String): String = {
    if (s == "" || n < 0 || pad == "") {
      if (n < 0) {
        return null
      }
      if (pad == "" && s.length < n) {
        return null
      }
    }
    if (s.length > n) {
      return s.substring(0, n)
    }
    StringUtils.rightPad(s, n, pad)
  }

  /**
    * Returns the hyperbolic sine of a big decimal value.
    */
  def sinh(x: Decimal): Double = {
    Math.sinh(x.doubleValue())
  }

  /**
    * Repeat target string n times.
    * If accept any null arguments, return null.
    *
    * @param s target string.
    * @param n repeat times.
    * @return result string.
    */
  def repeat(s: String, n: Int): String = {
    if (n == null || s == null) {
      return null
    }
    StringUtils.repeat(s, n)
  }

  /**
    * Reverse target string.
    * If accept any null arguments, return null.
    *
    * @param s target string.
    * @return reversed string.
    */
  def reverse(s: String): String = {
    if (s == null) {
      return null
    }
    StringUtils.reverse(s)
  }

  /**
    * Replaces all instances of search with replace in string.
    * return null if arguments has null.
    *
    * @param s
    * @param search
    * @param replace
    * @return
    */
  def replace(s: String, search: String, replace: String): String = {
    if (s == null || search == null || replace == null) {
      s
    } else {
      s.replace(search, replace)
    }
  }

  /**
    * Split target string with custom separator
    * and pick the index-th(start with 0) result.
    * If accept any null arguments, return null.
    *
    * @param str       target string.
    * @param separator custom separator.
    * @param index     index of the result which you want.
    * @return one of splited results.
    */
  def splitIndex(str: String, separator: String, index: Int): String = {
    if ((str == null) || (separator == null) || (index == null) || index < 0) {
      return null
    }

    val values: Array[String] = StringUtils.splitByWholeSeparatorPreserveAllTokens(str, separator)
    if (index >= values.length) {
      return null
    }
    values(index)
  }

  /**
    * Split target string with custom separator
    * and pick the index-th(start with 0) result.
    * If accept any null arguments, return null.
    *
    * @param str   target string.
    * @param ascii ascii code of custom separator.
    * @param index index of the result which you want.
    * @return one of splited results.
    */
  def splitIndex(str: String, ascii: Int, index: Int): String = {
    if (ascii == null || ascii > 255 || ascii < 1) {
      return null
    }
    val separator: String = new String(Array[Byte](ascii.byteValue))
    splitIndex(str, separator, index)
  }

  /**
    * Returns the string subject with all occurrences of
    * the regular expression pattern replaced by the string replace.
    * If no occurrences are found, then subject is returned as is.
    * If accept any null arguments, return null.
    *
    * @param s           string subject.
    * @param regex       regular expression pattern.
    * @param replacement the string replace.
    * @return replace result.
    */
  def regExpReplace(s: String, regex: String, replacement: String): String = {
    if (s == null || regex == null || replacement == null) {
      return null
    }
    if (regex == "") {
      return s
    }
    try {
      val sb: StringBuffer = new StringBuffer
      val m: Matcher = regexpPatternCache.get(regex).matcher(s)
      while (m.find) {
        m.appendReplacement(sb, replacement)
      }
      m.appendTail(sb)

      sb.toString
    }
    catch {
      case e: Exception => {
        LOG.error("Exception in regExpReplace", e)
        null
      }
    }
  }

  /**
    * Extracts a group that matches regexp.
    * If accept any null arguments, return null.
    *
    * @param s            string subject.
    * @param regex        regular expression pattern.
    * @param extractIndex the group index to extract.
    * @return replace result.
    */
  def regExpExtract(s: String, regex: String, extractIndex: Long): String = {
    if (s == null || StringUtils.isEmpty(regex) || extractIndex == null) {
      LOG.error("regexp_extract(s, p, o) return NULL when met NULL parameter.")
      return null
    }

    if (extractIndex < 0) {
      LOG.error("o of 'regexp_extract(s, p, o)' must >= 0, but o == " + extractIndex)
      return null
    }

    try {
      val p: Pattern = regexpPatternCache.get(regex)
      val m: Matcher = p.matcher(s)
      if (m.find) {
        val mr: MatchResult = m.toMatchResult
        return mr.group(extractIndex.toInt)
      }
      null
    } catch {
      case e: Exception => {
        LOG.error("Exception when compile and match", e)
        null
      }
    }
  }

  /**
    * Returns a string extracted with a specified regular expression and
    * a optional regex match group index.
    */
  def regExpExtract(str: String, regex: String): String = {
    regExpExtract(str, regex, 0)
  }

  def keyValue(
    str: BinaryString,
    split1: BinaryString,
    split2: BinaryString,
    keyName: BinaryString): BinaryString = {
    if (str == null || str.numBytes() == 0) {
      return null;
    }
    if (split1!= null && split1.numBytes() == 1 && split2 != null && split2.numBytes() == 1) {
      str.keyValue(split1.getByte(0), split2.getByte(0), keyName)
    } else {
      BinaryString.fromString(
        keyValue(
          BinaryString.safeToString(str),
          BinaryString.safeToString(split1),
          BinaryString.safeToString(split2),
          BinaryString.safeToString(keyName)))
    }
  }

  /**
    * Parse target string as key-value string and
    * return the value matches key name.
    * If accept any null arguments, return null.
    * example:
    * keyvalue('k1=v1;k2=v2', ';', '=', 'k2') = 'v2'
    * keyvalue('k1:v1,k2:v2', ',', ':', 'k3') = NULL
    *
    * @param str     target string.
    * @param split1  separator between key-value tuple.
    * @param split2  separator between key and value.
    * @param keyName name of the key whose value you want return.
    * @return target value.
    */
  def keyValue(str: String, split1: String, split2: String, keyName: String): String = {
    try {
      if (StringUtils.isEmpty(str)) {
        return null
      }
      val values1: Array[String] = StringUtils.split(str, split1)
      var i: Int = 0
      while (i < values1.length) {
        if (values1(i) != null && ("" != values1(i))) {
          val keyValueArr: Array[String] = StringUtils.split(values1(i), split2)
          if (keyValueArr != null
              && keyValueArr.length == 2
              && keyValueArr(0) == keyName) {
            return keyValueArr(1)
          }
        }
        i += 1
      }
      null
    }
    catch {
      case e: Exception => {
        LOG.error("exception when parse keyvalue", e)
        null
      }
    }
  }

  /**
    * Calculate the hash value of a given string.
    *
    * @param algorithm  message digest algorithm.
    * @param str        string to hash.
    * @return           hash value of string.
    */
  def hash(algorithm: String, str: String): String = {
    hash(algorithm, str, "")
  }

  /**
    * Calculate the hash value of a given string.
    *
    * @param algorithm    message digest algorithm.
    * @param str          string to hash.
    * @param charsetName  charset of string.
    * @return           hash value of string.
    */
  def hash(algorithm: String, str: String, charsetName: String): String = {
    Hex.encodeHexString(
      MessageDigest.getInstance(algorithm)
        .digest(strToBytesWithCharset(str, charsetName)))
  }

  /**
    * Calculate the hash value of a given string.
    *
    * @param md   message digest instance.
    * @param str  string to hash.
    * @return hash value of string.
    */
  def hash(md: MessageDigest, str: String): String = {
    hash(md, str, "")
  }

  /**
    * Calculate the hash value of a given string.
    *
    * @param md           message digest instance.
    * @param str          string to hash.
    * @param charsetName  charset of string.
    * @return hash value of string.
    */
  def hash(md: MessageDigest, str: String, charsetName: String): String = {
    Hex.encodeHexString(
      md.digest(strToBytesWithCharset(str, charsetName)))
  }

  private[flink] def strToBytesWithCharset(str: String, charsetName: String) = {
    var bArr: Array[Byte] = null
    if (!StringUtils.isEmpty(charsetName)) {
      try {
        bArr = str.getBytes(charsetName)
      }
      catch {
        case e: UnsupportedEncodingException =>
          LOG.error("Unsupported encoding:" + charsetName + ",use system default", e)
          bArr = null
      }
    }
    if (bArr == null) {
      bArr = str.getBytes
    }
    bArr
  }

  /**
    * Parse url and return various components of the URL.
    * If accept any null arguments, return null.
    *
    * @param urlStr        URL string.
    * @param partToExtract determines which components would return.
    *                      accept values:
    *                      HOST,PATH,QUERY,REF,
    *                      PROTOCOL,FILE,AUTHORITY,USERINFO
    * @return target value.
    */
  def parseUrl(urlStr: String, partToExtract: String): String = {
    ParseUrlUtils.parseUrl(urlStr, partToExtract)
  }

  /**
    * Parse url and return various parameter of the URL.
    * If accept any null arguments, return null.
    *
    * @param urlStr        URL string.
    * @param partToExtract must be QUERY, or return null.
    * @param key           parameter name.
    * @return target value.
    */
  def parseUrl(urlStr: String, partToExtract: String, key: String): String = {
    ParseUrlUtils.parseUrl(urlStr, partToExtract, key)
  }


  /**
    * Returns current timestamp(count by seconds).
    *
    * @return current timestamp.
    */
  def now: Long = {
    val date: Date = new Date
    date.getTime / 1000
  }

  /**
    * Returns current timestamp(count by seconds) with offset.
    *
    * @param offset value(count by seconds).
    * @return current timestamp with offset.
    */
  def now(offset: Long): Long = {
    val date: Date = new Date
    date.getTime / 1000 + offset
  }


  /**
    * Convert unix timestamp to datetime string.
    * If accept any null arguments, return null.
    *
    * @param unixtime unix timestamp.
    * @param format   datetime string format.
    * @return datetime string.
    */
  def fromUnixtime(unixtime: Long, format: String): String = {
    if (unixtime == null || format == null) {
      return null
    }
    try {
      val formatter = dateFormatterCache.get(format)
      val date = new Date(unixtime * 1000)
      formatter.format(date);
    } catch {
      case e: ParseException => {
        LOG.error("exception when formatting string: " + unixtime, e)
        null
      }
      case e: IllegalArgumentException => {
        LOG.error("exception when parse format string: " + format, e)
        null
      }
    }
  }

  /**
    * Convert unix timestamp to datetime string
    * with format yyyy-MM-dd HH:mm:ss
    * If accept any null arguments, return null.
    *
    * @param unixtime unix timestamp.
    * @return datetime string.
    */
  def fromUnixtime(unixtime: Long): String = {
    fromUnixtime(unixtime, "yyyy-MM-dd HH:mm:ss")
  }

  /**
    * Do subtraction on date string
    * If accept any null arguments, return null.
    *
    * @param dateString1 formatted date string.
    *                    support format: any string start with yyyy-MM-dd
    * @param days        days count you want to subtract.
    * @return datetime string.
    */
  def dateSub(dateString1: String, days: Int): String = {
    if (dateString1 == null) {
      return null
    }
    val calendar = calendarCache.get("calendar")
    val formatter: SimpleDateFormat = dateFormatterCache.get("yyyy-MM-dd")
    try {
      calendar.setTime(formatter.parse(dateString1))
      calendar.add(Calendar.DAY_OF_MONTH, -days)
      val newDate: Date = calendar.getTime
      formatter.format(newDate)
    }
    catch {
      case e: ParseException => {
        LOG.error("Parse exception in dateSub, input:" + dateString1, e)
        null
      }
    }
  }

  /**
    * Do subtraction on timestamp
    * If accept any null arguments, return null.
    *
    * @param t    the timestamp.
    * @param days days count you want to subtract.
    * @return datetime string.
    */
  def dateSub(t: Long, days: Int): String = {
    val calendar = calendarCache.get("calendar")
    val formatter = dateFormatterCache.get("yyyy-MM-dd")
    calendar.setTime(new Timestamp(t))
    calendar.add(Calendar.DAY_OF_MONTH, -days)
    val newDate = calendar.getTime()
    formatter.format(newDate)
  }

  /**
    * Do addition on date string
    * If accept any null arguments, return null.
    *
    * @param dateString1 formatted date string.
    *                    support format: any string start with yyyy-MM-dd
    * @param days        days count you want to add.
    * @return datetime string.
    */
  def dateAdd(dateString1: String, days: Int): String = {
    if (dateString1 == null) {
      return null
    }
    val calendar = calendarCache.get("calendar")
    val formatter = dateFormatterCache.get("yyyy-MM-dd")
    try {
      calendar.setTime(formatter.parse(dateString1))
      calendar.add(Calendar.DAY_OF_MONTH, days)
      val newDate: Date = calendar.getTime
      formatter.format(newDate)
    }
    catch {
      case e: ParseException => {
        LOG.error("Parse exception in dateAdd, input:" + dateString1, e)
        null
      }
    }
  }

  /**
    * Do addition on timestamp
    * If accept any null arguments, return null.
    *
    * @param t    the timestamp.
    * @param days days count you want to add.
    * @return datetime string.
    */
  def dateAdd(t: Long, days: Int): String = {
    val calendar = calendarCache.get("calendar")
    val formatter = dateFormatterCache.get("yyyy-MM-dd")
    calendar.setTime(new Timestamp(t))
    calendar.add(Calendar.DAY_OF_MONTH, days)
    val newDate = calendar.getTime()
    formatter.format(newDate)
  }

  def divideInteger(a: Integer, b: Integer): Integer = {
    if ((a == null) || (b == null)) {
      return null.asInstanceOf[Integer]
    }
    a / b
  }

  def subString(s: String, p: Long, l: Long): String = {
    if (s == null) {
      LOG.error(
        "return null due to str of 'substring(str, start, len)' is null."
      )
      return null
    }
    if (l < 0) {
      LOG.error(
        "len of 'substring(str, start, len)' must be >= 0 and Int type, but len = " + l
      )
      return null
    }
    if (l > Int.MaxValue || p < Int.MinValue || p > Int.MaxValue) {
      LOG.error(
        "len or pos of 'substring(str, start, len)' must be Int type, but len = " + l + ", pos=" + p
      )
      return null
    }
    val len = l.toInt
    val pos = p.toInt
    if (s.equals("")) {
      return ""
    }

    var start: Int = 0
    var end: Int = 0

    if (pos > 0) {
      start = pos - 1
      if (start >= s.length) {
        return ""
      }
    }
    else if (pos < 0) {
      start = s.length + pos
      if (start < 0) {
        return ""
      }
    }
    else {
      start = 0
    }

    if ((s.length - start) < len) {
      end = s.length
    }
    else {
      end = start + len
    }
    s.substring(start, end)
  }

  def subString(s: String, p: Long): String = {
    subString(s, p, Int.MaxValue)
  }

  def chr(chr: Long): String = {
    if (chr < 0) {
      ""
    } else if ((chr & 0xFF) == 0) {
      Character.MIN_VALUE.toString
    } else {
      (chr & 0xFF).toChar.toString
    }
  }

  def overlay(s: String, r: String, start: Long, length: Long): String = {
    // the semantic is like INSERT function in MySQL
    if (s == null || r == null) {
      null
    }
    else if (start <= 0 || start > s.length) {
      s
    }
    else {
      val sb = new StringBuilder
      val start_ = start.toInt
      val len = length.toInt
      sb.append(s.substring(0, start_ - 1))
      sb.append(r)
      if ((start_ + len) <= s.length && len > 0) {
        sb.append(s.substring(start_ - 1 + len))
      }
      sb.toString()
    }
  }

  def overlay(s: String, r: String, start: Long): String = {
    overlay(s, r, start, r.length)
  }

  def position(seek: BinaryString, s: BinaryString): Int = position(seek, s, 1)

  def position(seek: BinaryString, s: BinaryString, from: Int): Int = s.indexOf(seek, from - 1) + 1

  def instr(
      str: BinaryString,
      subString: BinaryString,
      startPosition: Int,
      nthAppearance: Int): Int = {
    if (nthAppearance <= 0) {
      throw new IllegalArgumentException("nthAppearance must be positive!")
    }
    if (startPosition == 0) {
      0
    } else if (startPosition > 0) {
      var startIndex = startPosition
      var index = 0
      for (appearance <- 1 to nthAppearance) {
        index = str.indexOf(subString, startIndex - 1) + 1
        if (index == 0) {
          return 0
        }
        startIndex = index + 1
      }
      index
    } else {
      val pos = instr(str.reverse, subString.reverse, -startPosition, nthAppearance)
      if (pos == 0) 0 else str.numChars() + 2 - pos - subString.numChars()
    }
  }

  /**
    * Returns the hex string of a long argument.
    */
  def hex(x: Long): String = JLong.toHexString(x).toUpperCase()

  /**
    * Returns the hex string of a string argument.
    */
  def hex(x: String): String = Hex.encodeHexString(x.getBytes(StandardCharsets.UTF_8)).toUpperCase()

}
