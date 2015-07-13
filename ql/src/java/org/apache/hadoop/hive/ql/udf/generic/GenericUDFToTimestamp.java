/**
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
package org.apache.hadoop.hive.ql.udf.generic;

import org.apache.hadoop.hive.common.ObjectPair;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 * GenericUDFTimestamp
 *
 * Example usage:
 * ... CAST(<Timestamp string> as TIMESTAMP) ...
 *
 * Creates a TimestampWritable object using PrimitiveObjectInspectorConverter
 *
 */
@Description(name = "TO_TIMESTAMP",
value = "TO_TIMESTAMP(string input, string format, string optional_calc) - Returns timestamp")
public class GenericUDFToTimestamp extends GenericUDF {

  private enum Calc {
    y { DateTime add(DateTime dateTime, int numeric) { return dateTime.plusYears(numeric);} },
    M { DateTime add(DateTime dateTime, int numeric) { return dateTime.plusMonths(numeric);} },
    d { DateTime add(DateTime dateTime, int numeric) { return dateTime.plusDays(numeric);} },
    H { DateTime add(DateTime dateTime, int numeric) { return dateTime.plusHours(numeric);} },
    m { DateTime add(DateTime dateTime, int numeric) { return dateTime.plusMinutes(numeric);} },
    s { DateTime add(DateTime dateTime, int numeric) { return dateTime.plusSeconds(numeric);} },
    S { DateTime add(DateTime dateTime, int numeric) { return dateTime.plusMillis(numeric);} },
    ;
    abstract DateTime add(DateTime dateTime, int numeric);
  }

  private transient DateTimeFormatter formatter;

  private transient PrimitiveObjectInspector inputOI;
  private transient List<ObjectPair<Calc, Integer>> calulations;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length < 1) {
      throw new UDFArgumentLengthException(
          "The function TO_TIMESTAMP requires at least one argument, got "
          + arguments.length);
    }

    try {
      inputOI = (PrimitiveObjectInspector) arguments[0];
    } catch (ClassCastException e) {
      throw new UDFArgumentException(
          "The function TO_TIMESTAMP takes only string input");
    }

    String format;
    try {
      format = ((ConstantObjectInspector) arguments[1]).getWritableConstantValue().toString();
    } catch (ClassCastException e) {
      throw new UDFArgumentException(
              "The function TO_TIMESTAMP takes only string format");
    }

    formatter = DateTimeFormat.forPattern(format);

    if (arguments.length > 2) {
      String calc;
      try {
        calc = ((ConstantObjectInspector) arguments[2]).getWritableConstantValue().toString();
      } catch (ClassCastException e) {
        throw new UDFArgumentException(
                "The function TO_TIMESTAMP takes only string input");
      }
      calulations = new ArrayList<ObjectPair<Calc, Integer>>();
      Pattern pattern = Pattern.compile("([+-]?\\d+)([yMdHmsS])$");
      for (String aCalc : calc.split(",")) {
        Matcher matcher = pattern.matcher(aCalc);
        if (!matcher.matches()) {
          throw new UDFArgumentException("Invalid third argument " + aCalc);
        }
        try {
          int value = Integer.valueOf(matcher.group(1));
          Calc type = Calc.valueOf(matcher.group(2));
          calulations.add(new ObjectPair<Calc, Integer>(type, value));
        } catch (RuntimeException e) {
          throw new UDFArgumentException("Invalid third argument " + aCalc);
        }
      }
    } else {
      calulations = Collections.emptyList();
    }

    return PrimitiveObjectInspectorFactory.writableTimestampObjectInspector;
  }

  private transient final TimestampWritable result = new TimestampWritable();

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    Object o0 = arguments[0].get();
    if (o0 == null) {
      return null;
    }
    DateTime dateTime = DateTime.parse(PrimitiveObjectInspectorUtils.getString(o0, inputOI), formatter);
    for (ObjectPair<Calc, Integer> calc : calulations) {
      dateTime = calc.getFirst().add(dateTime, calc.getSecond());
    }
    result.setTime(dateTime.getMillis());
    return result;
  }

  @Override
  public String getDisplayString(String[] children) {
    return getStandardDisplayString(getFuncName(), children);
  }

  @Override
  protected String getFuncName() {
    return "TO_TIMESTAMP";
  }
}
