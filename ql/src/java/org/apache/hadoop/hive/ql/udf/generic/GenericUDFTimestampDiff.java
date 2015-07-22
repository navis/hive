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

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;
import org.joda.time.DateTime;

/**
 *
 * GenericUDFTimestampDiff
 *
 * Example usage:
 * ... TIMESTAMPDIFF(TYPE, DAY_TIME_INTERVAL) ...
 *
 */
@Description(name = "TIMESTAMPDIFF",
value = "TIMESTAMPDIFF(int return_type, interval_day_time interval) - Returns int")
public class GenericUDFTimestampDiff extends GenericUDF {

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

  private transient int type;

  private transient ObjectInspectorConverters.Converter converter;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length < 2) {
      throw new UDFArgumentLengthException(
          "The function TIMESTAMPDIFF requires at least one argument, got "
          + arguments.length);
    }

    if (!(arguments[0] instanceof ConstantObjectInspector) || !(arguments[0] instanceof IntObjectInspector)) {
      throw new UDFArgumentException(
              "The function TIMESTAMPDIFF takes constant numeric value for first argument");
    }

    type = ((IntWritable)((ConstantObjectInspector)arguments[0]).getWritableConstantValue()).get();

    if (arguments[1].getCategory() != ObjectInspector.Category.PRIMITIVE) {
      throw new UDFArgumentException(
              "The function TIMESTAMPDIFF takes only interval/long type");
    }
    switch (((PrimitiveObjectInspector)arguments[1]).getPrimitiveCategory()) {
      case SHORT: case INT: case LONG: case DECIMAL:
        converter = ObjectInspectorConverters.getConverter(arguments[1],
                PrimitiveObjectInspectorFactory.javaLongObjectInspector);
        break;
      default:
        throw new UDFArgumentException(
                "The function TIMESTAMPDIFF takes only date,time interval/numeric type");
    }

    return PrimitiveObjectInspectorFactory.javaLongObjectInspector;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    Object o0 = arguments[1].get();
    if (o0 == null) {
      return null;
    }
    long totalSeconds = (Long) converter.convert(o0);
    switch (type) {
      case 2: return totalSeconds;
      case 4: return totalSeconds / 60; // min
      case 8: return totalSeconds / 60 / 60;  // hour
      case 16: return totalSeconds / 60 / 60 / 24;  // day
      case 32: return totalSeconds / 60 / 60 / 24 / 7;  // week
      case 64: return totalSeconds / 60 / 60 / 24 / 30; // month
      case 128: return totalSeconds / 60 / 60 / 24 / 90;  // quarter
      case 256: return totalSeconds / 60 / 60 / 24 / 365; // year
      default: throw new HiveException("Unsupported type value " + type);
    }
  }

  @Override
  public String getDisplayString(String[] children) {
    return getStandardDisplayString(getFuncName(), children);
  }

  @Override
  protected String getFuncName() {
    return "TIMESTAMPDIFF";
  }
}
