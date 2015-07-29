/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
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
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorConverter.StringConverter;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

/**
 * GenericUDFConcat.
 */
@Description(name = "right",
        value = "_FUNC_(str1, str2, ... strN) - returns the concatenation of str1, str2, ... strN or " +
                "_FUNC_(bin1, bin2, ... binN) - returns the concatenation of bytes in binary data " +
                " bin1, bin2, ... binN",
        extended = "Returns NULL if any argument is NULL.\n"
                + "Example:\n"
                + "  > SELECT _FUNC_('abc', 'def') FROM src LIMIT 1;\n"
                + "  'abcdef'")
public class GenericUDFRight extends GenericUDF {

    private transient StringConverter inputOI;
    private transient IntObjectInspector lengthOI;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {

        if (arguments.length != 2) {
            throw new UDFArgumentLengthException("two argument");
        }
        if (arguments[0].getCategory() != Category.PRIMITIVE ||
                !(arguments[1] instanceof IntObjectInspector)) {
            throw new UDFArgumentException();
        }
        inputOI = new StringConverter((PrimitiveObjectInspector) arguments[0]);
        lengthOI = (IntObjectInspector) arguments[1];

        return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        if (arguments[0] == null || arguments[1] == null) {
            return null;
        }
        String input = (String) inputOI.convert(arguments[0]);
        int length = lengthOI.get(arguments[1]);
        return input.substring(input.length() - length + 1, length);
    }

    @Override
    public String getDisplayString(String[] children) {
        StringBuilder sb = new StringBuilder();
        sb.append("right (");
        if (children.length > 0) {
            sb.append(children[0]);
            for (int i = 1; i < children.length; i++) {
                sb.append(", ");
                sb.append(children[i]);
            }
        }
        sb.append(")");
        return sb.toString();
    }
}
