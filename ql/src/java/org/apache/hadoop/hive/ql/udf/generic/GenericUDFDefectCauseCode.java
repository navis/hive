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

import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveVarcharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaHiveVarcharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorConverter.StringConverter;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;
import org.apache.hadoop.io.BytesWritable;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * GenericUDFConcat.
 */
@Description(name = "get_defect_cause_code",
        value = "_FUNC_(str1, str2, ... strN) - returns the concatenation of str1, str2, ... strN or " +
                "_FUNC_(bin1, bin2, ... binN) - returns the concatenation of bytes in binary data " +
                " bin1, bin2, ... binN",
        extended = "Returns NULL if any argument is NULL.\n"
                + "Example:\n"
                + "  > SELECT _FUNC_('abc', 'def') FROM src LIMIT 1;\n"
                + "  'abcdef'")
public class GenericUDFDefectCauseCode extends GenericUDF {

    private transient StringConverter[] stringConverters;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {

        if (arguments.length != 4) {
            throw new UDFArgumentLengthException("four argument");
        }
        stringConverters = new StringConverter[arguments.length];
        for (int i = 0; i < arguments.length; i++) {
            if (arguments[i].getCategory() != Category.PRIMITIVE) {
                throw new UDFArgumentException();
            }
            stringConverters[i] = new StringConverter((PrimitiveObjectInspector) arguments[1]);
        }

        return new JavaHiveVarcharObjectInspector(new VarcharTypeInfo(10));
    }

    private transient final HiveVarchar result = new HiveVarchar();

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        if (arguments[0] == null) {
            return null;
        }
        String p_supervision_department_code = (String)stringConverters[0].convert(arguments[0]);
        if (p_supervision_department_code.equals("SQA")) {
            result.setValue("PA", 10);
        } else if (p_supervision_department_code.equals("PQA")) {
            result.setValue("PQ", 10);
        } else if (p_supervision_department_code.equals("DEVELOPMENT")) {
            result.setValue("DV", 10);
        } else if (p_supervision_department_code.equals("MQA")) {
            result.setValue("MQA", 10);
        } else if (p_supervision_department_code.equals("MODULE") || p_supervision_department_code.equals("CONTRACTOR")) {
            if (arguments[1] == null || arguments[2] == null) {
                return null;
            }
            String p_repair_quality_level1_code = (String)stringConverters[1].convert(arguments[1]);
            String p_repair_quality_level2_code = (String)stringConverters[2].convert(arguments[2]);
            String p_responsibility_department_code = (String)stringConverters[3].convert(arguments[3]);
            if (!SET1.contains(p_repair_quality_level1_code) && p_repair_quality_level2_code.equals("B06")) {
                result.setValue("FM", 10);
            } else if (!p_repair_quality_level2_code.equals("B06") ||
                    (p_repair_quality_level2_code.equals("B06") && SET1.contains(p_repair_quality_level1_code)) &&
                    !SET2.contains(p_responsibility_department_code)) {
                result.setValue("OP", 10);
            } else if (!p_repair_quality_level2_code.equals("B06") ||
                    (p_repair_quality_level2_code.equals("B06") && SET1.contains(p_repair_quality_level1_code)) &&
                    SET3.contains(p_responsibility_department_code)) {
                result.setValue("EQ", 10);
            } else if (p_responsibility_department_code.equals("RD151") ||
                    p_responsibility_department_code.equals("RD152")) {
                result.setValue("PA", 10);
            } else {
                return null;
            }
        } else {
            return null;
        }
        return result;
    }

    private static final Set<String> SET1 = new HashSet<String>(Arrays.asList("A0J", "A1E", "A1S", "A1T"));
    private static final Set<String> SET2 = new HashSet<String>(Arrays.asList("RD004", "RD005", "RD006", "RD047",
            "RD060", "RD066", "RD068", "RD141", "RD142", "RD143", "RD149", "RD150", "RD151", "RD152"));
    private static final Set<String> SET3 = new HashSet<String>(Arrays.asList("RD004", "RD005", "RD006", "RD047",
            "RD060", "RD066", "RD068", "RD141", "RD142", "RD143", "RD149", "RD150"));

    @Override
    public String getDisplayString(String[] children) {
        StringBuilder sb = new StringBuilder();
        sb.append("get_defect_cause_code (");
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
