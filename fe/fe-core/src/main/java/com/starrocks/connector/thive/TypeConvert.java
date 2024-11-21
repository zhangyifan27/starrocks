// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.connector.thive;

import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Type;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaByteObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaFloatObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaIntObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaLongObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaShortObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaStringObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaVoidObjectInspector;

public class TypeConvert {

    /**
     * convert starrocks type to hive type
     */
    public static ObjectInspector fromStarRocksToHiveType(Type type) {
        if (type.isScalarType()) {
            switch (type.getPrimitiveType()) {
                case TINYINT:
                    return javaByteObjectInspector;
                case SMALLINT:
                    return javaShortObjectInspector;
                case INT:
                    return javaIntObjectInspector;
                case BIGINT:
                    return javaLongObjectInspector;
                case FLOAT:
                    return javaFloatObjectInspector;
                case DOUBLE:
                    return javaDoubleObjectInspector;
                case BOOLEAN:
                    return javaBooleanObjectInspector;
                case VARCHAR:
                    return javaStringObjectInspector;
                case NULL_TYPE:
                    return javaVoidObjectInspector;
            }
        }
        throw new RuntimeException("para type [" + type + "] is not supported for now");
    }

    /**
     * convert hive type to starrocks type
     */
    public static Type fromHiveTypeToStarRocks(ObjectInspector objectInspector) {
        ObjectInspector.Category category = objectInspector.getCategory();
        switch (category) {
            case PRIMITIVE:
                checkArgument(objectInspector instanceof PrimitiveObjectInspector);
                PrimitiveObjectInspector primitiveObjectInspector = (PrimitiveObjectInspector) objectInspector;
                return fromPrimitive(primitiveObjectInspector);
            case LIST:
            case MAP:
            case STRUCT:
            default:
                throw new RuntimeException("return type [" + category + "] is not supported for now");
        }
    }

    public static Type fromPrimitive(PrimitiveObjectInspector primitiveObjectInspector) {
        PrimitiveObjectInspector.PrimitiveCategory category = primitiveObjectInspector.getPrimitiveCategory();
        PrimitiveType primitiveType;
        switch (category) {
            case INT:
                primitiveType = PrimitiveType.INT;
                break;
            case BYTE:
                primitiveType = PrimitiveType.TINYINT;
                break;
            case LONG:
                primitiveType = PrimitiveType.BIGINT;
                break;
            case SHORT:
                primitiveType = PrimitiveType.SMALLINT;
                break;
            case FLOAT:
                primitiveType = PrimitiveType.FLOAT;
                break;
            case DOUBLE:
                primitiveType = PrimitiveType.DOUBLE;
                break;
            case BOOLEAN:
                primitiveType = PrimitiveType.BOOLEAN;
                break;
            case STRING:
                return ScalarType.createDefaultCatalogString();
            case VARCHAR:
                int length = ((VarcharTypeInfo) primitiveObjectInspector.getTypeInfo()).getLength();
                return ScalarType.createVarcharType(length);
            default:
                throw new RuntimeException("return type [" + category + "] is not supported for now");
        }
        return ScalarType.createType(primitiveType);
    }

}
