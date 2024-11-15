/**
* Tencent is pleased to support the open source community by making TDW available.
* Copyright (C) 2014 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the Apache License, Version 2.0 (the "License"); you may not use 
* this file except in compliance with the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software distributed 
* under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS 
* OF ANY KIND, either express or implied. See the License for the specific language governing
* permissions and limitations under the License.
*/
package FormatStorage1;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import Comm.ConstVar;

public class TypeConvertUtil {
  
  private final static Log LOG = LogFactory.getLog(TypeConvertUtil.class);

  interface TypeConvert {
    Object convert(Object data);
  }

  static TypeConvert[][] typeConverts = new TypeConvert[11][11];
  static {
    typeConverts[ConstVar.FieldType_Short][ConstVar.FieldType_Byte] = new TypeConvert() {
      @Override
      public Object convert(Object data) {
        Short d = (Short) data;
        byte res = d.byteValue();
        return res == d.shortValue() ? res : (d > 0 ? Byte.MAX_VALUE
            : Byte.MIN_VALUE);
      }
    };
    typeConverts[ConstVar.FieldType_Int][ConstVar.FieldType_Byte] = new TypeConvert() {
      @Override
      public Object convert(Object data) {
        Integer d = (Integer) data;
        byte res = d.byteValue();
        return res == d.intValue() ? res : (d > 0 ? Byte.MAX_VALUE
            : Byte.MIN_VALUE);
      }
    };
    typeConverts[ConstVar.FieldType_Long][ConstVar.FieldType_Byte] = new TypeConvert() {
      @Override
      public Object convert(Object data) {
        Long d = (Long) data;
        byte res = d.byteValue();
        return res == d.longValue() ? res : (d > 0 ? Byte.MAX_VALUE
            : Byte.MIN_VALUE);
      }
    };
    typeConverts[ConstVar.FieldType_Float][ConstVar.FieldType_Byte] = new TypeConvert() {
      @Override
      public Object convert(Object data) {
        Float d = (Float) data;
        byte res = d.byteValue();
        int resi = d.intValue();
        return res == resi ? res : (resi > 0 ? Byte.MAX_VALUE : Byte.MIN_VALUE);
      }
    };
    typeConverts[ConstVar.FieldType_Double][ConstVar.FieldType_Byte] = new TypeConvert() {
      @Override
      public Object convert(Object data) {
        Double d = (Double) data;
        byte res = d.byteValue();
        int resi = d.intValue();
        return res == resi ? res : (resi > 0 ? Byte.MAX_VALUE : Byte.MIN_VALUE);
      }
    };
    typeConverts[ConstVar.FieldType_String][ConstVar.FieldType_Byte] = new TypeConvert() {
      @Override
      public Object convert(Object data) {
        try {
          return Byte.parseByte((String) data);
        } catch (NumberFormatException nfe) {
          return null;
        }
      }
    };
    typeConverts[ConstVar.FieldType_Byte][ConstVar.FieldType_Short] = new TypeConvert() {
      @Override
      public Object convert(Object data) {
        return ((Byte) data).shortValue();
      }
    };
    typeConverts[ConstVar.FieldType_Int][ConstVar.FieldType_Short] = new TypeConvert() {
      @Override
      public Object convert(Object data) {
        Integer d = (Integer) data;
        short res = d.shortValue();
        return res == d ? res : (d > 0 ? Short.MAX_VALUE : Short.MIN_VALUE);
      }
    };
    typeConverts[ConstVar.FieldType_Long][ConstVar.FieldType_Short] = new TypeConvert() {
      @Override
      public Object convert(Object data) {
        Long d = (Long) data;
        short res = d.shortValue();
        return res == d ? res : (d > 0 ? Short.MAX_VALUE : Short.MIN_VALUE);
      }
    };
    typeConverts[ConstVar.FieldType_Float][ConstVar.FieldType_Short] = new TypeConvert() {
      @Override
      public Object convert(Object data) {
        short res = ((Float) data).shortValue();
        int resi = ((Float) data).intValue();
        return res == resi ? res : (resi > 0 ? Short.MAX_VALUE
            : Short.MIN_VALUE);
      }
    };
    typeConverts[ConstVar.FieldType_Double][ConstVar.FieldType_Short] = new TypeConvert() {
      @Override
      public Object convert(Object data) {
        short res = ((Double) data).byteValue();
        int resi = ((Double) data).intValue();
        return res == resi ? res : (resi > 0 ? Short.MAX_VALUE
            : Short.MIN_VALUE);
      }
    };
    typeConverts[ConstVar.FieldType_String][ConstVar.FieldType_Short] = new TypeConvert() {
      @Override
      public Object convert(Object data) {
        try {
          return Short.parseShort((String) data);
        } catch (NumberFormatException nfe) {
          return null;
        }
      }
    };
    typeConverts[ConstVar.FieldType_Byte][ConstVar.FieldType_Int] = new TypeConvert() {
      @Override
      public Object convert(Object data) {
        return ((Byte) data).intValue();
      }
    };
    typeConverts[ConstVar.FieldType_Short][ConstVar.FieldType_Int] = new TypeConvert() {
      @Override
      public Object convert(Object data) {
        return ((Short) data).intValue();
      }
    };
    typeConverts[ConstVar.FieldType_Long][ConstVar.FieldType_Int] = new TypeConvert() {
      @Override
      public Object convert(Object data) {
        Long d = (Long) data;
        int res = d.intValue();
        return res == d.longValue() ? res : (d > 0 ? Integer.MAX_VALUE
            : Integer.MIN_VALUE);
      }
    };
    typeConverts[ConstVar.FieldType_Float][ConstVar.FieldType_Int] = new TypeConvert() {
      @Override
      public Object convert(Object data) {
        return ((Float) data).intValue();
      }
    };
    typeConverts[ConstVar.FieldType_Double][ConstVar.FieldType_Int] = new TypeConvert() {
      @Override
      public Object convert(Object data) {
        return ((Double) data).intValue();
      }
    };
    typeConverts[ConstVar.FieldType_String][ConstVar.FieldType_Int] = new TypeConvert() {
      @Override
      public Object convert(Object data) {
        try {
          return Integer.parseInt((String) data);
        } catch (NumberFormatException nfe) {
          return null;
        }
      }
    };
    typeConverts[ConstVar.FieldType_Byte][ConstVar.FieldType_Long] = new TypeConvert() {
      @Override
      public Object convert(Object data) {
        return ((Byte) data).longValue();
      }
    };
    typeConverts[ConstVar.FieldType_Short][ConstVar.FieldType_Long] = new TypeConvert() {
      @Override
      public Object convert(Object data) {
        return ((Short) data).longValue();
      }
    };
    typeConverts[ConstVar.FieldType_Int][ConstVar.FieldType_Long] = new TypeConvert() {
      @Override
      public Object convert(Object data) {
        return ((Integer) data).longValue();
      }
    };
    typeConverts[ConstVar.FieldType_Float][ConstVar.FieldType_Long] = new TypeConvert() {
      @Override
      public Object convert(Object data) {
        return ((Float) data).longValue();
      }
    };
    typeConverts[ConstVar.FieldType_Double][ConstVar.FieldType_Long] = new TypeConvert() {
      @Override
      public Object convert(Object data) {
        return ((Double) data).longValue();
      }
    };
    typeConverts[ConstVar.FieldType_String][ConstVar.FieldType_Long] = new TypeConvert() {
      @Override
      public Object convert(Object data) {
        try {
          return Long.parseLong((String) data);
        } catch (NumberFormatException nfe) {
          return null;
        }
      }
    };
    typeConverts[ConstVar.FieldType_Byte][ConstVar.FieldType_Float] = new TypeConvert() {
      @Override
      public Object convert(Object data) {
        return ((Byte) data).floatValue();
      }
    };
    typeConverts[ConstVar.FieldType_Short][ConstVar.FieldType_Float] = new TypeConvert() {
      @Override
      public Object convert(Object data) {
        return ((Short) data).floatValue();
      }
    };
    typeConverts[ConstVar.FieldType_Int][ConstVar.FieldType_Float] = new TypeConvert() {
      @Override
      public Object convert(Object data) {
        return ((Integer) data).floatValue();
      }
    };
    typeConverts[ConstVar.FieldType_Long][ConstVar.FieldType_Float] = new TypeConvert() {
      @Override
      public Object convert(Object data) {
        return ((Long) data).floatValue();
      }
    };
    typeConverts[ConstVar.FieldType_Double][ConstVar.FieldType_Float] = new TypeConvert() {
      @Override
      public Object convert(Object data) {
        return ((Double) data).floatValue();
      }
    };
    typeConverts[ConstVar.FieldType_String][ConstVar.FieldType_Float] = new TypeConvert() {
      @Override
      public Object convert(Object data) {
        try {
          return Float.parseFloat((String) data);
        } catch (NumberFormatException nfe) {
          return null;
        }
      }
    };
    typeConverts[ConstVar.FieldType_Byte][ConstVar.FieldType_Double] = new TypeConvert() {
      @Override
      public Object convert(Object data) {
        return ((Byte) data).doubleValue();
      }
    };
    typeConverts[ConstVar.FieldType_Short][ConstVar.FieldType_Double] = new TypeConvert() {
      @Override
      public Object convert(Object data) {
        return ((Short) data).doubleValue();
      }
    };
    typeConverts[ConstVar.FieldType_Int][ConstVar.FieldType_Double] = new TypeConvert() {
      @Override
      public Object convert(Object data) {
        return ((Integer) data).doubleValue();
      }
    };
    typeConverts[ConstVar.FieldType_Long][ConstVar.FieldType_Double] = new TypeConvert() {
      @Override
      public Object convert(Object data) {
        return ((Long) data).doubleValue();
      }
    };
    typeConverts[ConstVar.FieldType_Float][ConstVar.FieldType_Double] = new TypeConvert() {
      @Override
      public Object convert(Object data) {
        return ((Float) data).doubleValue();
      }
    };
    typeConverts[ConstVar.FieldType_String][ConstVar.FieldType_Double] = new TypeConvert() {
      @Override
      public Object convert(Object data) {
        try {
          return Double.parseDouble((String) data);
        } catch (NumberFormatException nfe) {
          return null;
        }
      }
    };
    typeConverts[ConstVar.FieldType_Byte][ConstVar.FieldType_String] = new TypeConvert() {
      @Override
      public Object convert(Object data) {
        return data.toString();
      }
    };
    // binary <-> string
    typeConverts[ConstVar.FieldType_Binary][ConstVar.FieldType_String] = new TypeConvert() {
      @Override
      public Object convert(Object data) {
        return new String(java.util.Base64.getEncoder().encode((byte[]) data));
      }
    };
    typeConverts[ConstVar.FieldType_String][ConstVar.FieldType_Binary] = new TypeConvert() {
      @Override
      public Object convert(Object data) {
        return (byte[]) (java.util.Base64.getDecoder().decode(((String) data).getBytes()));
      }
    };
    
    typeConverts[ConstVar.FieldType_Short][ConstVar.FieldType_String] = typeConverts[ConstVar.FieldType_Byte][ConstVar.FieldType_String];
    typeConverts[ConstVar.FieldType_Int][ConstVar.FieldType_String] = typeConverts[ConstVar.FieldType_Byte][ConstVar.FieldType_String];
    typeConverts[ConstVar.FieldType_Long][ConstVar.FieldType_String] = typeConverts[ConstVar.FieldType_Byte][ConstVar.FieldType_String];
    typeConverts[ConstVar.FieldType_Float][ConstVar.FieldType_String] = typeConverts[ConstVar.FieldType_Byte][ConstVar.FieldType_String];
    typeConverts[ConstVar.FieldType_Double][ConstVar.FieldType_String] = typeConverts[ConstVar.FieldType_Byte][ConstVar.FieldType_String];
  }

  public static Object convert(byte oritype, byte returntype, Object data) {
    if (oritype == returntype)
      return data;
//    LOG.info("oritype=" + oritype + ", returntype=" + returntype);
//    LOG.info("roncen: data=" + data);
//    LOG.info("roncen: returnData=" + typeConverts[oritype][returntype].convert(data));
    return typeConverts[oritype][returntype].convert(data);
  }
}
