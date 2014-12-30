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

import com.googlecode.javaewah.EWAHCompressedBitmap;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.index.bitmap.BitmapObjectInput;
import org.apache.hadoop.hive.ql.index.bitmap.BitmapObjectOutput;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;

import java.io.IOException;
import java.util.List;

public class EWAHUtils {

  public static ListObjectInspector getPrimitiveListOI(ObjectInspector argument, String name)
      throws UDFArgumentTypeException {
    if (!argument.getCategory().equals(ObjectInspector.Category.LIST)) {
      throw new UDFArgumentTypeException(0, "\""
          + ObjectInspector.Category.LIST.toString().toLowerCase()
          + "\" is expected at function " + name + ", but \""
          + argument.getTypeName() + "\" is found");
    }
    ListObjectInspector b1OI = (ListObjectInspector) argument;
    if (!(b1OI.getListElementObjectInspector() instanceof PrimitiveObjectInspector)) {
      throw new UDFArgumentTypeException(0, "\""
          + "Primitive type elements are expected at function " + name + ", but \""
          + b1OI.getListElementObjectInspector().getTypeName() + "\" is found");
    }
    return b1OI;
  }

  public static EWAHCompressedBitmap wordArrayToBitmap(EWAHCompressedBitmap bitmap, 
      BitmapObjectInput bitmapIn, ListObjectInspector lloi, Object b) {
    long[] values = new long[lloi.getListLength(b)];
    for (int i = 0; i < values.length; i++) {
      values[i] = PrimitiveObjectInspectorUtils.getLong(
          lloi.getListElement(b, i),
          (PrimitiveObjectInspector) lloi.getListElementObjectInspector());
    }
    bitmap.clear();
    try {
      bitmap.readExternal(bitmapIn.reset(values));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return bitmap;
  }

  public static List<Long> bitmapToWordArray(
      EWAHCompressedBitmap bitmap, BitmapObjectOutput bitmapOut) {
    bitmapOut.clear();
    try {
      bitmap.writeExternal(bitmapOut);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return bitmapOut.list();
  }
}
