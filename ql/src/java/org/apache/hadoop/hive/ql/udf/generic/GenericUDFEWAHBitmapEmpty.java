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
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.index.bitmap.BitmapObjectInput;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

@Description(name = "ewah_bitmap_empty", value = "_FUNC_(bitmap) - "
    + "Predicate that tests whether an EWAH-compressed bitmap is all zeros ")
public class GenericUDFEWAHBitmapEmpty extends GenericUDF {
  
  private static final String name = "EWAH_BITMAP_EMPTY";
  
  private transient ListObjectInspector bitmapOI;

  private final EWAHCompressedBitmap bitmap = new EWAHCompressedBitmap();
  private final BitmapObjectInput bitmapIn = new BitmapObjectInput();
  
  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length != 1) {
      throw new UDFArgumentLengthException(
          "The function EWAH_BITMAP_EMPTY(b) takes exactly 1 argument");
    }
    bitmapOI = EWAHUtils.getPrimitiveListOI(arguments[0], name);
    return PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    assert (arguments.length == 1);
    Object b = arguments[0].get();
    return EWAHUtils.wordArrayToBitmap(bitmap, bitmapIn, bitmapOI, b).isEmpty();
  }

  @Override
  public String getDisplayString(String[] children) {
    return getStandardDisplayString(name, children);
  }
}
