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

import org.apache.hadoop.hive.ql.index.bitmap.BitmapObjectInput;
import org.apache.hadoop.hive.ql.index.bitmap.BitmapObjectOutput;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;

import com.googlecode.javaewah.EWAHCompressedBitmap;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

/**
 * An abstract class for a UDF that performs a binary operation between two EWAH-compressed bitmaps.
 * For example: Bitmap OR and AND operations between two EWAH-compressed bitmaps.
 */
abstract public class AbstractGenericUDFEWAHBitmapBop extends GenericUDF {

  private transient ListObjectInspector b1OI;
  private transient ListObjectInspector b2OI;
  private final String name;

  private final EWAHCompressedBitmap bitmap1 = new EWAHCompressedBitmap();
  private final EWAHCompressedBitmap bitmap2 = new EWAHCompressedBitmap();

  private final EWAHCompressedBitmap container = new EWAHCompressedBitmap();

  private final BitmapObjectInput bitmapIn = new BitmapObjectInput();
  private final BitmapObjectOutput bitmapOut = new BitmapObjectOutput();

  AbstractGenericUDFEWAHBitmapBop(String name) {
    this.name = name;
  }

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length != 2) {
      throw new UDFArgumentLengthException(
        "The function " + name + "(b1, b2) takes exactly 2 arguments");
    }
    b1OI = EWAHUtils.getPrimitiveListOI(arguments[0], name);
    b2OI = EWAHUtils.getPrimitiveListOI(arguments[1], name);

    return ObjectInspectorFactory
        .getStandardListObjectInspector(PrimitiveObjectInspectorFactory
            .javaLongObjectInspector);
  }

  protected abstract EWAHCompressedBitmap bitmapBop(
    EWAHCompressedBitmap bitmap1, EWAHCompressedBitmap bitmap2, EWAHCompressedBitmap container);
  
  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    assert (arguments.length == 2);

    Object b1 = arguments[0].get();
    Object b2 = arguments[1].get();

    EWAHCompressedBitmap arg1 = EWAHUtils.wordArrayToBitmap(bitmap1, bitmapIn, b1OI, b1);
    EWAHCompressedBitmap arg2 = EWAHUtils.wordArrayToBitmap(bitmap2, bitmapIn, b2OI, b2);

    EWAHCompressedBitmap bitmap = bitmapBop(arg1, arg2, container);
    return EWAHUtils.bitmapToWordArray(bitmap, bitmapOut);
  }

  @Override
  public String getDisplayString(String[] children) {
    return getStandardDisplayString(name, children, ",");
  }
}
