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

package org.apache.hadoop.hive.ql.exec.vector.expressions;

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;

public class StringColumnBetween extends VectorExpression {

  private static final long serialVersionUID = 1L;

  private int colNum;
  private int outputCol;

  // The comparison is of the form "column BETWEEN leftValue AND rightValue"
  private byte[] left;
  private byte[] right;

  public StringColumnBetween() { }

  public StringColumnBetween(int colNum, byte[] left, byte[] right, int outputCol) {
    this.colNum = colNum;
    this.left = left;
    this.right = right;
    this.outputCol = outputCol;
  }

  @Override
  public void evaluate(VectorizedRowBatch batch) {

    if (childExpressions != null) {
      super.evaluateChildren(batch);
    }

    final BytesColumnVector inputColVector = (BytesColumnVector) batch.cols[colNum];
    final LongColumnVector outputColVector = (LongColumnVector) batch.cols[outputCol];

    final int[] length = inputColVector.length;
    final int[] start = inputColVector.start;

    final int[] sel = batch.selected;
    final boolean[] nullPos = inputColVector.isNull;
    final boolean[] outNulls = outputColVector.isNull;
    final int n = batch.size;
    final byte[][] vector = inputColVector.vector;
    final long[] outputVector = outputColVector.vector;

    outputColVector.isRepeating = false;
    outputColVector.noNulls = inputColVector.noNulls;

    // return immediately if batch is empty
    if (n == 0) {
      return;
    }

    if (inputColVector.isRepeating) {
      outNulls[0] = nullPos[0];
      outputColVector.isRepeating = true;
    } else if (!inputColVector.noNulls) {
      System.arraycopy(nullPos, 0, outNulls, 0, nullPos.length);
    }

    if (inputColVector.noNulls) {
      if (inputColVector.isRepeating) {
        outputVector[0] =
            (StringExpr.compare(left, 0, left.length, vector[0], start[0], length[0]) <= 0 &&
             StringExpr.compare(vector[0], start[0], length[0], right, 0, right.length) <= 0) ? 1 : 0;
      } else if (batch.selectedInUse) {
        for (int j = 0; j != n; j++) {
          int i = sel[j];
          outputVector[i] =
              (StringExpr.compare(left, 0, left.length, vector[i], start[i], length[i]) <= 0 &&
               StringExpr.compare(vector[i], start[i], length[i], right, 0, right.length) <= 0) ? 1 : 0;
        }
      } else {
        for (int i = 0; i != n; i++) {
          outputVector[i] =
              (StringExpr.compare(left, 0, left.length, vector[i], start[i], length[i]) <= 0 &&
               StringExpr.compare(vector[i], start[i], length[i], right, 0, right.length) <= 0) ? 1 : 0;
        }
      }
    } else {
      if (inputColVector.isRepeating) {
        if (!nullPos[0]) {
          outputVector[0] =
              (StringExpr.compare(left, 0, left.length, vector[0], start[0], length[0]) <= 0 &&
               StringExpr.compare(vector[0], start[0], length[0], right, 0, right.length) <= 0) ? 1 : 0;
        }
      } else if (batch.selectedInUse) {
        for (int j = 0; j != n; j++) {
          int i = sel[j];
          if (!nullPos[i]) {
            outputVector[i] =
                (StringExpr.compare(left, 0, left.length, vector[i], start[i], length[i]) <= 0 &&
                 StringExpr.compare(vector[i], start[i], length[i], right, 0, right.length) <= 0) ? 1 : 0;
          }
        }
      } else {
        for (int i = 0; i != n; i++) {
          if (!nullPos[i]) {
            outputVector[i] =
                (StringExpr.compare(left, 0, left.length, vector[i], start[i], length[i]) <= 0 &&
                 StringExpr.compare(vector[i], start[i], length[i], right, 0, right.length) <= 0) ? 1 : 0;
          }
        }
      }
    }
  }

  @Override
  public int getOutputColumn() {
    return outputCol;
  }

  @Override
  public String getOutputType() {
    return "boolean";
  }

  public int getColNum() {
    return colNum;
  }

  public void setColNum(int colNum) {
    this.colNum = colNum;
  }

  public byte[] getLeftValue() {
    return left;
  }

  public void setLeftValue(byte[] value) {
    this.left = value;
  }

  public byte[] getRightValue() {
    return right;
  }

  public void setRightValue(byte[] value) {
    this.right = value;
  }

  public void setOutputColumn(int outputCol) {
    this.outputCol = outputCol;
  }

  @Override
  public VectorExpressionDescriptor.Descriptor getDescriptor() {
    return null;
  }
}
