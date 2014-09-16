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

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;

public class DecimalColumnBetween extends VectorExpression {

  private static final long serialVersionUID = 1L;

  private int colNum;
  private int outputCol;

  // The comparison is of the form "column BETWEEN leftValue AND rightValue"
  private HiveDecimal leftValue;
  private HiveDecimal rightValue;

  public DecimalColumnBetween() { }

  public DecimalColumnBetween(int colNum, HiveDecimal leftValue, HiveDecimal rightValue, int outputCol) {
    this.colNum = colNum;
    this.leftValue = leftValue;
    this.rightValue = rightValue;
    this.outputCol = outputCol;
  }

  @Override
  public void evaluate(VectorizedRowBatch batch) {

    if (childExpressions != null) {
      super.evaluateChildren(batch);
    }

    DecimalColumnVector inputColVector = (DecimalColumnVector) batch.cols[colNum];
    LongColumnVector outputColVector = (LongColumnVector) batch.cols[outputCol];

    final int[] sel = batch.selected;
    final boolean[] nullPos = inputColVector.isNull;
    final boolean[] outNulls = outputColVector.isNull;
    final int n = batch.size;
    final HiveDecimalWritable[] vector = inputColVector.vector;
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
        outputVector[0] = (DecimalUtil.compare(leftValue, vector[0]) <= 0 && DecimalUtil.compare(vector[0], rightValue) <= 0) ? 1 : 0;
      } else if (batch.selectedInUse) {
        for (int j = 0; j != n; j++) {
          int i = sel[j];
          outputVector[i] = (DecimalUtil.compare(leftValue, vector[i]) <= 0 && DecimalUtil.compare(vector[i], rightValue) <= 0) ? 1 : 0;
        }
      } else {
        for (int i = 0; i != n; i++) {
          outputVector[i] = (DecimalUtil.compare(leftValue, vector[i]) <= 0 && DecimalUtil.compare(vector[i], rightValue) <= 0) ? 1 : 0;
        }
      }
    } else {
      if (inputColVector.isRepeating) {
        if (!nullPos[0]) {
          outputVector[0] = (DecimalUtil.compare(leftValue, vector[0]) <= 0 && DecimalUtil.compare(vector[0], rightValue) <= 0) ? 1 : 0;
        }
      } else if (batch.selectedInUse) {
        for (int j = 0; j != n; j++) {
          int i = sel[j];
          if (!nullPos[i]) {
            outputVector[i] = (DecimalUtil.compare(leftValue, vector[i]) <= 0 && DecimalUtil.compare(vector[i], rightValue) <= 0) ? 1 : 0;
          }
        }
      } else {
        for (int i = 0; i != n; i++) {
          if (!nullPos[i]) {
            outputVector[i] = (DecimalUtil.compare(leftValue, vector[i]) <= 0 && DecimalUtil.compare(vector[i], rightValue) <= 0) ? 1 : 0;
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

  @Override
  public VectorExpressionDescriptor.Descriptor getDescriptor() {
    return null;
  }
}
