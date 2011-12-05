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

package org.apache.hadoop.hive.ql.exec;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.udf.UDAFPercentile;
import org.apache.hadoop.hive.ql.udf.UDFAbs;
import org.apache.hadoop.hive.ql.udf.UDFAcos;
import org.apache.hadoop.hive.ql.udf.UDFAscii;
import org.apache.hadoop.hive.ql.udf.UDFAsin;
import org.apache.hadoop.hive.ql.udf.UDFAtan;
import org.apache.hadoop.hive.ql.udf.UDFBin;
import org.apache.hadoop.hive.ql.udf.UDFCeil;
import org.apache.hadoop.hive.ql.udf.UDFConcat;
import org.apache.hadoop.hive.ql.udf.UDFConv;
import org.apache.hadoop.hive.ql.udf.UDFCos;
import org.apache.hadoop.hive.ql.udf.UDFDate;
import org.apache.hadoop.hive.ql.udf.UDFDateAdd;
import org.apache.hadoop.hive.ql.udf.UDFDateDiff;
import org.apache.hadoop.hive.ql.udf.UDFDateSub;
import org.apache.hadoop.hive.ql.udf.UDFDayOfMonth;
import org.apache.hadoop.hive.ql.udf.UDFDegrees;
import org.apache.hadoop.hive.ql.udf.UDFE;
import org.apache.hadoop.hive.ql.udf.UDFExp;
import org.apache.hadoop.hive.ql.udf.UDFFindInSet;
import org.apache.hadoop.hive.ql.udf.UDFFloor;
import org.apache.hadoop.hive.ql.udf.UDFFromUnixTime;
import org.apache.hadoop.hive.ql.udf.UDFHex;
import org.apache.hadoop.hive.ql.udf.UDFHour;
import org.apache.hadoop.hive.ql.udf.UDFJson;
import org.apache.hadoop.hive.ql.udf.UDFLTrim;
import org.apache.hadoop.hive.ql.udf.UDFLength;
import org.apache.hadoop.hive.ql.udf.UDFLike;
import org.apache.hadoop.hive.ql.udf.UDFLn;
import org.apache.hadoop.hive.ql.udf.UDFLog;
import org.apache.hadoop.hive.ql.udf.UDFLog10;
import org.apache.hadoop.hive.ql.udf.UDFLog2;
import org.apache.hadoop.hive.ql.udf.UDFLower;
import org.apache.hadoop.hive.ql.udf.UDFLpad;
import org.apache.hadoop.hive.ql.udf.UDFMinute;
import org.apache.hadoop.hive.ql.udf.UDFMonth;
import org.apache.hadoop.hive.ql.udf.UDFOPBitAnd;
import org.apache.hadoop.hive.ql.udf.UDFOPBitNot;
import org.apache.hadoop.hive.ql.udf.UDFOPBitOr;
import org.apache.hadoop.hive.ql.udf.UDFOPBitXor;
import org.apache.hadoop.hive.ql.udf.UDFOPDivide;
import org.apache.hadoop.hive.ql.udf.UDFOPLongDivide;
import org.apache.hadoop.hive.ql.udf.UDFOPMinus;
import org.apache.hadoop.hive.ql.udf.UDFOPMod;
import org.apache.hadoop.hive.ql.udf.UDFOPMultiply;
import org.apache.hadoop.hive.ql.udf.UDFOPNegative;
import org.apache.hadoop.hive.ql.udf.UDFOPPlus;
import org.apache.hadoop.hive.ql.udf.UDFOPPositive;
import org.apache.hadoop.hive.ql.udf.UDFPI;
import org.apache.hadoop.hive.ql.udf.UDFParseUrl;
import org.apache.hadoop.hive.ql.udf.UDFPosMod;
import org.apache.hadoop.hive.ql.udf.UDFPower;
import org.apache.hadoop.hive.ql.udf.UDFRTrim;
import org.apache.hadoop.hive.ql.udf.UDFRadians;
import org.apache.hadoop.hive.ql.udf.UDFRand;
import org.apache.hadoop.hive.ql.udf.UDFRegExp;
import org.apache.hadoop.hive.ql.udf.UDFRegExpExtract;
import org.apache.hadoop.hive.ql.udf.UDFRegExpReplace;
import org.apache.hadoop.hive.ql.udf.UDFRepeat;
import org.apache.hadoop.hive.ql.udf.UDFReverse;
import org.apache.hadoop.hive.ql.udf.UDFRound;
import org.apache.hadoop.hive.ql.udf.UDFRpad;
import org.apache.hadoop.hive.ql.udf.UDFSecond;
import org.apache.hadoop.hive.ql.udf.UDFSign;
import org.apache.hadoop.hive.ql.udf.UDFSin;
import org.apache.hadoop.hive.ql.udf.UDFSpace;
import org.apache.hadoop.hive.ql.udf.UDFSqrt;
import org.apache.hadoop.hive.ql.udf.UDFSubstr;
import org.apache.hadoop.hive.ql.udf.UDFTan;
import org.apache.hadoop.hive.ql.udf.UDFToBoolean;
import org.apache.hadoop.hive.ql.udf.UDFToByte;
import org.apache.hadoop.hive.ql.udf.UDFToDouble;
import org.apache.hadoop.hive.ql.udf.UDFToFloat;
import org.apache.hadoop.hive.ql.udf.UDFToInteger;
import org.apache.hadoop.hive.ql.udf.UDFToLong;
import org.apache.hadoop.hive.ql.udf.UDFToShort;
import org.apache.hadoop.hive.ql.udf.UDFToString;
import org.apache.hadoop.hive.ql.udf.UDFTrim;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.UDFUnhex;
import org.apache.hadoop.hive.ql.udf.UDFUnixTimeStamp;
import org.apache.hadoop.hive.ql.udf.UDFUpper;
import org.apache.hadoop.hive.ql.udf.UDFWeekOfYear;
import org.apache.hadoop.hive.ql.udf.UDFYear;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFAverage;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFBridge;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFCollectSet;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFContextNGrams;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFCorrelation;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFCount;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFCovariance;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFCovarianceSample;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEWAHBitmap;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFHistogramNumeric;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFMax;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFMin;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFPercentileApprox;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFResolver2;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFStd;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFStdSample;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFSum;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFVariance;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFVarianceSample;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFnGrams;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFArray;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFArrayContains;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBetween;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBridge;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFCase;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFCoalesce;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFConcatWS;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFEWAHBitmapAnd;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFEWAHBitmapEmpty;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFEWAHBitmapOr;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFElt;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFField;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFFromUtcTimestamp;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFHash;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFIf;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFIn;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFIndex;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFInstr;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFLocate;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFMap;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFMapKeys;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFMapValues;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFNamedStruct;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPAnd;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrLessThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPLessThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNot;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNotEqual;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNotNull;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNull;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPOr;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFReflect;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFSentences;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFSize;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFSplit;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFStringToMap;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFStruct;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFTimestamp;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFToUtcTimestamp;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUnion;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFWhen;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTFExplode;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTFJSONTuple;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTFParseUrlTuple;
import org.apache.hadoop.hive.ql.udf.generic.SimpleGenericUDAFParameterInfo;
import org.apache.hadoop.hive.ql.udf.xml.GenericUDFXPath;
import org.apache.hadoop.hive.ql.udf.xml.UDFXPathBoolean;
import org.apache.hadoop.hive.ql.udf.xml.UDFXPathDouble;
import org.apache.hadoop.hive.ql.udf.xml.UDFXPathFloat;
import org.apache.hadoop.hive.ql.udf.xml.UDFXPathInteger;
import org.apache.hadoop.hive.ql.udf.xml.UDFXPathLong;
import org.apache.hadoop.hive.ql.udf.xml.UDFXPathShort;
import org.apache.hadoop.hive.ql.udf.xml.UDFXPathString;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.util.ReflectionUtils;


/**
 * FunctionRegistry.
 */
public final class FunctionRegistry {

  private static Log LOG = LogFactory.getLog(FunctionRegistry.class);

  private static Registry instance = new Registry();
  static {
    instance.registerUDF("concat", UDFConcat.class, false);
    instance.registerUDF("substr", UDFSubstr.class, false);
    instance.registerUDF("substring", UDFSubstr.class, false);
    instance.registerUDF("space", UDFSpace.class, false);
    instance.registerUDF("repeat", UDFRepeat.class, false);
    instance.registerUDF("ascii", UDFAscii.class, false);
    instance.registerUDF("lpad", UDFLpad.class, false);
    instance.registerUDF("rpad", UDFRpad.class, false);

    instance.registerGenericUDF("size", GenericUDFSize.class);

    instance.registerUDF("round", UDFRound.class, false);
    instance.registerUDF("floor", UDFFloor.class, false);
    instance.registerUDF("sqrt", UDFSqrt.class, false);
    instance.registerUDF("ceil", UDFCeil.class, false);
    instance.registerUDF("ceiling", UDFCeil.class, false);
    instance.registerUDF("rand", UDFRand.class, false);
    instance.registerUDF("abs", UDFAbs.class, false);
    instance.registerUDF("pmod", UDFPosMod.class, false);

    instance.registerUDF("ln", UDFLn.class, false);
    instance.registerUDF("log2", UDFLog2.class, false);
    instance.registerUDF("sin", UDFSin.class, false);
    instance.registerUDF("asin", UDFAsin.class, false);
    instance.registerUDF("cos", UDFCos.class, false);
    instance.registerUDF("acos", UDFAcos.class, false);
    instance.registerUDF("log10", UDFLog10.class, false);
    instance.registerUDF("log", UDFLog.class, false);
    instance.registerUDF("exp", UDFExp.class, false);
    instance.registerUDF("power", UDFPower.class, false);
    instance.registerUDF("pow", UDFPower.class, false);
    instance.registerUDF("sign", UDFSign.class, false);
    instance.registerUDF("pi", UDFPI.class, false);
    instance.registerUDF("degrees", UDFDegrees.class, false);
    instance.registerUDF("radians", UDFRadians.class, false);
    instance.registerUDF("atan", UDFAtan.class, false);
    instance.registerUDF("tan", UDFTan.class, false);
    instance.registerUDF("e", UDFE.class, false);

    instance.registerUDF("conv", UDFConv.class, false);
    instance.registerUDF("bin", UDFBin.class, false);
    instance.registerUDF("hex", UDFHex.class, false);
    instance.registerUDF("unhex", UDFUnhex.class, false);

    instance.registerUDF("upper", UDFUpper.class, false);
    instance.registerUDF("lower", UDFLower.class, false);
    instance.registerUDF("ucase", UDFUpper.class, false);
    instance.registerUDF("lcase", UDFLower.class, false);
    instance.registerUDF("trim", UDFTrim.class, false);
    instance.registerUDF("ltrim", UDFLTrim.class, false);
    instance.registerUDF("rtrim", UDFRTrim.class, false);
    instance.registerUDF("length", UDFLength.class, false);
    instance.registerUDF("reverse", UDFReverse.class, false);
    instance.registerGenericUDF("field", GenericUDFField.class);
    instance.registerUDF("find_in_set", UDFFindInSet.class, false);

    instance.registerUDF("like", UDFLike.class, true);
    instance.registerUDF("rlike", UDFRegExp.class, true);
    instance.registerUDF("regexp", UDFRegExp.class, true);
    instance.registerUDF("regexp_replace", UDFRegExpReplace.class, false);
    instance.registerUDF("regexp_extract", UDFRegExpExtract.class, false);
    instance.registerUDF("parse_url", UDFParseUrl.class, false);
    instance.registerGenericUDF("split", GenericUDFSplit.class);
    instance.registerGenericUDF("str_to_map", GenericUDFStringToMap.class);

    instance.registerUDF("positive", UDFOPPositive.class, true, "+");
    instance.registerUDF("negative", UDFOPNegative.class, true, "-");

    instance.registerUDF("day", UDFDayOfMonth.class, false);
    instance.registerUDF("dayofmonth", UDFDayOfMonth.class, false);
    instance.registerUDF("month", UDFMonth.class, false);
    instance.registerUDF("year", UDFYear.class, false);
    instance.registerUDF("hour", UDFHour.class, false);
    instance.registerUDF("minute", UDFMinute.class, false);
    instance.registerUDF("second", UDFSecond.class, false);
    instance.registerUDF("from_unixtime", UDFFromUnixTime.class, false);
    instance.registerUDF("unix_timestamp", UDFUnixTimeStamp.class, false);
    instance.registerUDF("to_date", UDFDate.class, false);
    instance.registerUDF("weekofyear", UDFWeekOfYear.class, false);

    instance.registerUDF("date_add", UDFDateAdd.class, false);
    instance.registerUDF("date_sub", UDFDateSub.class, false);
    instance.registerUDF("datediff", UDFDateDiff.class, false);

    instance.registerUDF("get_json_object", UDFJson.class, false);

    instance.registerUDF("xpath_string", UDFXPathString.class, false);
    instance.registerUDF("xpath_boolean", UDFXPathBoolean.class, false);
    instance.registerUDF("xpath_number", UDFXPathDouble.class, false);
    instance.registerUDF("xpath_double", UDFXPathDouble.class, false);
    instance.registerUDF("xpath_float", UDFXPathFloat.class, false);
    instance.registerUDF("xpath_long", UDFXPathLong.class, false);
    instance.registerUDF("xpath_int", UDFXPathInteger.class, false);
    instance.registerUDF("xpath_short", UDFXPathShort.class, false);
    instance.registerGenericUDF("xpath", GenericUDFXPath.class);

    instance.registerUDF("+", UDFOPPlus.class, true);
    instance.registerUDF("-", UDFOPMinus.class, true);
    instance.registerUDF("*", UDFOPMultiply.class, true);
    instance.registerUDF("/", UDFOPDivide.class, true);
    instance.registerUDF("%", UDFOPMod.class, true);
    instance.registerUDF("div", UDFOPLongDivide.class, true);

    instance.registerUDF("&", UDFOPBitAnd.class, true);
    instance.registerUDF("|", UDFOPBitOr.class, true);
    instance.registerUDF("^", UDFOPBitXor.class, true);
    instance.registerUDF("~", UDFOPBitNot.class, true);

    instance.registerGenericUDF("isnull", GenericUDFOPNull.class);
    instance.registerGenericUDF("isnotnull", GenericUDFOPNotNull.class);

    instance.registerGenericUDF("if", GenericUDFIf.class);
    instance.registerGenericUDF("in", GenericUDFIn.class);
    instance.registerGenericUDF("and", GenericUDFOPAnd.class);
    instance.registerGenericUDF("or", GenericUDFOPOr.class);
    instance.registerGenericUDF("=", GenericUDFOPEqual.class);
    instance.registerGenericUDF("==", GenericUDFOPEqual.class);
    instance.registerGenericUDF("!=", GenericUDFOPNotEqual.class);
    instance.registerGenericUDF("<>", GenericUDFOPNotEqual.class);
    instance.registerGenericUDF("<", GenericUDFOPLessThan.class);
    instance.registerGenericUDF("<=", GenericUDFOPEqualOrLessThan.class);
    instance.registerGenericUDF(">", GenericUDFOPGreaterThan.class);
    instance.registerGenericUDF(">=", GenericUDFOPEqualOrGreaterThan.class);
    instance.registerGenericUDF("not", GenericUDFOPNot.class);
    instance.registerGenericUDF("!", GenericUDFOPNot.class);

    instance.registerGenericUDF("btw", GenericUDFBetween.class);

    instance.registerGenericUDF("ewah_bitmap_and", GenericUDFEWAHBitmapAnd.class);
    instance.registerGenericUDF("ewah_bitmap_or", GenericUDFEWAHBitmapOr.class);
    instance.registerGenericUDF("ewah_bitmap_empty", GenericUDFEWAHBitmapEmpty.class);


    // Aliases for Java Class Names
    // These are used in getImplicitConvertUDFMethod
    instance.registerUDF(Constants.BOOLEAN_TYPE_NAME, UDFToBoolean.class, false,
        UDFToBoolean.class.getSimpleName());
    instance.registerUDF(Constants.TINYINT_TYPE_NAME, UDFToByte.class, false,
        UDFToByte.class.getSimpleName());
    instance.registerUDF(Constants.SMALLINT_TYPE_NAME, UDFToShort.class, false,
        UDFToShort.class.getSimpleName());
    instance.registerUDF(Constants.INT_TYPE_NAME, UDFToInteger.class, false,
        UDFToInteger.class.getSimpleName());
    instance.registerUDF(Constants.BIGINT_TYPE_NAME, UDFToLong.class, false,
        UDFToLong.class.getSimpleName());
    instance.registerUDF(Constants.FLOAT_TYPE_NAME, UDFToFloat.class, false,
        UDFToFloat.class.getSimpleName());
    instance.registerUDF(Constants.DOUBLE_TYPE_NAME, UDFToDouble.class, false,
        UDFToDouble.class.getSimpleName());
    instance.registerUDF(Constants.STRING_TYPE_NAME, UDFToString.class, false,
        UDFToString.class.getSimpleName());

    instance.registerGenericUDF(Constants.TIMESTAMP_TYPE_NAME,
        GenericUDFTimestamp.class);

    // Aggregate functions
    instance.registerGenericUDAF("max", new GenericUDAFMax());
    instance.registerGenericUDAF("min", new GenericUDAFMin());

    instance.registerGenericUDAF("sum", new GenericUDAFSum());
    instance.registerGenericUDAF("count", new GenericUDAFCount());
    instance.registerGenericUDAF("avg", new GenericUDAFAverage());

    instance.registerGenericUDAF("std", new GenericUDAFStd());
    instance.registerGenericUDAF("stddev", new GenericUDAFStd());
    instance.registerGenericUDAF("stddev_pop", new GenericUDAFStd());
    instance.registerGenericUDAF("stddev_samp", new GenericUDAFStdSample());
    instance.registerGenericUDAF("variance", new GenericUDAFVariance());
    instance.registerGenericUDAF("var_pop", new GenericUDAFVariance());
    instance.registerGenericUDAF("var_samp", new GenericUDAFVarianceSample());
    instance.registerGenericUDAF("covar_pop", new GenericUDAFCovariance());
    instance.registerGenericUDAF("covar_samp", new GenericUDAFCovarianceSample());
    instance.registerGenericUDAF("corr", new GenericUDAFCorrelation());
    instance.registerGenericUDAF("histogram_numeric", new GenericUDAFHistogramNumeric());
    instance.registerGenericUDAF("percentile_approx", new GenericUDAFPercentileApprox());
    instance.registerGenericUDAF("collect_set", new GenericUDAFCollectSet());

    instance.registerGenericUDAF("ngrams", new GenericUDAFnGrams());
    instance.registerGenericUDAF("context_ngrams", new GenericUDAFContextNGrams());

    instance.registerGenericUDAF("ewah_bitmap", new GenericUDAFEWAHBitmap());

    instance.registerUDAF("percentile", UDAFPercentile.class);


    // Generic UDFs
    instance.registerGenericUDF("reflect", GenericUDFReflect.class);

    instance.registerGenericUDF("array", GenericUDFArray.class);
    instance.registerGenericUDF("map", GenericUDFMap.class);
    instance.registerGenericUDF("struct", GenericUDFStruct.class);
    instance.registerGenericUDF("named_struct", GenericUDFNamedStruct.class);
    instance.registerGenericUDF("create_union", GenericUDFUnion.class);

    instance.registerGenericUDF("case", GenericUDFCase.class);
    instance.registerGenericUDF("when", GenericUDFWhen.class);
    instance.registerGenericUDF("hash", GenericUDFHash.class);
    instance.registerGenericUDF("coalesce", GenericUDFCoalesce.class);
    instance.registerGenericUDF("index", GenericUDFIndex.class);
    instance.registerGenericUDF("instr", GenericUDFInstr.class);
    instance.registerGenericUDF("locate", GenericUDFLocate.class);
    instance.registerGenericUDF("elt", GenericUDFElt.class);
    instance.registerGenericUDF("concat_ws", GenericUDFConcatWS.class);
    instance.registerGenericUDF("array_contains", GenericUDFArrayContains.class);
    instance.registerGenericUDF("sentences", GenericUDFSentences.class);
    instance.registerGenericUDF("map_keys", GenericUDFMapKeys.class);
    instance.registerGenericUDF("map_values", GenericUDFMapValues.class);

    instance.registerGenericUDF("from_utc_timestamp", GenericUDFFromUtcTimestamp.class);
    instance.registerGenericUDF("to_utc_timestamp", GenericUDFToUtcTimestamp.class);


    // Generic UDTF's
    instance.registerGenericUDTF("explode", GenericUDTFExplode.class);
    instance.registerGenericUDTF("json_tuple", GenericUDTFJSONTuple.class);
    instance.registerGenericUDTF("parse_url_tuple", GenericUDTFParseUrlTuple.class);
  }

  public static Registry get() {
    return instance;
  }

  public static FunctionInfo getFunctionInfo(String functionName) {
    FunctionInfo info = null;
    Registry registry = SessionState.getRegistry();
    if (registry != null) {
      info = registry.getFunctionInfo(functionName);
    }
    return info != null ? info : instance.getFunctionInfo(functionName);
  }

  public static Set<String> getFunctionNames() {
    Set<String> names = new TreeSet<String>();
    Registry registry = SessionState.getRegistry();
    if (registry != null) {
      names.addAll(registry.getFunctionNames());
    }
    names.addAll(instance.getFunctionNames());
    return names;
  }

  public static Set<String> getFunctionNames(String funcPatternStr) {
    Set<String> funcNames = new TreeSet<String>();
    Registry registry = SessionState.getRegistry();
    if (registry != null) {
      funcNames.addAll(registry.getFunctionNames(funcPatternStr));
    }
    funcNames.addAll(instance.getFunctionNames(funcPatternStr));
    return funcNames;
  }

  public static Set<String> getFunctionSynonyms(String funcName) {
    Set<String> synonyms = new HashSet<String>();
    Registry registry = SessionState.getRegistry();
    if (registry != null) {
      synonyms.addAll(registry.getFunctionSynonyms(funcName));
    }
    synonyms.addAll(instance.getFunctionSynonyms(funcName));
    return synonyms;
  }

  public static GenericUDAFEvaluator getGenericUDAFEvaluator(String name,
      List<TypeInfo> argumentTypeInfos, boolean isDistinct,
      boolean isAllColumns) throws SemanticException {
    GenericUDAFEvaluator evaluator = null;
    Registry registry = SessionState.getRegistry();
    if (registry != null) {
      evaluator = registry.getGenericUDAFEvaluator(name, argumentTypeInfos, isDistinct, isAllColumns);
    }
    return evaluator != null ? evaluator : instance.getGenericUDAFEvaluator(name, argumentTypeInfos, isDistinct, isAllColumns);
  }

  public static GenericUDAFResolver getGenericUDAFResolver(String functionName) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Looking up GenericUDAF: " + functionName);
    }
    GenericUDAFResolver evaluator = null;
    Registry registry = SessionState.getRegistry();
    if (registry != null) {
      evaluator = registry.getGenericUDAFResolver(functionName);
    }
    return evaluator != null ? evaluator : instance.getGenericUDAFResolver(functionName);
  }

  public static class Registry {

    /**
   * The mapping from expression function names to expression classes.
   */
  final Map<String, FunctionInfo> mFunctions = new LinkedHashMap<String, FunctionInfo>();

  public void registerTemporaryUDF(String functionName,
      Class<? extends UDF> UDFClass, boolean isOperator) {
    registerUDF(false, functionName, UDFClass, isOperator);
  }

  void registerUDF(String functionName, Class<? extends UDF> UDFClass,
      boolean isOperator) {
    registerUDF(true, functionName, UDFClass, isOperator);
  }

  public void registerUDF(boolean isNative, String functionName,
      Class<? extends UDF> UDFClass, boolean isOperator) {
    registerUDF(isNative, functionName, UDFClass, isOperator, functionName
        .toLowerCase());
  }

  public void registerUDF(String functionName,
      Class<? extends UDF> UDFClass, boolean isOperator, String displayName) {
    registerUDF(true, functionName, UDFClass, isOperator, displayName);
  }

  public void registerUDF(boolean isNative, String functionName,
      Class<? extends UDF> UDFClass, boolean isOperator, String displayName) {
    if (UDF.class.isAssignableFrom(UDFClass)) {
      FunctionInfo fI = new FunctionInfo(isNative, displayName,
          new GenericUDFBridge(displayName, isOperator, UDFClass));
      mFunctions.put(functionName.toLowerCase(), fI);
    } else {
      throw new RuntimeException("Registering UDF Class " + UDFClass
          + " which does not extend " + UDF.class);
    }
  }

  public void registerTemporaryGenericUDF(String functionName,
      Class<? extends GenericUDF> genericUDFClass) {
    registerGenericUDF(false, functionName, genericUDFClass);
  }

  void registerGenericUDF(String functionName,
      Class<? extends GenericUDF> genericUDFClass) {
    registerGenericUDF(true, functionName, genericUDFClass);
  }

  public void registerGenericUDF(boolean isNative, String functionName,
      Class<? extends GenericUDF> genericUDFClass) {
    if (GenericUDF.class.isAssignableFrom(genericUDFClass)) {
      FunctionInfo fI = new FunctionInfo(isNative, functionName,
          (GenericUDF) ReflectionUtils.newInstance(genericUDFClass, null));
      mFunctions.put(functionName.toLowerCase(), fI);
    } else {
      throw new RuntimeException("Registering GenericUDF Class "
          + genericUDFClass + " which does not extend " + GenericUDF.class);
    }
  }

  public void registerTemporaryGenericUDTF(String functionName,
      Class<? extends GenericUDTF> genericUDTFClass) {
    registerGenericUDTF(false, functionName, genericUDTFClass);
  }

  void registerGenericUDTF(String functionName,
      Class<? extends GenericUDTF> genericUDTFClass) {
    registerGenericUDTF(true, functionName, genericUDTFClass);
  }

  public void registerGenericUDTF(boolean isNative, String functionName,
      Class<? extends GenericUDTF> genericUDTFClass) {
    if (GenericUDTF.class.isAssignableFrom(genericUDTFClass)) {
      FunctionInfo fI = new FunctionInfo(isNative, functionName,
          (GenericUDTF) ReflectionUtils.newInstance(genericUDTFClass, null));
      mFunctions.put(functionName.toLowerCase(), fI);
    } else {
      throw new RuntimeException("Registering GenericUDTF Class "
          + genericUDTFClass + " which does not extend " + GenericUDTF.class);
    }
  }

  public FunctionInfo getFunctionInfo(String functionName) {
    return mFunctions.get(functionName.toLowerCase());
  }

  /**
   * Returns a set of registered function names. This is used for the CLI
   * command "SHOW FUNCTIONS;"
   *
   * @return set of strings contains function names
   */
  public Set<String> getFunctionNames() {
    return mFunctions.keySet();
  }

  /**
   * Returns a set of registered function names. This is used for the CLI
   * command "SHOW FUNCTIONS 'regular expression';" Returns an empty set when
   * the regular expression is not valid.
   *
   * @param funcPatternStr
   *          regular expression of the interested function names
   * @return set of strings contains function names
   */
  public Set<String> getFunctionNames(String funcPatternStr) {
    Set<String> funcNames = new TreeSet<String>();
    Pattern funcPattern = null;
    try {
      funcPattern = Pattern.compile(funcPatternStr);
    } catch (PatternSyntaxException e) {
      return funcNames;
    }
    for (String funcName : mFunctions.keySet()) {
      if (funcPattern.matcher(funcName).matches()) {
        funcNames.add(funcName);
      }
    }
    return funcNames;
  }

  /**
   * Returns the set of synonyms of the supplied function.
   *
   * @param funcName
   *          the name of the function
   * @return Set of synonyms for funcName
   */
  public Set<String> getFunctionSynonyms(String funcName) {
    Set<String> synonyms = new HashSet<String>();

    FunctionInfo funcInfo = getFunctionInfo(funcName);
    if (null == funcInfo) {
      return synonyms;
    }

    Class<?> funcClass = funcInfo.getFunctionClass();
    for (String name : mFunctions.keySet()) {
      if (name.equals(funcName)) {
        continue;
      }
      if (mFunctions.get(name).getFunctionClass().equals(funcClass)) {
        synonyms.add(name);
      }
    }

    return synonyms;
  }

  /**
   * Get the GenericUDAF evaluator for the name and argumentClasses.
   *
   * @param name
   *          the name of the UDAF
   * @param argumentTypeInfos
   * @return The UDAF evaluator
   */
  @SuppressWarnings("deprecation")
  public GenericUDAFEvaluator getGenericUDAFEvaluator(String name,
      List<TypeInfo> argumentTypeInfos, boolean isDistinct,
      boolean isAllColumns) throws SemanticException {

    GenericUDAFResolver udafResolver = getGenericUDAFResolver(name);
    if (udafResolver == null) {
      return null;
    }

    TypeInfo[] parameters = new TypeInfo[argumentTypeInfos.size()];
    for (int i = 0; i < parameters.length; i++) {
      parameters[i] = argumentTypeInfos.get(i);
    }

    GenericUDAFEvaluator udafEvaluator = null;
    if (udafResolver instanceof GenericUDAFResolver2) {
    GenericUDAFParameterInfo paramInfo =
        new SimpleGenericUDAFParameterInfo(
              parameters, isDistinct, isAllColumns);
      udafEvaluator =
          ((GenericUDAFResolver2) udafResolver).getEvaluator(paramInfo);
    } else {
      udafEvaluator = udafResolver.getEvaluator(parameters);
    }
    return udafEvaluator;
  }

  public void registerTemporaryGenericUDAF(String functionName,
      GenericUDAFResolver genericUDAFResolver) {
    registerGenericUDAF(false, functionName, genericUDAFResolver);
  }

  void registerGenericUDAF(String functionName,
      GenericUDAFResolver genericUDAFResolver) {
    registerGenericUDAF(true, functionName, genericUDAFResolver);
  }

  public void registerGenericUDAF(boolean isNative, String functionName,
      GenericUDAFResolver genericUDAFResolver) {
    mFunctions.put(functionName.toLowerCase(), new FunctionInfo(isNative,
        functionName.toLowerCase(), genericUDAFResolver));
  }

  public void registerTemporaryUDAF(String functionName,
      Class<? extends UDAF> udafClass) {
    registerUDAF(false, functionName, udafClass);
  }

  void registerUDAF(String functionName, Class<? extends UDAF> udafClass) {
    registerUDAF(true, functionName, udafClass);
  }

  public void registerUDAF(boolean isNative, String functionName,
      Class<? extends UDAF> udafClass) {
    mFunctions.put(functionName.toLowerCase(), new FunctionInfo(isNative,
        functionName.toLowerCase(), new GenericUDAFBridge(
        (UDAF) ReflectionUtils.newInstance(udafClass, null))));
  }

  public void unregisterUDF(String functionName) throws HiveException {
    FunctionInfo fi = mFunctions.get(functionName.toLowerCase());
    if (fi != null) {
      if (!fi.isNative()) {
        mFunctions.remove(functionName.toLowerCase());
      } else {
        throw new HiveException("Function " + functionName
            + " is hive native, it can't be dropped");
      }
    }
  }

  public GenericUDAFResolver getGenericUDAFResolver(String functionName) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Looking up GenericUDAF: " + functionName);
    }
    FunctionInfo finfo = mFunctions.get(functionName.toLowerCase());
    if (finfo == null) {
      return null;
    }
    GenericUDAFResolver result = finfo.getGenericUDAFResolver();
    return result;
  }
  }

  public static Object invoke(Method m, Object thisObject, Object... arguments)
      throws HiveException {
    Object o;
    try {
      o = m.invoke(thisObject, arguments);
    } catch (Exception e) {
      String thisObjectString = "" + thisObject + " of class "
          + (thisObject == null ? "null" : thisObject.getClass().getName());

      StringBuilder argumentString = new StringBuilder();
      if (arguments == null) {
        argumentString.append("null");
      } else {
        argumentString.append("{");
        for (int i = 0; i < arguments.length; i++) {
          if (i > 0) {
            argumentString.append(", ");
          }
          if (arguments[i] == null) {
            argumentString.append("null");
          } else {
            argumentString.append("" + arguments[i] + ":"
                + arguments[i].getClass().getName());
          }
        }
        argumentString.append("} of size " + arguments.length);
      }

      throw new HiveException("Unable to execute method " + m + " "
          + " on object " + thisObjectString + " with arguments "
          + argumentString.toString(), e);
    }
    return o;
  }

  /**
   * Returns -1 if passed does not match accepted. Otherwise return the cost
   * (usually 0 for no conversion and 1 for conversion).
   */
  public static int matchCost(TypeInfo argumentPassed,
      TypeInfo argumentAccepted, boolean exact) {
    if (argumentAccepted.equals(argumentPassed)) {
      // matches
      return 0;
    }
    if (argumentPassed.equals(TypeInfoFactory.voidTypeInfo)) {
      // passing null matches everything
      return 0;
    }
    if (argumentPassed.getCategory().equals(Category.LIST)
        && argumentAccepted.getCategory().equals(Category.LIST)) {
      // lists are compatible if and only-if the elements are compatible
      TypeInfo argumentPassedElement = ((ListTypeInfo) argumentPassed)
          .getListElementTypeInfo();
      TypeInfo argumentAcceptedElement = ((ListTypeInfo) argumentAccepted)
          .getListElementTypeInfo();
      return matchCost(argumentPassedElement, argumentAcceptedElement, exact);
    }
    if (argumentPassed.getCategory().equals(Category.MAP)
        && argumentAccepted.getCategory().equals(Category.MAP)) {
      // lists are compatible if and only-if the elements are compatible
      TypeInfo argumentPassedKey = ((MapTypeInfo) argumentPassed)
          .getMapKeyTypeInfo();
      TypeInfo argumentAcceptedKey = ((MapTypeInfo) argumentAccepted)
          .getMapKeyTypeInfo();
      TypeInfo argumentPassedValue = ((MapTypeInfo) argumentPassed)
          .getMapValueTypeInfo();
      TypeInfo argumentAcceptedValue = ((MapTypeInfo) argumentAccepted)
          .getMapValueTypeInfo();
      int cost1 = matchCost(argumentPassedKey, argumentAcceptedKey, exact);
      int cost2 = matchCost(argumentPassedValue, argumentAcceptedValue, exact);
      if (cost1 < 0 || cost2 < 0) {
        return -1;
      }
      return Math.max(cost1, cost2);
    }

    if (argumentAccepted.equals(TypeInfoFactory.unknownTypeInfo)) {
      // accepting Object means accepting everything,
      // but there is a conversion cost.
      return 1;
    }
    if (!exact && implicitConvertable(argumentPassed, argumentAccepted)) {
      return 1;
    }

    return -1;
  }

  /**
   * This method is shared between UDFRegistry and UDAFRegistry. methodName will
   * be "evaluate" for UDFRegistry, and "aggregate"/"evaluate"/"evaluatePartial"
   * for UDAFRegistry.
   * @throws UDFArgumentException
   */
  public static <T> Method getMethodInternal(Class<? extends T> udfClass,
      String methodName, boolean exact, List<TypeInfo> argumentClasses)
      throws UDFArgumentException {

    List<Method> mlist = new ArrayList<Method>();

    for (Method m : udfClass.getMethods()) {
      if (m.getName().equals(methodName)) {
        mlist.add(m);
      }
    }

    return getMethodInternal(udfClass, mlist, exact, argumentClasses);
  }

  /**
   * Gets the closest matching method corresponding to the argument list from a
   * list of methods.
   *
   * @param mlist
   *          The list of methods to inspect.
   * @param exact
   *          Boolean to indicate whether this is an exact match or not.
   * @param argumentsPassed
   *          The classes for the argument.
   * @return The matching method.
   */
  public static Method getMethodInternal(Class<?> udfClass, List<Method> mlist, boolean exact,
      List<TypeInfo> argumentsPassed) throws UDFArgumentException {

    // result
    List<Method> udfMethods = new ArrayList<Method>();
    // The cost of the result
    int leastConversionCost = Integer.MAX_VALUE;

    for (Method m : mlist) {
      List<TypeInfo> argumentsAccepted = TypeInfoUtils.getParameterTypeInfos(m,
          argumentsPassed.size());
      if (argumentsAccepted == null) {
        // null means the method does not accept number of arguments passed.
        continue;
      }

      boolean match = (argumentsAccepted.size() == argumentsPassed.size());
      int conversionCost = 0;

      for (int i = 0; i < argumentsPassed.size() && match; i++) {
        int cost = matchCost(argumentsPassed.get(i), argumentsAccepted.get(i),
            exact);
        if (cost == -1) {
          match = false;
        } else {
          conversionCost += cost;
        }
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Method " + (match ? "did" : "didn't") + " match: passed = "
                  + argumentsPassed + " accepted = " + argumentsAccepted +
                  " method = " + m);
      }
      if (match) {
        // Always choose the function with least implicit conversions.
        if (conversionCost < leastConversionCost) {
          udfMethods.clear();
          udfMethods.add(m);
          leastConversionCost = conversionCost;
          // Found an exact match
          if (leastConversionCost == 0) {
            break;
          }
        } else if (conversionCost == leastConversionCost) {
          // Ambiguous call: two methods with the same number of implicit
          // conversions
          udfMethods.add(m);
          // Don't break! We might find a better match later.
        } else {
          // do nothing if implicitConversions > leastImplicitConversions
        }
      }
    }

    if (udfMethods.size() == 0) {
      // No matching methods found
      throw new NoMatchingMethodException(udfClass, argumentsPassed, mlist);
    }
    if (udfMethods.size() > 1) {
      // Ambiguous method found
      throw new AmbiguousMethodException(udfClass, argumentsPassed, mlist);
    }
    return udfMethods.get(0);
  }

  /**
   * A shortcut to get the "index" GenericUDF. This is used for getting elements
   * out of array and getting values out of map.
   */
  public static GenericUDF getGenericUDFForIndex() {
    return FunctionRegistry.getFunctionInfo("index").getGenericUDF();
  }

  /**
   * A shortcut to get the "and" GenericUDF.
   */
  public static GenericUDF getGenericUDFForAnd() {
    return FunctionRegistry.getFunctionInfo("and").getGenericUDF();
  }

  /**
   * Create a copy of an existing GenericUDF.
   */
  public static GenericUDF cloneGenericUDF(GenericUDF genericUDF) {
    if (null == genericUDF) {
      return null;
    }

    if (genericUDF instanceof GenericUDFBridge) {
      GenericUDFBridge bridge = (GenericUDFBridge) genericUDF;
      return new GenericUDFBridge(bridge.getUdfName(), bridge.isOperator(),
          bridge.getUdfClass());
    }

    return (GenericUDF) ReflectionUtils
        .newInstance(genericUDF.getClass(), null);
  }

  /**
   * Create a copy of an existing GenericUDTF.
   */
  public static GenericUDTF cloneGenericUDTF(GenericUDTF genericUDTF) {
    if (null == genericUDTF) {
      return null;
    }
    return (GenericUDTF) ReflectionUtils.newInstance(genericUDTF.getClass(),
        null);
  }

  /**
   * Get the UDF class from an exprNodeDesc. Returns null if the exprNodeDesc
   * does not contain a UDF class.
   */
  private static Class<? extends GenericUDF> getGenericUDFClassFromExprDesc(ExprNodeDesc desc) {
    if (!(desc instanceof ExprNodeGenericFuncDesc)) {
      return null;
    }
    ExprNodeGenericFuncDesc genericFuncDesc = (ExprNodeGenericFuncDesc) desc;
    return genericFuncDesc.getGenericUDF().getClass();
  }

  /**
   * Get the UDF class from an exprNodeDesc. Returns null if the exprNodeDesc
   * does not contain a UDF class.
   */
  private static Class<? extends UDF> getUDFClassFromExprDesc(ExprNodeDesc desc) {
    if (!(desc instanceof ExprNodeGenericFuncDesc)) {
      return null;
    }
    ExprNodeGenericFuncDesc genericFuncDesc = (ExprNodeGenericFuncDesc) desc;
    if (!(genericFuncDesc.getGenericUDF() instanceof GenericUDFBridge)) {
      return null;
    }
    GenericUDFBridge bridge = (GenericUDFBridge) (genericFuncDesc
        .getGenericUDF());
    return bridge.getUdfClass();
  }

  /**
   * Returns whether a GenericUDF is deterministic or not.
   */
  public static boolean isDeterministic(GenericUDF genericUDF) {
    if (isStateful(genericUDF)) {
      // stateful implies non-deterministic, regardless of whatever
      // the deterministic annotation declares
      return false;
    }
    UDFType genericUDFType = genericUDF.getClass().getAnnotation(UDFType.class);
    if (genericUDFType != null && genericUDFType.deterministic() == false) {
      return false;
    }

    if (genericUDF instanceof GenericUDFBridge) {
      GenericUDFBridge bridge = (GenericUDFBridge) (genericUDF);
      UDFType bridgeUDFType = bridge.getUdfClass().getAnnotation(UDFType.class);
      if (bridgeUDFType != null && bridgeUDFType.deterministic() == false) {
        return false;
      }
    }

    return true;
  }

  /**
   * Returns whether a GenericUDF is stateful or not.
   */
  public static boolean isStateful(GenericUDF genericUDF) {
    UDFType genericUDFType = genericUDF.getClass().getAnnotation(UDFType.class);
    if (genericUDFType != null && genericUDFType.stateful()) {
      return true;
    }

    if (genericUDF instanceof GenericUDFBridge) {
      GenericUDFBridge bridge = (GenericUDFBridge) genericUDF;
      UDFType bridgeUDFType = bridge.getUdfClass().getAnnotation(UDFType.class);
      if (bridgeUDFType != null && bridgeUDFType.stateful()) {
        return true;
      }
    }

    return false;
  }

  /**
   * Returns whether the exprNodeDesc is a node of "and", "or", "not".
   */
  public static boolean isOpAndOrNot(ExprNodeDesc desc) {
    Class<? extends GenericUDF> genericUdfClass = getGenericUDFClassFromExprDesc(desc);
    return GenericUDFOPAnd.class == genericUdfClass
        || GenericUDFOPOr.class == genericUdfClass
        || GenericUDFOPNot.class == genericUdfClass;
  }

  /**
   * Returns whether the exprNodeDesc is a node of "and".
   */
  public static boolean isOpAnd(ExprNodeDesc desc) {
    return GenericUDFOPAnd.class == getGenericUDFClassFromExprDesc(desc);
  }

  /**
   * Returns whether the exprNodeDesc is a node of "or".
   */
  public static boolean isOpOr(ExprNodeDesc desc) {
    return GenericUDFOPOr.class == getGenericUDFClassFromExprDesc(desc);
  }

  /**
   * Returns whether the exprNodeDesc is a node of "not".
   */
  public static boolean isOpNot(ExprNodeDesc desc) {
    return GenericUDFOPNot.class == getGenericUDFClassFromExprDesc(desc);
  }

  /**
   * Returns whether the exprNodeDesc is a node of "positive".
   */
  public static boolean isOpPositive(ExprNodeDesc desc) {
    Class<? extends UDF> udfClass = getUDFClassFromExprDesc(desc);
    return UDFOPPositive.class == udfClass;
  }

  static Map<TypeInfo, Integer> numericTypes = new HashMap<TypeInfo, Integer>();
  static List<TypeInfo> numericTypeList = new ArrayList<TypeInfo>();

  static void registerNumericType(String typeName, int level) {
    TypeInfo t = TypeInfoFactory.getPrimitiveTypeInfo(typeName);
    numericTypeList.add(t);
    numericTypes.put(t, level);
  }

  static {
    registerNumericType(Constants.TINYINT_TYPE_NAME, 1);
    registerNumericType(Constants.SMALLINT_TYPE_NAME, 2);
    registerNumericType(Constants.INT_TYPE_NAME, 3);
    registerNumericType(Constants.BIGINT_TYPE_NAME, 4);
    registerNumericType(Constants.FLOAT_TYPE_NAME, 5);
    registerNumericType(Constants.DOUBLE_TYPE_NAME, 6);
    registerNumericType(Constants.STRING_TYPE_NAME, 7);
  }

  /**
   * Find a common class that objects of both TypeInfo a and TypeInfo b can
   * convert to. This is used for comparing objects of type a and type b.
   *
   * When we are comparing string and double, we will always convert both of
   * them to double and then compare.
   *
   * @return null if no common class could be found.
   */
  public static TypeInfo getCommonClassForComparison(TypeInfo a, TypeInfo b) {
    // If same return one of them
    if (a.equals(b)) {
      return a;
    }

    for (TypeInfo t : numericTypeList) {
      if (FunctionRegistry.implicitConvertable(a, t)
          && FunctionRegistry.implicitConvertable(b, t)) {
        return t;
      }
    }
    return null;
  }

  /**
   * Find a common class that objects of both TypeInfo a and TypeInfo b can
   * convert to. This is used for places other than comparison.
   *
   * The common class of string and double is string.
   *
   * @return null if no common class could be found.
   */
  public static TypeInfo getCommonClass(TypeInfo a, TypeInfo b) {
    if (a.equals(b)) {
      return a;
    }
    Integer ai = numericTypes.get(a);
    Integer bi = numericTypes.get(b);
    if (ai == null || bi == null) {
      // If either is not a numeric type, return null.
      return null;
    }
    return (ai > bi) ? a : b;
  }

  /**
   * Returns whether it is possible to implicitly convert an object of Class
   * from to Class to.
   */
  public static boolean implicitConvertable(TypeInfo from, TypeInfo to) {
    if (from.equals(to)) {
      return true;
    }
    // Allow implicit String to Double conversion
    if (from.equals(TypeInfoFactory.stringTypeInfo)
        && to.equals(TypeInfoFactory.doubleTypeInfo)) {
      return true;
    }
    // Void can be converted to any type
    if (from.equals(TypeInfoFactory.voidTypeInfo)) {
      return true;
    }

    if (from.equals(TypeInfoFactory.timestampTypeInfo)
        && to.equals(TypeInfoFactory.stringTypeInfo)) {
      return true;
    }

    // Allow implicit conversion from Byte -> Integer -> Long -> Float -> Double
    // -> String
    Integer f = numericTypes.get(from);
    Integer t = numericTypes.get(to);
    if (f == null || t == null) {
      return false;
    }
    if (f.intValue() > t.intValue()) {
      return false;
    }
    return true;
  }
}