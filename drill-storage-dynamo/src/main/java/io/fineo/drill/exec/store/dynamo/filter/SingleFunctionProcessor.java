/*
 *    Copyright 2016 Fineo, Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package io.fineo.drill.exec.store.dynamo.filter;

import com.google.common.collect.ImmutableMap;
import org.apache.drill.common.expression.CastExpression;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.expression.TypedNullConstant;
import org.apache.drill.common.expression.ValueExpressions;
import org.apache.drill.common.expression.ValueExpressions.BooleanExpression;
import org.apache.drill.common.expression.ValueExpressions.DateExpression;
import org.apache.drill.common.expression.ValueExpressions.DoubleExpression;
import org.apache.drill.common.expression.ValueExpressions.FloatExpression;
import org.apache.drill.common.expression.ValueExpressions.IntExpression;
import org.apache.drill.common.expression.ValueExpressions.LongExpression;
import org.apache.drill.common.expression.ValueExpressions.QuotedString;
import org.apache.drill.common.expression.ValueExpressions.TimeExpression;
import org.apache.drill.common.expression.visitors.AbstractExprVisitor;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;

/**
 * Process a single function/comparison, i.e. a = '1' into the component parts.
 */
public class SingleFunctionProcessor {

  private boolean success;
  private Object value;
  private SchemaPath path;
  private String functionName;

  public static boolean isCompareFunction(String functionName) {
    return COMPARE_FUNCTIONS_TRANSPOSE_MAP.keySet().contains(functionName);
  }

  public static SingleFunctionProcessor process(FunctionCall call) {
    String functionName = call.getName();
    functionName = functionName.toLowerCase().replace(" ", "");

    LogicalExpression nameArg = call.args.get(0);
    LogicalExpression valueArg = call.args.size() >= 2 ? call.args.get(1) : null;
    SingleFunctionProcessor evaluator = new SingleFunctionProcessor(functionName);

    ValueVisitor value = new ValueVisitor();
    PathVisitor path = new PathVisitor(value);
    boolean hasPath = nameArg.accept(path, valueArg);
    if (!hasPath) {
      functionName = COMPARE_FUNCTIONS_TRANSPOSE_MAP.get(functionName);
      hasPath = valueArg.accept(path, nameArg);
    }

    evaluator.success = hasPath && value.success;
    evaluator.path = path.path;
    evaluator.value = path.value;
    evaluator.functionName = functionName;

    return evaluator;
  }

  private SingleFunctionProcessor(String functionName) {
    this.success = false;
    this.functionName = functionName;
  }

  public Object getValue() {
    return value;
  }

  public boolean isSuccess() {
    return success;
  }

  public SchemaPath getPath() {
    return path;
  }

  public String getFunctionName() {
    return functionName;
  }

  private static class PathVisitor
    extends AbstractExprVisitor<Boolean, LogicalExpression, RuntimeException> {

    private SchemaPath path = null;
    private Object value = null;
    private final ValueVisitor valueVisitor;

    private PathVisitor(ValueVisitor valueVisitor) {
      this.valueVisitor = valueVisitor;
    }

    @Override
    public Boolean visitCastExpression(CastExpression e, LogicalExpression valueArg)
      throws RuntimeException {
      if (e.getInput() instanceof CastExpression || e.getInput() instanceof SchemaPath) {
        return e.getInput().accept(this, valueArg);
      }
      return false;
    }

    @Override
    public Boolean visitUnknown(LogicalExpression e, LogicalExpression valueArg)
      throws RuntimeException {
      return false;
    }

    @Override
    public Boolean visitSchemaPath(SchemaPath path, LogicalExpression valueArg)
      throws RuntimeException {
      this.path = path;
      if (valueArg != null) {
        this.value = valueArg.accept(valueVisitor, null);
      }
      return true;
    }
  }

  private static class ValueVisitor
    extends AbstractExprVisitor<Object, Void, RuntimeException> {
    public boolean success = true;

    @Override
    public Object visitUnknown(LogicalExpression e, Void valueArg)
      throws RuntimeException {
      this.success = false;
      return null;
    }

    @Override
    public Object visitSchemaPath(SchemaPath path, Void value)
      throws RuntimeException {
      return path.getAsUnescapedPath();
    }

    @Override
    public Object visitBooleanConstant(BooleanExpression e, Void value)
      throws RuntimeException {
      return e.getBoolean();
    }

    @Override
    public Object visitIntConstant(IntExpression intExpr, Void value)
      throws RuntimeException {
      return intExpr.getInt();
    }

    @Override
    public Object visitFloatConstant(FloatExpression fExpr, Void value)
      throws RuntimeException {
      return fExpr.getFloat();
    }

    @Override
    public Object visitLongConstant(LongExpression longExpr, Void value)
      throws RuntimeException {
      return longExpr.getLong();
    }

    @Override
    public Object visitDecimal28Constant(ValueExpressions.Decimal28Expression decExpr,
      Void value) throws RuntimeException {
      return decExpr.getBigDecimal();
    }

    @Override
    public Object visitDecimal38Constant(ValueExpressions.Decimal38Expression decExpr,
      Void value) throws RuntimeException {
      return decExpr.getBigDecimal();
    }

    @Override
    public Object visitDecimal9Constant(ValueExpressions.Decimal9Expression decExpr,
      Void value) throws RuntimeException {
      return new BigDecimal(BigInteger.valueOf(decExpr.getIntFromDecimal()), decExpr
        .getScale(), new MathContext(decExpr.getPrecision()));
    }

    @Override
    public Object visitDecimal18Constant(ValueExpressions.Decimal18Expression decExpr,
      Void value) throws RuntimeException {
      return new BigDecimal(BigInteger.valueOf(decExpr.getLongFromDecimal()), decExpr
        .getScale(), new MathContext(decExpr.getPrecision()));
    }

    @Override
    public Object visitDateConstant(DateExpression intExpr, Void value)
      throws RuntimeException {
      return intExpr.getDate();
    }

    @Override
    public Object visitTimeConstant(TimeExpression intExpr, Void value)
      throws RuntimeException {
      return intExpr.getTime();
    }

    @Override
    public Object visitTimeStampConstant(ValueExpressions.TimeStampExpression intExpr,
      Void value) throws RuntimeException {
      return intExpr.getTimeStamp();
    }

    @Override
    public Object visitDoubleConstant(DoubleExpression dExpr,
      Void value) throws RuntimeException {
      return dExpr.getDouble();
    }

    @Override
    public Object visitQuotedStringConstant(QuotedString e, Void value)
      throws RuntimeException {
      return e.getString();
    }

    @Override
    public Object visitCastExpression(CastExpression e, Void value)
      throws RuntimeException {
      switch (e.getMajorType().getMinorType()) {
        case NULL:
          return e.getInput().accept(this, value);
      }
      return super.visitCastExpression(e, value);
    }

    @Override
    public Object visitNullConstant(TypedNullConstant e, Void value)
      throws RuntimeException {
      return null;
    }
  }

  private static final ImmutableMap<String, String> COMPARE_FUNCTIONS_TRANSPOSE_MAP;

  static {
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    COMPARE_FUNCTIONS_TRANSPOSE_MAP = builder
      // unary functions
      .put("isnotnull", "isnotnull")
      .put("isNotNull", "isNotNull")
      .put("is not null", "is not null")
      .put("isnull", "isnull")
      .put("isNull", "isNull")
      .put("is null", "is null")
      // binary functions
      .put("like", "like")
      .put("equal", "equal")
      .put("not_equal", "not_equal")
      .put("greater_than_or_equal_to", "less_than_or_equal_to")
      .put("greater_than", "less_than")
      .put("less_than_or_equal_to", "greater_than_or_equal_to")
      .put("less_than", "greater_than")
      .build();
  }
}
