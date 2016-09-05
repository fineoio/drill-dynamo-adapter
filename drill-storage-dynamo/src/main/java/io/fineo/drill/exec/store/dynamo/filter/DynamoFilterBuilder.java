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

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import io.fineo.drill.exec.store.dynamo.DynamoGroupScan;
import io.fineo.drill.exec.store.dynamo.spec.filter.DynamoFilterSpec;
import io.fineo.drill.exec.store.dynamo.spec.DynamoGroupScanSpec;
import io.fineo.drill.exec.store.dynamo.spec.DynamoReadFilterSpec;
import io.fineo.drill.exec.store.dynamo.spec.DynamoTableDefinition;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.drill.common.expression.BooleanOperator;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.expression.visitors.AbstractExprVisitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Filter builder heavily based on the Drill HBaseFilterBuilder. Depth-first exploration of the
 * logical expression and converts them into a filter by finding the 'leaf' expressions (i.e. a =
 * '1') and then progressively combing them with AND/OR expressions based on the function at the
 * layer above.
 * <p>
 * There is a subtle difference in how we handle nulls. If the request is <tt>a = null</tt> (with
 * an optional cast) we do an actual check for the field value being null, e.g. <tt>a = null</tt>.
 * However, if the request is <tt>isNull(a)</tt> we do <tt>attribute_exists(a)</tt>, which
 * is also a null check, but only returns if the attribute <i>has not been set</i>, compared to
 * the former, where it returns if the attribute <i><b>has been set to null</b></i>.
 * </p>
 */
public class DynamoFilterBuilder {

  private static final Logger LOG = LoggerFactory.getLogger(DynamoFilterBuilder.class);
  private static final String AND = "booleanAnd";
  private static final String OR = "booleanOr";

  final private DynamoGroupScan groupScan;

  final private LogicalExpression le;
  private final DynamoTableDefinition.PrimaryKey rangeKey;
  private DynamoTableDefinition.PrimaryKey hashKey;

  private boolean allExpressionsConverted = true;

  DynamoFilterBuilder(DynamoGroupScan groupScan, LogicalExpression le) {
    this.groupScan = groupScan;
    this.le = le;

    // figure out the pks
    List<DynamoTableDefinition.PrimaryKey> pks = groupScan.getSpec().getTable().getKeys();
    this.hashKey = pks.get(0);
    if (pks.size() > 1) {
      if (hashKey.getIsHashKey()) {
        this.rangeKey = pks.get(1);
      } else {
        this.rangeKey = hashKey;
        this.hashKey = pks.get(1);
      }
    } else {
      this.rangeKey = null;
    }
  }

  public DynamoGroupScanSpec parseTree() {
    DynamoQuerySpecBuilder builder = new DynamoQuerySpecBuilder();
    DynamoReadBuilder parsedSpec = le.accept(builder, null);
    if (parsedSpec != null) {
      if (!parsedSpec.handledFilter()) {
        this.allExpressionsConverted = false;
      }
      // combine with the existing scan
      return merge(this.groupScan.getSpec(), parsedSpec);
    }
    return null;
  }

  private DynamoGroupScanSpec merge(DynamoGroupScanSpec spec, DynamoReadBuilder parsedSpec) {
    // convert the spec into a read builder
    if (spec.getScan() != null) {
      DynamoReadFilterSpec scan = spec.getScan();
      parsedSpec.andScanSpec(scan);
    } else {
      List<DynamoReadFilterSpec> getOrQuery = spec.getGetOrQuery();
      if (getOrQuery != null && getOrQuery.size() > 0) {
        parsedSpec.andGetOrQuery(getOrQuery);
      }
    }
    return parsedSpec.buildSpec(spec.getTable());
  }

  public boolean isAllExpressionsConverted() {
    return allExpressionsConverted;
  }

  private class DynamoQuerySpecBuilder
    extends AbstractExprVisitor<DynamoReadBuilder, Void, RuntimeException> {

    @Override
    public DynamoReadBuilder visitUnknown(LogicalExpression e, Void value) throws RuntimeException {
      allExpressionsConverted = false;
      return null;
    }

    @Override
    public DynamoReadBuilder visitBooleanOperator(BooleanOperator op, Void value)
      throws RuntimeException {
      // between gets compiled down into <= AND >=. Try to 'unconvert' it so we can use the
      // dynamo between function
      if (op.getName().equals(AND)) {
        if (op.args.size() >= 2) {
          // find the map of arg -> processed function
          Multimap<String, Pair<FunctionCall, SingleFunctionProcessor>> columnFunctions =
            ArrayListMultimap.create();
          for (LogicalExpression le : op.args) {
            if (le instanceof FunctionCall) {
              FunctionCall func = (FunctionCall) le;
              SingleFunctionProcessor processor = SingleFunctionProcessor.process(func);
              if (processor != null) {
                columnFunctions.put(processor.getPath().getAsUnescapedPath(), new ImmutablePair<>
                  (func, processor));
              }
            }
          }

          // find any columns that have lte && gte and are not the primary key
          for (String column : columnFunctions.keySet()) {
            if (column.equals(hashKey.getName())) {
              continue;
            }

            Collection<Pair<FunctionCall, SingleFunctionProcessor>> calls = columnFunctions.get
              (column);
            if (calls.size() <= 1) {
              continue;
            }
            List<FunctionCall> handled = new ArrayList<>();
            BetweenBuilder between =
              new BetweenBuilder(rangeKey != null && rangeKey.getName().equals(column));
            calls.stream().forEach(pair -> {
              if (between.addFunction(pair.getValue())) {
                handled.add(pair.getKey());
              }
            });
            FilterFragment fragment = between.build();
            // no enough information to replace a function
            if (fragment == null) {
              continue;
            }
            DynamoReadBuilder builder = new DynamoReadBuilder(hashKey, rangeKey);
            builder.set(fragment);
            if (!builder.handledFilter()) {
              allExpressionsConverted = false;
            }
            // done with this op
            if (op.args.size() == 2) {
              return builder;
            }
            // create the operation, but without the handled columns
            List<LogicalExpression> pending = op.args.stream().filter(expr -> !(
              handled.contains
                (expr))).collect(Collectors
              .toList());
            op = new BooleanOperator(op.getName(), pending, op.getPosition());
            DynamoReadBuilder subset = this.visitFunctionCall(op, null);
            if (subset != null) {
              builder.and(subset);
            }
            return builder;
          }
        }
      }
      return this.visitFunctionCall(op, null);
    }

    @Override
    public DynamoReadBuilder visitFunctionCall(FunctionCall call, Void value)
      throws RuntimeException {
      String functionName = call.getName();
      ImmutableList<LogicalExpression> args = call.args;

      // its a simple function call, i.e. a = '1', so just build the scan spec for that
      if (SingleFunctionProcessor.isCompareFunction(functionName)) {
        DynamoReadBuilder builder = null;
        SingleFunctionProcessor processor = SingleFunctionProcessor.process(call);
        if (processor.isSuccess()) {
          FilterFragment fragment = createDynamoFilter(processor);
          if (fragment == null) {
            allExpressionsConverted = false;
            return null;
          }
          builder = new DynamoReadBuilder(hashKey, rangeKey);
          builder.set(fragment);
        }
        return builder;
      }

      // its a more complicated function, so try and break it down as a combination of and/or
      switch (functionName) {
        case AND:
        case OR:
          boolean first = true;
          DynamoReadBuilder left = null;
          for (LogicalExpression expr : args) {
            if (first || left == null) {
              left = expr.accept(this, null);
              first = false;
              if (left == null) {
                allExpressionsConverted = false;
              }
              continue;
            }
            DynamoReadBuilder right = expr.accept(this, null);
            if (right == null) {
              allExpressionsConverted = false;
              continue;
            }
            if (!right.handledFilter()) {
              allExpressionsConverted = false;
            }
            switch (functionName) {
              case AND:
                left.and(right);
                break;
              case OR:
                left.or(right);
                break;
            }
          }

          return left;
      }

      return null;
    }

    private FilterFragment createDynamoFilter(SingleFunctionProcessor processor) {
      String functionName = processor.getFunctionName();
      SchemaPath field = processor.getPath();
      String fieldName = field.getAsUnescapedPath();
      Object fieldValue = processor.getValue();
      boolean isHashKey = DynamoFilterBuilder.this.hashKey.getName().equals(fieldName);
      boolean isRangeKey = DynamoFilterBuilder.this.rangeKey != null &&
                           DynamoFilterBuilder.this.rangeKey.getName().equals(fieldName);
      assert !(isHashKey && isRangeKey) : fieldName + " is both hashKey and rangeKey key";
      boolean equals = false;
      boolean equalityTest = false;

      // normalize function names, since drill can't do this already...apparently
      switch (functionName) {
        case "isnull":
        case "isNull":
        case "is null":
          functionName = "isNull";
          break;
        case "isnotnull":
        case "isNotNull":
        case "is not null":
          functionName = "isNotNull";
          break;
        case "equal":
          equals = true;
        case "not_equal":
        case "greater_than_or_equal_to":
        case "greater_than":
        case "less_than_or_equal_to":
        case "less_than":
          equalityTest = true;
          break;
      }
      DynamoFilterSpec filter = DynamoFilterSpec.create(functionName, fieldName, fieldValue);
      // we don't know how to handle this function
      if (filter == null) {
        return null;
      }

      return new FilterFragment(filter, equals, equalityTest, isHashKey, isRangeKey);
    }
  }
}
