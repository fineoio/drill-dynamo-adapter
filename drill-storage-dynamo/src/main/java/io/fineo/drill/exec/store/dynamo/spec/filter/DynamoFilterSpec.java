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

package io.fineo.drill.exec.store.dynamo.spec.filter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * Tree structured filter specification for building a Dynamo filter
 */
@JsonTypeName("dynamo-filter-spec")
public class DynamoFilterSpec {

  static final String AND = "AND";
  static final String OR = "OR";

  // mapping of functions for checking individual columns
  private static final Map<String, Function<LeafInfo, FilterTree.FilterLeaf>> COLUMN_FUNCTION_MAP = new
    HashMap<>();

  static {
    COLUMN_FUNCTION_MAP.put("isNull", func0("attribute_not_exists"));
    COLUMN_FUNCTION_MAP.put("isNotNull", func0("attribute_exists"));
    COLUMN_FUNCTION_MAP.put("between",
      info -> new FilterTree.FilterLeaf(info.key, "BETWEEN", "%1$s %2$s %3$s AND %4$s",
        info.values));
    COLUMN_FUNCTION_MAP.put("equal", op("="));
    COLUMN_FUNCTION_MAP.put("not_equal", op("<>"));
    COLUMN_FUNCTION_MAP.put("greater_than_or_equal_to", op(">="));
    COLUMN_FUNCTION_MAP.put("greater_than", op(">"));
    COLUMN_FUNCTION_MAP.put("less_than_or_equal_to", op("<="));
    COLUMN_FUNCTION_MAP.put("less_than", op("<"));
  }

  // functions for interrogating the contents of a document (list, map, set)
  private static final Map<String, Function<LeafInfo, FilterTree.FilterLeaf>> DOCUMENT_FUNCTION_MAP =
    new HashMap<>();

  static {
    DOCUMENT_FUNCTION_MAP.put("isNotNull", func0("contains"));
    // count? -> size
  }

  private static class LeafInfo {
    private String key;
    private Object[] values;

    public LeafInfo(String fieldName, Object[] fieldValue) {
      this.key = fieldName;
      this.values = fieldValue;
    }
  }

  private static Function<LeafInfo, FilterTree.FilterLeaf> op(String name) {
    return info -> new FilterTree.FilterLeaf(info.key, name, "%s %s %s", info.values);
  }

  private static Function<LeafInfo, FilterTree.FilterLeaf> func0(String name) {
    return info -> new FilterTree.FilterLeaf(info.key, name, "%2$s(%1$s)");
  }


  public static DynamoFilterSpec copy(FilterTree.FilterLeaf leaf) {
    return new DynamoFilterSpec(new FilterTree(leaf.deepClone()));
  }

  public static DynamoFilterSpec create(String functionName, String fieldName, Object...
    fieldValue) {
    Function<LeafInfo, FilterTree.FilterLeaf> func;
    LeafInfo info = new LeafInfo(fieldName, fieldValue);
    // map or list
    if (fieldName.contains(".") || fieldName.contains("[")) {
      func = DOCUMENT_FUNCTION_MAP.get(functionName);
    } else {
      func = COLUMN_FUNCTION_MAP.get(functionName);
    }
    return func == null ? null : new DynamoFilterSpec(new FilterTree(func.apply(info)));
  }

  private FilterTree tree;

  @JsonCreator
  public DynamoFilterSpec(@JsonProperty("tree") FilterTree tree) {
    this.tree = tree;
  }

  public DynamoFilterSpec() {
  }

  @JsonIgnore
  public DynamoFilterSpec and(DynamoFilterSpec rightKey) {
    if (rightKey == null) {
      return this;
    }
    this.tree.and(rightKey.tree.getRoot());
    return this;
  }

  @JsonIgnore
  public DynamoFilterSpec or(DynamoFilterSpec rightKey) {
    if (rightKey == null) {
      return this;
    }
    this.tree.or(rightKey.tree.getRoot());
    return this;
  }

  @JsonProperty
  public FilterTree getTree() {
    return tree;
  }

  @JsonIgnore
  public DynamoFilterSpec deepClone() {
    FilterTree.FilterNode
      root = this.getTree().visit(new FilterTree.FilterNodeVisitor<FilterTree.FilterNode>() {
      @Override
      public FilterTree.FilterNode visitInnerNode(FilterTree.FilterNodeInner inner) {
        FilterTree.FilterNode left = inner.getLeft().visit(this);
        FilterTree.FilterNode right = inner.getRight().visit(this);
        FilterTree.FilterNode
          innerCopy = new FilterTree.FilterNodeInner(inner.getCondition(), left, right);
        return innerCopy;
      }

      @Override
      public FilterTree.FilterNode visitLeafNode(FilterTree.FilterLeaf leaf) {
        return leaf.deepClone();
      }
    });
    FilterTree copy = new FilterTree(root);
    return new DynamoFilterSpec(copy);
  }

  @Override
  public String toString() {
    return "DynamoFilterSpec{" + tree + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (!(o instanceof DynamoFilterSpec))
      return false;

    DynamoFilterSpec that = (DynamoFilterSpec) o;

    return getTree() != null ? getTree().equals(that.getTree()) : that.getTree() == null;

  }

  @Override
  public int hashCode() {
    return getTree() != null ? getTree().hashCode() : 0;
  }
}
