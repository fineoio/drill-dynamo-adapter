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
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeName;

import java.util.Arrays;

@JsonTypeName("dyamo-filter-tree")
//@JsonDeserialize(using = FilterTreeDeserializer.class)
public class FilterTree {
  private FilterNode root;

  @JsonCreator
  public FilterTree(@JsonProperty("root") FilterNode root) {
    this.root = root;
  }

  private FilterTree op(String op, FilterNode right) {
    FilterNodeInner inner = new FilterNodeInner(op, root, right);
    root = inner;
    return this;
  }

  public FilterTree and(FilterNode root) {
    return op(DynamoFilterSpec.AND, root);
  }

  public FilterTree or(FilterNode root) {
    return op(DynamoFilterSpec.OR, root);
  }

  public String toString() {
    return this.root.toString();
  }

  @JsonProperty
  public FilterNode getRoot() {
    return root;
  }

  public <T> T visit(FilterNodeVisitor<T> visitor) {
    if (this.root == null) {
      return null;
    }
    FilterNode node = getRoot();
    return node.visit(visitor);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (!(o instanceof FilterTree))
      return false;

    FilterTree that = (FilterTree) o;

    return getRoot() != null ? getRoot().equals(that.getRoot()) : that.getRoot() == null;
  }

  @Override
  public int hashCode() {
    return getRoot() != null ? getRoot().hashCode() : 0;
  }

  @JsonTypeInfo(use=JsonTypeInfo.Id.CLASS, include= JsonTypeInfo.As.PROPERTY, property="@class")
  public abstract static class FilterNode {
    private FilterNode parent;

    @JsonIgnore
    public void setParent(FilterNode parent) {
      this.parent = parent;
    }

    @JsonIgnore
    public FilterNode getParent() {
      return parent;
    }

    public abstract <T> T visit(FilterNodeVisitor<T> visitor);
  }

  @JsonTypeName("dynamo-filter-tree-inner-node")
  public static class FilterNodeInner extends FilterNode {
    private String condition;
    private FilterNode left;
    private FilterNode right;

    public FilterNodeInner(@JsonProperty("condition") String bool,
      @JsonProperty("left") FilterNode left, @JsonProperty("right") FilterNode right) {
      this.condition = bool;
      this.left = left;
      left.setParent(this);
      this.right = right;
      right.setParent(this);
    }

    @Override
    public String toString() {
      return "( " + left.toString() + " ) " + condition + " ( " + right.toString() + " )";
    }

    @JsonProperty
    public String getCondition() {
      return condition;
    }

    @JsonProperty
    public FilterNode getLeft() {
      return left;
    }

    @JsonProperty
    public FilterNode getRight() {
      return right;
    }

    @JsonIgnore
    public boolean and() {
      return this.condition.equals(DynamoFilterSpec.AND);
    }

    @Override
    public <T> T visit(FilterNodeVisitor<T> visitor) {
      return visitor.visitInnerNode(this);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o)
        return true;
      if (!(o instanceof FilterNodeInner))
        return false;

      FilterNodeInner that = (FilterNodeInner) o;

      if (getCondition() != null ? !getCondition().equals(that.getCondition()) :
          that.getCondition() != null)
        return false;
      if (getLeft() != null ? !getLeft().equals(that.getLeft()) : that.getLeft() != null)
        return false;
      return getRight() != null ? getRight().equals(that.getRight()) : that.getRight() == null;

    }

    @Override
    public int hashCode() {
      int result = getCondition() != null ? getCondition().hashCode() : 0;
      result = 31 * result + (getLeft() != null ? getLeft().hashCode() : 0);
      result = 31 * result + (getRight() != null ? getRight().hashCode() : 0);
      return result;
    }
  }

  @JsonTypeName("dynamo-filter-tree-leaf")
  public static class FilterLeaf extends FilterNode {
    protected String operand;
    protected String format;
    protected String key;
    protected Object[] values;

    @JsonCreator
    public FilterLeaf(@JsonProperty("key") String key, @JsonProperty("operand") String operand,
      @JsonProperty("format") String format, @JsonProperty("values") Object... values) {
      this.format = format;
      this.operand = operand;
      this.key = key;
      this.values = values;
    }

    @JsonProperty
    public String getKey() {
      return key;
    }

    @JsonProperty
    public String getOperand() {
      return this.operand;
    }

    @JsonProperty
    public String getFormat() {
      return this.format;
    }

    @JsonProperty
    public Object[] getValues() {
      return values;
    }

    @Override
    public <T> T visit(FilterNodeVisitor<T> visitor) {
      return visitor.visitLeafNode(this);
    }

    @JsonIgnore
    public FilterNode deepClone() {
      return new FilterLeaf(key, operand, format, values);
    }

    @JsonIgnore
    public void setKey(String key) {
      this.key = key;
    }

    @JsonIgnore
    public void setValue(int i, String value) {
      this.values[i] = value;
    }

    @Override
    public String toString() {
      Object[] formatted = new Object[values == null ? 2 : values.length + 2];
      formatted[0] = key;
      formatted[1] = operand;
      System.arraycopy(values, 0, formatted, 2, values.length);
      return String.format(format, formatted);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o)
        return true;
      if (!(o instanceof FilterLeaf))
        return false;

      FilterLeaf that = (FilterLeaf) o;

      if (getOperand() != null ? !getOperand().equals(that.getOperand()) :
          that.getOperand() != null)
        return false;
      if (getFormat() != null ? !getFormat().equals(that.getFormat()) : that.getFormat() != null)
        return false;
      if (getKey() != null ? !getKey().equals(that.getKey()) : that.getKey() != null)
        return false;
      // Probably incorrect - comparing Object[] arrays with Arrays.equals
      return Arrays.equals(getValues(), that.getValues());

    }

    @Override
    public int hashCode() {
      int result = getOperand() != null ? getOperand().hashCode() : 0;
      result = 31 * result + (getFormat() != null ? getFormat().hashCode() : 0);
      result = 31 * result + (getKey() != null ? getKey().hashCode() : 0);
      result = 31 * result + Arrays.hashCode(getValues());
      return result;
    }
  }

  public static interface FilterNodeVisitor<T> {
    T visitInnerNode(FilterNodeInner inner);

    T visitLeafNode(FilterLeaf leaf);
  }
}
