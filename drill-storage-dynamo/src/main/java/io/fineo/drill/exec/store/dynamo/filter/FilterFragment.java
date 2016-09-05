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

import io.fineo.drill.exec.store.dynamo.spec.filter.DynamoFilterSpec;

/**
 *
 */
public class FilterFragment {
  private DynamoFilterSpec filter;
  private boolean equals;
  private boolean equality;
  private boolean isHash;
  private boolean isRange;

  public FilterFragment(DynamoFilterSpec filter, boolean equals, boolean equality, boolean isHash,
    boolean isRange) {
    this.filter = filter;
    this.equals = equals;
    this.equality = equality;
    this.isHash = isHash;
    this.isRange = isRange;
  }

  public DynamoFilterSpec getFilter() {
    return filter;
  }

  public boolean isEquals() {
    return equals;
  }

  public void setEquals(boolean equals) {
    this.equals = equals;
  }

  public boolean isEquality() {
    return equality;
  }

  public boolean isHash() {
    return isHash;
  }

  public boolean isRange() {
    return isRange;
  }

  public boolean isAttribute() {
    return !(isHash() || isRange());
  }

  @Override
  public String toString() {
    return "FilterFragment{" +
           "filter=" + filter +
           ", equals=" + equals +
           ", equality=" + equality +
           ", isHash=" + isHash +
           ", isRange=" + isRange +
           '}';
  }
}
