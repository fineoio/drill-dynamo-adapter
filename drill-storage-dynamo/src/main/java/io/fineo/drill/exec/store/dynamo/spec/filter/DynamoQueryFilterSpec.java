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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.fineo.drill.exec.store.dynamo.spec.DynamoReadFilterSpec;


@JsonTypeName("dynamo-get-filter-spec")
public class DynamoQueryFilterSpec extends DynamoReadFilterSpec {

  private DynamoFilterSpec attributeFilter;

  @JsonCreator
  public DynamoQueryFilterSpec(@JsonProperty("key") DynamoFilterSpec keyFilter,
    @JsonProperty("attr") DynamoFilterSpec attributeFilter) {
    super(keyFilter);
    this.attributeFilter = attributeFilter;
  }

  @JsonProperty("attr")
  public DynamoFilterSpec getAttributeFilter() {
    return attributeFilter;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (!(o instanceof DynamoQueryFilterSpec))
      return false;
    if (!super.equals(o))
      return false;

    DynamoQueryFilterSpec that = (DynamoQueryFilterSpec) o;

    return getAttributeFilter() != null ? getAttributeFilter().equals(that.getAttributeFilter()) :
           that.getAttributeFilter() == null;

  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + (getAttributeFilter() != null ? getAttributeFilter().hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "DynamoQueryFilterSpec{" +
           "keyFilter=" + key +
           ", attributeFilter=" + attributeFilter +
           '}';
  }
}
