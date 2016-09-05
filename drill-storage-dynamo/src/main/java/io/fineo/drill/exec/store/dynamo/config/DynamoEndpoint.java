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

package io.fineo.drill.exec.store.dynamo.config;

import com.amazonaws.regions.RegionUtils;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName(DynamoEndpoint.NAME)
public class DynamoEndpoint {

  public static final String NAME = "aws";

  private final String regionOrUrl;

  public DynamoEndpoint(@JsonProperty("regionOrUrl") String regionOrUrl) {
    this.regionOrUrl = regionOrUrl;
  }

  @JsonIgnore
  public void configure(AmazonDynamoDBAsyncClient client) {
    // set the region or the url, depending on the format
    if (regionOrUrl.contains(":")) {
      client.setEndpoint(regionOrUrl);
    } else {
      client.setRegion(RegionUtils.getRegion(regionOrUrl));
    }
  }

  @JsonProperty("regionOrUrl")
  public String getRegionOrUrl() {
    return regionOrUrl;
  }
}
