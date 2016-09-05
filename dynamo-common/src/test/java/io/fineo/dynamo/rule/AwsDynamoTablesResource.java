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

package io.fineo.dynamo.rule;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import io.fineo.dynamo.LocalDynamoTestUtil;
import org.junit.rules.ExternalResource;


/**
 * Manage aws tables and getting a connection to them. Generally, this should be used at the
 * {@link org.junit.Rule} level.
 */
public class AwsDynamoTablesResource extends ExternalResource {

  private final AwsDynamoResource dynamoResource;
  private LocalDynamoTestUtil util;
  private AmazonDynamoDBAsyncClient client;

  public AwsDynamoTablesResource(AwsDynamoResource dynamo) {
    this.dynamoResource = dynamo;
  }

  @Override
  protected void after() {
    if (getAsyncClient().listTables().getTableNames().size() == 0) {
      return;
    }

    util.cleanupTables();
    // reset any open clients
    if (client != null) {
      client.shutdown();
      client = null;
    }
  }

  public String getTestTableName() {
    return getUtil().getCurrentTestTable();
  }

  public AmazonDynamoDBAsyncClient getAsyncClient() {
    if (this.client == null) {
      this.client = getUtil().getAsyncClient();
    }
    return this.client;
  }

  private LocalDynamoTestUtil getUtil() {
    if (this.util == null) {
      this.util = dynamoResource.getUtil();
    }
    return this.util;
  }
}
