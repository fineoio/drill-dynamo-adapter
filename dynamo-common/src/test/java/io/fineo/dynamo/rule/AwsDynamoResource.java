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

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import io.fineo.dynamo.LocalDynamoTestUtil;
import org.junit.rules.ExternalResource;

/**
 * Helper resource to standup and shutdown a local dynamo instance.
 * <p>
 * This handles setting up a local DynamoDB when the rule is created and shutting down the
 * database when the rule is stopped. If you are using {@link org.junit.ClassRule}, then the
 * database will be up before the {@link org.junit.BeforeClass} method is called and shutdown
 * after {@link org.junit.AfterClass}.
 * </p>
 * <p>
 * Sometimes, you will want to remove tables between tests. In that case, you can call {@link
 * #cleanup()} to remove all the current tables
 * </p>
 */
public class AwsDynamoResource extends ExternalResource {

  private final AWSCredentialsProvider credentials;
  private LocalDynamoTestUtil util;

  public AwsDynamoResource(AWSCredentialsProvider credentials){
    this.credentials = credentials;
  }

  private LocalDynamoTestUtil start() throws Exception {
    util = new LocalDynamoTestUtil(credentials);
    util.start();
    return util;
  }

  @Override
  protected void before() throws Exception {
    start();
  }

  @Override
  protected void after() {
    if (util != null) {
      try {
        util.stop();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  public LocalDynamoTestUtil getUtil() {
    return this.util;
  }

  public AWSCredentialsProvider getCredentialsProvider() {
    return credentials;
  }

  public AmazonDynamoDBClient getClient() {
    return util.getClient();
  }

  public void cleanup() {
    this.util.cleanupTables();
  }
}
