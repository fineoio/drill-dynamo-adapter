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

package io.fineo.lambda.dynamo.rule;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.internal.StaticCredentialsProvider;
import org.junit.ClassRule;
import org.junit.Rule;

/**
 * Helper base class that creates a local Dynamo cluster and cleans up tables after the test run.
 * Notethat you must use the <tt>maven-dependency-plugin</tt> to copy the depdendencies to the
 * <tt>target/</tt> directory when using a local Dynamo instance.
 */
public class BaseDynamoTableTest {

  public static final StaticCredentialsProvider STATIC_CREDENTIALS_PROVIDER =
    new StaticCredentialsProvider(
      // use fake credentials, but need to have some
      new BasicAWSCredentials("AKIAIZFKPYAKBFDZPAEA", "18S1bF4bpjCKZP2KRgbqOn7xJLDmqmwSXqq5GAWq"));
  @ClassRule
  public static AwsDynamoResource dynamo = new AwsDynamoResource(STATIC_CREDENTIALS_PROVIDER);
  @Rule
  public AwsDynamoTablesResource tables = new AwsDynamoTablesResource(dynamo);
}
