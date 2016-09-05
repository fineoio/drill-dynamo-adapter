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

package io.fineo.lambda.dynamo;

import com.amazonaws.AmazonWebServiceClient;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.local.main.ServerRunner;
import com.amazonaws.services.dynamodbv2.local.server.DynamoDBProxyServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.ServerSocket;
import java.util.UUID;
import java.util.stream.StreamSupport;

/**
 * Utility instance to help create and run a local dynamo instance
 */
public class LocalDynamoTestUtil {

private static final Logger LOG = LoggerFactory.getLogger(LocalDynamoTestUtil.class);
  private AmazonDynamoDBClient dynamodb;
  private DynamoDBProxyServer server;

  private int port;
  private String url;
  private final AWSCredentialsProvider credentials;
  private String randomTableName = generateTableName();

  public LocalDynamoTestUtil(AWSCredentialsProvider credentials) {
    this.credentials = credentials;
  }

  public void start() throws Exception {
    // create a local database instance with an local server url on an open port
    ServerSocket socket = new ServerSocket(0);
    port = socket.getLocalPort();
    socket.close();

    final String[] localArgs = {"-inMemory", "-port", String.valueOf(port)};
    server = ServerRunner.createServerFromCommandLineArgs(localArgs);
    server.start();
    url = "http://localhost:" + port;

    // internal client connection so we can easily stop, cleanup, etc. later
    this.dynamodb = getClient();
  }

  public void stop() throws Exception {
    server.stop();
    dynamodb.shutdown();
  }

  public void cleanupTables() {
    cleanupTables(this.dynamodb);
    // get the next table name
    this.randomTableName = generateTableName();
  }

  public static void cleanupTables(AmazonDynamoDBClient client) {
    StreamSupport.stream(new DynamoDB(client).listTables()
                                             .pages().spliterator(), false)
                 .flatMap(page -> StreamSupport.stream(page.spliterator(), false))
                 .map(table -> table.getTableName())
                 .parallel().peek(name -> LOG.info("Deleting table: " + name))
                 .forEach(name -> client.deleteTable(name));

  }

  private String generateTableName() {
    return "test-" + UUID.randomUUID().toString();
  }

  public String getCurrentTestTable() {
    return this.randomTableName;
  }

  public AmazonDynamoDBClient getClient() {
    return withProvider(new AmazonDynamoDBClient(credentials));
  }

  public AmazonDynamoDBAsyncClient getAsyncClient() {
    return withProvider(new AmazonDynamoDBAsyncClient(credentials));
  }

  private <T extends AmazonWebServiceClient> T withProvider(T client) {
    client.setEndpoint("http://localhost:" + port);
    return client;
  }

  public String getUrl() {
    return this.url;
  }
}
