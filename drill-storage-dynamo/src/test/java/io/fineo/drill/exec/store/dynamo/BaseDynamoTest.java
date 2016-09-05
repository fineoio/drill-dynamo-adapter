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

package io.fineo.drill.exec.store.dynamo;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import io.fineo.drill.exec.store.dynamo.config.DynamoEndpoint;
import io.fineo.drill.exec.store.dynamo.config.DynamoStoragePluginConfig;
import io.fineo.drill.exec.store.dynamo.config.ParallelScanProperties;
import io.fineo.drill.exec.store.dynamo.config.StaticCredentialsConfig;
import io.fineo.lambda.dynamo.rule.AwsDynamoResource;
import io.fineo.lambda.dynamo.rule.AwsDynamoTablesResource;
import io.fineo.lambda.dynamo.rule.BaseDynamoTableTest;
import org.apache.drill.BaseTestQuery;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.rpc.user.QueryDataBatch;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.util.Text;
import org.apache.htrace.fasterxml.jackson.databind.ObjectMapper;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.google.common.collect.Lists.newArrayList;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 *
 */
public class BaseDynamoTest extends BaseTestQuery {

  @ClassRule
  public static AwsDynamoResource dynamo =
    new AwsDynamoResource(BaseDynamoTableTest.STATIC_CREDENTIALS_PROVIDER);
  @Rule
  public AwsDynamoTablesResource tables = new AwsDynamoTablesResource(dynamo);

  protected static final String PK = "pk";
  protected static final String COL1 = "col1";

  @BeforeClass
  public static void setupDefaultTestCluster() throws Exception {
    BaseTestQuery.setupDefaultTestCluster();

    updatePlugin(storagePluginConfig -> {
        storagePluginConfig.setEnabled(true);

        DynamoEndpoint endpoint = new DynamoEndpoint(dynamo.getUtil().getUrl());
        storagePluginConfig.setEndpointForTesting(endpoint);

        Map<String, Object> credentials = new HashMap<>();
        AWSCredentials creds = BaseDynamoTableTest.STATIC_CREDENTIALS_PROVIDER.getCredentials();
        StaticCredentialsConfig credentialsConfig = new StaticCredentialsConfig(creds
          .getAWSAccessKeyId(), creds.getAWSSecretKey());
        credentialsConfig.setCredentials(credentials);
        // map the credentials to a generic map and back again so the actual credential config gets
        // converted to a map, just like if/when we would configure the plugin 'normally'
        ObjectMapper mapper = new ObjectMapper();
        try {
          credentials = mapper.readValue(mapper.writeValueAsString(credentials), Map.class);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
        storagePluginConfig.setCredentialsForTesting(credentials);

        ParallelScanProperties scan = new ParallelScanProperties();
        scan.setMaxSegments(10);
        scan.setLimit(1);
        scan.setSegmentsPerEndpoint(1);
        storagePluginConfig.setScanPropertiesForTesting(scan);
      }
    );
  }

  protected static void updatePlugin(Consumer<DynamoStoragePluginConfig> update)
    throws ExecutionSetupException {
    final StoragePluginRegistry pluginRegistry = getDrillbitContext().getStorage();
    DynamoStoragePlugin storagePlugin = (DynamoStoragePlugin) pluginRegistry.getPlugin
      (DynamoStoragePlugin.NAME);
    DynamoStoragePluginConfig storagePluginConfig = (DynamoStoragePluginConfig) storagePlugin
      .getConfig();
    update.accept(storagePluginConfig);
    pluginRegistry.createOrUpdate(DynamoStoragePlugin.NAME, storagePluginConfig, true);
  }

  protected Item item() {
    Item item = new Item();
    item.with(PK, "pk");
    return item;
  }

  protected Map<String, Object> justOneRow(List<Map<String, Object>> rows) {
    assertEquals("Got more rows than expected! Rows: " + rows, 1, rows.size());
    return rows.get(0);
  }

  protected Table putAndSelectStar(Item... items) throws Exception {
    Table table = createHashTable();
    for (Item item : items) {
      table.putItem(item);
    }
    selectStar(table, items);
    return table;
  }

  protected void selectStar(Table table, Item... items) throws Exception {
    selectStar(table, true, items);
  }

  protected List<Map<String, Object>> selectStar(Table table, boolean verify, Item... items) throws
    Exception {
    String sql = "SELECT *" + from(table);
    List<Map<String, Object>> rows = runAndReadResults(sql);
    if (verify) {
      verify(rows, items);
    }
    return rows;
  }

  protected String from(Table table) {
    return " FROM dynamo." + table.getTableName() + " ";
  }

  protected List<Map<String, Object>> runAndReadResults(String sql) throws Exception {
    List<QueryDataBatch> results = testSqlWithResults(sql);
    return readObjects(results);
  }

  protected void verify(List<Map<String, Object>> rows, Item... items) {
    assertEquals("Wrong number of expected rows!" +
                 "\nExpected: " + toString(items) +
                 "\nGot rows: " + rows,
      items.length, rows.size());
    for (int i = 0; i < items.length; i++) {
      Map<String, Object> row = rows.get(i);
      Item item = items[i];
      assertEquals("Wrong number of fields in row! Got row: " + row + "\nExpected: " + item,
        row.size(), item.asMap().size());
      for (Map.Entry<String, Object> field : row.entrySet()) {
        String name = field.getKey();
        Object o = field.getValue();
        if (o instanceof Text) {
          o = o.toString();
        }
        if (item.get(name) instanceof Number) {
          equalsNumber(item, name, row);
        } else if (o instanceof byte[]) {
          assertArrayEquals("Array mismatch for: " + name, (byte[]) item.get(name), (byte[]) o);
        } else {
          assertEquals("Mismatch for: " + name, item.get(name), o);
        }
      }
    }
  }

  private String toString(Item... items) {
    List<Item> list = newArrayList(items);
    return list.stream().map(item -> item.asMap()).collect(Collectors.toList()).toString();
  }

  protected List<Map<String, Object>> readObjects(List<QueryDataBatch> results) throws
    SchemaChangeException {
    List<Map<String, Object>> rows = new ArrayList<>();
    final RecordBatchLoader loader = new RecordBatchLoader(getAllocator());
    for (final QueryDataBatch result : results) {
      loader.load(result.getHeader().getDef(), result.getData());
      List<Map<String, Object>> read = readRow(loader);
      rows.addAll(read);
      loader.clear();
      result.release();
    }
    return rows;
  }


  protected List<Map<String, Object>> readRow(RecordBatchLoader loader) {
    List<Map<String, Object>> rows = new ArrayList<>();
    for (int row = 0; row < loader.getRecordCount(); row++) {
      Map<String, Object> rowMap = new HashMap<>();
      rows.add(rowMap);
      for (VectorWrapper<?> vw : loader) {
        MaterializedField field = vw.getField();
        String name = field.getName();
        Object o = vw.getValueVector().getAccessor().getObject(row);
        rowMap.put(name, o);
      }
    }

    for (VectorWrapper<?> vw : loader) {
      vw.clear();
    }
    return rows;
  }

  protected Table createTableWithItems(Item... items) throws InterruptedException {
    Table table = createHashTable();
    for (Item item : items) {
      table.putItem(item);
    }
    return table;
  }

  protected Table createHashTable() throws InterruptedException {
    // single hash PK
    ArrayList<AttributeDefinition> attributeDefinitions = new ArrayList<>();
    attributeDefinitions.add(new AttributeDefinition()
      .withAttributeName(PK).withAttributeType("S"));
    ArrayList<KeySchemaElement> keySchema = new ArrayList<>();
    keySchema.add(new KeySchemaElement().withAttributeName(PK).withKeyType(KeyType.HASH));

    CreateTableRequest request = new CreateTableRequest()
      .withKeySchema(keySchema)
      .withAttributeDefinitions(attributeDefinitions);
    return createTable(request);
  }

  protected Table createHashAndSortTable(String pk, String sort) throws InterruptedException {
    ArrayList<AttributeDefinition> attributeDefinitions = new ArrayList<>();
    ScalarAttributeType type = ScalarAttributeType.S;
    attributeDefinitions.add(new AttributeDefinition()
      .withAttributeName(pk).withAttributeType(type));
    attributeDefinitions
      .add(new AttributeDefinition().withAttributeName(sort).withAttributeType(type));
    ArrayList<KeySchemaElement> keySchema = new ArrayList<>();
    keySchema.add(new KeySchemaElement().withAttributeName(pk).withKeyType(KeyType.HASH));
    keySchema.add(new KeySchemaElement().withAttributeName(sort).withKeyType(KeyType.RANGE));

    CreateTableRequest request = new CreateTableRequest()
      .withKeySchema(keySchema)
      .withAttributeDefinitions(attributeDefinitions);
    return createTable(request);
  }

  protected Table createTable(CreateTableRequest request) throws InterruptedException {
    DynamoDB dynamoDB = new DynamoDB(tables.getAsyncClient());
    request.withProvisionedThroughput(new ProvisionedThroughput()
      .withReadCapacityUnits(5L)
      .withWriteCapacityUnits(6L));

    if (request.getTableName() == null) {
      String tableName = tables.getTestTableName();
      tableName = tableName.replace('-', '_');
      request.setTableName(tableName);
    }

    Table table = dynamoDB.createTable(request);
    table.waitForActive();
    return table;
  }

  protected void equalsNumber(Item expected, String key, Map<String, Object> row) {
    assertEquals(expected.get(key), new BigDecimal(row.get(key).toString()));
  }

  protected void equalsText(Item expected, String key, Map<String, Object> row) {
    Object actual = row.get(key);
    if (actual instanceof Text) {
      actual = actual.toString();
    }
    assertEquals(expected.get(key), actual);
  }
}
