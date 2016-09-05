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

package io.fineo.drill.exec.store.dynamo.physical;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Page;
import io.fineo.drill.exec.store.dynamo.config.DynamoEndpoint;
import io.fineo.drill.exec.store.dynamo.config.ParallelScanProperties;
import io.fineo.drill.exec.store.dynamo.spec.DynamoTableDefinition;
import io.fineo.drill.exec.store.dynamo.spec.sub.DynamoSubQuerySpec;
import io.fineo.drill.exec.store.dynamo.spec.sub.DynamoSubScanSpec;
import org.apache.drill.common.expression.SchemaPath;

import java.util.Iterator;
import java.util.List;

/**
 * Actually do the get/query/scan based on the {@link DynamoSubScanSpec}.
 */
public class DynamoQueryRecordReader extends DynamoRecordReader<DynamoSubQuerySpec>{

  private final ParallelScanProperties scanProps;

  public DynamoQueryRecordReader(AWSCredentialsProvider credentials, ClientConfiguration clientConf,
    DynamoEndpoint endpoint, DynamoSubQuerySpec scanSpec,
    List<SchemaPath> columns, boolean consistentRead, ParallelScanProperties scanProperties,
    DynamoTableDefinition scan) {
    super(credentials, clientConf, endpoint, scanSpec, columns, consistentRead, scan);
    this.scanProps = scanProperties;
  }

  @Override
  protected Iterator<Page<Item, ?>> buildQuery(DynamoQueryBuilder builder,
    AmazonDynamoDBAsyncClient client) {
    return builder.withProps(scanProps).build(client).query();
  }
}
