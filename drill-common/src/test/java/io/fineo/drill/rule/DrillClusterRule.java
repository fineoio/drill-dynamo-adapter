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

package io.fineo.drill.rule;

import io.fineo.drill.LocalDrillCluster;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.rules.ExternalResource;

import java.sql.Connection;

/**
 * Create and destroy a drill cluster as a junit rule
 */
public class DrillClusterRule extends ExternalResource {

  private static final Log LOG = LogFactory.getLog(DrillClusterRule.class);
  private final LocalDrillCluster drill;

  public DrillClusterRule(int serverCount) {
    drill = new LocalDrillCluster(serverCount);
  }

  @Override
  protected void before() throws Throwable {
    drill.setup();
  }

  @Override
  protected void after() {
    drill.shutdown();
  }

  public Connection getConnection() throws Exception {
    return drill.getConnection();
  }
}
