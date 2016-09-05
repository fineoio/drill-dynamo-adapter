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

package io.fineo.drill;

import com.google.common.collect.ImmutableList;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.ZookeeperHelper;
import org.apache.drill.exec.metrics.DrillMetrics;
import org.apache.drill.exec.server.Drillbit;
import org.apache.drill.jdbc.ConnectionFactory;
import org.apache.drill.jdbc.ConnectionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;
import java.util.Properties;

import static java.lang.String.format;

public class LocalDrillCluster {
  private static final Logger LOG = LoggerFactory.getLogger(LocalDrillCluster.class);
  private ZookeeperHelper zkHelper;
  private final int serverCount;
  private final SingleConnectionCachingFactory factory;
  private final Properties props = new Properties();
  private List<Drillbit> servers;

  {
    props.put(ExecConstants.SYS_STORE_PROVIDER_LOCAL_ENABLE_WRITE, "false");
    props.put(ExecConstants.HTTP_ENABLE, "false");
  }

  public LocalDrillCluster(int serverCount) {
    this.serverCount = serverCount;
    this.factory =  new SingleConnectionCachingFactory(new ConnectionFactory() {
      @Override
      public Connection getConnection(ConnectionInfo info) throws Exception {
        Class.forName("org.apache.drill.jdbc.Driver");
        return DriverManager.getConnection(info.getUrl(), info.getParamsAsProperties());
      }
    });
    zkHelper = new ZookeeperHelper();
  }

  public void setup() throws Throwable {
    zkHelper.startZookeeper(1);

    // turn off the HTTP server to avoid port conflicts between the drill bits
    System.setProperty(ExecConstants.HTTP_ENABLE, "false");
    ImmutableList.Builder<Drillbit> servers = ImmutableList.builder();
    for (int i = 0; i < serverCount; i++) {
      servers.add(Drillbit.start(zkHelper.getConfig()));
    }
    this.servers = servers.build();
  }


  public void shutdown() {
    DrillMetrics.resetMetrics();

    if (servers != null) {
      for (Drillbit server : servers) {
        try {
          server.close();
        } catch (Exception e) {
          LOG.error("Error shutting down Drillbit", e);
        }
      }
    }

    zkHelper.stopZookeeper();
  }

  public String getUrl() {
    String zkConnection = zkHelper.getConfig().getString("drill.exec.zk.connect");
    return format("jdbc:drill:zk=%s", zkConnection);
  }

  public Connection getConnection() throws Exception {
    return factory.getConnection(new ConnectionInfo(getUrl(), new Properties()));
  }
}
