/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Forked from drill-common:1.6.0:tests:org.apache.drill.jdbc.SingleConnectionCachingFactory
 * to drop the JdbcTestBase reference
 */
package io.fineo.drill;

import com.google.common.base.Strings;
import org.apache.drill.jdbc.CachingConnectionFactory;
import org.apache.drill.jdbc.ConnectionFactory;
import org.apache.drill.jdbc.ConnectionInfo;
import org.apache.drill.jdbc.NonClosableConnection;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

/**
 * A connection factory that creates and caches a single connection instance.
 *
 * <p>
 *   Not thread safe.
 * </p>
 */
public class SingleConnectionCachingFactory{

  private final ConnectionFactory delegate;
  private Connection connection;

  public SingleConnectionCachingFactory(ConnectionFactory delegate) {
    this.delegate = delegate;
  }

  /**
   * {@inheritDoc}
   * <p>
   *   For this implementation, calls to {@code createConnection} without any
   *   intervening calls to {@link #closeConnections} return the same Connection
   *   instance.
   * </p>
   */
  public Connection getConnection(ConnectionInfo info) throws Exception {
    if (connection == null) {
      connection = delegate.getConnection(info);
    } else {
      changeSchemaIfSupplied(connection, info.getParamsAsProperties());
    }
    return new NonClosableConnection(connection);
  }

  public void closeConnections() throws SQLException {
    if (connection != null) {
      connection.close();
      connection = null;
    }
  }

  protected static void changeSchemaIfSupplied(Connection conn, Properties info) {
    final String schema = info.getProperty("schema", null);
    if (!Strings.isNullOrEmpty(schema)) {
      changeSchema(conn, schema);
    }
  }

  // TODO:  Purge nextUntilEnd(...) and calls when remaining fragment race
  // conditions are fixed (not just DRILL-2245 fixes).
  ///**
  // * Calls {@link ResultSet#next} on given {@code ResultSet} until it returns
  // * false.  (For TEMPORARY workaround for query cancelation race condition.)
  // */
  //private static void nextUntilEnd(final ResultSet resultSet) throws SQLException {
  //  while (resultSet.next()) {
  //  }
  //}

  protected static void changeSchema(Connection conn, String schema) {
    final String query = String.format("use %s", schema);
    try ( Statement s = conn.createStatement() ) {
      ResultSet r = s.executeQuery(query);
      // TODO:  Purge nextUntilEnd(...) and calls when remaining fragment
      // race conditions are fixed (not just DRILL-2245 fixes).
      // nextUntilEnd(r);
    } catch (SQLException e) {
      throw new RuntimeException("unable to change schema", e);
    }
  }
}
