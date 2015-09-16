package org.apache.hive.jdbc;

import com.google.common.collect.Iterators;

import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

public interface ServiceDiscovery {

  void init(Utils.JdbcConnectionParams connParams) throws SQLException;

  Discovered discover() throws SQLException;

}

class Discovered {
  String host;
  int port;
  String jdbcUriString;
}

class NullDiscovery implements ServiceDiscovery {
  private Utils.JdbcConnectionParams connParams;

  @Override
  public void init(Utils.JdbcConnectionParams connParams) throws SQLException {
    this.connParams = connParams;
  }

  @Override
  public Discovered discover() throws SQLException {
    Discovered discovered = new Discovered();
    discovered.host = connParams.getHost();
    discovered.port = connParams.getPort();
    discovered.jdbcUriString = connParams.getJdbcUriString();
    return discovered;
  }
}

class RoundRobinDiscovery implements ServiceDiscovery {

  private Utils.JdbcConnectionParams connParams;
  private Iterator<Map.Entry<String, URI>> iterator;

  @Override
  public void init(Utils.JdbcConnectionParams connParams) throws SQLException {
    Map<String, URI> servers = new LinkedHashMap<>();
    for (String server : connParams.getAuthorityList()) {
      servers.put(server, getServerURI(server));
    }
    this.iterator = Iterators.cycle(servers.entrySet());
    this.connParams = connParams;
  }

  @Override
  public Discovered discover() throws SQLException {
    Discovered discovered = new Discovered();
    URI serverUri = iterator.next().getValue();
    discovered.host = serverUri.getHost();
    discovered.port = serverUri.getPort();
    discovered.jdbcUriString = connParams.getJdbcUriString().replace(
        connParams.getHost() + ":" + connParams.getPort(),
        serverUri.getHost() + ":" + serverUri.getPort());

    return discovered;
  }

  private URI getServerURI(String server) throws SQLException {
    try {
      return new URI(null, server, null, null, null);
    } catch (URISyntaxException e) {
      throw new SQLException(e);
    }
  }
}

class ZookeeperDiscovery implements ServiceDiscovery {

  Utils.JdbcConnectionParams connParams;

  @Override
  public void init(Utils.JdbcConnectionParams connParams) {
    this.connParams = connParams;
  }

  public Discovered discover() throws SQLException {
    // Add current host to the rejected list
    connParams.getRejectedHostZnodePaths().add(connParams.getCurrentHostZnodePath());
    // Get another HiveServer2 uri from ZooKeeper
    URI serverUri = getServerURI();

    Discovered discovered = new Discovered();
    discovered.host = serverUri.getHost();
    discovered.port = serverUri.getPort();
    discovered.jdbcUriString = connParams.getJdbcUriString().replace(
        connParams.getHost() + ":" + connParams.getPort(),
        serverUri.getHost() + ":" + serverUri.getPort());

    return discovered;
  }

  private URI getServerURI() throws SQLException {
    // Parse serverUri to a java URI and extract host, port
    try {
      String serverUri = ZooKeeperHiveClientHelper.getNextServerUriFromZooKeeper(connParams);
      // Note URL_PREFIX is not a valid scheme format, therefore leaving it null in the constructor
      // to construct a valid URI
      return new URI(null, serverUri, null, null, null);
    } catch (URISyntaxException e) {
      throw new SQLException(
          "Could not open client transport for any of the Server URI's in ZooKeeper: " +
          e.getMessage(), " 08S01", e);
    } catch (ZooKeeperHiveClientException e) {
      throw new SQLException(e);
    }
  }
}


