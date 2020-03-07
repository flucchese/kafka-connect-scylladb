package com.scylla.db.kafka;

import java.io.Closeable;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManagerFactory;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.RemoteEndpointAwareNettySSLOptions;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.SSLOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.scylla.db.kafka.codec.StringTimeUuidCodec;
import com.scylla.db.kafka.codec.StringUuidCodec;

import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;

public class SinkSession implements Closeable {
	
	private static final Logger log = LoggerFactory.getLogger(SinkSession.class);

	private static final CodecRegistry CODEC_REGISTRY = CodecRegistry.DEFAULT_INSTANCE;

	public static SinkSession newInstance(SinkConnectorConfig config) {
		Cluster.Builder clusterBuilder = Cluster.builder()
				.withPort(config.port)
				.addContactPoints(config.contactPoints)
				.withProtocolVersion(ProtocolVersion.NEWEST_SUPPORTED)
				.withCodecRegistry(CODEC_REGISTRY);
		
		if (!config.loadBalancingLocalDc.isEmpty()) {
			clusterBuilder.withLoadBalancingPolicy(
					DCAwareRoundRobinPolicy.builder().withLocalDc(config.loadBalancingLocalDc).build());
		} else {
			log.warn("`scylladb.loadbalancing.localdc` has not been configured, "
					+ "which is recommended configuration in case of more than one DC.");
		}
		
		if (config.securityEnabled) {
			clusterBuilder.withCredentials(config.username, config.password);
		}
		
		if (config.sslEnabled) {
			SslContextBuilder sslContextBuilder = SslContextBuilder.forClient();
			sslContextBuilder.sslProvider(config.sslProvider);

			if (null != config.trustStorePath) {
				log.info("Configuring SSLContext to use Truststore {}", config.trustStorePath);
				try {
					KeyStore keyStore = KeyStore.getInstance("JKS");
					
					try (InputStream inputStream = new FileInputStream(config.trustStorePath)) {
						keyStore.load(inputStream, config.trustStorePassword);
					} catch (IOException e) {
						throw new ConnectException("Exception while reading keystore", e);
					} catch (CertificateException | NoSuchAlgorithmException e) {
						throw new ConnectException("Exception while loading keystore", e);
					}
					
					TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
					trustManagerFactory.init(keyStore);
					
					sslContextBuilder.trustManager(trustManagerFactory);
				} catch (NoSuchAlgorithmException e) {
					throw new ConnectException("Exception while creating TrustManagerFactory", e);
				} catch (KeyStoreException e) {
					throw new ConnectException("Exception while creating keystore", e);
				}
			}

			try {
				SslContext context = sslContextBuilder.build();
				SSLOptions sslOptions = new RemoteEndpointAwareNettySSLOptions(context);
				clusterBuilder.withSSL(sslOptions);
			} catch (SSLException e) {
				throw new ConnectException(e);
			}
		}
		
		clusterBuilder.withCompression(config.compression);
		Cluster cluster = clusterBuilder.build();
		log.info("Creating session");
		
		return new SinkSession(config,
								cluster.getMetadata().getKeyspace(config.keyspace),
								cluster.newSession());
	}
	
	static {
		// Register custom codec once at class loading time; duplicates will be logged
		// via warning
		CODEC_REGISTRY.register(StringUuidCodec.INSTANCE);
		CODEC_REGISTRY.register(StringTimeUuidCodec.INSTANCE);
	}
	
	private final KeyspaceMetadata keyspaceMetadata;
	private final Session session;

	private final String tableOffsetName;
	private final String ttl;
	
	private boolean sessionValid = true;
	private final SinkSchemaBuilder schemaBuilder;
	private final Map<String, TableMetadata.Table> tableMetadataCache;
	private final Map<String, PreparedStatement> deleteStatementCache;
	private final Map<String, PreparedStatement> insertStatementCache;

	private PreparedStatement offsetPreparedStatement;

	private SinkSession(SinkConnectorConfig config,
								KeyspaceMetadata keyspaceMetadata,
								Session session) {
		this.keyspaceMetadata = keyspaceMetadata;
		this.session = session;

		tableOffsetName = config.tableOffsetName;
		ttl = config.ttl();
		
		schemaBuilder = new SinkSchemaBuilder(config.keyspace,
												config.tableManageEnabled,
												config.tableCompressionAlgorithm);
		tableMetadataCache = new HashMap<>();
		deleteStatementCache = new HashMap<>();
		insertStatementCache = new HashMap<>();
	}

	public ResultSet executeStatement(Statement statement) {
		log.trace("executeStatement() - Executing statement\n{}", statement);
		return session.execute(statement);
	}

	public ResultSetFuture executeStatementAsync(Statement statement) {
		log.trace("executeStatement() - Executing statement\n{}", statement);
		return session.executeAsync(statement);
	}

	public ResultSet executeQuery(String query) {
		log.trace("executeQuery() - Executing query\n{}", query);
		return session.execute(query);
	}

	public boolean isValid() {
		return sessionValid;
	}

	public void setInvalid() {
		sessionValid = false;
	}

	public void createOrAlterTable(String tableName, Schema keySchema, Schema valueSchema) {
		log.trace("build() - tableName = '{}'", tableName);
		
		String keyspaceName = keyspaceMetadata.getName();
		schemaBuilder.heatCache(tableName, keySchema, valueSchema);
		TableMetadata.Table tableMetadata = tableMetadata(tableName);
		if (null != tableMetadata) {
			if (schemaBuilder.alter(session, tableName, valueSchema, tableMetadata)) {
				onTableChanged(keyspaceName, tableName);
			}
		} else {
			schemaBuilder.create(session, tableName, keySchema, valueSchema);
		}
	}

	public TableMetadata.Table tableMetadata(String tableName) {
		log.trace("tableMetadata() - tableName = '{}'", tableName);
		return tableMetadataCache.computeIfAbsent(tableName, t -> {
			com.datastax.driver.core.TableMetadata tableMetadata = keyspaceMetadata.getTable(t);
			if (null != tableMetadata) {
				return new TableMetadata.Table(tableMetadata);
			}
			
			return null;
		});
	}

	public boolean tableExists(String tableName) {
		return null != tableMetadata(tableName);
	}

	public PreparedStatement delete(String tableName) {
		return deleteStatementCache.computeIfAbsent(tableName, t -> {
			Delete statement = QueryBuilder.delete().from(keyspaceMetadata.getName(), t);
			TableMetadata.Table tableMetadata = tableMetadata(t);
			for (TableMetadata.Column columnMetadata : tableMetadata.primaryKey()) {
				statement.where(QueryBuilder.eq(columnMetadata.getName(),
						QueryBuilder.bindMarker(columnMetadata.getName())));
			}
			log.debug("delete() - Preparing statement. '{}'", statement);
			
			return session.prepare(statement);
		});
	}

	private PreparedStatement createInsertPreparedStatement(String tableName) {
		Insert statement = QueryBuilder.insertInto(keyspaceMetadata.getName(), tableName);
		TableMetadata.Table tableMetadata = tableMetadata(tableName);
		for (TableMetadata.Column columnMetadata : tableMetadata.columns()) {
			statement.value(columnMetadata.getName(), QueryBuilder.bindMarker(columnMetadata.getName()));
		}
		log.debug("insert() - Preparing statement. '{}'", statement);
		return (ttl == null) ? session.prepare(statement)
				: session.prepare(statement.using(QueryBuilder.ttl(Integer.parseInt(ttl))));
	}

	public PreparedStatement insert(String tableName) {
		return insertStatementCache.computeIfAbsent(tableName, t -> createInsertPreparedStatement(t));
	}

	public void addOffsetsToBatch(BatchStatement batch, Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
		for (Map.Entry<TopicPartition, OffsetAndMetadata> kvp : currentOffsets.entrySet()) {
			TopicPartition topicPartition = kvp.getKey();
			OffsetAndMetadata metadata = kvp.getValue();

			String topic = topicPartition.topic();
			int partition = topicPartition.partition();
			long offset = metadata.offset();
			
			log.debug("addOffsetsToBatch() - Setting offset to {}:{}:{}",
					topic,
					partition,
					offset);

			if (null == offsetPreparedStatement) {
				offsetPreparedStatement = createInsertPreparedStatement(tableOffsetName);
			}
			
			BoundStatement statement = offsetPreparedStatement.bind();
			statement.setString("topic", topic);
			statement.setInt("partition", partition);
			statement.setLong("offset", offset);
			batch.add(statement);
		}
	}

	public Map<TopicPartition, Long> loadOffsets(Set<TopicPartition> assignment) {
		Map<TopicPartition, Long> result = new HashMap<>();
		if (null != assignment && !assignment.isEmpty()) {
			Select.Where partitionQuery = QueryBuilder.select().column("offset")
					.from(keyspaceMetadata.getName(), tableOffsetName)
					.where(QueryBuilder.eq("topic", QueryBuilder.bindMarker("topic")))
					.and(QueryBuilder.eq("partition", QueryBuilder.bindMarker("partition")));

			log.debug("loadOffsets() - Preparing statement. {}", partitionQuery);
			PreparedStatement preparedStatement = session.prepare(partitionQuery);

			for (TopicPartition topicPartition : assignment) {
				log.debug("loadOffsets() - Querying for {}", topicPartition);
				BoundStatement boundStatement = preparedStatement.bind();
				boundStatement.setString("topic", topicPartition.topic());
				boundStatement.setInt("partition", topicPartition.partition());

				ResultSet resultSet = executeStatement(boundStatement);
				Row row = resultSet.one();
				if (null != row) {
					long offset = row.getLong("offset");
					log.info("Found offset of {} for {}", offset, topicPartition);
					result.put(topicPartition, offset);
				}
			}
		}

		return result;
	}

	public void close() throws IOException {
		session.close();
	}

	public void onTableChanged(String keyspace, String tableName) {
		this.tableMetadataCache.remove(tableName);
		this.deleteStatementCache.remove(tableName);
		this.insertStatementCache.remove(tableName);
	}
}
