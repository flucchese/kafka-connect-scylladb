package com.scylla.db.kafka;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ProtocolOptions;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import com.datastax.driver.core.schemabuilder.TableOptions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;

import io.confluent.kafka.connect.utils.config.ConfigUtils;
import io.confluent.kafka.connect.utils.config.ValidEnum;
import io.confluent.kafka.connect.utils.config.ValidPort;
import io.netty.handler.ssl.SslProvider;

/**
 * Configuration class for {@link SinkConnectorImpl}.
 */
public class SinkConnectorConfig extends AbstractConfig {

	public final int port;
	public final String[] contactPoints;
	public final ConsistencyLevel consistencyLevel;
	public final boolean securityEnabled;
	public final String username;
	public final String password;
	public final ProtocolOptions.Compression compression;
	public final boolean sslEnabled;
	public final SslProvider sslProvider;
	public final boolean deletesEnabled;
	public final String keyspace;
	public final boolean keyspaceCreateEnabled;
	public final int keyspaceReplicationFactor;
	public final boolean tableCreateEnabled;
	public final boolean tableManageEnabled;
	public final TableOptions.CompressionOptions tableCompressionAlgorithm;
	public final String tableOffsetName;
	public final char[] trustStorePassword;
	public final File trustStorePath;
	public final long statementTimeoutMs;
	public final int maxBatchSizeKb;
	public final String loadBalancingLocalDc;
	public final long timestampResolutionMs;

	public SinkConnectorConfig(Map<?, ?> originals) {
		super(config(), originals);
		this.port = getInt(PORT_CONFIG);
		final List<String> contactPointList = this.getList(CONTACT_POINTS_CONFIG);
		this.contactPoints = contactPointList.toArray(new String[contactPointList.size()]);
		this.consistencyLevel = ConfigUtils.getEnum(ConsistencyLevel.class, this, CONSISTENCY_LEVEL_CONFIG);
		this.username = getString(USERNAME_CONFIG);
		this.password = getPassword(PASSWORD_CONFIG).value();
		this.securityEnabled = getBoolean(SECURITY_ENABLE_CONFIG);
		this.sslEnabled = getBoolean(SSL_ENABLED_CONFIG);
		this.deletesEnabled = getBoolean(DELETES_ENABLE_CONFIG);

		this.keyspace = getString(KEYSPACE_CONFIG);

		final String trustStorePathAtr = this.getString(SSL_TRUSTSTORE_PATH_CONFIG);
		this.trustStorePath = Strings.isNullOrEmpty(trustStorePathAtr) ? null : new File(trustStorePathAtr);
		this.trustStorePassword = this.getPassword(SSL_TRUSTSTORE_PASSWORD_CONFIG).value().toCharArray();

		final String compressionAtr = getString(COMPRESSION_CONFIG);
		this.compression = ProtocolOptions.Compression.valueOf(compressionAtr);
		this.sslProvider = ConfigUtils.getEnum(SslProvider.class, this, SSL_PROVIDER_CONFIG);
		this.keyspaceCreateEnabled = getBoolean(KEYSPACE_CREATE_ENABLED_CONFIG);
		this.tableCreateEnabled = getBoolean(TABLE_OFFSET_ENABLED_CONFIG);
		this.keyspaceReplicationFactor = getInt(KEYSPACE_REPLICATION_FACTOR_CONFIG);
		this.tableManageEnabled = getBoolean(TABLE_MANAGE_ENABLED_CONFIG);
		TableOptions.CompressionOptions.Algorithm tableCompressionAlgo = ConfigUtils.getEnum(
				TableOptions.CompressionOptions.Algorithm.class, this, TABLE_COMPRESSION_CONFIG);

		switch (tableCompressionAlgo) {
		case SNAPPY:
			tableCompressionAlgorithm = SchemaBuilder.snappy();
			break;
		case LZ4:
			tableCompressionAlgorithm = SchemaBuilder.lz4();
			break;
		case DEFLATE:
			tableCompressionAlgorithm = SchemaBuilder.deflate();
			break;
		default:
			tableCompressionAlgorithm = SchemaBuilder.noCompression();
		}

		this.tableOffsetName = getString(TABLE_OFFSET_NAME_CONFIG);
		this.statementTimeoutMs = getLong(EXECUTE_STATEMENT_TIMEOUT_MS_CONF);
		this.maxBatchSizeKb = getInt(MAX_BATCH_SIZE_CONFIG);
		this.loadBalancingLocalDc = getString(LOAD_BALANCING_LOCAL_DC_CONFIG);
		this.timestampResolutionMs = getLong(TIMESTAMP_RESOLUTION_MS_CONF);
	}

	public static final String PORT_CONFIG = "scylladb.port";
	private static final String PORT_DOC = "The port the Scylladb hosts are listening on. "
			+ "Eg. When using a docker image, connect to the port it uses(use docker ps)";

	public static final String CONTACT_POINTS_CONFIG = "scylladb.contactPoints";
	static final String CONTACT_POINTS_DOC = "The Scylladb hosts to connect to. "
			+ "Scylla nodes use this list of hosts to find each other and learn "
			+ "the topology of the ring. You must change this if you are running "
			+ "multiple nodes. It's essential to put at least 2 hosts in case of "
			+ "bigger cluster, since if first host is down, it will contact second "
			+ "one and get the state of the cluster from it. Eg. When using the docker "
			+ "image, connect to the host it uses.";

	public static final String CONSISTENCY_LEVEL_CONFIG = "scylladb.consistencyLevel";
	private static final String CONSISTENCY_LEVEL_DOC = "The requested consistency level to use when writing to Scylladb. "
			+ "The Consistency Level (CL) determines how many replicas in a cluster "
			+ "that must acknowledge read or write operations before it is considered successful.";

	public static final String SSL_ENABLED_CONFIG = "scylladb.ssl.enabled";
	private static final String SSL_ENABLED_DOC = "Flag to determine if SSL is enabled when connecting to Scylladb.";

	public static final String SSL_PROVIDER_CONFIG = "scylladb.ssl.provider";
	private static final String SSL_PROVIDER_DOC = "The SSL Provider to use when connecting to Scylladb.";

	public static final String SECURITY_ENABLE_CONFIG = "scylladb.security.enabled";
	static final String SECURITY_ENABLE_DOC = "To enable security while loading "
			+ "the sink connector and connecting to ScyllaDB.";

	public static final String DELETES_ENABLE_CONFIG = "scylladb.deletes.enabled";
	private static final String DELETES_ENABLE_DOC = "Flag to determine if the connector should process deletes.";

	public static final String USERNAME_CONFIG = "scylladb.username";
	private static final String USERNAME_DOC = "The username to connect to ScyllaDB with. "
			+ "Set scylladb.security.enable = true to use this config.";

	public static final String PASSWORD_CONFIG = "scylladb.password";
	private static final String PASSWORD_DOC = "The password to connect to ScyllaDB with. "
			+ "Set scylladb.security.enable = true to use this config.";

	public static final String COMPRESSION_CONFIG = "scylladb.compression";
	private static final String COMPRESSION_DOC = "Compression algorithm to use when connecting to Scylladb.";

	public static final String KEYSPACE_CONFIG = "scylladb.keyspace";
	private static final String KEYSPACE_DOC = "The keyspace to write to. "
			+ "This keyspace is like a database in the ScyllaDB cluster.";

	public static final String KEYSPACE_CREATE_ENABLED_CONFIG = "scylladb.keyspace.create";
	private static final String KEYSPACE_CREATE_ENABLED_DOC = "Flag to determine if the keyspace should be created if it does not exist. "
			+ "**Note**: Error if a new keyspace has to be created and the config is false.";

	public static final String KEYSPACE_REPLICATION_FACTOR_CONFIG = "scylladb.keyspace.replicationFactor";
	private static final String KEYSPACE_REPLICATION_FACTOR_DOC = "The replication factor to use if a keyspace is created by the connector. "
			+ "The Replication Factor (RF) is equivalent to the number of nodes where data "
			+ "(rows and partitions) are replicated. Data is replicated to multiple (RF=N) nodes";

	public static final String TABLE_MANAGE_ENABLED_CONFIG = "scylladb.table.manage";
	private static final String TABLE_MANAGE_ENABLED_DOC = "Flag to determine if the connector should manage the table.";

	public static final String TABLE_COMPRESSION_CONFIG = "scylladb.table.compression";
	private static final String TABLE_COMPRESSION_DOC = "Compression algorithm to use when the table is created.";

	public static final String TABLE_OFFSET_ENABLED_CONFIG = "scylladb.table.offset.create";
	private static final Boolean TABLE_OFFSET_ENABLED_DEFAULT = true;
	private static final String TABLE_OFFSET_ENABLED_DOC = "If true, Kafka consumer offsets will be stored in Scylladb table. If false, connector will "
			+ "skip writing offset information into Scylladb (this might imply duplicate writes "
			+ "into Scylladb when a task restarts).";

	public static final String TABLE_OFFSET_NAME_CONFIG = "scylladb.table.offset";
	private static final String TABLE_OFFSET_NAME_DOC = "The table within the Scylladb keyspace "
			+ "to store the offsets that have been read from Kafka. This is used to enable exactly once "
			+ "delivery to ScyllaDb.";

	public static final String EXECUTE_STATEMENT_TIMEOUT_MS_CONF = "scylladb.execute.timeout";
	private static final String EXECUTE_STATEMENT_TIMEOUT_MS_DOC = "The timeout for executing a ScyllaDB statement (in ms).";

	public static final String SSL_TRUSTSTORE_PATH_CONFIG = "scylladb.ssl.truststore.path";
	private static final String SSL_TRUSTSTORE_PATH_DOC = "Path to the Java Truststore.";

	public static final String SSL_TRUSTSTORE_PASSWORD_CONFIG = "scylladb.ssl.truststore.password";
	private static final String SSL_TRUSTSTORE_PASSWORD_DOC = "Password to open the Java Truststore with.";

	public static final String TTL_CONFIG = "scylladb.ttl";
	/*
	 * If TTL value is not specified then skip setting ttl value while making insert
	 * query
	 */
	public static final String TTL_DEFAULT = null;
	private static final String TTL_DOC = "The retention period for the data in ScyllaDB. "
			+ "After this interval elapses, Scylladb will remove these records. "
			+ "If this configuration is not provided, the Sink Connector will perform "
			+ "insert operations in ScyllaDB  without TTL setting.";

	public static final String MAX_BATCH_SIZE_CONFIG = "scylladb.batch.maxSize";
	public static final int MAX_BATCH_SIZE_DEFAULT = 5;
	private static final String MAX_BATCH_SIZE_DOC = "Maximum size (in kilobytes) of a single batch "
			+ "consisting ScyllaDB operations. The should be equal to batch_size_warn_threshold_in_kb "
			+ "and 1/10th of the batch_size_fail_threshold_in_kb configured in scylla.yaml. "
			+ "The default value is set to 5kb, any change in this configuration should be accompanied by "
			+ "change in scylla.yaml.";

	public static final String TIMESTAMP_RESOLUTION_MS_CONF = "scylladb.batch.resolutionTimestamp";
	private static final String TIMESTAMP_RESOLUTION_MS_DOC = "The batch resolution time (in ms), "
			+ "in case of this value being zero, the Connector will not batch the records, else, "
			+ "kafka records within the resolution time will be batched. Default value is set to zero.";

	private static final String LOAD_BALANCING_LOCAL_DC_CONFIG = "scylladb.loadbalancing.localdc";
	private static final String LOAD_BALANCING_LOCAL_DC_DEFAULT = "";
	private static final String LOAD_BALANCING_LOCAL_DC_DOC = "The case-sensitive Data Center name "
			+ "local to the machine on which the connector is running. It is a recommended config if "
			+ "we have more than one DC.";

	public static final String CONNECTION_GROUP = "Connection";
	public static final String SSL_GROUP = "SSL";
	public static final String KEYSPACE_GROUP = "Keyspace";
	public static final String TABLE_GROUP = "Table";
	public static final String WRITE_GROUP = "Write";

	public static ConfigDef config() {
		ProtocolOptions.Compression[] compArray = ProtocolOptions.Compression.values();
		TableOptions.CompressionOptions.Algorithm[] algArray = TableOptions.CompressionOptions.Algorithm.values();
		
		return new ConfigDef()
				.define(CONTACT_POINTS_CONFIG, ConfigDef.Type.LIST, ImmutableList.of("localhost"),
						ConfigDef.Importance.HIGH, CONTACT_POINTS_DOC, CONNECTION_GROUP, 0, ConfigDef.Width.SHORT,
						"Contact Point(s)")
				.define(PORT_CONFIG, ConfigDef.Type.INT, 9042, ValidPort.of(), ConfigDef.Importance.MEDIUM, PORT_DOC,
						CONNECTION_GROUP, 1, ConfigDef.Width.SHORT, "Port")
				.define(LOAD_BALANCING_LOCAL_DC_CONFIG, ConfigDef.Type.STRING, LOAD_BALANCING_LOCAL_DC_DEFAULT,
						ConfigDef.Importance.HIGH, LOAD_BALANCING_LOCAL_DC_DOC, CONNECTION_GROUP, 2,
						ConfigDef.Width.LONG, "Load Balancing Local DC")
				.define(SECURITY_ENABLE_CONFIG, ConfigDef.Type.BOOLEAN, true, ConfigDef.Importance.HIGH,
						SECURITY_ENABLE_DOC, CONNECTION_GROUP, 2, ConfigDef.Width.SHORT, "Security Enabled?")
				.define(USERNAME_CONFIG, ConfigDef.Type.STRING, "cassandra", ConfigDef.Importance.HIGH, USERNAME_DOC,
						CONNECTION_GROUP, 3, ConfigDef.Width.SHORT, "Username")
				.define(PASSWORD_CONFIG, ConfigDef.Type.PASSWORD, "cassandra", ConfigDef.Importance.HIGH, PASSWORD_DOC,
						CONNECTION_GROUP, 4, ConfigDef.Width.SHORT, "Password")
				.define(COMPRESSION_CONFIG, ConfigDef.Type.STRING, ProtocolOptions.Compression.NONE.toString(),
						ConfigDef.ValidString.in(Arrays.copyOf(compArray, compArray.length, String[].class)),
						ConfigDef.Importance.LOW, COMPRESSION_DOC, CONNECTION_GROUP, 5, ConfigDef.Width.SHORT,
						"Compression")
				.define(SSL_ENABLED_CONFIG, ConfigDef.Type.BOOLEAN, false, ConfigDef.Importance.HIGH, SSL_ENABLED_DOC,
						CONNECTION_GROUP, 6, ConfigDef.Width.SHORT, CONNECTION_GROUP)
				.define(SSL_PROVIDER_CONFIG, ConfigDef.Type.STRING, SslProvider.JDK.toString(),
						ValidEnum.of(SslProvider.class), ConfigDef.Importance.LOW, SSL_PROVIDER_DOC, SSL_GROUP, 2,
						ConfigDef.Width.SHORT, "SSL Provider")
				// TODO recommender(Recommenders.visibleIf(SSL_ENABLED_CONFIG, true))
				.define(SSL_TRUSTSTORE_PATH_CONFIG, ConfigDef.Type.STRING, "", ConfigDef.Importance.MEDIUM,
						SSL_TRUSTSTORE_PATH_DOC, SSL_GROUP, 0, ConfigDef.Width.SHORT, "SSL Truststore Path")
				// TODO .validator(Validators.blankOr(ValidFile.of()))
				// TODO .recommender(Recommenders.visibleIf(SSL_ENABLED_CONFIG, true))
				.define(SSL_TRUSTSTORE_PASSWORD_CONFIG, ConfigDef.Type.PASSWORD, "password123",
						ConfigDef.Importance.MEDIUM, SSL_TRUSTSTORE_PASSWORD_DOC, SSL_GROUP, 1, ConfigDef.Width.SHORT,
						"SSL Truststore Password")
				// TODO .validator(Validators.blankOr(ValidFile.of()))
				// TODO .recommender(Recommenders.visibleIf(SSL_ENABLED_CONFIG, true))
				.define(CONSISTENCY_LEVEL_CONFIG, ConfigDef.Type.STRING, ConsistencyLevel.LOCAL_QUORUM.toString(),
						ConfigDef.Importance.HIGH, CONSISTENCY_LEVEL_DOC, WRITE_GROUP, 0, ConfigDef.Width.SHORT,
						"Consistency Level")
				.define(DELETES_ENABLE_CONFIG, ConfigDef.Type.BOOLEAN, true, ConfigDef.Importance.HIGH,
						DELETES_ENABLE_DOC, WRITE_GROUP, 1, ConfigDef.Width.SHORT, "Perform Deletes")
				.define(KEYSPACE_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, KEYSPACE_DOC, KEYSPACE_GROUP,
						0, ConfigDef.Width.SHORT, "Cassandra Keyspace")
				.define(KEYSPACE_CREATE_ENABLED_CONFIG, ConfigDef.Type.BOOLEAN, true, ConfigDef.Importance.HIGH,
						KEYSPACE_CREATE_ENABLED_DOC, KEYSPACE_GROUP, 1, ConfigDef.Width.SHORT, "Create Keyspace")
				.define(KEYSPACE_REPLICATION_FACTOR_CONFIG, ConfigDef.Type.INT, 3, ConfigDef.Range.atLeast(1),
						ConfigDef.Importance.HIGH, KEYSPACE_REPLICATION_FACTOR_DOC, KEYSPACE_GROUP, 2,
						ConfigDef.Width.SHORT, "Keyspace replication factor")
				// TODO .recommender(Recommenders.visibleIf(KEYSPACE_CREATE_ENABLED_CONFIG,
				// true))
				.define(TABLE_MANAGE_ENABLED_CONFIG, ConfigDef.Type.BOOLEAN, true, ConfigDef.Importance.HIGH,
						TABLE_MANAGE_ENABLED_DOC, TABLE_GROUP, 0, ConfigDef.Width.SHORT, "Manage Table Schema(s)?")
				.define(TABLE_COMPRESSION_CONFIG, ConfigDef.Type.STRING, TableOptions.CompressionOptions.Algorithm.NONE.toString(),
						ConfigDef.ValidString.in(Arrays.copyOf(algArray, algArray.length, String[].class)),
						ConfigDef.Importance.MEDIUM, TABLE_COMPRESSION_DOC, TABLE_GROUP, 1,
						ConfigDef.Width.SHORT, "Table Compression")
				// TODO .recommender(Recommenders.visibleIf(TABLE_MANAGE_ENABLED_CONFIG, true))
				.define(TABLE_OFFSET_NAME_CONFIG, ConfigDef.Type.STRING, "kafka_connect_offsets",
						ConfigDef.Importance.LOW, TABLE_OFFSET_NAME_DOC, TABLE_GROUP, 2, ConfigDef.Width.SHORT,
						"Offset storage table")
				.define(EXECUTE_STATEMENT_TIMEOUT_MS_CONF, ConfigDef.Type.LONG, 30000, ConfigDef.Range.atLeast(0),
						ConfigDef.Importance.LOW, EXECUTE_STATEMENT_TIMEOUT_MS_DOC, WRITE_GROUP, 2,
						ConfigDef.Width.SHORT, "Execute statement timeout")
				.define(TTL_CONFIG, ConfigDef.Type.STRING, TTL_DEFAULT, ConfigDef.Importance.MEDIUM, TTL_DOC,
						WRITE_GROUP, 3, ConfigDef.Width.SHORT, "Time to live")
				.define(TABLE_OFFSET_ENABLED_CONFIG, ConfigDef.Type.BOOLEAN, TABLE_OFFSET_ENABLED_DEFAULT,
						ConfigDef.Importance.MEDIUM, TABLE_OFFSET_ENABLED_DOC, WRITE_GROUP, 4,
						ConfigDef.Width.SHORT, "Enable offset stored in cassandra")
				.define(MAX_BATCH_SIZE_CONFIG, ConfigDef.Type.INT, MAX_BATCH_SIZE_DEFAULT, ConfigDef.Range.atLeast(1),
						ConfigDef.Importance.HIGH, MAX_BATCH_SIZE_DOC, WRITE_GROUP, 5, ConfigDef.Width.LONG,
						"Max Batch Size in KB")
				.define(TIMESTAMP_RESOLUTION_MS_CONF, ConfigDef.Type.LONG, 0, ConfigDef.Range.atLeast(0),
						ConfigDef.Importance.LOW, TIMESTAMP_RESOLUTION_MS_DOC, WRITE_GROUP, 6, ConfigDef.Width.SHORT,
						"Timestamp Threshold in MS");
	}

	public String ttl() {
		return getString(TTL_CONFIG);
	}

	public boolean isTableOffsetEnabled() {
		return getBoolean(TABLE_OFFSET_ENABLED_CONFIG);
	}

	public static void main(String[] args) {
		System.out.println(config().toEnrichedRst());
	}
}