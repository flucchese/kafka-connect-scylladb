package com.scylla.db.kafka;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.DataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.schemabuilder.Alter;
import com.datastax.driver.core.schemabuilder.Create;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import com.datastax.driver.core.schemabuilder.TableOptions;
import com.google.common.base.Joiner;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ComparisonChain;

class SinkSchemaBuilder {

	private static final Logger log = LoggerFactory.getLogger(SinkSchemaBuilder.class);

	private static final Object DEFAULT = new Object();

	private final Cache<ScyllaDbSchemaKey, Object> schemaLookup;

	private String keyspaceName;
	private boolean tableManageEnabled;
	private TableOptions.CompressionOptions tableCompressionAlgorithm;

	public SinkSchemaBuilder(String keyspaceName,
								boolean tableManageEnabled,
								TableOptions.CompressionOptions tableCompressionAlgorithm) {
		this.keyspaceName = keyspaceName;
		this.tableManageEnabled = tableManageEnabled;
		this.tableCompressionAlgorithm = tableCompressionAlgorithm;
		
		schemaLookup = CacheBuilder.newBuilder().expireAfterWrite(500L, TimeUnit.SECONDS).build();
	}
	/*
	public void onTableChanged(com.datastax.driver.core.TableMetadata current,
			com.datastax.driver.core.TableMetadata previous) {
		final com.datastax.driver.core.TableMetadata actual;
		if (null != current) {
			actual = current;
		} else if (null != previous) {
			actual = previous;
		} else {
			actual = null;
		}

		if (null != actual) {
			final String keyspace = actual.getKeyspace().getName();
			if (this.config.keyspace.equalsIgnoreCase(keyspace)) {
				ScyllaDbSchemaKey key = ScyllaDbSchemaKey.of(actual.getKeyspace().getName(), actual.getName());
				log.info("onTableChanged() - {} changed. Invalidating...", key);
				this.schemaLookup.invalidate(key);
				this.session.onTableChanged(actual.getKeyspace().getName(), actual.getName());
			}
		}
	}
	*/
	private DataType getDataType(Schema schema) {
		final DataType dataType;

		if (Timestamp.LOGICAL_NAME.equals(schema.name())) {
			dataType = DataType.timestamp();
		} else if (Time.LOGICAL_NAME.equals(schema.name())) {
			dataType = DataType.time();
		} else if (Date.LOGICAL_NAME.equals(schema.name())) {
			dataType = DataType.date();
		} else if (Decimal.LOGICAL_NAME.equals(schema.name())) {
			dataType = DataType.decimal();
		} else {
			switch (schema.type()) {
			case MAP:
				final DataType mapKeyType = getDataType(schema.keySchema());
				final DataType mapValueType = getDataType(schema.valueSchema());
				dataType = DataType.map(mapKeyType, mapValueType);
				break;
			case ARRAY:
				final DataType listValueType = getDataType(schema.valueSchema());
				dataType = DataType.list(listValueType);
				break;
			case BOOLEAN:
				dataType = DataType.cboolean();
				break;
			case BYTES:
				dataType = DataType.blob();
				break;
			case FLOAT32:
				dataType = DataType.cfloat();
				break;
			case FLOAT64:
				dataType = DataType.cdouble();
				break;
			case INT8:
				dataType = DataType.tinyint();
				break;
			case INT16:
				dataType = DataType.smallint();
				break;
			case INT32:
				dataType = DataType.cint();
				break;
			case INT64:
				dataType = DataType.bigint();
				break;
			case STRING:
				dataType = DataType.varchar();
				break;
			default:
				throw new UnsupportedOperationException(String.format("Unsupported type %s", schema.type()));
			}
		}
		
		return dataType;
	}

	void heatCache(String tableName, Schema keySchema, Schema valueSchema) {
		ScyllaDbSchemaKey key = ScyllaDbSchemaKey.of(keyspaceName, tableName);
		if (null != schemaLookup.getIfPresent(key)) {
			return;
		}
		
		if (null == keySchema || null == valueSchema) {
			log.warn("build() - Schemaless mode detected. Cannot generate DDL so assuming table is correct.");
			schemaLookup.put(key, DEFAULT);
		}
	}
	
	boolean alter(Session session,
					String tableName,
					Schema valueSchema,
					TableMetadata.Table tableMetadata) {
		boolean result = false;
		
		ScyllaDbSchemaKey key = ScyllaDbSchemaKey.of(keyspaceName, tableName);
		
		Preconditions.checkNotNull(tableMetadata, "tableMetadata cannot be null.");
		Preconditions.checkNotNull(valueSchema, "valueSchema cannot be null.");
		log.trace("alter() - tableMetadata = '{}' ", tableMetadata);

		Map<String, DataType> addedColumns = new LinkedHashMap<>();

		for (Field field : valueSchema.fields()) {
			String fieldName = field.name();
			log.trace("alter() - Checking if table has '{}' column.", fieldName);
			final TableMetadata.Column columnMetadata = tableMetadata.columnMetadata(fieldName);

			if (null == columnMetadata) {
				log.debug("alter() - Adding column '{}'", fieldName);
				DataType dataType = getDataType(field.schema());
				addedColumns.put(field.name(), dataType);
			} else {
				log.trace("alter() - Table already has '{}' column.", fieldName);
			}
		}

		/*
		 * CQL does not allow more than one column in an alter statement.
		 * Check out this issue for more. https://datastax-oss.atlassian.net/browse/JAVA-731
		 */

		if (!addedColumns.isEmpty()) {
			final Alter alterTable = SchemaBuilder.alterTable(keyspaceName, tableName);
			if (!tableManageEnabled) {
				List<String> requiredAlterStatements = addedColumns.entrySet().stream()
						.map(e -> alterTable.addColumn(e.getKey()).type(e.getValue()).toString())
						.collect(Collectors.toList());

				throw new DataException(String.format("Alter statement(s) needed. Missing column(s): '%s'%n%s;",
						Joiner.on("', '").join(addedColumns.keySet()), Joiner.on(';').join(requiredAlterStatements)));
			} else {
				String query = alterTable.withOptions().compressionOptions(tableCompressionAlgorithm)
						.buildInternal();
				session.execute(query);
				for (Map.Entry<String, DataType> e : addedColumns.entrySet()) {
					final String columnName = e.getKey();
					final DataType dataType = e.getValue();
					final Statement alterStatement = alterTable.addColumn(columnName).type(dataType);
					session.execute(alterStatement);
				}
				result = true;
			}
		}

		schemaLookup.put(key, DEFAULT);
		
		return result;
	}

	void create(Session session,
				String tableName,
				Schema keySchema,
				Schema valueSchema) {
		ScyllaDbSchemaKey key = ScyllaDbSchemaKey.of(keyspaceName, tableName);

		log.trace("create() - tableName = '{}'", tableName);
		Preconditions.checkState(Schema.Type.STRUCT == keySchema.type(),
				"record.keySchema() must be a struct. Received '%s'", keySchema.type());
		Preconditions.checkState(!keySchema.fields().isEmpty(), "record.keySchema() must have some fields.");
		for (Field field : keySchema.fields()) {
			String fieldName = field.name();
			log.trace("create() - Checking key schema against value schema. fieldName={}", fieldName);
			final Field valueField = valueSchema.field(field.name());

			if (null == valueField) {
				throw new DataException(String.format(
						"record.valueSchema() must contain all of the fields in record.keySchema(). "
								+ "record.keySchema() is used by the connector to determine the key for the "
								+ "table. record.valueSchema() is missing field '%s'. record.valueSchema() is "
								+ "used by the connector to persist data to the table in ScyllaDb. Here are "
								+ "the available fields for record.valueSchema(%s) and record.keySchema(%s).",
								fieldName,
						Joiner.on(", ")
								.join(valueSchema.fields().stream().map(Field::name).collect(Collectors.toList())),
						Joiner.on(", ")
								.join(keySchema.fields().stream().map(Field::name).collect(Collectors.toList()))));
			}
		}

		Create create = SchemaBuilder.createTable(keyspaceName, tableName);
		final TableOptions<?> tableOptions = create.withOptions();
		if (!Strings.isNullOrEmpty(valueSchema.doc())) {
			tableOptions.comment(valueSchema.doc());
		}

		Set<String> fields = new HashSet<>();
		for (final Field keyField : keySchema.fields()) {
			final DataType dataType = getDataType(keyField.schema());
			create.addPartitionKey(keyField.name(), dataType);
			fields.add(keyField.name());
		}

		for (final Field valueField : valueSchema.fields()) {
			if (fields.contains(valueField.name())) {
				log.trace("create() - Skipping '{}' because it's already in the key.", valueField.name());
				continue;
			}

			final DataType dataType = getDataType(valueField.schema());
			create.addColumn(valueField.name(), dataType);
		}

		if (tableManageEnabled) {
			tableOptions.compressionOptions(tableCompressionAlgorithm).buildInternal();
			log.info("create() - Adding table {}.{}\n{}", keyspaceName, tableName, tableOptions);
			session.execute(tableOptions);
		} else {
			throw new DataException(String.format("Create statement needed:%n%s", create));
		}

		schemaLookup.put(key, DEFAULT);
	}

	private static class ScyllaDbSchemaKey implements Comparable<ScyllaDbSchemaKey> {
		final String tableName;
		final String keyspace;

		private ScyllaDbSchemaKey(String keyspace, String tableName) {
			this.tableName = tableName;
			this.keyspace = keyspace;
		}

		@Override
		public int compareTo(ScyllaDbSchemaKey that) {
			return ComparisonChain.start().compare(this.keyspace, that.keyspace).compare(this.tableName, that.tableName)
					.result();
		}

		@Override
		public int hashCode() {
			return Objects.hash(this.keyspace, this.tableName);
		}

		@Override
		public String toString() {
			return MoreObjects.toStringHelper(this).add("keyspace", this.keyspace).add("tableName", this.tableName)
					.toString();
		}

		@Override
		public boolean equals(Object obj) {
			if (obj instanceof ScyllaDbSchemaKey) {
				return 0 == compareTo((ScyllaDbSchemaKey) obj);
			} else {
				return false;
			}
		}

		public static ScyllaDbSchemaKey of(String keyspace, String tableName) {
			return new ScyllaDbSchemaKey(keyspace, tableName);
		}
	}
}
