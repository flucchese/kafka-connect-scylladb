package com.scylla.db.kafka;

import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.DataType;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

class TableMetadata {
	
	private TableMetadata() {
		// do nothing
	}
	
	static class Column {
		final ColumnMetadata columnMetadata;

		Column(ColumnMetadata columnMetadata) {
			this.columnMetadata = columnMetadata;
		}

		public String getName() {
			return this.columnMetadata.getName();
		}

		public DataType getType() {
			return this.columnMetadata.getType();
		}

		public String toString() {
			return MoreObjects.toStringHelper(this).add("name", this.columnMetadata.getName())
					.add("type", this.columnMetadata.getType().getName()).toString();
		}
	}

	static class Table {
		final String name;
		final String keyspace;
		final com.datastax.driver.core.TableMetadata tableMetadata;
		final Map<String, TableMetadata.Column> columns;
		final List<TableMetadata.Column> primaryKey;

		Table(com.datastax.driver.core.TableMetadata tableMetadata) {
			this.tableMetadata = tableMetadata;
			this.name = this.tableMetadata.getName();
			this.keyspace = this.tableMetadata.getKeyspace().getName();
			this.primaryKey = this.tableMetadata.getPrimaryKey().stream().map(Column::new)
					.collect(Collectors.toList());
			List<TableMetadata.Column> allColumns = new ArrayList<>();
			allColumns
					.addAll(this.tableMetadata.getColumns().stream().map(Column::new).collect(Collectors.toList()));
			this.columns = allColumns.stream().collect(Collectors.toMap(TableMetadata.Column::getName, c -> c,
					(o, n) -> n, () -> new TreeMap<>(String.CASE_INSENSITIVE_ORDER)));
		}

		public String keyspace() {
			return this.keyspace;
		}

		public TableMetadata.Column columnMetadata(String columnName) {
			return this.columns.get(columnName);
		}

		public List<TableMetadata.Column> columns() {
			return ImmutableList.copyOf(this.columns.values());
		}

		public List<TableMetadata.Column> primaryKey() {
			return this.primaryKey;
		}

		public String toString() {
			return MoreObjects.toStringHelper(this).add("keyspace", this.keyspace).add("name", this.name)
					.add("columns", this.columns).add("primaryKey", this.primaryKey).toString();
		}
	}
}
