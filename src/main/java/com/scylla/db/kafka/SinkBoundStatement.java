package com.scylla.db.kafka;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.LocalDate;
import com.datastax.driver.core.PreparedStatement;
import com.google.common.base.Preconditions;

class SinkBoundStatement extends BoundStatement {
	
	private static final Logger log = LoggerFactory.getLogger(SinkBoundStatement.class);

	private static void convertMap(SinkBoundStatement boundStatement, Map<?,?> value) {
		Iterator<?> valueIterator = value.keySet().iterator();

		while (valueIterator.hasNext()) {
			Object key = valueIterator.next();
			Preconditions.checkState(key instanceof String, "Map key must be a String.");
			String fieldName = (String) key;
			Object fieldValue = value.get(key);

			try {
				if (null == fieldValue) {
					log.trace("convertStruct() - Setting '{}' to null.", fieldName);
					boundStatement.setToNull(fieldName);
				} else if (fieldValue instanceof String) {
					log.trace("convertStruct() - Processing '{}' as string.", fieldName);
					boundStatement.setString(fieldName, (String) fieldValue);
				} else if (fieldValue instanceof Byte) {
					log.trace("convertStruct() - Processing '{}' as int8.", fieldName);
					boundStatement.setByte(fieldName, (Byte) fieldValue);
				} else if (fieldValue instanceof Short) {
					log.trace("convertStruct() - Processing '{}' as int16.", fieldName);
					boundStatement.setShort(fieldName, (Short) fieldValue);
				} else if (fieldValue instanceof Integer) {
					log.trace("convertStruct() - Processing '{}' as int32.", fieldName);
					boundStatement.setInt(fieldName, (Integer) fieldValue);
				} else if (fieldValue instanceof Long) {
					log.trace("convertStruct() - Processing '{}' as long.", fieldName);
					boundStatement.setLong(fieldName, (Long) fieldValue);
				} else if (fieldValue instanceof BigInteger) {
					log.trace("convertStruct() - Processing '{}' as long.", fieldName);
					boundStatement.setLong(fieldName, ((BigInteger) fieldValue).longValue());
				} else if (fieldValue instanceof Double) {
					log.trace("convertStruct() - Processing '{}' as float64.", fieldName);
					boundStatement.setDouble(fieldName, (Double) fieldValue);
				} else if (fieldValue instanceof Float) {
					log.trace("convertStruct() - Processing '{}' as float32.", fieldName);
					boundStatement.setFloat(fieldName, (Float) fieldValue);
				} else if (fieldValue instanceof BigDecimal) {
					log.trace("convertStruct() - Processing '{}' as decimal.", fieldName);
					boundStatement.setDecimal(fieldName, (BigDecimal) fieldValue);
				} else if (fieldValue instanceof Boolean) {
					log.trace("convertStruct() - Processing '{}' as boolean.", fieldName);
					boundStatement.setBool(fieldName, (Boolean) fieldValue);
				} else if (fieldValue instanceof Date) {
					log.trace("convertStruct() - Processing '{}' as timestamp.", fieldName);
					boundStatement.setTimestamp(fieldName, (Date) fieldValue);
				} else if (fieldValue instanceof byte[]) {
					log.trace("convertStruct() - Processing '{}' as bytes.", fieldName);
					boundStatement.setBytes(fieldName, ByteBuffer.wrap(((byte[]) fieldValue)));
				} else if (fieldValue instanceof List) {
					log.trace("convertStruct() - Processing '{}' as array.", fieldName);
					boundStatement.setList(fieldName, (List<?>) fieldValue);
				} else {
					if (!(fieldValue instanceof Map)) {
						throw new DataException(
								String.format("%s is not a supported data type.", fieldValue.getClass().getName()));
					}

					log.trace("convertStruct() - Processing '{}' as map.", fieldName);
					boundStatement.setMap(fieldName, (Map<?,?>) fieldValue);
				}
			} catch (Exception ex) {
				throw new DataException(String.format("Exception thrown while processing field '%s'", fieldName), ex);
			}
		}
	}

	private static void convertStruct(SinkBoundStatement boundStatement, Struct struct) {
		Schema schema = struct.schema();
		Iterator<?> fieldsIterator = schema.fields().iterator();

		while (fieldsIterator.hasNext()) {
			Field field = (Field) fieldsIterator.next();
			String fieldName = field.name();
			log.trace("convertStruct() - Processing '{}'", field.name());
			Object fieldValue = struct.get(field);

			try {
				if (null == fieldValue) {
					log.trace("convertStruct() - Setting '{}' to null.", fieldName);
					boundStatement.setToNull(fieldName);
				} else {
					log.trace("convertStruct() - Field '{}'.field().schema().type() = '{}'", fieldName,
							field.schema().type());
					switch (field.schema().type()) {
					case STRING:
						log.trace("convertStruct() - Processing '{}' as string.", fieldName);
						boundStatement.setString(fieldName, (String) fieldValue);
						break;
					case INT8:
						log.trace("convertStruct() - Processing '{}' as int8.", fieldName);
						boundStatement.setByte(fieldName, (Byte) fieldValue);
						break;
					case INT16:
						log.trace("convertStruct() - Processing '{}' as int16.", fieldName);
						boundStatement.setShort(fieldName, (Short) fieldValue);
						break;
					case INT32:
						if ("org.apache.kafka.connect.data.Date".equals(field.schema().name())) {
							log.trace("convertStruct() - Processing '{}' as date.", fieldName);
							boundStatement.setDate(fieldName, LocalDate.fromMillisSinceEpoch(((Date) fieldValue).getTime()));
						} else if ("org.apache.kafka.connect.data.Time".equals(field.schema().name())) {
							log.trace("convertStruct() - Processing '{}' as time.", fieldName);
							final long nanoseconds = TimeUnit.MILLISECONDS.convert(((Date) fieldValue).getTime(), TimeUnit.NANOSECONDS);
							boundStatement.setTime(fieldName, nanoseconds);
						} else {
							log.trace("convertStruct() - Processing '{}' as int32.", fieldName);
							boundStatement.setInt(fieldName, (Integer) fieldValue);
						}
						break;
					case INT64:
						if ("org.apache.kafka.connect.data.Timestamp".equals(field.schema().name())) {
							log.trace("convertStruct() - Processing '{}' as timestamp.", fieldName);
							boundStatement.setTimestamp(fieldName, (Date) fieldValue);
						} else {
							Long int64Value = (Long) fieldValue;
							log.trace("convertStruct() - Processing '{}' as int64.", fieldName);
							boundStatement.setLong(fieldName, int64Value);
						}
						break;
					case BYTES:
						if ("org.apache.kafka.connect.data.Decimal".equals(field.schema().name())) {
							log.trace("convertStruct() - Processing '{}' as decimal.", fieldName);
							boundStatement.setDecimal(fieldName, (BigDecimal) fieldValue);
						} else {
							byte[] bytes = (byte[]) ((byte[]) fieldValue);
							log.trace("convertStruct() - Processing '{}' as bytes.", fieldName);
							boundStatement.setBytes(fieldName, ByteBuffer.wrap(bytes));
						}
						break;
					case FLOAT32:
						log.trace("convertStruct() - Processing '{}' as float32.", fieldName);
						boundStatement.setFloat(fieldName, (Float) fieldValue);
						break;
					case FLOAT64:
						log.trace("convertStruct() - Processing '{}' as float64.", fieldName);
						boundStatement.setDouble(fieldName, (Double) fieldValue);
						break;
					case BOOLEAN:
						log.trace("convertStruct() - Processing '{}' as boolean.", fieldName);
						boundStatement.setBool(fieldName, (Boolean) fieldValue);
						break;
					case STRUCT:
						throw new UnsupportedOperationException();
					case ARRAY:
						log.trace("convertStruct() - Processing '{}' as array.", fieldName);
						boundStatement.setList(fieldName, (List<?>) fieldValue);
						break;
					case MAP:
						log.trace("convertStruct() - Processing '{}' as map.", fieldName);
						boundStatement.setMap(fieldName, (Map<?,?>) fieldValue);
						break;
					default:
						throw new DataException("Unsupported schema.type(): " + schema.type());
					}
				}
			} catch (Exception ex) {
				throw new DataException(String.format("Exception thrown while processing field '%s'", fieldName), ex);
			}
		}
	}

	/* package */ static SinkBoundStatement convertFrom(PreparedStatement statement, Object value) {
		Preconditions.checkNotNull(value, "value cannot be null.");
		SinkBoundStatement result = new SinkBoundStatement(statement);
		if (value instanceof Struct) {
			convertStruct(result, (Struct) value);
		} else {
			if (!(value instanceof Map)) {
				throw new DataException(
						String.format("Only Schema (%s) or Schema less (%s) are supported. %s is not a supported type.",
								Struct.class.getName(), Map.class.getName(), value.getClass().getName()));
			}

			convertMap(result, (Map<?,?>) value);
		}

		return result;
	}

	public SinkBoundStatement(PreparedStatement statement) {
		super(statement);
	}

	public boolean isAnyPropertySet() {
		int i = 0;
		try {
			while (!isSet(i)) i++;
			return true;
		} catch (IndexOutOfBoundsException e) {
			// do nothing
		}
		
		return false;
	}
}
