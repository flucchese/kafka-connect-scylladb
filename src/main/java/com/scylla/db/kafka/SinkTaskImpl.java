package com.scylla.db.kafka;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.TransportException;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;

/**
 * Task class for ScyllaDB Sink Connector.
 */
public class SinkTaskImpl extends SinkTask {

	private static final Logger log = LoggerFactory.getLogger(SinkTaskImpl.class);

	private SinkConnectorConfig config;
	private SinkSession session;

	/**
	 * Starts the sink task. If <code>scylladb.offset.storage.table.enable</code> is
	 * set to true, the task will load offsets for each Kafka topic-parition from
	 * ScyllaDB offset table into task context.
	 */
	@Override
	public void start(Map<String, String> settings) {
		this.config = new SinkConnectorConfig(settings);
		if (config.isTableOffsetEnabled()) {
			Set<TopicPartition> assignment = context.assignment();
			this.session = getValidSession();
			Map<TopicPartition, Long> offsets = session.loadOffsets(assignment);
			if (!offsets.isEmpty()) {
				context.offset(offsets);
			}
		}
	}

	/*
	 * Returns a ScyllaDB session. Creates a session, if not already exists. In case
	 * the when session is not valid, it closes the existing session and creates a
	 * new one.
	 */
	private SinkSession getValidSession() {

		if (session == null) {
			log.info("Creating ScyllaDb Session.");
			session = SinkSession.newInstance(config);
		}

		if (!session.isValid()) {
			log.warn("ScyllaDb Session is invalid. Closing and creating new.");
			close();
			session = SinkSession.newInstance(config);
		}
		
		return session;
	}

	private void validateRecord(SinkRecord record) {
		if (null == record.key()) {
			throw new DataException("Record with a null key was encountered. This connector requires that records "
					+ "from Kafka contain the keys for the ScyllaDb table. Please use a "
					+ "transformation like org.apache.kafka.connect.transforms.ValueToKey "
					+ "to create a key with the proper fields.");
		}

		if (!(record.key() instanceof Struct) && !(record.key() instanceof Map)) {
			throw new DataException("Key must be a struct or map. This connector requires that records from Kafka "
					+ "contain the keys for the ScyllaDb table. Please use a transformation like "
					+ "org.apache.kafka.connect.transforms.ValueToKey to create a key with the " + "proper fields.");
		}
	}

	private BoundStatement getBoundStatementForRecord(SinkRecord record) {
		BoundStatement boundStatement = null;
		String topic = record.topic();
		if (null == record.value()) {
			if (config.deletesEnabled) {
				if (session.tableExists(topic)) {
					PreparedStatement statement = session.delete(topic);
					boundStatement = SinkBoundStatement.convertFrom(statement, record.key());
					Preconditions.checkState(((SinkBoundStatement) boundStatement).isAnyPropertySet(),
												"key must contain the columns in the primary key.");
				} else {
					log.warn("put() - table '{}' does not exist. Skipping delete.", topic);
				}
			} else {
				throw new DataException(String.format(
						"Record with null value found for the key '%s'. If you are trying to delete the record set "
								+ "scylladb.deletes.enabled = true in your connector configuration.",
						record.key()));
			}
		}

		if (boundStatement == null) {
			session.createOrAlterTable(topic, record.keySchema(), record.valueSchema());
			PreparedStatement statement = session.insert(topic);
			boundStatement = SinkBoundStatement.convertFrom(statement, record.value());
		}
		
		boundStatement.setDefaultTimestamp(record.timestamp());
		log.trace("put() - Adding Bound Statement for {}:{}:{}", topic, record.kafkaPartition(), record.kafkaOffset());
		
		return boundStatement;
	}

	/**
	 * <ol>
	 * <li>Validates the kafka records.
	 * <li>Writes or deletes records from Kafka topic into ScyllaDB.
	 * <li>Requests to commit the records when the scyllaDB operations are
	 * successful.
	 * </ol>
	 */
	@Override
	public void put(Collection<SinkRecord> records) {
		int count = 0;
		final List<ResultSetFuture> futures = new ArrayList<>(records.size());

		// create a map containing topic, partition number and list of records
		Map<TopicPartition, List<BatchStatement>> batchesPerTopicPartition = new HashMap<>();

		int configuredMaxBatchSize = config.maxBatchSizeKb * 1024;
		for (SinkRecord record : records) {
			validateRecord(record);

			String topic = record.topic();
			int kafkaPartition = record.kafkaPartition();

			TopicPartition topicPartition = new TopicPartition(topic, kafkaPartition);
			List<BatchStatement> batchStatementList = batchesPerTopicPartition.get(topicPartition);
			if (batchStatementList == null) {
				batchStatementList = new ArrayList<>();
			}

			BatchStatement latestBatchStatement = !batchStatementList.isEmpty() ?
					batchStatementList.get(batchStatementList.size() - 1) :
					new BatchStatement(BatchStatement.Type.UNLOGGED);

			BoundStatement boundStatement = getBoundStatementForRecord(record);

			int totalBatchSize = (latestBatchStatement.size() > 0 ? sizeInBytes(latestBatchStatement) : 0) + sizeInBytes(boundStatement);

			boolean shouldWriteStatementInCurrentBatch = latestBatchStatement.size() > 0 &&
					isRecordWithinTimestampResolution(boundStatement, latestBatchStatement);

			if (totalBatchSize <= configuredMaxBatchSize && shouldWriteStatementInCurrentBatch) {
				latestBatchStatement.add(boundStatement);
				latestBatchStatement.setDefaultTimestamp(boundStatement.getDefaultTimestamp());
			} else {
				BatchStatement newBatchStatement = new BatchStatement(BatchStatement.Type.UNLOGGED);
				newBatchStatement.add(boundStatement);
				newBatchStatement.setDefaultTimestamp(boundStatement.getDefaultTimestamp());
				batchStatementList.add(newBatchStatement);
			}
			
			batchesPerTopicPartition.put(topicPartition, batchStatementList);
		}

		for (List<BatchStatement> batchStatementList : batchesPerTopicPartition.values()) {
			for (BatchStatement batchStatement : batchStatementList) {
				batchStatement.setConsistencyLevel(config.consistencyLevel);
				log.trace("put() - Executing Batch Statement {} of size {}", batchStatement, batchStatement.size());
				ResultSetFuture resultSetFuture = this.getValidSession().executeStatementAsync(batchStatement);
				futures.add(resultSetFuture);
				count++;
			}
		}

		if (count > 0) {
			try {
				log.debug("put() - Checking future(s)");
				for (ResultSetFuture future : futures) {
					future.getUninterruptibly(this.config.statementTimeoutMs, TimeUnit.MILLISECONDS);
				}
				context.requestCommit();
				// TODO : Log the records that fail in Queue/Kafka Topic.
			} catch (TransportException ex) {
				log.debug("put() - Setting clusterValid = false", ex);
				getValidSession().setInvalid();
				throw new RetriableException(ex);
			} catch (TimeoutException ex) {
				log.error("put() - TimeoutException.", ex);
				throw new RetriableException(ex);
			} catch (Exception ex) {
				log.error("put() - Unknown exception. Setting clusterValid = false", ex);
				getValidSession().setInvalid();
				throw new RetriableException(ex);
			}
		}
	}

	private boolean isRecordWithinTimestampResolution(BoundStatement boundStatement,
			BatchStatement latestBatchStatement) {
		long timeDiffFromInitialRecord = boundStatement.getDefaultTimestamp()
				- Iterables.get(latestBatchStatement.getStatements(), 0).getDefaultTimestamp();
		
		return timeDiffFromInitialRecord <= config.timestampResolutionMs;
	}

	private static int sizeInBytes(Statement statement) {
		return statement.requestSizeInBytes(ProtocolVersion.V4, CodecRegistry.DEFAULT_INSTANCE);
	}

	/**
	 * If <code>scylladb.offset.storage.table.enable</code> is set to true, updates
	 * offsets in ScyllaDB table. Else, assumes all the records in previous @put
	 * call were successfully written in to ScyllaDB and returns the same offsets.
	 */
	@Override
	public Map<TopicPartition, OffsetAndMetadata> preCommit(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
		if (config.isTableOffsetEnabled()) {
			BatchStatement batch = new BatchStatement();
			this.getValidSession().addOffsetsToBatch(batch, currentOffsets);

			try {
				log.debug("flush() - Flushing offsets to {}", this.config.tableOffsetName);
				getValidSession().executeStatement(batch);
			} catch (TransportException ex) {
				log.debug("put() - Setting clusterValid = false", ex);
				getValidSession().setInvalid();
				throw new RetriableException(ex);
			} catch (Exception ex) {
				log.error("put() - Unknown exception. Setting clusterValid = false", ex);
				getValidSession().setInvalid();
				throw new RetriableException(ex);
			}
		}
		
		return super.preCommit(currentOffsets);
	}

	/**
	 * Closes the ScyllaDB session and proceeds to closing sink task.
	 */
	@Override
	public void stop() {
		close();
	}

	// Visible for testing
	void close() {
		if (null != session) {
			log.info("Closing getValidSession");
			try {
				session.close();
			} catch (IOException ex) {
				log.error("Exception thrown while closing ScyllaDB session.", ex);
			}
			session = null;
		}
	}

	@Override
	public String version() {
		return VersionUtil.getVersion();
	}
}