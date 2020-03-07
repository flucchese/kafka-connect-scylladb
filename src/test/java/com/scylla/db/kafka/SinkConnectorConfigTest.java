package com.scylla.db.kafka;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

public class SinkConnectorConfigTest {

	Map<String, String> settings;
	SinkConnectorConfig config;

	@Before
	public void before() {
		settings = new HashMap<>();
		settings.put(SinkConnectorConfig.KEYSPACE_CONFIG, "scylladb");
		config = null;
	}

	@Test
	public void shouldAcceptValidConfig() {
		settings.put(SinkConnectorConfig.PORT_CONFIG, "9042");
		config = new SinkConnectorConfig(settings);
		assertNotNull(config);
	}

	@Test
	public void shouldUseDefaults() {
		config = new SinkConnectorConfig(settings);
		assertEquals(true, config.keyspaceCreateEnabled);
	}

	@Test(expected = IllegalStateException.class)
	public void shouldNotAllowInvalidSSLProvide() {
		settings.put(SinkConnectorConfig.SSL_PROVIDER_CONFIG, "DKJ");
		new SinkConnectorConfig(settings);
	}

	// TODO: Add more tests
}