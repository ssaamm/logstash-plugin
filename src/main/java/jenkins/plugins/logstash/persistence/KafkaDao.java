package jenkins.plugins.logstash.persistence;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.logging.Logger;

import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;

import net.sf.json.JSONObject;

public class KafkaDao extends AbstractLogstashIndexerDao {
	private String eventKey = "";
	private final Properties kafka_config;
	private final String truststore_location;
	private final String truststore_password;
	private final String keystore_location;
	private final String keystore_password;
	private final String key_password;

	public KafkaDao(String host, int port, String key, String username, String password, String truststore_location, String truststore_password, String keystore_location, String keystore_password, String key_password) {
		super(host, port, key, username, password);
		this.truststore_location = truststore_location;
		this.truststore_password = truststore_password;
		this.keystore_location = keystore_location;
		this.keystore_password = keystore_password;
		this.key_password = key_password;
		
		String[] ssl_options = {this.truststore_location, this.truststore_password, this.keystore_location, this.keystore_password, this.key_password};
		boolean should_use_ssl = true;
		for (String option : ssl_options) {
			if (StringUtils.isBlank(option)) {
				should_use_ssl = false;
			}
		}

		StringBuilder brokersBuilder = new StringBuilder();
		String[] hosts = host.split(",");
		for (String aHost : hosts) {
			if (brokersBuilder.length() > 0) {
				brokersBuilder.append(',');
			}
			brokersBuilder.append(String.format("%s:%d", aHost, this.port));
		}

		kafka_config = new Properties();
		kafka_config.put("bootstrap.servers", brokersBuilder.toString());
		kafka_config.put("acks", "1");
		kafka_config.put("retries", 0);
		kafka_config.put("batch.size", 16384);
		kafka_config.put("linger.ms", 1);
		kafka_config.put("buffer.memory", 33554432);
		kafka_config.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		kafka_config.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		
		if (should_use_ssl) {
			kafka_config.put("security.protocol", "SSL");
			kafka_config.put("ssl.truststore.location", this.truststore_location);
			kafka_config.put("ssl.truststore.password", this.truststore_password);
			kafka_config.put("ssl.keystore.location", this.keystore_location);
			kafka_config.put("ssl.keystore.password", this.keystore_password);
			kafka_config.put("ssl.key.password", this.key_password);
		}
	}

	@Override
	public IndexerType getIndexerType() {
		return IndexerType.KAFKA;
	}

	@Override
	public void push(String data) throws IOException {
		Thread.currentThread().setContextClassLoader(null);

		KafkaProducer<String, String> producer;
		try {
			producer = new KafkaProducer<>(kafka_config);
		} catch(KafkaException e) {
			Logger logger = Logger.getLogger("KafkaDao");
			logger.warning(e.getCause().getMessage() + "\n=====================\n" + e.getCause().getStackTrace());

			throw new IOException(e);
		}
		ProducerRecord<String, String> record = new ProducerRecord<>(this.key, this.getEventKey(), data);
		Future<RecordMetadata> send_future = producer.send(record);
		try {
			send_future.get();
		} catch (InterruptedException e) {
			throw new IOException(e);
		} catch (ExecutionException e) {
			throw new IOException(e);
		} finally {
			producer.close();
		}
	}

	@Override
	public JSONObject buildPayload(BuildData buildData, String jenkinsUrl, List<String> logLines) {
		this.setEventKey(jenkinsUrl + buildData.getProjectName() + buildData.getBuildNum());
		return super.buildPayload(buildData, jenkinsUrl, logLines);
	}

	private String getEventKey() {
		return this.eventKey;
	}

	private void setEventKey(String key) {
		this.eventKey = key;
	}

}
