package jenkins.plugins.logstash.persistence;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import net.sf.json.JSONObject;

public class KafkaDao extends AbstractLogstashIndexerDao {
	private String eventKey = "";

	public KafkaDao(String host, int port, String key, String username, String password) {
		super(host, port, key, username, password);
	}

	@Override
	public IndexerType getIndexerType() {
		return IndexerType.KAFKA;
	}

	@Override
	public void push(String data) throws IOException {
		Thread.currentThread().setContextClassLoader(null);
		Properties props = new Properties();
		props.put("bootstrap.servers", String.format("%s:%d", host, port));
		props.put("acks", "1");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		KafkaProducer<String, String> producer = new KafkaProducer<>(props);
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
