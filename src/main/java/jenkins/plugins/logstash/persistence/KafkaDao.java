package jenkins.plugins.logstash.persistence;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class KafkaDao extends AbstractLogstashIndexerDao {

	public KafkaDao(String host, int port, String key, String username, String password) {
		super(host, port, key, username, password);
	}

	@Override
	public IndexerType getIndexerType() {
		return IndexerType.KAFKA;
	}

	@Override
	public void push(String data) throws IOException {
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
		ProducerRecord<String, String> record = new ProducerRecord<>(this.key, data);
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

}
