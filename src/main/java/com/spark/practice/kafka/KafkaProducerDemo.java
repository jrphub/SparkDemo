package com.spark.practice.kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class KafkaProducerDemo {
	private final static String TOPIC = "test-producer";
	private final static String BOOTSTRAP_SERVERS = "localhost:9092";

	private static Producer<Long, String> createProducer() {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		/*
		 * The CLIENT_ID_CONFIG (“client.id”) is an id to pass to the server
		 * when making requests so the server can track the source of requests
		 * beyond just IP/port by passing a producer name for things like
		 * server-side request logging.
		 */
		props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaExampleProducer");
		/*
		 * KafkaProducerExample imports LongSerializer which gets configured as
		 * the Kafka record key serializer, and imports StringSerializer which
		 * gets configured as the record value serializer
		 */
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
				LongSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
				StringSerializer.class.getName());
		return new KafkaProducer<>(props);
	}

	/*
	 * The below just iterates through a for loop, creating a ProducerRecord
	 * sending an example message ("Hello Prwatech " + index) as the record value and
	 * the for loop index as the record key. For each iteration, runProducer
	 * calls the send method of the producer (RecordMetadata metadata =
	 * producer.send(record).get()). The send method returns a Java Future.
	 * 
	 * The response RecordMetadata has ‘partition’ where the record was written
	 * and the ‘offset’ of the record in that partition.
	 * 
	 * Notice the call to flush and close. Kafka will auto flush on its own, but
	 * you can also call flush explicitly which will send the accumulated
	 * records now. It is polite to close the connection when we are done.
	 */
	static void runProducer(final int sendMessageCount) throws Exception {
		final Producer<Long, String> producer = createProducer();
		long time = System.currentTimeMillis();

		try {
			for (long index = time; index < time + sendMessageCount; index++) {
				final ProducerRecord<Long, String> record = new ProducerRecord<>(
						TOPIC, index, "Hello Prwatech " + index);

				RecordMetadata metadata = producer.send(record).get();

				long elapsedTime = System.currentTimeMillis() - time;
				System.out.printf("sent record(key=%s value=%s) "
						+ "meta(partition=%d, offset=%d) time=%d\n",
						record.key(), record.value(), metadata.partition(),
						metadata.offset(), elapsedTime);

			}
		} finally {
			producer.flush();
			producer.close();
		}
	}

	public static void main(String... args) throws Exception {
		if (args.length == 0) {
			runProducer(5);
		} else {
			runProducer(Integer.parseInt(args[0]));
		}
	}
}
