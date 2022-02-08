package producer;

import java.util.List;
//import util.properties packages
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.kafka.clients.producer.KafkaProducer;
//import ProducerRecord packages
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import producer.Producer;
import producer_parse.JSONParse;

public class Producer {
	private static Logger logger = LoggerFactory.getLogger(Producer.class);

	private static KafkaProducer<String, String> producer;
	private static JSONObject json;

	public static KafkaProducer<String, String> setupProducer(String filePath) {
		if (producer != null)
			return producer;
		// create instance for properties to access producer configs
		json = JSONParse.parseJSON(filePath);
		Properties props = JSONParse.getProducerProperties(json);
		producer = new KafkaProducer<String, String>(props);
		return producer;
	}

	public static void produce(String filePath, String message) {
		setupProducer(filePath);
		List<String> topics = JSONParse.getTopics(json);
		String topicsCommaSeperated = topics.stream().collect(Collectors.joining(","));
		producer.send(new ProducerRecord<String, String>(topicsCommaSeperated, "key", message));
	}
	
	public static void closeProducer() {
		if(producer != null) producer.close();
	}

	public static void main(String[] args) throws Exception {
		// Check arguments length value
		if (args.length == 0) {
			logger.error("No config file path specified");
			return;
		}
		produce(args[0], "message");
		closeProducer();
	}
}