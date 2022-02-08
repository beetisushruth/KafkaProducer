package producer_parse;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JSONParse {
	private static Logger logger = LoggerFactory.getLogger(JSONParse.class);

	public static JSONObject parseJSON(String filePath) {
		logger.info("Parsing json at file path: " + filePath);
		// JSON parser object to parse read file
		JSONParser jsonParser = new JSONParser();
		JSONObject jsonObject = null;
		try (FileReader reader = new FileReader(filePath)) {
			// Read JSON file
			Object obj = jsonParser.parse(reader);
			jsonObject = (JSONObject) obj;
		} catch (FileNotFoundException e) {
			logger.error("File not found " + filePath);
		} catch (IOException e) {
			logger.error("Exception occured " + e.getMessage());
		} catch (ParseException e) {
			logger.error("Parse exception occured " + e.getMessage());
		} catch (Exception e) {
			logger.error("Error occured while parsing the file " + filePath);
		}
		return jsonObject;
	}

	public static List<String> getTopics(JSONObject json) {
		JSONArray topics = (JSONArray) json.get("topics");
		List<String> topicList = new ArrayList<>();
		for (int i = 0; i < topics.size(); i++) {
			topicList.add((String) topics.get(i));
		}
		return topicList;
	}

	public static List<String> getBrokerServers(JSONObject json) {
		JSONArray brokerServers = (JSONArray) json.get("broker_servers");
		List<String> brokerList = new ArrayList<>();
		for (int i = 0; i < brokerServers.size(); i++) {
			JSONObject server = (JSONObject) brokerServers.get(i);
			String address = server.get("host") + ":" + server.get("port");
			brokerList.add(address);
		}
		return brokerList;
	}

	public static Properties getProducerProperties(JSONObject json) {
		List<String> brokerServers = getBrokerServers(json);
		String servers = brokerServers.stream().collect(Collectors.joining(","));
		String acks = (String) json.get("acks");
		json.get("retries");
		json.get("batch_size");
		json.get("linger_ms");
		json.get("buffer_memory");
		String keySerializer = (String) json.get("key_serializer");
		String valueSerializer = (String) json.get("value_serializer");
		Properties props = new Properties();

		// Assign localhost id
		props.put("bootstrap.servers", servers);

		// Set acknowledgements for producer requests.
		props.put("acks", acks);

		// If the request fails, the producer can automatically retry,
		props.put("retries", 0);

		// Specify buffer size in config
		props.put("batch.size", 16384);

		// Reduce the no of requests less than 0
		props.put("linger.ms", 1);

		// The buffer.memory controls the total amount of memory available to the
		// producer for buffering.
		props.put("buffer.memory", 33554432);

		props.put("key.serializer", keySerializer);

		props.put("value.serializer", valueSerializer);
		return props;
	}

	public static void main(String[] args) {
		String filePath = "/src/config.json";
		if (args.length == 0) {
			logger.info("Config file path arguments not provided, using default filePath : " + filePath);
		} else {
			filePath = args[0];
		}
	}
}