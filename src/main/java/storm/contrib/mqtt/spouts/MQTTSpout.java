package storm.contrib.mqtt.spouts;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;

import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class MQTTSpout implements MqttCallback, IRichSpout {
	MqttClient client;
	SpoutOutputCollector _collector;
	LinkedList<String> messages;

	String _broker_url;
	String _client_id;
	String _topic;

	public MQTTSpout(String broker_url, String clientId, String topic) {
		_broker_url = broker_url;
		_client_id = clientId;
		_topic = topic;
		messages = new LinkedList<String>();
	}

	public void messageArrived(String topic, MqttMessage message)
			throws Exception {
		messages.add(message.toString());
	}

	public void connectionLost(Throwable cause) {
	}

	public void deliveryComplete(IMqttDeliveryToken token) {
	}

	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		_collector = collector;

		try {
			client = new MqttClient(_broker_url, _client_id);
			client.connect();
			client.setCallback(this);
			client.subscribe(_topic);

		} catch (MqttException e) {
			e.printStackTrace();
		}
	}

	public void close() {
	}

	public void activate() {
	}

	public void deactivate() {
	}

	public void nextTuple() {
		while (!messages.isEmpty()) {
			_collector.emit(new Values(messages.poll()));
		}
	}

	public void ack(Object msgId) {
	}

	public void fail(Object msgId) {
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	public static void main(String[] args) {
		TopologyBuilder builder = new TopologyBuilder();
		List<String> hosts = new ArrayList<String>();
		hosts.add("localhost");

		builder.setSpout("spout", new MQTTSpout(
				"tcp://test.mosquitto.org:1883", "storm-mqtt-test", "#"), 3);

		Config conf = new Config();
		// conf.setDebug(true);

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("kafka-test", conf, builder.createTopology());

		Utils.sleep(600000);
	}
}
