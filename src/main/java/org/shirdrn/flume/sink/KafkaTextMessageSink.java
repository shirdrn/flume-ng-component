package org.shirdrn.flume.sink;

import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.shirdrn.flume.common.sink.AbstractKafkaSink;

public class KafkaTextMessageSink extends AbstractKafkaSink<String, String> {

	private static final Log LOG = LogFactory.getLog(KafkaTextMessageSink.class);

    private static final String DEFAULT_ENCODING = "UTF-8";
    private static final String TOPIC = "topic";
    
    private String topic;
    
    @Override
    public void configure(Context context) {
        super.configure(context);
        topic = this.context.getParameters().get(TOPIC);
        checkNotNull(topic, TOPIC);
    }
    
    @Override
    public synchronized void start() {
        super.start();
        Properties props = createOriginalProps();
        ProducerConfig config = new ProducerConfig(props);
        this.producer = new Producer<String, String>(config);
    }

    @Override
    protected void sendMessage(Event event) throws Exception {
    	String eventData = new String(event.getBody(), DEFAULT_ENCODING);
        KeyedMessage<String, String> data = null;
        if(partitionKey == null || partitionKey.isEmpty()) {
        	data = new KeyedMessage<String, String>(topic, eventData);
        } else {
        	data = new KeyedMessage<String, String>(topic, partitionKey, eventData);
        }
        producer.send(data);
        LOG.debug("Message sent to Kafka : topic=" + topic + ", data=" + eventData + "]");
    }

    @Override
    public synchronized void stop() {
    	super.stop();
        producer.close();
    }

    
}
