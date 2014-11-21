package org.shirdrn.flume.sink.common;

import java.util.Properties;

import kafka.javaapi.producer.Producer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flume.Channel;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;

import com.google.common.base.Preconditions;

public abstract class AbstractKafkaSink<K, V> extends AbstractSink implements Configurable {

	private static final Log LOG = LogFactory.getLog(AbstractKafkaSink.class);
	
	protected static final String PARTITION_KEY = "partition.key";
	protected static final String REQUEST_REQUIRED_ACKS = "request.required.acks";
	protected static final String REQUEST_TIMEOUT_MS = "request.timeout.ms";
	protected static final String BROKER_LIST = "metadata.broker.list";
    protected static final String SERIALIZER_CLASS = "serializer.class";
    protected static final String BATCH_NUM_MESSAGES = "batch.num.messages";
    protected static final String PRODUCER_TYPE = "producer.type";
    protected static final String MESSAGE_SEND_MAX_RETRIES = "message.send.max.retries";
    protected static final String CLIENT_ID = "client.id";
    protected static final String SEND_BUFFER_BYTES = "send.buffer.bytes";
    protected static final String QUEUE_ENQUEUE_TIMEOUT_MS = "queue.enqueue.timeout.ms";
    protected static final String QUEUE_BUFFERING_MAX_MESSAGES = "queue.buffering.max.messages";
    
    protected static final String DEFAULT_SERIALIZER_CLASS = "kafka.serializer.DefaultEncoder";
    protected static final String DEFAULT_PRODUCER_TYPE = ProducerType.SYNC.getType();
    protected static final int DEFAULT_MESSAGE_SEND_MAX_RETRIES = 3;
    protected static final int DEFAULT_REQUEST_REQUIRED_ACKS = 0;
    protected static final int DEFAULT_REQUEST_TIMEOUT_MS = 10000;
    protected static final int DEFAULT_BATCH_NUM_MESSAGES = 200;
    protected static final int DEFAULT_SEND_BUFFER_BYTES = 100 * 1024;
    protected static final int DEFAULT_QUEUE_ENQUEUE_TIMEOUT_MS = -1;
    protected static final int DEFAULT_QUEUE_BUFFERING_MAX_MESSAGES = 10000;
    
    protected Context context;
	protected Producer<K, V> producer;
	protected String partitionKey;
    protected String brokerList;
    protected String producerType = DEFAULT_PRODUCER_TYPE;
    protected String serializerClass = DEFAULT_SERIALIZER_CLASS;
    protected String clientId;
    
    protected int requestRequiredAcks = DEFAULT_REQUEST_REQUIRED_ACKS;
    protected int requestTimeoutMs = DEFAULT_REQUEST_TIMEOUT_MS;
    protected int sendBufferBytes = DEFAULT_SEND_BUFFER_BYTES;
    protected int batchNumMessages = DEFAULT_BATCH_NUM_MESSAGES;
    protected int messageSendMaxRetries = DEFAULT_MESSAGE_SEND_MAX_RETRIES;
    protected int queueEnqueueTimeoutMs = DEFAULT_QUEUE_ENQUEUE_TIMEOUT_MS;
    protected int queueBufferingMaxMessages = DEFAULT_QUEUE_BUFFERING_MAX_MESSAGES;
	
    @Override
    public void configure(Context context) {
        this.context = context;
        partitionKey = this.context.getParameters().get(PARTITION_KEY);
        
        clientId = this.context.getParameters().get(CLIENT_ID);
        checkNotNull(clientId, CLIENT_ID);
        
        brokerList = this.context.getParameters().get(BROKER_LIST);
        checkNotNull(brokerList, BROKER_LIST);
        
        String configProducerType = this.context.getParameters().get(PRODUCER_TYPE);
        if(configProducerType != null) {
        	if(configProducerType.trim().equals(ProducerType.SYNC.getType()) 
        			|| configProducerType.trim().equals(ProducerType.ASYNC.getType()) ) {
        		producerType = configProducerType;
        	} else {
        		LOG.warn("Unknown configured producer type: " + configProducerType);
        		LOG.warn("Use default producer type: " + DEFAULT_PRODUCER_TYPE);
        	}
        }
        
        messageSendMaxRetries = this.context.getInteger(MESSAGE_SEND_MAX_RETRIES, DEFAULT_MESSAGE_SEND_MAX_RETRIES);
        if(messageSendMaxRetries <= 0) {
        	messageSendMaxRetries = DEFAULT_MESSAGE_SEND_MAX_RETRIES;
        }
        
        String configSerializerClass = this.context.getParameters().get(SERIALIZER_CLASS);
        if(configSerializerClass != null) {
        	serializerClass = configSerializerClass;
        }
        
        requestRequiredAcks = this.context.getInteger(REQUEST_REQUIRED_ACKS, DEFAULT_REQUEST_REQUIRED_ACKS);
        requestTimeoutMs = this.context.getInteger(REQUEST_TIMEOUT_MS, DEFAULT_REQUEST_TIMEOUT_MS);
    }
    
    protected Properties createOriginalProps() {
    	Properties props = new Properties();
    	props.setProperty(BROKER_LIST, brokerList);
    	props.setProperty(REQUEST_REQUIRED_ACKS, String.valueOf(requestRequiredAcks));
    	props.setProperty(REQUEST_TIMEOUT_MS, String.valueOf(requestTimeoutMs));
    	props.setProperty(CLIENT_ID, clientId);
    	props.setProperty(PRODUCER_TYPE, producerType);
    	props.setProperty(SERIALIZER_CLASS, serializerClass);
    	props.setProperty(SEND_BUFFER_BYTES, String.valueOf(sendBufferBytes));
    	props.setProperty(BATCH_NUM_MESSAGES, String.valueOf(batchNumMessages));
    	props.setProperty(MESSAGE_SEND_MAX_RETRIES, String.valueOf(messageSendMaxRetries));
    	props.setProperty(QUEUE_ENQUEUE_TIMEOUT_MS, String.valueOf(queueEnqueueTimeoutMs));
    	props.setProperty(QUEUE_BUFFERING_MAX_MESSAGES, String.valueOf(queueBufferingMaxMessages));
    	return props;
	}
    
    protected void checkNotNull(String key, String value) {
    	Preconditions.checkNotNull(value, "Value for property \"" + key + "\" MUST not be null!");
    }
    
    @Override
    public Status process() throws EventDeliveryException {
    	Status status = Status.BACKOFF;
        Channel channel = getChannel();
        Transaction tx = channel.getTransaction();
        try {
        	tx.begin();
        	Event event = channel.take();
        	if(event != null) {
        		sendMessage(event);
        		tx.commit();
        		status = Status.READY;
        	} else {
        		 tx.rollback();
        	}
        	
        } catch (ChannelException e) {
            LOG.error("Channel error: ", e);
            tx.rollback();
        } catch (Exception e) {
        	LOG.error("Send message failed!", e);
        	 tx.rollback();
        } finally {
            tx.close();
        }
        return status;
    }
	
    /**
     * Send a event, 
     * @param event
     * @throws Exception
     */
	protected abstract void sendMessage(Event event) throws Exception;

	protected enum ProducerType {
		SYNC("sync"),
		ASYNC("async");
		
		private String type;
		
		ProducerType(String type) {
			this.type = type;
		}
		
		public String getType() {
			return type;
		}
	}
}
