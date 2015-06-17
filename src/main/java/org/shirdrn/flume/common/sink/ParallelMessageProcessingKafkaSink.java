package org.shirdrn.flume.common.sink;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.shirdrn.flume.common.RawMessageProcessor;
import org.shirdrn.flume.utils.NamedThreadFactory;
import org.shirdrn.flume.utils.ReflectionUtils;

import com.google.common.collect.Lists;

/**
 * Process Flume routed raw messages, and save processed keyed message
 * to Kafka MQ. Both perhaps are performed in parallel.
 * 
 * @author yanjun
 *
 * @param <K>
 * @param <V>
 */
public class ParallelMessageProcessingKafkaSink<K, V> extends AbstractKafkaSink<K, V> {

	private static final Log LOG = LogFactory.getLog(ParallelMessageProcessingKafkaSink.class);
	private static final String DEFAULT_ENCODING = "UTF-8";
	private static final String TOPIC = "topic";
	private String topic;
	
    private int rawMessageQueueSize = 10;
    private BlockingQueue<String> rawMessageQueue;
    private RawMessageProcessor<K, V> rawMessageProcessor;
    private int rawMessageProcessorCount = 1;
    private ExecutorService messageProcessorExecutorService;
    
    private int keyedMessageQueueSize = 10;
    private BlockingQueue<KeyedMessage<K, V>> keyedMessageQueue;
    private int producerCount = 1;
    private ExecutorService messageSenderExecutorService;
    
    private final List<InternalMessageSender> messageSenders = Lists.newArrayList(); 
    
    @SuppressWarnings("unchecked")
	@Override
    public void configure(Context context) {
        super.configure(context);
        topic = this.context.getParameters().get(TOPIC);
        checkNotNull(topic, TOPIC);
        // raw message queue
        try {
        	rawMessageQueueSize = Integer.parseInt(this.context.getParameters().get("raw.message.queue.size"));
        } catch (Exception e) {
        	LOG.warn("Use dufault: rawMsgQueueSize=" + rawMessageQueueSize);
        } finally {
        	rawMessageQueue = new ArrayBlockingQueue<String>(rawMessageQueueSize);
        }
        
        // keyed message queue
        try {
        	keyedMessageQueueSize = Integer.parseInt(this.context.getParameters().get("keyed.message.queue.size"));
        } catch (Exception e) {
        	LOG.warn("Use dufault: keyedMessageQueueSize=" + keyedMessageQueueSize);
        } finally {
        	keyedMessageQueue = new ArrayBlockingQueue<KeyedMessage<K,V>>(keyedMessageQueueSize);
        }
        
        // create message processor instance
        String rawMessageProcessorClazz = this.context.getParameters().get("raw.message.processor.class");
        LOG.info("Configured raw.message.processor.class: " + rawMessageProcessorClazz);
        rawMessageProcessor = ReflectionUtils.newInstance(rawMessageProcessorClazz, RawMessageProcessor.class);
        
        // message processor count
        try {
        	rawMessageProcessorCount = Integer.parseInt(this.context.getParameters().get("raw.message.processor.count"));
        } catch (Exception e) {
        	LOG.warn("Use dufault: rawMessageProcessorCount=" + rawMessageProcessorCount);
        }
        
        // producer count
        try {
			producerCount = Integer.parseInt(this.context.getParameters().get("keyed.message.producer.count"));
		} catch (Exception e) {
			LOG.warn("Use dufault: producerCount=" + producerCount);
		}
    }

    @Override
    public synchronized void start() {
        super.start();
        Properties props = createOriginalProps();
        ProducerConfig config = new ProducerConfig(props);
        
        // start event decoder
        messageProcessorExecutorService = Executors.newFixedThreadPool(rawMessageProcessorCount, new NamedThreadFactory("PROCESSOR"));
        for (int i = 0; i < rawMessageProcessorCount; i++) {
        	InternalMessageProcessorWorker decoder = new InternalMessageProcessorWorker();
        	messageProcessorExecutorService.execute(decoder);
		}
        
        // start producer
        messageSenderExecutorService = Executors.newFixedThreadPool(producerCount, new NamedThreadFactory("SENDER"));
        for (int i = 0; i < producerCount; i++) {
        	Producer<K, V> producer = new Producer<K, V>(config);
        	InternalMessageSender sender = new InternalMessageSender(producer);
        	messageSenderExecutorService.execute(sender);
        	messageSenders.add(sender);
		}
    }

    @Override
    protected void sendMessage(Event event) throws Exception {
    	String eventData = new String(event.getBody(), DEFAULT_ENCODING);
    	rawMessageQueue.put(eventData);
    }

    @Override
    public synchronized void stop() {
        super.stop();
        // shutdown executor service
        messageSenderExecutorService.shutdown();
        messageProcessorExecutorService.shutdown();
        // close producers
        for(InternalMessageSender messageSender : messageSenders) {
        	messageSender.producer.close();
        }
    }

    private final class InternalMessageProcessorWorker extends Thread {
    	
    	@Override
    	public void run() {
    		while(true) {
    			try {
    				// use non-blocking mode
					String message = rawMessageQueue.poll();
					if(message != null) {
						KeyedMessage<K, V> keyedMessage = rawMessageProcessor.process(topic, message);
						if(keyedMessage != null) {
							keyedMessageQueue.add(keyedMessage);
						}
					} else {
						Thread.sleep(1500);
					}
				} catch (Exception e) {
					LOG.warn("Fail to process raw message: ", e);
				}
    		}
    	}
    	
    }
    
    private final class InternalMessageSender extends Thread {
    	
    	private final Producer<K, V> producer;
    	
    	public InternalMessageSender(Producer<K, V> producer) {
    		this.producer = producer;
    	}
    	
    	@Override
    	public void run() {
    		while(true) {
    			try {
    				// use non-blocking mode
					KeyedMessage<K,V> event = keyedMessageQueue.poll();
					if(event != null) {
						producer.send(event);
					} else {
						Thread.sleep(2000);
					}
				} catch (Exception e) {
					LOG.error("Fail to send keyed message: ", e);
				}
    		}
    	}
    }

}
