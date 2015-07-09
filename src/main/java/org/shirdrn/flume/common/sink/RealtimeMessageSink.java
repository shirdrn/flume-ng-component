package org.shirdrn.flume.common.sink;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.shirdrn.flume.common.RawMessageProcessor;
import org.shirdrn.flume.utils.NamedThreadFactory;

import com.google.common.collect.Lists;

public class RealtimeMessageSink<K, V> extends AbstractKafkaSink<K, V> {

	private static final Log LOG = LogFactory.getLog(RealtimeMessageSink.class);

	private static final String DEFAULT_ENCODING = "UTF-8";
	private static final String TOPIC = "topic";

	private RawMessageProcessor<K, V> rawMessageProcessor;
	
	private String topic;
	private int messageProcessingWorkerCount = 1;
	private int messageProcessingWorkerQueueCapacity = 1000;
	private int outputStatEventBatchSize = 500;
	
	private ExecutorService messageProcessingWorkerExecutorService;
	
	private final List<MessageProcessingWroker> messageProcessingWorkers = Lists.newArrayList();
	private int sendBatchSize = 100;
	
	private volatile long dispatchedCnt = 0;
	private volatile long proccessedCnt = 0;
	private volatile long sentCnt = 0;

	@Override
	public void configure(Context context) {
		super.configure(context);

		topic = this.context.getParameters().get(TOPIC);
		checkNotNull(topic, TOPIC);
		
		// message processing worker queue capacity
		try {
			messageProcessingWorkerQueueCapacity = Integer.parseInt(this.context.getParameters().get("message.processing.worker.queue.capacity"));
		} catch (Exception e) {
			LOG.warn("Use dufault: messageProcessingWorkerQueueSize=" + messageProcessingWorkerQueueCapacity);
		}

		// message processing worker count
		try {
			messageProcessingWorkerCount = Integer.parseInt(this.context.getParameters().get("message.processing.worker.count"));
		} catch (Exception e) {
			LOG.warn("Use dufault: messageProcessingWorkerCount=" + messageProcessingWorkerCount);
		}

		// monitor to output statistics when reached event batch size
		try {
			outputStatEventBatchSize = Integer.parseInt(this.context.getParameters().get("output.stat.event.batch.size"));
		} catch (Exception e) {
			LOG.warn("Use dufault: outputStatEventBatchSize=" + outputStatEventBatchSize);
		}
		
		try {
			sendBatchSize = Integer.parseInt(this.context.getParameters().get("producer.send.batch.size"));
		} catch (Exception e) {
			LOG.warn("Use dufault: outputStatEventBatchSize=" + outputStatEventBatchSize);
		}
		
	}

	@Override
	public synchronized void start() {
		super.start();
		// start event decoder
		messageProcessingWorkerExecutorService = 
				Executors.newFixedThreadPool(messageProcessingWorkerCount, new NamedThreadFactory("WORKER"));
		for (int i = 0; i < messageProcessingWorkerCount; i++) {
			MessageProcessingWroker decoder = new MessageProcessingWroker(messageProcessingWorkerQueueCapacity);
			messageProcessingWorkerExecutorService.execute(decoder);
			messageProcessingWorkers.add(decoder);
		}
		
		workers = messageProcessingWorkers.size();
	}

	@Override
	protected void sendMessage(Event event) throws Exception {
		String eventData = new String(event.getBody(), DEFAULT_ENCODING);
		dispatch(eventData);
	}
	
	private long pointer = 0L;
	private long workers;
	
	private void dispatch(String event) {
		try {
			if (event != null) {
				selectWorker().q.put(event);
				// print monitoring details
				if (++dispatchedCnt % outputStatEventBatchSize == 0) {
					StringBuffer sb = new StringBuffer().append(
							"DC:" + dispatchedCnt + 
							"/PC:" + proccessedCnt + 
							"/SC:" + sentCnt);
					for (MessageProcessingWroker decoder : messageProcessingWorkers) {
						sb.append("/Q[T-" + decoder.getId() + "]:" + decoder.getQueuedEventCount());
					}
					LOG.info(sb.toString());
				}
			} else {
				Thread.sleep(60);
			}
		} catch (Exception e) {
			LOG.error("Message dispatch error: ", e);
		}
	}

	private synchronized MessageProcessingWroker selectWorker() {
		long which = 0L;
		which = (++pointer) % workers;
		// avoid overflowing
		if(pointer < 0) {
			pointer = 0L;
			which = 0L;
		}
		return messageProcessingWorkers.get((int) which);
	}

	@Override
	public synchronized void stop() {
		super.stop();
		messageProcessingWorkerExecutorService.shutdown();
		// close producers
		for (MessageProcessingWroker decoder : messageProcessingWorkers) {
			decoder.kafkaProducer.close();
		}
	}

	private final class MessageProcessingWroker extends Thread {

		private final BlockingQueue<String> q;
		private final Producer<K, V> kafkaProducer;
		private List<KeyedMessage<K, V>> batchedMessages = Lists.newArrayList();

		public MessageProcessingWroker(int qsize) {
			q = new LinkedBlockingQueue<String>(qsize);
			Properties props = createOriginalProps();
			ProducerConfig config = new ProducerConfig(props);
			kafkaProducer = new Producer<K, V>(config);
		}

		public int getQueuedEventCount() {
			return q.size();
		}
		
		@Override
		public void run() {
			while (true) {
				try {
					// use non-blocking mode
					String eventData = q.poll();
					if (eventData != null) {
						KeyedMessage<K, V> data = rawMessageProcessor.process(topic, eventData);
						++proccessedCnt;
						
						// save to Kafka MQ
						batchedMessages.add(data);
						// check whether a batch reached
						if(batchedMessages.size() >= sendBatchSize) {
							send();
						}
					} else {
						if(!batchedMessages.isEmpty()) {
							send();
						}
						Thread.sleep(200);
					}
				} catch (Exception e) {
					LOG.warn("Message decode error: ", e);
				}
			}
		}
		
		private void send() {
			kafkaProducer.send(batchedMessages);
			sentCnt += batchedMessages.size();
			batchedMessages = Lists.newArrayList();
		}

	}
	
}