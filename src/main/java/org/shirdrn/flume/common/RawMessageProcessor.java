package org.shirdrn.flume.common;

import kafka.producer.KeyedMessage;

/**
 * Process the raw message taken from the channel. Generally a raw message
 * may be not human-readable, or not normalization. After messages are processed
 * by the implementation class of this interface, we just hope cleaned and normalized
 * messages were persisted into Kafka MQ.</br>
 * 
 * You can think that {@link RawMessageProcessor} interface orients to business logic
 * developers, and any procession logic can be added to method {{@link #process(String)}.
 * 
 * @author yanjun
 *
 * @param <K>
 * @param <V>
 */
public interface RawMessageProcessor<K, V> {

	KeyedMessage<K, V> process(String topic, String message);
}
