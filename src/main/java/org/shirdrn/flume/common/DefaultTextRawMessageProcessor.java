package org.shirdrn.flume.common;

import kafka.producer.KeyedMessage;

public class DefaultTextRawMessageProcessor implements RawMessageProcessor<String, String> {

	@Override
	public KeyedMessage<String, String> process(String topic, String message) {
		return new KeyedMessage<String, String>(topic, message);
	}

}
