/*
 * Copyright 2016 Pigumer Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jp.pigumer;

import com.microsoft.eventhubs.client.*;
import org.apache.qpid.amqp_1_0.client.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

public class MessageReceiver implements Runnable {

	private static final Logger LOG = LoggerFactory.getLogger(MessageReceiver.class);
	
	final EventHubClient client;
	
	final String partitionId;
	
	volatile boolean cancel = false;
	
	public MessageReceiver(EventHubClient client, String partitionId) {
		this.client = client;
		this.partitionId = partitionId;
	}
	
	public void cancel() {
		cancel = true;
	}
	
	@Override
	public void run() {
		EventHubReceiver receiver = null;
		try {
			receiver = client.getConsumerGroup(null).createReceiver(partitionId, new EventHubEnqueueTimeFilter(Instant.now().toEpochMilli()), Constants.DefaultAmqpCredits);
			
			while (!cancel) {
				Message message = receiver.receive(5000);
				EventHubMessage msg = EventHubMessage.parseAmqpMessage(message);
				if (msg != null) {
					LOG.info(msg.getDataAsString());
				}
			}
		} catch (Exception e) {
			LOG.warn("run", e);
		} finally {
			if (null != receiver) {
				receiver.close();
			}
		}
	}

	
}
