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

import com.microsoft.azure.iot.service.sdk.IotHubServiceClientProtocol;
import com.microsoft.azure.iot.service.sdk.ServiceClient;
import com.microsoft.eventhubs.client.ConnectionStringBuilder;
import com.microsoft.eventhubs.client.EventHubClient;
import com.microsoft.eventhubs.client.EventHubException;

import java.io.IOException;

public class Client {

	final String policyName;
	
	final String policyKey;
	
	final String namespace;
	
	final String name;
	
	final String connectionString;
	
	public Client(String policyName, String policyKey, String namespace, String name, String connectionString) {
		this.policyName = policyName;
		this.policyKey = policyKey;
		this.namespace = namespace;
		this.name = name;
		this.connectionString = connectionString;
	}
	
	public EventHubClient createEventHubClient() throws IOException, EventHubException {
		ConnectionStringBuilder builder = new ConnectionStringBuilder(policyName, policyKey, namespace);
		String connectionString = builder.getConnectionString();
		EventHubClient client = EventHubClient.create(connectionString, name);
		
		return client;
	}
	
	public ServiceClient createServiceClient() throws Exception {
		return ServiceClient.createFromConnectionString(connectionString, IotHubServiceClientProtocol.AMQPS);
	}
}
