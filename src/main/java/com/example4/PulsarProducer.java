package com.example4;

import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.PulsarClientException;


public class PulsarProducer{

        private Producer<PulsarJsonSchema> producer = null;
        private PulsarClient client = null; 

        public void CreateSender (String serviceUrl, String topicName) throws PulsarClientException {

            // Step 1: Create a Pulsar client
            client = PulsarClient.builder()
                .serviceUrl(serviceUrl)
                .build();

            // Step 2: Create a producer
            producer = client.newProducer(Schema.JSON(PulsarJsonSchema.class))
                    .topic(topicName)
                    .create();
        }

        public void Send (String message) throws PulsarClientException {

            // Create an instance of JSON class
            PulsarJsonSchema myJson = new PulsarJsonSchema();
            JsonConvert jsvalue = new JsonConvert();
            myJson.setSensorID(jsvalue.getSensorIdValue(message));
            myJson.setCoordinate(jsvalue.getCoordinateValue(message));
            myJson.setStatus(jsvalue.getStatusValue(message));
            

            // Send the message
            producer.newMessage().value(myJson).send();
        }

        public void CloseSender() throws PulsarClientException {
            if (producer != null) {
                producer.close();
            }
            if (client != null) {
                client.close();
            }
        }
}