package com.example4;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;

import org.apache.pulsar.client.api.PulsarClientException;

import com.scylladb.cdc.lib.CDCConsumer;
import com.scylladb.cdc.lib.RawChangeConsumerProvider;
import com.scylladb.cdc.model.TableName;
import com.scylladb.cdc.model.worker.ChangeSchema;
import com.scylladb.cdc.model.worker.RawChange;
import com.scylladb.cdc.model.worker.RawChangeConsumer;
import com.scylladb.cdc.model.worker.cql.Cell;



import sun.misc.Signal;

public class Main 
{
    public static void main( String[] args ) throws PulsarClientException {

        // Get Configuring values
        GetConfigValue cpvalue = new GetConfigValue();
        String cdccontactpoint = cpvalue.GetCDCContactPoint();
        String cdckeyspace = cpvalue.GetCDCKeyspace();
        String cdctable = cpvalue.GetCDCTable();

        String pulsarserviceurl = cpvalue.GetPulsarServiceURL();
        String topicname = cpvalue.GetTopicName();


        PulsarProducer producer = new PulsarProducer();
        producer.CreateSender(pulsarserviceurl, topicname);

        
        @SuppressWarnings("deprecation")
        RawChangeConsumerProvider changeConsumerProvider = threadId -> {
            
            RawChangeConsumer changeConsumer = change -> {

                //printChange(change);
                String cdcchange = printChange(change);
                //System.out.println("my record to send : " + cdcchange);
                try {
                    producer.Send(cdcchange);
                } catch (PulsarClientException e) {
                    e.printStackTrace();
                }

                return CompletableFuture.completedFuture(null);
            };
            return changeConsumer;
        };
 

        try (CDCConsumer consumer = CDCConsumer.builder()
                .addContactPoint(cdccontactpoint)
                .addTable(new TableName(cdckeyspace, cdctable))
                .withConsumerProvider(changeConsumerProvider)
                .withWorkersCount(1)
                .build()) {
            

            consumer.start();
            

            CountDownLatch terminationLatch = new CountDownLatch(1);
            Signal.handle(new Signal("INT"), signal -> terminationLatch.countDown());
            terminationLatch.await();
        } catch (InterruptedException ex) {
            System.err.println("Exception occurred while running the Printer: "
                + ex.getMessage());
        }

        // The CDCConsumer is gracefully stopped after try-with-resources.
    }

    private synchronized static String printChange(RawChange change) {
        StringBuilder output = new StringBuilder();
        
        ChangeSchema changeSchema = change.getSchema();
        List<ChangeSchema.ColumnDefinition> nonCdcColumnDefinitions = changeSchema.getNonCdcColumnDefinitions();
        int columnIndex = 0; // For pretty printing.
    
        for (ChangeSchema.ColumnDefinition columnDefinition : nonCdcColumnDefinitions) {
            String columnName = columnDefinition.getColumnName();
            Cell cell = change.getCell(columnName);
            Object cellValue = cell.getAsObject();
            
            output.append(prettyPrintCell(columnName, cellValue, ++columnIndex));
        }
        
        String result = output.toString();
        System.out.println(result); // Still print to console if needed
        return result;
    }
    
    private static String prettyPrintCell(String columnName, Object cellValue, int columnIndex) {
        if (columnIndex == 1 && columnName.equals("coordinate")) {
            return prettyPrintField(columnName, Objects.toString(cellValue));
        } else if (columnIndex == 2 && columnName.equals("sensor_id")) {
            return prettyPrintField(columnName, Objects.toString(cellValue));
        } else if (columnIndex == 3 && columnName.equals("status")) {
            return prettyPrintField(columnName, Objects.toString(cellValue));
        }
        return "";
    }
    
    private static String prettyPrintField(String fieldName, Object fieldValue) {
        if (fieldName.equals("coordinate")){
            return "{\"" + fieldName + "\":" + fieldValue + ",";
        } else if (fieldName.equals("sensor_id")){
            return "\"" + fieldName + "\":\"" + fieldValue + "\",";
        } else if (fieldName.equals("status")){
            return "\"" + fieldName + "\":\"" + fieldValue + "\"}";
        } else{
            return fieldName + fieldValue;
        }
    }

    /*
    private synchronized static void printChange(RawChange change) {
        
        ChangeSchema changeSchema = change.getSchema();

        List<ChangeSchema.ColumnDefinition> nonCdcColumnDefinitions = changeSchema.getNonCdcColumnDefinitions();
        int columnIndex = 0; // For pretty printing.

        for (ChangeSchema.ColumnDefinition columnDefinition : nonCdcColumnDefinitions) {
            String columnName = columnDefinition.getColumnName();
            
            Cell cell = change.getCell(columnName);

            Object cellValue = cell.getAsObject();
            
            prettyPrintCell(columnName, cellValue, ++columnIndex);
        }
        prettyPrintEnd();
    }


    private static void prettyPrintCell(String columnName, Object cellValue, int columnIndex) {

        if (columnIndex == 1 && columnName.equals("coordinate")) {
            prettyPrintField(columnName, Objects.toString(cellValue));
        } else if (columnIndex == 2 && columnName.equals("sensor_id")) {
            prettyPrintField(columnName, Objects.toString(cellValue));
        } else if (columnIndex == 3 && columnName.equals("status")) {
            prettyPrintField(columnName, Objects.toString(cellValue));
        }
    }

    
    private static void prettyPrintEnd() {
        System.out.println();
    }
    
    private static void prettyPrintField(String fieldName, Object fieldValue) {

        if (fieldName.equals("coordinate")){
            System.out.print("{\"" + fieldName + "\":" + fieldValue + ",");
        } else if (fieldName.equals("sensor_id")){
            System.out.print("\"" + fieldName + "\":\"" + fieldValue + "\",");
        } else if (fieldName.equals("status")){
            System.out.println("\"" + fieldName + "\":\"" + fieldValue + "\"}");
        } else{
            System.out.print(fieldName);
            System.out.println(fieldValue);
        }
    }


    private static String getChangeString(RawChange change) {
        return change.toString();
    }
    */
}
