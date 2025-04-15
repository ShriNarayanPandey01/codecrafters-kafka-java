import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

public class ApiHandler {

    static KafkaKRaftMetadataParser parser = new KafkaKRaftMetadataParser();
    static byteArrayManipulation byteTool = new byteArrayManipulation(); 

    public static void fetchRequestHandler(LogFileInfo logFileInfo , ArrayList<byte[]> responses , InputStream inputStream ){
        try{        
            byte[] maxWaitTime = new byte[4];
            inputStream.read(maxWaitTime);

            byte[] minBytes = new byte[4];
            inputStream.read(minBytes);

            byte[] maxBytes = new byte[4];
            inputStream.read(maxBytes);

            byte[] isolationLevel = new byte[1];
            inputStream.read(isolationLevel);

            byte[] sessionId = new byte[4];
            inputStream.read(sessionId);

            byte[] sessionEpoch = new byte[4];
            inputStream.read(sessionEpoch);

            // topics array
            byte[] topicCountBytes = new byte[4];
            inputStream.read(topicCountBytes);
            int topicCount = byteTool.byteArrayToInt(topicCountBytes);
            System.out.println("got here");
            System.out.println(topicCount);
            for (int i = 0; i < topicCount; i++) {
                byte[] topicId = new byte[16]; // UUID
                inputStream.read(topicId);

                byte[] partitionCountBytes = new byte[4];
                inputStream.read(partitionCountBytes);
                int partitionCount = byteTool.byteArrayToInt(partitionCountBytes);

                for (int j = 0; j < partitionCount; j++) {
                    byte[] partition = new byte[4];
                    inputStream.read(partition);

                    byte[] currentLeaderEpoch = new byte[4];
                    inputStream.read(currentLeaderEpoch);

                    byte[] fetchOffset = new byte[8];
                    inputStream.read(fetchOffset);

                    byte[] lastFetchedEpoch = new byte[4];
                    inputStream.read(lastFetchedEpoch);

                    byte[] logStartOffset = new byte[8];
                    inputStream.read(logStartOffset);

                    byte[] partitionMaxBytes = new byte[4];
                    inputStream.read(partitionMaxBytes);

                    // tagged fields
                    int taggedSize = readVarInt(inputStream);
                    inputStream.skip(taggedSize);  // skip the tagged fields
                }

                // topic tagged fields
                int topicTaggedSize = readVarInt(inputStream);
                inputStream.skip(topicTaggedSize);
            }

            // forgotten topics
            byte[] forgottenTopicCountBytes = new byte[4];
            inputStream.read(forgottenTopicCountBytes);
            int forgottenTopicCount = byteTool.byteArrayToInt(forgottenTopicCountBytes);

            for (int i = 0; i < forgottenTopicCount; i++) {
                byte[] topicId = new byte[16];
                inputStream.read(topicId);

                byte[] partitionCountBytes = new byte[4];
                inputStream.read(partitionCountBytes);
                int partitionCount = byteTool.byteArrayToInt(partitionCountBytes);

                for (int j = 0; j < partitionCount; j++) {
                    byte[] partition = new byte[4];
                    inputStream.read(partition);
                }

                int taggedSize = readVarInt(inputStream);
                inputStream.skip(taggedSize);
            }

            // rack_id: compact string
            int rackIdLength = readVarInt(inputStream) - 1;
            byte[] rackId = new byte[rackIdLength];
            inputStream.read(rackId);

            // final tagged fields
            int finalTaggedSize = readVarInt(inputStream);
            inputStream.skip(finalTaggedSize);
            responses.add(new byte[]{0});
            responses.add(new byte[]{0,0,0,0});
            responses.add(new byte[]{0,0});
            responses.add(new byte[]{0,0,0,0});
            responses.add(new byte[]{(byte)(topicList.size()+1)});

            
            for(int i = 0 ; i < topicList.size() ; i++){
                String topic = new String(Arrays.copyOfRange(topicList.get(i),0,3));
                TopicRecord topicRecord = logFileInfo.topics.get(topic);
                responses.add(topicRecord.topicUUID);
                responses.add(new byte[]{0,(byte)(topicRecord.partitions.size()+1)});
                for(int j = 0 ; j < topicRecord.partitions.size() ; j++){
                    PartitionRecord partitionRecord = topicRecord.partitions.get(j);
                    responses.add(partitionRecord.partitionID);
                    responses.add(new byte[]{0,0});
                    responses.add(new byte[]{0,0,0,0,0,0,0,0});
                    responses.add(new byte[]{0,0,0,0,0,0,0,0});
                    responses.add(new byte[]{0,0,0,0,0,0,0,0});
                    responses.add(new byte[]{0});

                }
                responses.add(new byte[]{0});
            }
            responses.add(new byte[]{0});
            
        }
        catch(IOException e) {
            e.printStackTrace();
        }
    }
    public static void describePartitionAPI(ArrayList<byte[]> responses , byte[] errorCode , TopicRecord topicRecord) {
        if(topicRecord != null){
            responses.add(topicRecord.nameLength); // topic length
            responses.add(topicRecord.nameA); // topicName
            responses.add(topicRecord.topicUUID); // topic id
        }
        responses.add(new byte[]{0x00}); // is internal

        byte[] partitionLength ;
        if(topicRecord != null) partitionLength = new byte[]{(byte)(topicRecord.partitions.size() + 1)};
        else partitionLength = new byte[]{(byte)1};
        responses.add(partitionLength); // array length 


        for(int i = 1 ; i < byteTool.byteArrayToInt(partitionLength); i++){
            if(errorCode[1] == 3){   
                responses.add(new byte[] {0, 0, 0, 0, 1, 1, 0, 1, 1, 1, 1, 1, 1, 0, 0, 0});// topic authorization operation
            } 
            else{
                addPartitionArray(responses , partitionLength ,errorCode,i);
            }
        }
        responses.add(new byte[]{0x00,0x00,0x0d,(byte)248}); // tag buffer
        responses.add(new byte[]{(byte)0}); // tag buffer
   
    }

    public static void addPartitionArray(ArrayList<byte[]> responses , byte[] partitionIndex , byte[] errorCode , int ind) {
        responses.add(errorCode); // error code
        responses.add(new byte[]{0,0,0,(byte)(ind-1)}); //partition index
        responses.add(new byte[]{0,0,0,(byte)(1)}); // leader id
        responses.add(new byte[]{0,0,0,0}); // leader epoch
        responses.add(new byte[]{(byte)2}); // replica array length
        responses.add(new byte[]{0,0,0,(byte)1}); // replica array content
        responses.add(new byte[]{(byte)2}); // ISR length
        responses.add(new byte[]{0,0,0,(byte)1}); // ISR ARRAY content
        responses.add(new byte[]{(byte)1}); // eligible leader replica
        responses.add(new byte[]{(byte)1}); // last known ELR
        responses.add(new byte[]{(byte)1}); // offline replica
        responses.add(new byte[]{(byte)0}); // buffer
    } 
    public static void apiVersionsHandler(InputStream inputStream, int mssg ,int version ,ArrayList<byte[]> responses){
            if(version < 0 || version >4){
                responses.add(new byte[]{0,35}); // error code
            }
            else{
                responses.add(new byte[]{0,0}); //error code
            }
            responses.add(new byte[]{4});
            responses.add(new byte[]{0,18}); //api key
            responses.add(new byte[]{0,0}); // min  version 
            responses.add(new byte[]{0,4}); // max version
            responses.add(new byte[]{(byte)0}); // null
            responses.add(new byte[]{0,1}); // api key
            responses.add(new byte[]{0,4}); // min  version 
            responses.add(new byte[]{0,16}); // max version
            responses.add(new byte[]{(byte)0}); //null
            responses.add(new byte[]{0,75}); // api key
            responses.add(new byte[]{0,0}); // min  version 
            responses.add(new byte[]{0,0}); // max version
            responses.add(new byte[]{(byte)0}); //null
            responses.add(new byte[]{0, 0, 0, 0}); // throttle
            responses.add(new byte[]{(byte)0});  //null
    }
    public static void searchTopicId(byte[] topicName , byte[] topicLength){
        try{
            FileInputStream fstream = new FileInputStream("/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log");
            BufferedReader br = new BufferedReader(new InputStreamReader(fstream));
            String strLine;
            while ((strLine = br.readLine()) != null)   {
                System.out.println (strLine);
            }
            System.out.println("triggerd the function");
            fstream.close();
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
        }
    }
    public static void describePartitionHandler(InputStream inputStream, int mssg ,ArrayList<byte[]> responses , LogFileInfo logfile){
        try
        {   
            ArrayList<byte[]> topicNameList = new ArrayList<>();
            ArrayList<byte[]> topicNameLengthList = new ArrayList<>();
            byte[] buffer = new byte[1];
            inputStream.read(buffer);
            byte[] arrayLength = new byte[1];
            inputStream.read(arrayLength);
            for(int i = 1 ; i<byteTool.byteArrayToInt(arrayLength) ; i++){
                byte[] topicNameLength = new byte[1];
                inputStream.read(topicNameLength);
                byte[] topicName = new byte[byteTool.byteArrayToInt(topicNameLength)-1];
                inputStream.read(topicName);
                inputStream.read(buffer);
                topicNameList.add(topicName);
                topicNameLengthList.add(topicNameLength);
            }
            byte[] responsePartitionLimit = new byte[4];
            inputStream.read(responsePartitionLimit);
            byte[] cursor = new byte[1];
            inputStream.read(cursor);

            // LogFileInfo logfile = new LogFileInfo();
           
            // parser.parseLogSegment("/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log" , logfile);
            // parser.parsePartitionMetadata("/tmp/kraft-combined-logs/__cluster_metadata-0/partition.metadata");
            // // parser.partitioncount();
            

            for(String key : logfile.topics.keySet()){
                TopicRecord topicRecord = logfile.topics.get(key);
                System.out.println("topic name = "+topicRecord.name);
                System.out.println(topicRecord.partitions.size());
                byteTool.printByteArray(topicRecord.topicUUID);
            }
            int totalNumOfTopics = logfile.topics.size();
            responses.add(new byte[]{(byte)0}); // tag buffer
            responses.add(new byte[]{0,0,0,0}); // throttle
            responses.add(new byte[]{(byte)(topicNameList.size()+1)}); // ArrayLength 
            
            for(int i = 0 ; i < topicNameList.size() ; i++){
                byte[] errorCode;
                byte[] topicName = topicNameList.get(i);
                byte[] topicNameLength =  topicNameLengthList.get(i);
                String TOPIC = new String(Arrays.copyOfRange(topicName, 0, 3));
                if(!logfile.topics.containsKey(TOPIC.substring(0,3))){
                    errorCode = new byte[]{0,3};
                    responses.add(errorCode); // error code
                    responses.add(topicNameLength); // topic length
                    responses.add(topicName); // topicName
                    responses.add(new byte[16]);//topicUUID
                    describePartitionAPI(responses, errorCode, null);
                }
                else{
                    errorCode = new byte[]{0,0};
                    TopicRecord topicRecord = logfile.topics.get(TOPIC);
                    responses.add(errorCode); // error code
                    describePartitionAPI(responses,errorCode ,topicRecord);
                    
                }
            }
            responses.add(new byte[]{(byte)0xff}); // next cursor
            responses.add(new byte[]{(byte)0}); // tag buffer
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }
}
