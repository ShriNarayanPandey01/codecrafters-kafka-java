import java.io.BufferedReader;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

public class ApiHandler {

    static KafkaKRaftMetadataParser parser = new KafkaKRaftMetadataParser();
    static byteArrayManipulation byteTool = new byteArrayManipulation(); 

    public static int readVarInt(InputStream input) throws IOException {
        int value = 0;
        int position = 0;
        int currentByte;

        while (true) {
            currentByte = input.read();
            if (currentByte == -1) {
                throw new EOFException("Unexpected end of stream while reading VarInt");
            }

            value |= (currentByte & 0x7F) << position;

            if ((currentByte & 0x80) == 0) {  // high bit is not set — last byte
                break;
            }

            position += 7;

            if (position > 28) {  // VarInt shouldn't exceed 5 bytes (7x5=35 bits)
                throw new IOException("VarInt is too long");
            }
        }

        return value;
    }
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
            byte[] topicCountBytes = new byte[1];
            inputStream.read(topicCountBytes);

            int topicCount = byteTool.byteArrayToInt(topicCountBytes);

            ArrayList<byte[]> topicuuidList = new ArrayList<>();
            for (int i = 1; i < topicCount; i++) {
                byte[] topicId = new byte[16]; // UUID
                inputStream.read(topicId);
                topicuuidList.add(topicId);
            }
            responses.add(new byte[]{0});
            responses.add(new byte[]{0,0,0,0});
            responses.add(new byte[]{0,0});
            responses.add(new byte[]{0,0,0,0});

            if(topicuuidList.size()>0) responses.add(new byte[]{(byte)(2)});
            else responses.add(new byte[]{(byte)1});
            for(int j = 0 ; j < topicuuidList.size() ; j++){
                responses.add(topicuuidList.get(j));
                if(logFileInfo.topicNames.containsKey(byteTool.byteArrayToString(topicuuidList.get(j)))){
                    String name = logFileInfo.topicNames.get(byteTool.byteArrayToString(topicuuidList.get(j)));
                    TopicRecord topicRecord = logFileInfo.topics.get(name);
                   
                    // LogFileInfo log = new LogFileInfo();
                    // parser.parseLogSegment(recordPath , log);

                    

                    responses.add(new byte[]{(byte)(2)});
                    responses.add(new byte[]{0,0,0,0}); // partition index
                    responses.add(new byte[]{0,0}); // error code
                    responses.add(new byte[]{0, 0, 0, 0, 0, 0, 0, 0}); // high watermark
                    responses.add(new byte[]{0, 0, 0, 0, 0, 0, 0, 0}); // last stable offset
                    responses.add(new byte[]{0, 0, 0, 0, 0, 0, 0, 0}); // log start offset
                    responses.add(new byte[]{0,0}); // number of aborted transaction
                    responses.add(new byte[]{0, 0}); // preferred_read_replica
                    responses.add(new byte[]{0, 2}); // compact_records_length
                    
                        String recordPath = "/tmp/kraft-combined-logs/"+name+"-0/00000000000000000000.log";
                        File file = new File(recordPath);
                        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
                            long fileLength = raf.length();
                            byte[] offset = new byte[8];
                            raf.read(offset);
                            responses.add(offset);
                            byte[] length = new byte[4];
                            raf.read(length);
                            responses.add(length);
                            byte[] record = new byte[byteTool.byteArrayToInt(length)];
                            raf.read(record);
                            responses.add(record);
                
                        } catch (IOException e) {
                            System.err.println("Failed to parse log segment: " + e.getMessage());
                        }

                    responses.add(new byte[]{0});
                }
                else
                {
                    responses.add(new byte[]{(byte)2});
                    for(int k = 1 ; k < 2 ; k++){
                        responses.add(new byte[]{0,0,0,0}); // partition index
                        responses.add(new byte[]{0,100}); // error code
                        responses.add(new byte[]{0, 0, 0, 0, 0, 0, 0, 0}); // high watermark
                        responses.add(new byte[]{0, 0, 0, 0, 0, 0, 0, 0}); // last stable offset
                        responses.add(new byte[]{0, 0, 0, 0, 0, 0, 0, 0}); // log start offset
                        responses.add(new byte[]{0,0}); // number of aborted transaction
                        responses.add(new byte[]{0, 0}); // preferred_read_replica
                        responses.add(new byte[]{0, 0}); // compact_records_length
                        responses.add(new byte[]{0});
                    }
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
