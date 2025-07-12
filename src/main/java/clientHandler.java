import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.file.Files;
import java.util.ArrayList;

class ClientHandler extends Thread {
    private Socket clientSocket;
    
    // Static instances to avoid repeated creation
    private static final KafkaKRaftMetadataParser parser = new KafkaKRaftMetadataParser();
    private static final byteArrayManipulation byteTool = new byteArrayManipulation();
    
    // Pre-allocated byte arrays for common operations
    private final byte[] mssgSizeBuffer = new byte[4];
    private final byte[] apiKeyBuffer = new byte[2];
    private final byte[] apiVersionBuffer = new byte[2];
    private final byte[] correlationIdBuffer = new byte[4];
    private final byte[] clientLengthBuffer = new byte[2];
    private final byte[] singleByteBuffer = new byte[1];
    
    // Reusable response list
    private final ArrayList<byte[]> responses = new ArrayList<>();
    
    // Reusable API handler
    private final ApiHandler apiHandler = new ApiHandler();

    public ClientHandler(Socket socket) {
        this.clientSocket = socket;
    }

    public void run() {
        LogFileInfo logfile = new LogFileInfo();
        parser.parseLogSegment("/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log", logfile);
        
        // Use buffered streams for better performance
        try (BufferedInputStream inputStream = new BufferedInputStream(clientSocket.getInputStream(), 8192);
             BufferedOutputStream outputStream = new BufferedOutputStream(clientSocket.getOutputStream(), 8192)) {
            
            while (true) {
                // Clear responses list for reuse
                responses.clear();
                
                // Read message size
                if (readFully(inputStream, mssgSizeBuffer) == -1) {
                    break; // Client closed connection
                }

                int mssg = byteTool.byteArrayToInt(mssgSizeBuffer);
                
                // Read API key
                if (readFully(inputStream, apiKeyBuffer) == -1) break;
                short api = byteTool.byteArrayToShort(apiKeyBuffer);
                
                // Read API version
                if (readFully(inputStream, apiVersionBuffer) == -1) break;
                short version = byteTool.byteArrayToShort(apiVersionBuffer);
                
                // Read correlation ID
                if (readFully(inputStream, correlationIdBuffer) == -1) break;
                int correlation = byteTool.byteArrayToInt(correlationIdBuffer);

                // Add correlation ID to responses (reuse the buffer)
                byte[] correlationIdCopy = new byte[4];
                System.arraycopy(correlationIdBuffer, 0, correlationIdCopy, 0, 4);
                responses.add(correlationIdCopy);
                
                int responseSize;
                
                if (api == 75) {
                    // Handle describe partition request
                    if (readFully(inputStream, clientLengthBuffer) == -1) break;
                    int clientIdLength = byteTool.byteArrayToInt(clientLengthBuffer);
                    
                    byte[] clientId = new byte[clientIdLength];
                    if (readFully(inputStream, clientId) == -1) break;
                    
                    apiHandler.describePartitionHandler(inputStream, mssg, responses, logfile);
                    responseSize = byteArrayManipulation.sizeOfMessage(responses);
                    
                } else if (api == 1) {
                    // Handle fetch request
                    if (readFully(inputStream, clientLengthBuffer) == -1) break;
                    int clientIdLength = byteTool.byteArrayToInt(clientLengthBuffer);
                    
                    byte[] clientId = new byte[clientIdLength];
                    if (readFully(inputStream, clientId) == -1) break;
                    
                    if (readFully(inputStream, singleByteBuffer) == -1) break;
                    
                    apiHandler.fetchRequestHandler(logfile, responses, inputStream);
                    responseSize = byteArrayManipulation.sizeOfMessage(responses);
                    
                } else {
                    // Handle API versions request
                    apiHandler.apiVersionsHandler(inputStream, mssg, version, responses);
                    responseSize = byteArrayManipulation.sizeOfMessage(responses);
                    
                    // Read remaining bytes
                    int remainingBytesCount = mssg - 8;
                    if (remainingBytesCount > 0) {
                        skipBytes(inputStream, remainingBytesCount);
                    }
                }
                
                // Write response
                byte[] responseSizeBytes = byteTool.intToByteArray(responseSize);
                outputStream.write(responseSizeBytes);
                
                // Write all response parts
                for (byte[] response : responses) {
                    outputStream.write(response);
                }
                
                outputStream.flush();
            }
            
        } catch (IOException e) {
            System.out.println("IOException in ClientHandler: " + e.getMessage());
        } finally {
            closeSocket();
        }
    }
    
    /**
     * Reads exactly the specified number of bytes from the input stream
     * Returns -1 if end of stream is reached before all bytes are read
     */
    private int readFully(InputStream inputStream, byte[] buffer) throws IOException {
        int totalBytesRead = 0;
        int bytesToRead = buffer.length;
        
        while (totalBytesRead < bytesToRead) {
            int bytesRead = inputStream.read(buffer, totalBytesRead, bytesToRead - totalBytesRead);
            if (bytesRead == -1) {
                return -1; // End of stream
            }
            totalBytesRead += bytesRead;
        }
        return totalBytesRead;
    }
    
    /**
     * Efficiently skips the specified number of bytes
     */
    private void skipBytes(InputStream inputStream, int bytesToSkip) throws IOException {
        long totalSkipped = 0;
        while (totalSkipped < bytesToSkip) {
            long skipped = inputStream.skip(bytesToSkip - totalSkipped);
            if (skipped <= 0) {
                // skip() didn't work, fall back to reading
                int remaining = (int)(bytesToSkip - totalSkipped);
                byte[] skipBuffer = new byte[Math.min(remaining, 8192)];
                int read = inputStream.read(skipBuffer, 0, Math.min(remaining, skipBuffer.length));
                if (read == -1) {
                    break; // End of stream
                }
                totalSkipped += read;
            } else {
                totalSkipped += skipped;
            }
        }
    }
    
    /**
     * Safely close the client socket
     */
    private void closeSocket() {
        try {
            if (clientSocket != null && !clientSocket.isClosed()) {
                clientSocket.close();
            }
        } catch (IOException e) {
            System.out.println("Error closing socket: " + e.getMessage());
        }
    }
}