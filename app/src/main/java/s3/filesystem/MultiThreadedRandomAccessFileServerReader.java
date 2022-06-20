package s3.filesystem;

import com.google.common.collect.Lists;

import java.io.*;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class MultiThreadedRandomAccessFileServerReader {

    private final long totalLength;
    private final int recordSize;
    private int numberOfRecordsPerFileServerRequest;
    private final long numberOfRecordsPerThread;
    private String fileName;
    private String fileServerHost;
    private int fileServerPort;
    private int numThreads;

    public MultiThreadedRandomAccessFileServerReader(String fileName, String fileServerHost,
                                                     int fileServerPort, int numThreads,
                                                     long numberOfRecords, String recordSize,
                                                     int numberOfRecordsPerFileServerRequest) throws IOException {
        this.fileName = fileName;
        this.fileServerHost = fileServerHost;
        this.fileServerPort = fileServerPort;
        this.numThreads = numThreads;
        this.recordSize = Integer.parseInt(recordSize);
        this.numberOfRecordsPerFileServerRequest = numberOfRecordsPerFileServerRequest;
        this.numberOfRecordsPerThread = numberOfRecords / numThreads;
        totalLength = getLengthFromFileServer(fileServerHost, fileServerPort, fileName);
    }

    public void read() throws InterruptedException {
        long totalLength = getTotalLength();
        long lengthPerThread = totalLength / numThreads;
        System.out.println("Total length of file: " + totalLength);
        System.out.println("Number of threads: " + numThreads);
        System.out.println("Length per thread: " + lengthPerThread);
        long startOffset = 0;
        long t1 = System.currentTimeMillis();
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < numThreads; i++) {
            long finalStartOffset = startOffset;
            Thread thread = new Thread(() -> {
                try {
                    System.out.println("Starting thread: " + Thread.currentThread().getId() + ", offset: " + finalStartOffset + ", end: " + (finalStartOffset + lengthPerThread));
                    openStreamAndRead(finalStartOffset, lengthPerThread, numberOfRecordsPerThread, recordSize, numberOfRecordsPerFileServerRequest, fileName, fileServerHost, fileServerPort);
                } catch (IOException | InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            });
            startOffset = startOffset + lengthPerThread;
            threads.add(thread);
        }
        for (Thread thread : threads) {
            thread.start();
        }
        for (Thread thread : threads) {
            thread.join();
        }
        long t2 = System.currentTimeMillis();
        System.out.println("total time taken for all threads : " + (t2-t1) + " ms");
    }

    private long getTotalLength() {
        return totalLength;
    }

    private static void openStreamAndRead(long startOffsetForThread,
                                          long endOffsetForThread,
                                          long numberOfRecordsPerThread,
                                          int recordSize,
                                          int numberOfRecordsPerFileServerRequest,
                                          String fileName,
                                          String fileServerHost,
                                          int fileServerPort) throws InterruptedException, ExecutionException, IOException {
        long t1 = System.currentTimeMillis();
        Random random = new Random();
        List<Long> offsets = new ArrayList<>();
        for (int i = 0; i < numberOfRecordsPerThread; i++) {
            offsets.add(random.nextLong(startOffsetForThread, startOffsetForThread + endOffsetForThread - recordSize));
        }
        System.out.println("Thread id: " + Thread.currentThread().getId() + ", number of offsets: " + offsets.size());
        List<List<Long>> partitions = Lists.partition(offsets, numberOfRecordsPerFileServerRequest);
        System.out.println("Thread id: " + Thread.currentThread().getId() + ", number of batches for offsets: " + partitions.size());
        for (List<Long> partition : partitions) {
            String requestForRecords = fileContentsRequest(partition, fileName, recordSize);
            makeFileContentsRequest(requestForRecords, fileServerHost, fileServerPort);
        }
        long t2 = System.currentTimeMillis();
        System.out.println("Thread id: " + Thread.currentThread().getId() + ", total time taken for all requests in a single thread : " + (t2-t1) + " ms");
    }


    private long getLengthFromFileServer(String fileServerHost, int fileServerPort, String fileName) throws IOException {
        Socket socket = new Socket(fileServerHost, fileServerPort);
        OutputStream outputStream = socket.getOutputStream();
        InputStream inputStream = socket.getInputStream();
        StringBuilder stringBuilder = fileLengthRequest(fileName);
        outputStream.write(stringBuilder.toString().getBytes());
        socket.shutdownOutput();

        byte[] buffer = new byte[1024];
        while(true){
            int read = inputStream.read(buffer);
            if(read == -1){
                System.out.println("Received enf of stream");
                break;
            }
            System.out.println("Read: " + read + " bytes");
        }
        socket.close();
        String s = new String(buffer).trim();
        System.out.println("Received length: " + s);
        return Long.parseLong(s);
    }
    private static String fileContentsRequest(List<Long> partition, String fileName, int recordLength) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("FILE_CONTENTS");
        stringBuilder.append("|");
        stringBuilder.append(fileName);
        stringBuilder.append("|");
        for (long offset : partition) {
            stringBuilder.append(offset + "," + recordLength);
            stringBuilder.append("|");
        }
        return stringBuilder.toString();
    }

    private static void makeFileContentsRequest(String requestForRecords, String fileServerHost, int fileServerPort) throws IOException {
        System.out.println("Thread id: " + Thread.currentThread().getId() + ", Making request for a set of records");
        long t3 = System.currentTimeMillis();
        Socket socket = new Socket(fileServerHost, fileServerPort);
        OutputStream outputStream = socket.getOutputStream();
        InputStream inputStream = socket.getInputStream();
        outputStream.write(requestForRecords.getBytes());
        socket.shutdownOutput();
        long totalBytes = 0;
        while(true){
            byte[] buffer = new byte[1024*1024*10];
            int read = inputStream.read(buffer);
            if(read == -1){
                System.out.println("Received enf of stream");
                break;
            }
            totalBytes += read;
        }
        System.out.println("Thread id: " + Thread.currentThread().getId() + ", Total bytes read in request: " + totalBytes);
        long t4 = System.currentTimeMillis();
        System.out.println("Thread id: " + Thread.currentThread().getId() + ", total time taken for a single batched request: " + (t4-t3) + " ms");
        socket.close();
    }

    private StringBuilder fileLengthRequest(String fileName) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("FILE_LENGTH");
        stringBuilder.append("|");
        stringBuilder.append(fileName);
        return stringBuilder;
    }
}
