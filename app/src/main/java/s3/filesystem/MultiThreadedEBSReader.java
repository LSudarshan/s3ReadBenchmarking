package s3.filesystem;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.fs.s3a.S3AFileSystem;

import java.io.*;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class MultiThreadedEBSReader {

    private final long totalLength;
    private int PAGE_CACHE_SIZE = 1024 * 1024 * 10;
    private String input;
    private int numThreads;

    public MultiThreadedEBSReader(String input, String pageCacheSize, int numThreads) throws IOException {
        this.numThreads = numThreads;
        if(!StringUtils.isBlank(pageCacheSize)){
            PAGE_CACHE_SIZE = Integer.parseInt(pageCacheSize);
        }
        File file = new File(input);
        totalLength = file.length();
        this.input = input;
    }

    public void read() throws InterruptedException {
        long totalLength = getTotalLength();
        long lengthPerThread = totalLength / numThreads;
        System.out.println("Total length of file: " + totalLength);
        System.out.println("Number of threads: " + numThreads);
        System.out.println("Length per thread: " + lengthPerThread);
        long startOffset = 0;
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < numThreads; i++) {
            long finalStartOffset = startOffset;
            Thread thread = new Thread(() -> {
                try {
                    System.out.println("Starting thread: " + Thread.currentThread().getId() + ", offset: " + finalStartOffset + ", end: " + (finalStartOffset + lengthPerThread));
                    openStreamAndRead(finalStartOffset, lengthPerThread);
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
    }

    private long getTotalLength() {
        return totalLength;
    }

    private void openStreamAndRead(long currentPosition, long lengthPerThread) throws InterruptedException, ExecutionException, IOException {
//        FileInputStream inputStream = new FileInputStream(input);
//        byte[] buffer = new byte[PAGE_CACHE_SIZE];
//        long t1 = System.currentTimeMillis();
//        int b;
//        int length=0;
//        int totalBytesRead = 0;
//        while ((b = s3InputStreamV2.read()) != -1) {
//            buffer[length++] = (byte) b;
//            totalBytesRead++;
//            if(totalBytesRead >= totalLengthToBeRead){
//                System.out.println("Finished reading in thread id: " + Thread.currentThread().getId());
//                break;
//            }
//            if (length >= pageCacheSize){
//                System.out.println("Finished reading page of size " + pageCacheSize + " in thread id: " + Thread.currentThread().getId());
//                length = 0;
//            }
//        }
//        System.out.println("Total bytes read: " + totalBytesRead + " in Thread: " + Thread.currentThread().getId());
//        long t2 = System.currentTimeMillis();
//        System.out.println("Time to read " + totalBytesRead + " bytes = " + (t2-t1) + "ms" + " in Thread: " + Thread.currentThread().getId());
    }
}
