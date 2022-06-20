package s3.filesystem;

import org.apache.commons.lang3.StringUtils;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;

public class MultiThreadedSequentialAccessEBSReader {

    private int pageCacheSize = 1024 * 1024 * 10;
    private final long totalLength;
    private String input;
    private int numThreads;

    public MultiThreadedSequentialAccessEBSReader(String input, int numThreads, String pageCacheSizeStr) throws IOException {
        this.numThreads = numThreads;
        if(!StringUtils.isBlank(pageCacheSizeStr)){
            pageCacheSize = Integer.parseInt(pageCacheSizeStr);
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

    private void openStreamAndRead(long startPositionForThread, long lengthPerThread) throws InterruptedException, ExecutionException, IOException {
        byte[] buffer = new byte[pageCacheSize];
        BufferedInputStream bufferedInputStream = new BufferedInputStream(new FileInputStream(input), pageCacheSize);
        long t1 = System.currentTimeMillis();
        long totalBytesRead = 0;
        while (true) {
            long t3 = System.currentTimeMillis();
            int read = bufferedInputStream.read(buffer, 0, pageCacheSize);
            long t4 = System.currentTimeMillis();
            System.out.println("bytes read: " + read + " in : " + (t4-t3) + " ms, in thread id: " + Thread.currentThread().getId());
            if (read == -1){
                System.out.println("Reached end of stream, total bytes read: " + totalBytesRead + " in thread id: " + Thread.currentThread().getId());
                break;
            }

            totalBytesRead+= read;

            if(totalBytesRead >= lengthPerThread){
                System.out.println("Finished reading in thread id: " + Thread.currentThread().getId());
                break;
            }

        }
        System.out.println("Total bytes read: " + totalBytesRead + " in Thread: " + Thread.currentThread().getId());
        long t2 = System.currentTimeMillis();
        System.out.println("Time to read " + totalBytesRead + " bytes = " + (t2-t1) + "ms" + " in Thread: " + Thread.currentThread().getId());
    }
}
