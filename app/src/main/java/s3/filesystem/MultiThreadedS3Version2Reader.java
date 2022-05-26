package s3.filesystem;

import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MultiThreadedS3Version2Reader {

    private final String bucket;
    private final String file;
    private int PAGE_CACHE_SIZE = 1024 * 1024 * 10;
    private String accessKey;
    private String secretKey;
    private int numThreads;

    public MultiThreadedS3Version2Reader(String accessKey, String secretKey, String input, String pageCacheSize, int numThreads) {
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.numThreads = numThreads;
        if(!StringUtils.isBlank(pageCacheSize)) {
            PAGE_CACHE_SIZE = Integer.parseInt(pageCacheSize);
        }
        input = input.replace("s3a://", "");
        this.bucket = input.substring(0, input.lastIndexOf("/"));
        this.file = input.substring(input.lastIndexOf("/") + 1);
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
                    S3InputStreamV2 s3Stream = new S3InputStreamV2(bucket, file, new byte[PAGE_CACHE_SIZE], accessKey, secretKey, finalStartOffset);
                    openStreamAndRead(PAGE_CACHE_SIZE, s3Stream, lengthPerThread);
                } catch (IOException e) {
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
        S3InputStreamV2 s3InputStreamV2 = new S3InputStreamV2(bucket, file, new byte[PAGE_CACHE_SIZE], accessKey, secretKey, 0);
        long totalLength = s3InputStreamV2.length();
        return totalLength;
    }

    private void openStreamAndRead(int pageCacheSize, S3InputStreamV2 s3InputStreamV2, long totalLengthToBeRead) throws IOException {
        byte[] buffer = new byte[pageCacheSize];
        long t1 = System.currentTimeMillis();
        int b;
        int length=0;
        int totalBytesRead = 0;
        while ((b = s3InputStreamV2.read()) != -1) {
            buffer[length++] = (byte) b;
            totalBytesRead++;
            if(totalBytesRead >= totalLengthToBeRead){
                System.out.println("Finished reading in thread id: " + Thread.currentThread().getId());
                break;
            }
            if (length >= pageCacheSize){
                System.out.println("Finished reading page of size " + pageCacheSize + " in thread id: " + Thread.currentThread().getId());
                length = 0;
            }
        }
        System.out.println("Total bytes read: " + totalBytesRead + " in Thread: " + Thread.currentThread().getId());
        long t2 = System.currentTimeMillis();
        System.out.println("Time to read " + totalBytesRead + " bytes = " + (t2-t1) + "ms" + " in Thread: " + Thread.currentThread().getId());
    }
}
