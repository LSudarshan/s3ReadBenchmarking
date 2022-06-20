package s3.filesystem;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.fs.s3a.S3AFileSystem;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class MultiThreadedS3Version1SequentialAccessReader {

    private final S3AFileSystem s3AFileSystem;
    private final long totalLength;
    private int PAGE_CACHE_SIZE = 1024 * 1024 * 10;
    private String input;
    private int numThreads;

    public MultiThreadedS3Version1SequentialAccessReader(String accessKey, String secretKey, String input, String pageCacheSize, int numThreads) throws IOException {
        this.numThreads = numThreads;
        s3AFileSystem = new S3AFileSystem();
        if(!StringUtils.isBlank(pageCacheSize)){
            PAGE_CACHE_SIZE = Integer.parseInt(pageCacheSize);
        }
        Configuration conf = new Configuration();
        conf.set("fs.s3a.access.key", accessKey);
        conf.set("fs.s3a.secret.key", secretKey);
        String rootPath = input.substring(0, input.lastIndexOf("/"));
        s3AFileSystem.initialize(URI.create(rootPath), conf);
        s3AFileSystem.setWorkingDirectory(new Path("/"));
        this.input = input.substring(input.lastIndexOf("/") + 1);

        input = input.replace("s3a://", "");
        String bucket = input.substring(0, input.lastIndexOf("/"));
        String file = input.substring(input.lastIndexOf("/") + 1);
        S3InputStreamV2 s3InputStreamV2 = new S3InputStreamV2(bucket, file, new byte[PAGE_CACHE_SIZE], accessKey, secretKey, 0);
        totalLength = s3InputStreamV2.length();
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
        FSDataInputStream inputStream = s3AFileSystem.openFile(new Path(input)).build().get();
        long t3 = System.currentTimeMillis();
        byte[] page = new byte[PAGE_CACHE_SIZE];
        long totalBytesRead = 0;
        while(true){
            long t1 = System.currentTimeMillis();
            ((Seekable) inputStream).seek(currentPosition);
            long t2 = System.currentTimeMillis();
            System.out.println("Time to seek to position: " + currentPosition + " = " + (t2-t1) + "ms");
            t1 = System.currentTimeMillis();
            int read = ((InputStream) inputStream).read(page, 0, PAGE_CACHE_SIZE);
            totalBytesRead += read;
            t2 = System.currentTimeMillis();
            System.out.println("Time to read " + read + " bytes = " + (t2-t1) + "ms");
            if (totalBytesRead >= lengthPerThread){
                break;
            }
            if(read == -1){
                break;
            }
            currentPosition += read;
        }
        long t4 = System.currentTimeMillis();
        System.out.println("Total Time to read = " + (t4-t3) + "ms");
    }
}
