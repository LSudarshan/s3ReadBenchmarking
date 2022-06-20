package s3.filesystem;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;

public class MultiThreadedRandomAccessEBSReader {

    private final long totalLength;
    private final int recordSize;
    private final long numberOfRecordsPerThread;
    private String input;
    private int numThreads;

    public MultiThreadedRandomAccessEBSReader(String input, int numThreads, long numberOfRecords, String recordSize) throws IOException {
        this.numThreads = numThreads;
        this.recordSize = Integer.parseInt(recordSize);
        this.numberOfRecordsPerThread = numberOfRecords / numThreads;
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
        RandomAccessFile raf = new RandomAccessFile(input, "r");
        long t3 = System.currentTimeMillis();
        byte[] page = new byte[this.recordSize];
        Random random = new Random();
        long numberOfRecordsRead = 0;
        while(true){
            long currentRandomAccessRecordOffset = random.nextLong(startPositionForThread, startPositionForThread + lengthPerThread - recordSize);
            long t1 = System.currentTimeMillis();

            raf.seek(currentRandomAccessRecordOffset);
            try {
                int read = raf.read(page);
                long t2 = System.currentTimeMillis();
                System.out.println("Thread id: " + Thread.currentThread().getId() + ", Time to read " + read + " bytes = " + (t2-t1) + "ms" + ", with number of records read: " + numberOfRecordsRead);
            } catch (EOFException e) {
                System.out.println("Trying to read from offset: " + currentRandomAccessRecordOffset + " with size: " + recordSize + ", but got EOFException: " + e.getMessage());
            }
            numberOfRecordsRead++;
            if (numberOfRecordsRead >= this.numberOfRecordsPerThread){
                break;
            }
        }
        long t4 = System.currentTimeMillis();
        System.out.println("Thread id: " + Thread.currentThread().getId() + ", Total Time to read = " + (t4-t3) + "ms");
    }
}
