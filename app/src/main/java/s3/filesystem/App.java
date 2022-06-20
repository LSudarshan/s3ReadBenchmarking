package s3.filesystem;

import org.apache.commons.cli.*;

import java.io.*;
import java.util.concurrent.ExecutionException;

public class App {
    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = null;
        Options options = setupCommandlineOptions();
        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            new HelpFormatter().printHelp("file.writing.App", options);
            System.exit(1);
        }

        if (cmd.getOptionValue("filesystem").equals("ebs")){
            int numThreads = Integer.parseInt(cmd.getOptionValue("numThreads"));
            if (cmd.getOptionValue("accessType", "Sequential").equals("Sequential")) {
                MultiThreadedSequentialAccessEBSReader reader = new MultiThreadedSequentialAccessEBSReader(cmd.getOptionValue("inputPath"), numThreads, cmd.getOptionValue("pageCacheSize"));
                reader.read();
            } else {
                MultiThreadedRandomAccessEBSReader reader = new MultiThreadedRandomAccessEBSReader(cmd.getOptionValue("inputPath"), numThreads, Long.parseLong(cmd.getOptionValue("numberOfRecords")), cmd.getOptionValue("recordSize"));
                reader.read();
            }
        } else if (cmd.getOptionValue("filesystem").equals("s3v2")){
            int numThreads = Integer.parseInt(cmd.getOptionValue("numThreads"));
            MultiThreadedS3Version2SequentialAccessReader s3ReaderV2 = new MultiThreadedS3Version2SequentialAccessReader(cmd.getOptionValue("awsAccessKey"), cmd.getOptionValue("awsSecretKey"), cmd.getOptionValue("inputPath"), cmd.getOptionValue("pageCacheSize"), numThreads);
            s3ReaderV2.read();
        }
        else if (cmd.getOptionValue("filesystem").equals("s3v1")){
            int numThreads = Integer.parseInt(cmd.getOptionValue("numThreads"));
            if (cmd.getOptionValue("accessType", "Sequential").equals("Sequential")) {
                MultiThreadedS3Version1SequentialAccessReader s3ReaderV1 = new MultiThreadedS3Version1SequentialAccessReader(cmd.getOptionValue("awsAccessKey"), cmd.getOptionValue("awsSecretKey"), cmd.getOptionValue("inputPath"), cmd.getOptionValue("pageCacheSize"), numThreads);
                s3ReaderV1.read();
            } else {
                MultiThreadedS3Version1RandomAccessReader reader = new MultiThreadedS3Version1RandomAccessReader(cmd.getOptionValue("awsAccessKey"), cmd.getOptionValue("awsSecretKey"), cmd.getOptionValue("inputPath"), numThreads, Long.parseLong(cmd.getOptionValue("numberOfRecords")), cmd.getOptionValue("recordSize"));
                reader.read();
            }
        }

    }

    private static Options setupCommandlineOptions() {
        Options options = new Options();
        Option fileSystemoption = new Option("f", "filesystem", true, "File system - s3v1 | s3v2 ");
        fileSystemoption.setRequired(true);
        options.addOption(fileSystemoption);
        Option inputPathoption = new Option("i", "inputPath", true, "input path - s3path - e.g. s3a://bucket/file");
        inputPathoption.setRequired(true);
        options.addOption(inputPathoption);
        Option accessKey = new Option("a", "awsAccessKey", true, "AWS access key");
        accessKey.setRequired(false);
        options.addOption(accessKey);
        Option secretKey = new Option("s", "awsSecretKey", true, "AWS secret key");
        secretKey.setRequired(false);
        options.addOption(secretKey);
        Option pageCacheSize = new Option("p", "pageCacheSize", true, "Page cache size");
        pageCacheSize.setRequired(false);
        options.addOption(pageCacheSize);
        Option numThreads = new Option("t", "numThreads", true, "Number of threads");
        numThreads.setRequired(false);
        options.addOption(numThreads);
        Option accessType = new Option("accessType", "accessType", true, "Access type = sequential | randomAccess");
        accessType.setRequired(false);
        options.addOption(accessType);
        Option numberOfRecords = new Option("numberOfRecords", "numberOfRecords", true, "Number of records to be read for random access type");
        numberOfRecords.setRequired(false);
        options.addOption(accessType);
        Option recordSize = new Option("recordSize", "recordSize", true, "record Size for random access type");
        recordSize.setRequired(false);
        options.addOption(recordSize);
        return options;
    }


}
