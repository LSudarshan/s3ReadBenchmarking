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
            readFile(cmd.getOptionValue("inputPath"), cmd.getOptionValue("outputPath"));
        } else if (cmd.getOptionValue("filesystem").equals("s3v2")){
            int numThreads = Integer.parseInt(cmd.getOptionValue("numThreads"));
            MultiThreadedS3Version2Reader s3ReaderV2 = new MultiThreadedS3Version2Reader(cmd.getOptionValue("awsAccessKey"), cmd.getOptionValue("awsSecretKey"), cmd.getOptionValue("inputPath"), cmd.getOptionValue("pageCacheSize"), numThreads);
            s3ReaderV2.read();
        }
        else {
            int numThreads = Integer.parseInt(cmd.getOptionValue("numThreads"));
            MultiThreadedS3Version1Reader s3ReaderV1 = new MultiThreadedS3Version1Reader(cmd.getOptionValue("awsAccessKey"), cmd.getOptionValue("awsSecretKey"), cmd.getOptionValue("inputPath"), cmd.getOptionValue("pageCacheSize"), numThreads);
            s3ReaderV1.read();
        }

    }

    private static Options setupCommandlineOptions() {
        Options options = new Options();
        Option fileSystemoption = new Option("f", "filesystem", true, "File system - ebs | s3v1 | s3v2 ");
        fileSystemoption.setRequired(true);
        options.addOption(fileSystemoption);
        Option inputPathoption = new Option("i", "inputPath", true, "input path - output file path or s3path");
        inputPathoption.setRequired(true);
        options.addOption(inputPathoption);
        Option outputPathoption = new Option("o", "outputPath", true, "output file path");
        outputPathoption.setRequired(false);
        options.addOption(outputPathoption);
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
        return options;
    }


    public static void readFile(String input, String output) throws IOException {
        InputStream is = new BufferedInputStream(new FileInputStream(input), 8192);

        OutputStream baos = new BufferedOutputStream(new FileOutputStream(output), 8192);


        long t3 = System.currentTimeMillis();
        byte[] bytes = is.readAllBytes();
        baos.write(bytes,0,bytes.length - 1);
        long t4 = System.currentTimeMillis();
        System.out.println("Time to read = " + (t4-t3) + "ms");
        is.close();
        baos.close();
    }
}
