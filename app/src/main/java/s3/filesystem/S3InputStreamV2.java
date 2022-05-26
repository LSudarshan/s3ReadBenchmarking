package s3.filesystem;


import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;

import java.io.IOException;
import java.io.InputStream;

public class S3InputStreamV2 extends InputStream {
    private class LazyHolder {
        String appID;
        String secretKey;
        Region region = Region.AWS_GLOBAL;
        public S3Client S3 = null;

        public void connect() {
            AwsBasicCredentials awsCreds = AwsBasicCredentials.create(appID, secretKey);
            S3 =  S3Client.builder().region(region).credentialsProvider(StaticCredentialsProvider.create(awsCreds))
                    .build();
        }

        private HeadObjectResponse getHead(String keyName, String bucketName) {
            HeadObjectRequest objectRequest = HeadObjectRequest.builder().key(keyName).bucket(bucketName).build();

            HeadObjectResponse objectHead = S3.headObject(objectRequest);
            return objectHead;
        }

    }

    private LazyHolder lazyHolder = new LazyHolder();

    private final String bucket;
    private final String file;
    private final byte[] buffer;
    private long lastByteOffset;

    private long offset;
    private int next = 0;
    private int length = 0;

    public S3InputStreamV2(final String bucket, final String file, final byte[] buffer, String appID, String secret, long offset) {
        this.bucket = bucket;
        this.file = file;
        this.buffer = buffer;
        this.offset = offset;
        lazyHolder.appID = appID;
        lazyHolder.secretKey = secret;
        lazyHolder.connect();
        this.lastByteOffset = getLastByteOffset(bucket, file);
    }

    private Long getLastByteOffset(String bucket, String file) {
        return lazyHolder.getHead(file, bucket).contentLength();
    }

    @Override
    public int read() throws IOException {
        if (next >= length || (next == buffer.length && length == buffer.length)) {
            fill();

            if (length <= 0) {
                return -1;
            }

            next = 0;
        }

        if (next >= length) {
            return -1;
        }

        return buffer[this.next++] & 0xFF;
    }

    public void fill() throws IOException {
        System.out.println("Filling the buffer in S3inputStreamV2");
        if (offset >= lastByteOffset) {
            length = -1;
        } else {
            try (final InputStream inputStream = s3Object()) {
                length = 0;
                int b;

                while ((b = inputStream.read()) != -1) {
                    buffer[length++] = (byte) b;
                }

                if (length > 0) {
                    offset += length;
                }
            }
        }
    }

    public long length(){
        return lastByteOffset;
    }

    private InputStream s3Object() {
        final Long rangeEnd = offset + buffer.length - 1;
        final String rangeString = "bytes=" + offset + "-" + rangeEnd;
        final GetObjectRequest getObjectRequest = GetObjectRequest.builder().bucket(bucket).key(file).range(rangeString)
                .build();
        System.out.println("Making an S3 GET request starting at " + offset + " and with length: " + buffer.length);
        return lazyHolder.S3.getObject(getObjectRequest);
    }
}