### Intent

The intent of this app is to test the sequential and random access read throughput in an EC2 instance against different backends


### Table of results

| Backend                | Sequential Read throughput | Random access Read throuput |
|------------------------|----------------------------|-----------------------------|
| S3 with S3AFilesystem  |                       | |
| S3 with AWS SDK V2 api |                        | |
| EBS with NVME          | | |
| Simple file server     | | |

### Building

* Make sure java and gradle are installed locally
* Run command `gradle shadowjar`
* This will generate the jar we need : `./app/build/libs/app-all.jar`
* Run the app with the following options
  ![Startup options](./images/options.png)
  Example run command:

`java  -jar /tmp/app-all.jar -f s3v1 -i "s3a://<bucket>/<file>" -a $ACCESS_KEY -s $SECRET_KEY -p "10000000" -t 1`

### The commands to run

* S3 with S3AFileSystem
  * Sequential access: `java  -jar /tmp/app-all.jar --filesystem s3v1 --inputPath "s3a://<bucket>/<file>" --awsAccessKey $ACCESS_KEY --awsSecretKey $SECRET_KEY --pageCacheSize "10000000" --numThreads 1`
  * Random access: `java  -jar /tmp/app-all.jar --filesystem s3v1 --inputPath "s3a://<bucket>/<file>" --awsAccessKey $ACCESS_KEY --awsSecretKey $SECRET_KEY --numThreads 1 --accessType RandomAccess --numberOfRecords 10000 --recordSize 1048576`
* S3 with AWS SDK V2 api
  * Sequential access: `java  -jar /tmp/app-all.jar --filesystem s3v2 --inputPath "s3a://<bucket>/<file>" --awsAccessKey $ACCESS_KEY --awsSecretKey $SECRET_KEY --pageCacheSize "8388608" --numThreads 1`
  * Random access: `java  -jar /tmp/app-all.jar --filesystem s3v2 --inputPath "s3a://<bucket>/<file>" --awsAccessKey $ACCESS_KEY --awsSecretKey $SECRET_KEY --numThreads 1 --accessType RandomAccess --numberOfRecords 10000 --recordSize 1048576`
* EBS
  * Sequential access: `java  -jar /tmp/app-all.jar --filesystem ebs --inputPath "/path/to/file" --pageCacheSize "8388608" --numThreads 1`
  * Random access: `java  -jar /tmp/app-all.jar --filesystem ebs --inputPath "/path/to/file" --numThreads 1 --accessType RandomAccess --numberOfRecords 100000 --recordSize 1048576`
* File server
  * Random access: `java  -jar /tmp/app-all.jar --filesystem fileServer --fileServerHost 127.0.0.1 --fileServerPort 9000 --inputPath "/path/to/file" --numThreads 1 --numberOfRecords 100000 --recordSize 1048576 --numberOfRecordsPerFileServerRequest 10`


### Monitoring 

* Run the following commands
    * `vnstat -l -ru`
    * This will give IO throughput

### Results

* aws cli throughput: we get around 100 MBps
* Using s3v1, with 1 thread, we get 21 MBps
  * `java  -jar /tmp/app-all.jar -f s3v1 -i "s3a://<bucket>/<file>" -a $ACCESS_KEY -s $SECRET_KEY -p "10000000" -t 1`
* Using s3v1, with 10 threads, we get 90 MBps
  * `java  -jar /tmp/app-all.jar -f s3v1 -i "s3a://<bucket>/<file>" -a $ACCESS_KEY -s $SECRET_KEY -p "10000000" -t 10`
* Using s3v2, with 1 thread, we get 12 MBps
  * `java  -jar /tmp/app-all.jar -f s3v2 -i "s3a://<bucket>/<file>" -a $ACCESS_KEY -s $SECRET_KEY -p "10000000" -t 1`
* Using s3v2, with 10 threads, we get 100 MBps
  * `java  -jar /tmp/app-all.jar -f s3v2 -i "s3a://<bucket>/<file>" -a $ACCESS_KEY -s $SECRET_KEY -p "10000000" -t 10`

