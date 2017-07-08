

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.RunInstancesResult;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.util.*;
import com.google.gson.Gson;
import models.NewPDFTask;
import models.NewTask;

import java.io.*;
import java.util.*;


/**
 * this class takes care of downloading the input file from s3, parsing it and creating NewPDFTask messages.
 * then it updates the data structures of the Manager accordingly, calculates how many worker machines are needed for the the task and activates more worker machines
 * if more are needed. finally it sends all the NewPDFTasks that were generated to workers SQS queue
 */
public class NewTaskHandler implements Runnable {

    private AmazonSQS sqs;

    private AmazonEC2 ec2;

    private AmazonS3 s3;

    private Manager manager;

    private String sqsMessageId;

    private NewTask task;

    private String workersQueueUrl;

    private String bucketName;

    public NewTaskHandler(AmazonSQS sqsClient, AmazonEC2 ec2Client, AmazonS3 s3Client,
                          Manager manager, String sqsMessageId, NewTask task, String workersQueueUrl) {
        this.sqs = sqsClient;
        this.ec2 = ec2Client;
        this.s3 = s3Client;
        this.manager = manager;
        this.sqsMessageId = sqsMessageId;
        this.task = task;
        this.workersQueueUrl = workersQueueUrl;
    }

    public void run() {
        Gson gson = new Gson();
        //get task details from json object
        bucketName = task.getBucketName();
        String fileName = task.getKey();
        int n = task.getN();
        //download the localApp input file and save it on the manager machine
        File inputFile = new File( UUID.randomUUID() + ".txt");
        s3.getObject(new GetObjectRequest(bucketName, fileName), inputFile);
        System.out.println("input file downloaded to manager machine");
        //parsing the input file and creating NewPDFTasks objects which will be sent to the workers
        ArrayList<NewPDFTask> workerTasksArray = null;
        try {
            workerTasksArray = new ArrayList<NewPDFTask>();
            BufferedReader reader = new BufferedReader(new FileReader(inputFile));
            String line;
            while ((line = reader.readLine()) != null) {
                String[] lineAfterSplit = line.split("\t");
                System.out.println(String.format("operation is:%s", lineAfterSplit[0]));
                System.out.println(String.format("url is:%s", lineAfterSplit[1]));
                workerTasksArray.add(new NewPDFTask(lineAfterSplit[0], lineAfterSplit[1], this.sqsMessageId, bucketName));
            }
            //after we know the "size" of the task, we put an entry for it in the task hash maps of the manager
            manager.initializeHashMapEntry(sqsMessageId, workerTasksArray.size());
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("calculating how many worker machines we need to boot");
        int numberOfWorkersNeeded = (int) Math.ceil(((double)workerTasksArray.size() / n));
        int numberOfWorkersToActivate = manager.setActiveWorkersCount(numberOfWorkersNeeded);
        //activating workers
        if (numberOfWorkersToActivate > 0) {
            System.out.println(String.format("booting %d worker machines", numberOfWorkersToActivate));
            RunInstancesRequest runInstancesRequest = new RunInstancesRequest()
                    .withKeyName("gabiLazard")
                    .withInstanceType("t2.micro")
                    .withMaxCount(numberOfWorkersToActivate)
                    .withMinCount(numberOfWorkersToActivate)
                    .withImageId("ami-c58c1dd3")
                    .withUserData(getUserData());
            //need to save worker instances to delete them when we terminate
            List<Instance> instances = ec2.runInstances(runInstancesRequest).getReservation().getInstances();
            manager.addInstances(instances);
        }
        //send the pdf tasks to the workers
        System.out.println("sending the parsed pdfTasks to workers queue");
        for (NewPDFTask task : workerTasksArray) {
            sqs.sendMessage(workersQueueUrl, gson.toJson(task));
        }
    }

    private static String getUserData() {
        ArrayList<String> lines = new ArrayList<String>();
        lines.add("#!/bin/bash");
        lines.add("aws --no-sign-request s3 cp s3://project-jar-gabi-daniel/assignment1-1.0.0-jar-with-dependencies.jar project.jar");
        lines.add("java -cp project.jar Worker");
        String str = new String(com.amazonaws.util.Base64.encode(join(lines, "\n").getBytes()));
        return str;
    }


    private static String join(Collection<String> s, String delimiter) {
        StringBuilder builder = new StringBuilder();
        Iterator<String> iter = s.iterator();
        while (iter.hasNext()) {
            builder.append(iter.next());
            if (!iter.hasNext())
                break;
            builder.append(delimiter);
        }
        return builder.toString();
    }
}


