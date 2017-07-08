import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.*;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.*;
import com.google.gson.Gson;
import models.DonePDFTask;
import models.DoneTask;
import models.NewTask;
import com.amazonaws.util.Base64;

import java.io.*;
import java.util.*;


public class LocalApp {

    private static final String BUCKETNAME = "project-jar-gabi-daniel";

    private static final String LOCAL_TO_MANAGER = "manager-input";
    private static final String MANAGER_TO_LOCAL = "manager-output";
    private static final String MANAGER_TO_WORKER = "worker-incoming";
    private static final String WORKER_TO_MANAGER = "worker-outgoing";

    public static void main(String[] args) throws IOException, InterruptedException {
        Boolean shouldTerminate = false;
        Boolean responseReceived = false;
        Gson gson = new Gson();
        //load arguments
        String filePath = args[0];
        String outputPath = args[1];
        String n = args[2];
        if (args.length > 3 && args[3].equals("terminate"))
            shouldTerminate = true;
        File inputFile = new File(filePath);
        //load credentials
        PropertiesCredentials credentials;
        try {
            credentials = new PropertiesCredentials(
                    LocalApp.class.getResourceAsStream("creds.properties"));
        } catch (Exception e) {
            throw new AmazonClientException(
                    "Cannot load the credentials from the credential profiles file. ", e);
        }
        //initialize AWS service clients
        AmazonS3 s3 = AmazonS3ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .withRegion("us-east-1")
                .build();
        AmazonSQS sqs = AmazonSQSClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .withRegion("us-east-1")
                .build();
        AmazonEC2 ec2 = AmazonEC2ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .withRegion("us-east-1")
                .build();

        if (!s3.doesBucketExist(BUCKETNAME))
            s3.createBucket(BUCKETNAME);

        //upload input file to s3
        String inputFileKey = inputFile.getName()+UUID.randomUUID();//the name of the input file will be the key
        System.out.println("Uploading a new object to S3 from a file\n");
        s3.putObject(new PutObjectRequest(BUCKETNAME, inputFileKey, inputFile));

        // Checks if a Manager node is active on the EC2 cloud. If it is not, the application will start the manager node.
        String localToManagerQueue;
        if (!findManager(ec2)) {
            //create tag specs for the manager instance
            Tag tag = new Tag("Type", "Manager");
            TagSpecification spec = new TagSpecification();
            spec.setResourceType("instance");
            spec.withTags(tag);

            // start the manager machine
            RunInstancesRequest runInstancesRequest = new RunInstancesRequest()
                    .withKeyName("gabiLazard")
                    .withInstanceType("t2.micro")
                    .withMaxCount(1)
                    .withMinCount(1)
                    .withImageId("ami-c58c1dd3")
                    .withTagSpecifications(spec)
                    .withUserData(getUserData());
            ec2.runInstances(runInstancesRequest);

            //create the main system SQS queues
            System.out.println("Creating new SQS queues");
            sqs.createQueue(new CreateQueueRequest(MANAGER_TO_WORKER + UUID.randomUUID()));
            sqs.createQueue(new CreateQueueRequest(WORKER_TO_MANAGER + UUID.randomUUID()));
            localToManagerQueue = sqs.createQueue(new CreateQueueRequest(LOCAL_TO_MANAGER + UUID.randomUUID())).getQueueUrl();
        } else {
            //manager already exists, need to get the manager input queue
            ListQueuesResult queuesList = sqs.listQueues("manager-input");
            ArrayList<String> queuesUrl = (ArrayList<String>) queuesList.getQueueUrls();
            //fail safe
            while (queuesUrl.isEmpty()) {
                Thread.sleep(1000);
                queuesList = sqs.listQueues("manager-input");
                queuesUrl = (ArrayList<String>) queuesList.getQueueUrls();
            }
            localToManagerQueue = queuesUrl.get(0);
        }

        //unique queue per local App
        String managerToLocalQueue = sqs.createQueue(new CreateQueueRequest(MANAGER_TO_LOCAL + UUID.randomUUID())).getQueueUrl();

        //create newTask object and send message to manager
        NewTask task = new NewTask(inputFileKey, BUCKETNAME, "no", Integer.parseInt(n), managerToLocalQueue);
        sqs.sendMessage(localToManagerQueue, gson.toJson(task));

        //wait for manager to respond
        while (!responseReceived) {
            if (!findManager(ec2)) {
                System.out.println("Program terminated by other user. Shutting down..");
                break;
            } else {
                List<Message> messages = sqs.receiveMessage(managerToLocalQueue).getMessages();
                for (Message message : messages) {
                    System.out.println("received summary file from manager");
                    responseReceived = true;
                    DoneTask doneTask = gson.fromJson(message.getBody(), DoneTask.class);
                    File summaryFile = new File("summary.txt");
                    System.out.println("downloading summary file from s3");
                    s3.getObject(new GetObjectRequest(doneTask.getBucketName(), doneTask.getKey()), summaryFile);
                    System.out.println("summary file downloaded");
                    sqs.deleteMessage(managerToLocalQueue, message.getReceiptHandle());
                    System.out.println("creating html file");
                    createHtml(summaryFile, outputPath);
                }
            }
        }
        //delete localApp input queue
        sqs.deleteQueue(managerToLocalQueue);
        System.out.println("sqs for local app deleted");
        //send termination message to manager if he's still running
        if (shouldTerminate && findManager(ec2)) {
            System.out.println("sending termination message to manager");
            NewTask terminateMessage = new NewTask("", "", "yes", 0, "");
            sqs.sendMessage(localToManagerQueue, gson.toJson(terminateMessage));
        }
        System.out.println("localApp is finished");
    }


    private static void createHtml(File summaryFile, String outputFilePath) throws IOException {
        Gson gson = new Gson();
        File f = new File(outputFilePath + ".html");
        BufferedWriter bw = new BufferedWriter(new FileWriter(f));
        StringBuilder buf = new StringBuilder();
        buf.append("<html>" +
                "<body>" +
                "<h1>PDF CONVERTER</h1>" +
                "<table>");
        BufferedReader reader = new BufferedReader(new FileReader(summaryFile));
        String line;
        while ((line = reader.readLine()) != null) {
            DonePDFTask doneTask = gson.fromJson(line, DonePDFTask.class);
            String operation = doneTask.getOperation();
            String inputUrl = doneTask.getUrl();
            String outputUrl;
            if (doneTask.getExceptionFlag() == 1)
                outputUrl = doneTask.getExceptionMsg();
            else
                outputUrl = "http://s3.amazonaws.com/" + doneTask.getBucketName() + "/" + doneTask.getKey();

            buf.append("<tr><td>" + operation + "    " + inputUrl + "    " + outputUrl + "</td></tr>");
        }
        reader.close();
        buf.append("</table>" +
                "</body>" +
                "</html>");
        bw.write(buf.toString());
        bw.close();
    }


    private static boolean findManager(AmazonEC2 ec2) {
        DescribeInstancesRequest request = new DescribeInstancesRequest();
        List<String> values = new ArrayList<String>();
        values.add("Manager");
        Filter filter1 = new Filter("tag:Type", values);
        DescribeInstancesResult result = ec2.describeInstances(request.withFilters(filter1));
        List<Reservation> reservations = result.getReservations();
        for (Reservation reservation : reservations) {
            List<Instance> instances = reservation.getInstances();
            for (Instance instance : instances) {
                if (instance.getState().getName().equals("running") || instance.getState().getName().equals("pending")) {
                    return true;
                }
            }
        }
        return false;
    }


    //returns string of script to run in manager instance
    private static String getUserData() {
        ArrayList<String> lines = new ArrayList<String>();
        lines.add("#!/bin/bash");
        lines.add("aws --no-sign-request s3 cp s3://project-jar-gabi-daniel/assignment1-1.0.0-jar-with-dependencies.jar project.jar");
        lines.add("java -cp project.jar Manager");
        return new String(Base64.encode(join(lines, "\n").getBytes()));
    }


    //creates string from bash file of getUserData function
    private static String join(Collection<String> s, String delimiter) {
        StringBuilder builder = new StringBuilder();
        Iterator<String> iter = s.iterator();
        while (iter.hasNext()) {
            builder.append(iter.next());
            if (!iter.hasNext()) {
                break;
            }
            builder.append(delimiter);
        }
        return builder.toString();
    }

}
