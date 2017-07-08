import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.ListQueuesResult;
import com.amazonaws.services.sqs.model.Message;
import com.google.gson.Gson;
import models.DonePDFTask;
import models.NewTask;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;


public class Manager {

    private String localToManagerQueueUrl;

    private String managerToWorkersQueueUrl;

    private String workersToManagerQueueUrl;

    private AmazonSQS sqs;

    private AmazonEC2 ec2;

    private AmazonS3 s3;

    private volatile int activeWorkersCount = 0;

    private boolean terminate = false;

    private AtomicInteger taskCounter;

    private ArrayList<Instance> workers;

    private ExecutorService newTaskHandlerExecutor;

    private ExecutorService donePDFTaskHandlerExecutor;

    private ExecutorService doneTaskHandlerExecutor;

    private ConcurrentHashMap<String, ArrayList<DonePDFTask>> taskId_tasks;

    private ConcurrentHashMap<String, Integer> taskId_size;

    private ConcurrentHashMap<String, String> taskId_localAppQueue;

    private String bucketName;

    private Manager() {
        workers = new ArrayList<Instance>();
        // 10 is the maximum number of massages that can be received at once
        // pool size = 20: 10 for incoming messages, and 10 for outgoing.
        newTaskHandlerExecutor = Executors.newFixedThreadPool(10);
        donePDFTaskHandlerExecutor = Executors.newFixedThreadPool(10);
        doneTaskHandlerExecutor = Executors.newFixedThreadPool(10);
        taskId_tasks = new ConcurrentHashMap<String, ArrayList<DonePDFTask>>();
        taskId_size = new ConcurrentHashMap<String, Integer>();
        taskCounter = new AtomicInteger(0);
        taskId_localAppQueue = new ConcurrentHashMap<String, String>();
        //load creds
        AWSCredentials credentials = null;
        try {
            credentials = new PropertiesCredentials(
                    Manager.class.getResourceAsStream("creds.properties"));
        } catch (Exception e) {
            throw new AmazonClientException(
                    "Cannot load the credentials from the credential profiles file. ", e);
        }
        //initialize service clients
        sqs = AmazonSQSClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .withRegion("us-east-1")
                .build();
        s3 = AmazonS3ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .withRegion("us-east-1")
                .build();
        ec2 = AmazonEC2ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .withRegion("us-east-1")
                .build();


    }


    public static void main(String[] args) throws InterruptedException {

        //initialization of the manager
        Manager manager = new Manager();
        Gson gson = new Gson();

        //find the urls of the sqs queues
        manager.localToManagerQueueUrl = manager.getQueueUrl("manager-input");
        manager.managerToWorkersQueueUrl = manager.getQueueUrl("worker-incoming");
        manager.workersToManagerQueueUrl = manager.getQueueUrl("worker-outgoing");

        System.out.println("starting PDFtasksHandlers");
        //here we should start the sorter threads that handle messages from workers containing donePDFTasks
        for (int i = 0; i < 10; i++)
            manager.donePDFTaskHandlerExecutor.execute(new DonePDFTaskHandler(manager, manager.sqs, manager.workersToManagerQueueUrl));


        //Main loop
        while (!manager.terminate) {
            ArrayList<Message> tasksFromLocal = (ArrayList<Message>) manager.sqs.receiveMessage(manager.localToManagerQueueUrl).getMessages();
            for (Message m : tasksFromLocal) {
                NewTask task = gson.fromJson(m.getBody(), NewTask.class);
                String sqsMessageId = m.getMessageId();
                manager.taskId_localAppQueue.put(sqsMessageId, task.getQueueUrl());
                if (task.getShouldTerminate().equals("yes")) {
                    //if we got a terminate, we break so we don't read more messages and wait until all open tasks are finished
                    System.out.println("terminate message is accepted");
                    manager.terminate = true;
                    manager.sqs.deleteMessage(manager.localToManagerQueueUrl, m.getReceiptHandle());
                    break;
                } else {
                    if (manager.bucketName == null)
                        manager.bucketName = task.getBucketName();
                    manager.taskCounter.incrementAndGet();
                    System.out.println("starting taskHandler for the job");
                    manager.newTaskHandlerExecutor.execute(new NewTaskHandler(manager.sqs, manager.ec2, manager.s3, manager, sqsMessageId, task, manager.managerToWorkersQueueUrl));
                    manager.sqs.deleteMessage(manager.localToManagerQueueUrl, m.getReceiptHandle());
                }
            }
        }
        System.out.println("termination recieve waiting for all tasks to end");
        //once we get out of the main loop, we must wait until all pending tasks are over and only then terminate
        while (manager.taskCounter.get() > 0) {
            try {
                synchronized (manager.taskCounter) {
                    manager.taskCounter.wait();
                }
            } catch (InterruptedException e) {
                System.out.println("InterruptedException : " + e.getMessage());
                e.printStackTrace();
            }
        }

        //after all pending tasks are over, we will now terminate
        System.out.println("terminate system is begin");
        manager.shutDownSystem();
    }

    /**
     * this function finds the SQS queue starting with @param prefix
     * @param prefix
     * @return url of SQS queue
     */
    public String getQueueUrl(String prefix) {
        ListQueuesResult queueList = sqs.listQueues(prefix);
        ArrayList<String> queuesUrl = (ArrayList<String>) queueList.getQueueUrls();
        return queuesUrl.get(0);
    }

    private int getActiveWorkersCount() {
        return activeWorkersCount;
    }

    /**
     * this function receives the number of workers needed for a new job and checks how many are already running,
     * and determines if more workers are needed
     * @param workersNeeded
     * @return
     */
    public synchronized int setActiveWorkersCount(int workersNeeded) {
        if (this.getActiveWorkersCount() < workersNeeded) {
            int ans = workersNeeded - this.getActiveWorkersCount();
            activeWorkersCount += ans;
            return ans;
        }
        return 0;
    }

    /**
     * this function adds the instances of the workers that were activated to the array of worker instances
     * @param instances
     */
    public void addInstances(List<Instance> instances) {
        workers.addAll(instances);
    }

    /**
     * this function inserts the received DonePDFTask into the map of job it is a part of,
     * and if it was the last one, it starts a new DoneTaskHandler for the finished job
     * @param doneTask
     * @return
     */
    public boolean updateDoneTaskTable(DonePDFTask doneTask) {
        ArrayList<DonePDFTask> taskArray = taskId_tasks.get(doneTask.getLocalAppTaskId());
        boolean success = taskArray.add(doneTask);
        if (!success) {
            return false;
        }
        if (taskArray.size() == taskId_size.get(doneTask.getLocalAppTaskId())) {
            System.out.println("received all PDF tasks from workers, starting outgoing message process");
            startOutgoingMessage(doneTask.getLocalAppTaskId());
        }
        return true;

    }

    public void initializeHashMapEntry(String newTaskId, int sumUrls) {
        taskId_size.put(newTaskId, sumUrls);
        taskId_tasks.put(newTaskId, new ArrayList<DonePDFTask>());
    }

    public void startOutgoingMessage(String taskId) {
        doneTaskHandlerExecutor.execute(new DoneTaskHandler(sqs, s3, this, taskId, this.taskId_localAppQueue.get(taskId), bucketName));
    }

    public ArrayList<DonePDFTask> getMessagesList(String messageId) {
        if (taskId_tasks.containsKey(messageId)) {
            taskId_size.remove(messageId);
            taskId_localAppQueue.remove(messageId);
            return taskId_tasks.remove(messageId);
        }
        return null;
    }

    public void decreaseMessage() {
        taskCounter.decrementAndGet();
        synchronized (taskCounter) {
            taskCounter.notifyAll();
        }
    }

    /**
     * this function shuts down the system
     */
    public void shutDownSystem() {
        //close all executors
        System.out.println("close all executors");
        donePDFTaskHandlerExecutor.shutdown();
        doneTaskHandlerExecutor.shutdown();
        newTaskHandlerExecutor.shutdown();
        //shutdown all worker instances
        System.out.println("shutdown all worker instances");
        ArrayList<String> instancesIds = new ArrayList<String>();
        for (Instance instance : workers) {
            instancesIds.add(instance.getInstanceId());
        }
        ec2.terminateInstances(new TerminateInstancesRequest(instancesIds));
        //delete queues
        System.out.println("delete sqs queues");
        sqs.deleteQueue(localToManagerQueueUrl);
        sqs.deleteQueue(managerToWorkersQueueUrl);
        sqs.deleteQueue(workersToManagerQueueUrl);

        //shutdown manager instance
        System.out.println("shutdown manager instance");
        List<Reservation> reservations = ec2.describeInstances().getReservations();
        for (Reservation reservation : reservations) {
            List<Instance> instances = reservation.getInstances();
            for (Instance instance : instances) {
                ArrayList<String> instanceList = new ArrayList<String>();
                instanceList.add(instance.getInstanceId());
                ec2.terminateInstances(new TerminateInstancesRequest(instanceList));
            }


        }
    }
}
