import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.Message;
import com.google.gson.Gson;
import models.DonePDFTask;

import java.util.List;


/**
 * This class takes care of receiving DonePDFTasks from worker machines, and updating the data structures of the manager accordingly
 */
public class DonePDFTaskHandler implements Runnable {

    private Manager manager;

    private AmazonSQS sqs;

    private String workersToManagerQueueUrl;


    public DonePDFTaskHandler(Manager manager, AmazonSQS sqsClient, String workersToManagerQueueUrl) {
        this.manager = manager;
        this.sqs = sqsClient;
        this.workersToManagerQueueUrl = workersToManagerQueueUrl;
    }

    public void run() {
        //all this thread does is receive messages from workers about completed worker jobs and insert the to the managers done task tables
        Gson gson = new Gson();
        while (true) {
            List<Message> messages = sqs.receiveMessage(workersToManagerQueueUrl).getMessages();
            for (Message m : messages) {
                sqs.changeMessageVisibility(workersToManagerQueueUrl, m.getReceiptHandle(), 60);
            }
            for (Message m : messages) {
                DonePDFTask doneTask = gson.fromJson(m.getBody(), DonePDFTask.class);
                // if done pdf task was added successfully to manager hash table we can delete the sqs message
                if (manager.updateDoneTaskTable(doneTask))
                    sqs.deleteMessage(workersToManagerQueueUrl, m.getReceiptHandle());
            }
        }
    }


}
