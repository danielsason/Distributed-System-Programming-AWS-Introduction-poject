import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.sqs.AmazonSQS;
import com.google.gson.Gson;
import models.DonePDFTask;
import models.DoneTask;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.UUID;


/**
 * This class takes care of creating a summary file for a finished job, uploading it to s3,
 * creating a new DoneTask message and sending it over SQS to the LocalApp that ordered the job
 */
public class DoneTaskHandler implements Runnable {

    private AmazonSQS sqs;

    private AmazonS3 s3;

    private Manager manager;

    private String messageId;

    private String respondQueueUrl;

    private String bucket;

    public DoneTaskHandler(AmazonSQS sqs, AmazonS3 s3, Manager manager, String messageId,
                           String respondQueueUrl, String bucket) {
        this.sqs = sqs;
        this.s3 = s3;
        this.manager = manager;
        this.messageId = messageId; //input message Id (from local application).
        this.respondQueueUrl = respondQueueUrl;
        this.bucket = bucket;
    }

    public void run() {
        //get the list of DonePDFTasks associated with the job that was finished
        ArrayList<DonePDFTask> donePDFTasksArray = manager.getMessagesList(messageId);
        //create summary file
        Gson gson = new Gson();
        File summaryFile = new File("summaryFile" + UUID.randomUUID() + ".txt");
        FileWriter fw;
        BufferedWriter bw;
        try {
            fw = new FileWriter(summaryFile);
            bw = new BufferedWriter(fw);
            for (DonePDFTask donePDFtask : donePDFTasksArray) {
                bw.write(gson.toJson(donePDFtask));
                bw.newLine();
            }
            bw.close();
            fw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        //upload summary file to s3
        s3.putObject(new PutObjectRequest(bucket, summaryFile.getName(), summaryFile));
        //create new DoneTask message
        DoneTask donetask = new DoneTask(summaryFile.getName(), bucket);
        //send message in SQS to LocalApp
        sqs.sendMessage(respondQueueUrl, gson.toJson(donetask));
        //decrement number of total jobs still pending
        manager.decreaseMessage();
    }
}
