import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.ListQueuesResult;
import com.amazonaws.services.sqs.model.Message;
import com.google.gson.Gson;
import models.DonePDFTask;
import models.NewPDFTask;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.util.PDFText2HTML;
import org.apache.pdfbox.util.PDFTextStripper;
import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.*;
import java.util.ArrayList;
import java.util.UUID;


public class Worker {

    private AmazonSQS sqs;

    private AmazonEC2 ec2;

    private AmazonS3 s3;

    public Worker() {
        AWSCredentials credentials;
        try {
            credentials = new PropertiesCredentials(
                    Worker.class.getResourceAsStream("creds.properties"));
        } catch (Exception e) {
            throw new AmazonClientException(
                    "Cannot load the credentials from the credential profiles file. ", e);
        }
        //initialize service clients
        this.sqs = AmazonSQSClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .withRegion("us-east-1")
                .build();
        this.s3 = AmazonS3ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .withRegion("us-east-1")
                .build();
        this.ec2 = AmazonEC2ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .withRegion("us-east-1")
                .build();
    }


    public static void main(String[] args) {
        //initialize services
        Gson gson = new Gson();
        Worker worker = new Worker();
        String downloadSuccesfull = "success";//weird exceptions
        //find the needed sqs queues
        String managerToWorkersQueueUrl = worker.getQueueUrl("worker-incoming");
        String workersToManagerQueueUrl = worker.getQueueUrl("worker-outgoing");
        //Get a message from an SQS queue
        while (true) {
            ArrayList<Message> messages = (ArrayList<Message>) worker.sqs.receiveMessage(managerToWorkersQueueUrl).getMessages();
            //change visibility of all received messages
            System.out.println("changing visibilty of received messages");
            for (Message m : messages) {
                downloadSuccesfull = "success";//weird exceptions
                worker.sqs.changeMessageVisibility(managerToWorkersQueueUrl, m.getReceiptHandle(), 60);
            }
            for (Message m : messages) {
                downloadSuccesfull = "success";//weird exceptions
                System.out.println("working on a new pdf task");
                NewPDFTask task = gson.fromJson(m.getBody(), NewPDFTask.class);
                // Download the PDF file indicated in the message.
                File inputFile = new File("input" + UUID.randomUUID() + ".pdf");
                System.out.println("downloading pdf from url");
                //need to handle exceptions for the downloading of the file
                downloadSuccesfull = worker.downloadPdf(task.getUrl(), inputFile);
                // Perform the operation requested on the file.
                if (downloadSuccesfull.equals("success")) {
                    System.out.println("performing operation on pdf file");
                    System.out.println(task.getOperation());
                    System.out.println(task.getUrl());
                    String operationResult = "";
                    File output = null;
                    if (task.getOperation().equals("ToImage")) {
                        output = new File(inputFile.getName().replace(".pdf", ".png"));
                        operationResult = worker.toImage(inputFile, output);
                    } else if (task.getOperation().equals("ToText")) {
                        output = new File(inputFile.getName().replace(".pdf", ".txt"));
                        operationResult = worker.toText(inputFile, output);
                    } else {
                        output = new File(inputFile.getName().replace(".pdf", ".html"));
                        operationResult = worker.toHtml(inputFile, output);
                    }
                    if (operationResult == null) {
                        System.out.println("Fatal error from pdfbox which isnt an excpetion");
                        DonePDFTask doneMessage = new DonePDFTask("", task.getBucketName(), task.getUrl(), task.getOperation(), task.getLocalAppTaskId(), "Fatal error from pdfbox which isnt an excpetion", 1);
                        worker.sqs.sendMessage(workersToManagerQueueUrl, gson.toJson(doneMessage));
                        System.out.println("remove the processed message from the SQS queue");
                        // remove the processed message from the SQS queue.
                        worker.sqs.deleteMessage(managerToWorkersQueueUrl, m.getReceiptHandle());
                    } else {
                        if (operationResult.equals("success")) {
                            // Upload the resulting output file to S3.
                            System.out.println("Upload the resulting output file to S3");
                            //there is a problem with the output file
                            PutObjectRequest por = new PutObjectRequest(task.getBucketName(), output.getName(), output);
                            por.setCannedAcl(CannedAccessControlList.PublicRead);
                            worker.s3.putObject(por);
                            // Put a message in an SQS queue indicating the original URL of the PDF, the S3 url of the new image file, and the operation that was performed.
                            System.out.println("Put a message in an SQS queue indicating the original URL of the PDF, the S3 url of the new image file, and the operation that was performed");
                            DonePDFTask doneMessage = new DonePDFTask(output.getName(), task.getBucketName(), task.getUrl(), task.getOperation(), task.getLocalAppTaskId(), "", 0);
                            worker.sqs.sendMessage(workersToManagerQueueUrl, gson.toJson(doneMessage));
                            System.out.println("remove the processed message from the SQS queue");
                            // remove the processed message from the SQS queue.
                            worker.sqs.deleteMessage(managerToWorkersQueueUrl, m.getReceiptHandle());
                        } else {
                            System.out.println("exception occured while handling the pdf file");
                            DonePDFTask doneMessage = new DonePDFTask("", task.getBucketName(), task.getUrl(), task.getOperation(), task.getLocalAppTaskId(), operationResult, 1);
                            worker.sqs.sendMessage(workersToManagerQueueUrl, gson.toJson(doneMessage));
                            System.out.println("remove the processed message from the SQS queue");
                            // remove the processed message from the SQS queue.
                            worker.sqs.deleteMessage(managerToWorkersQueueUrl, m.getReceiptHandle());
                        }
                    }
                } else {
                    System.out.println("exception occured while handling the url");
                    DonePDFTask doneMessage = new DonePDFTask("", task.getBucketName(), task.getUrl(), task.getOperation(), task.getLocalAppTaskId(), downloadSuccesfull, 1);
                    worker.sqs.sendMessage(workersToManagerQueueUrl, gson.toJson(doneMessage));
                    System.out.println("remove the processed message from the SQS queue");
                    // remove the processed message from the SQS queue.
                    worker.sqs.deleteMessage(managerToWorkersQueueUrl, m.getReceiptHandle());
                }
            }

        }
    }


    //get the queue url by its prefix
    private String getQueueUrl(String prefix) {
        ListQueuesResult linputQueue = sqs.listQueues(prefix);
        ArrayList<String> queuesUrl = (ArrayList<String>) linputQueue.getQueueUrls();
        String url = queuesUrl.get(0);
        return url;
    }

    public String toImage(File input, File output) {
        PDDocument document = null;

        try {
            document = PDDocument.load(input);
            PDPage firstPage = (PDPage) document.getDocumentCatalog().getAllPages().get(0);
            BufferedImage image = firstPage.convertToImage();
            ImageIO.write(image, "PNG", output);
            firstPage.clear();
            document.close();
        } catch (Exception e) {
            return e.getMessage();
        }
        return "success";
    }

    public String toText(File input, File output) {
        PDDocument document = null;
        FileWriter fw = null;
        BufferedWriter bw = null;
        try {
            document = PDDocument.load(input);
            PDFTextStripper pdfStripper = new PDFTextStripper();
            pdfStripper.setStartPage(0);
            pdfStripper.setEndPage(1);
            String text = pdfStripper.getText(document);
            fw = new FileWriter(output);
            bw = new BufferedWriter(fw);
            bw.write(text);
            bw.close();
            fw.close();
        } catch (Exception e) {
            return e.getMessage();
        }
        return "success";
    }

    public String toHtml(File input, File output) {
        PDDocument document = null;
        try {
            document = PDDocument.load(input);
            PDFTextStripper textStripper = new PDFText2HTML("UTF-8");
            textStripper.setStartPage(0);
            textStripper.setEndPage(1);
            String text = textStripper.getText(document);
            BufferedWriter bw = new BufferedWriter(new FileWriter(output));
            bw.write(text);
            bw.close();
            document.close();
        } catch (Exception e) {
            return e.getMessage();
        }
        return "success";
    }

    public String downloadPdf(String url, File inputFile) {
        try {
            //set timeout of http client to 10 seconds
            RequestConfig requestConfig = RequestConfig.custom().setConnectTimeout(10000).build();
            CloseableHttpClient client = HttpClientBuilder.create().setDefaultRequestConfig(requestConfig).build();
            HttpGet request = new HttpGet(url);
            HttpResponse response = client.execute(request);
            HttpEntity entity = response.getEntity();

            int responseCode = response.getStatusLine().getStatusCode();
            System.out.println("Request Url: " + request.getURI());
            System.out.println("Response Code: " + responseCode);
            if (responseCode != 200) {
                return "couldn't get file from the url, the response code is: " + Integer.toString(responseCode);
            }

            InputStream is = entity.getContent();
            FileOutputStream fos = new FileOutputStream(inputFile);
            int inByte;
            while ((inByte = is.read()) != -1) {
                fos.write(inByte);
            }
            is.close();
            fos.close();
            client.close();
            System.out.println("File Download Completed!!!");
            return "success";
        } catch (Exception e) {
            return e.getMessage();
        }
    }


}