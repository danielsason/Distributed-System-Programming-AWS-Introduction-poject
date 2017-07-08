package models;


import java.io.Serializable;



public class NewTask implements Serializable {
    private String key;
    private String bucketName;
    private String shouldTerminate;
    private int n;
    private String queueUrl;

    public NewTask(String key, String bucketName, String shouldTerminate, int n, String queueUrl) {
        this.key = key;
        this.bucketName = bucketName;
        this.n = n;
        this.shouldTerminate = shouldTerminate;
        this.queueUrl = queueUrl;
    }

    public String getKey() {
        return this.key;
    }

    public String getBucketName() {
        return this.bucketName;
    }

    public String getShouldTerminate() {
        return this.shouldTerminate;
    }

    public int getN() {
        return this.n;
    }

    public String getQueueUrl() {
        return this.queueUrl;
    }


}


