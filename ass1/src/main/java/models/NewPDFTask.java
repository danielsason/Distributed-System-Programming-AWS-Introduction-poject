package models;


import java.io.Serializable;

public class NewPDFTask implements Serializable{

    private String url;
    private String operation;
    private String localAppTaskId;
    private String bucketName;

    public NewPDFTask(String operation, String url, String localAppTaskId, String bucketName){
        this.url = url;
        this.operation = operation;
        this.localAppTaskId = localAppTaskId;
        this.bucketName = bucketName;
    }

    public String getUrl(){ return this.url;}

    public String getOperation(){ return this.operation;}

    public String getLocalAppTaskId(){ return this.localAppTaskId;}

    public String getBucketName() { return this.bucketName;}

}

