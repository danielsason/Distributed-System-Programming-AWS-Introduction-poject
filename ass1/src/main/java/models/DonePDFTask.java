package models;



import java.io.Serializable;


public class DonePDFTask implements Serializable {

    private String key;
    private String bucketName;
    private String url;
    private String operation;
    private String localAppTaskId;
    private String exceptionMsg;
    private int exceptionFlag;

    public DonePDFTask(String key, String bucketName, String url, String operation, String localAppTaskId, String exceptionMsg, int exceptionFlag) {
        this.key = key;
        this.bucketName = bucketName;
        this.url = url;
        this.operation = operation;
        this.localAppTaskId = localAppTaskId;
        this.exceptionMsg = exceptionMsg;
        this.exceptionFlag = exceptionFlag;
    }

    public String getKey(){
        return this.key;
    }

    public String getBucketName(){
        return this.bucketName;
    }

    public String getUrl(){
        return this.url;
    }

    public String getOperation(){
        return this.operation;
    }

    public String getLocalAppTaskId() { return this.localAppTaskId;}

    public String getExceptionMsg() { return this.exceptionMsg;}

    public int getExceptionFlag() { return this.exceptionFlag;}
}
