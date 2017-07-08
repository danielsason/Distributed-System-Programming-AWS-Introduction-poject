package models;

import java.io.Serializable;

public class DoneTask implements Serializable {
    private String key;
    private String bucketName;

    public DoneTask(String key, String bucketName){
        this.key = key;
        this.bucketName = bucketName;
    }

    public String getBucketName (){
        return this.bucketName;
    }

    public String getKey (){
        return this.key;
    }
}