package io.siddhi.extension.store.s3;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import io.siddhi.query.api.annotation.Annotation;

import java.util.Arrays;
import java.util.List;

public class S3StoreConfig {

    private String contentType;
    private String credentialProvider;
    private String bucketName;
    private String region;
    private List<String> objectFields;
    private boolean enableVersioning;

    public S3StoreConfig(Annotation annotation) {
        this.credentialProvider = annotation.getElement("credential.provider");
        if (this.credentialProvider != null && this.credentialProvider.isEmpty()) {
            this.credentialProvider = null;
        }

        this.bucketName = annotation.getElement("bucket.name");
        if (this.bucketName == null || this.bucketName.isEmpty()) {
            throw new IllegalArgumentException("Bucket name cannot be null.");
        }

        this.region = annotation.getElement("region");
        if (this.region == null || this.bucketName.isEmpty()) {
            this.region = Region.getRegion(Regions.DEFAULT_REGION).getName();
        }

        String objectFieldList = annotation.getElement("object.fields");
        if (objectFieldList == null || objectFieldList.isEmpty()) {
            throw new IllegalArgumentException("Object fields cannot be null/empty.");
        }
        this.objectFields = Arrays.asList(objectFieldList.toLowerCase().split(","));

        this.enableVersioning = Boolean.parseBoolean(annotation.getElement("enable.versioning"));

        this.contentType = annotation.getElement("content.type");
        if (this.contentType == null || this.contentType.isEmpty()) {
            this.contentType = "application/octet-stream";
        }

    }

    public String getCredentialProvider() {
        return credentialProvider;
    }

    public void setCredentialProvider(String credentialProvider) {
        this.credentialProvider = credentialProvider;
    }

    public String getBucketName() {
        return bucketName;
    }

    public void setBucketName(String bucketName) {
        this.bucketName = bucketName;
    }

    public boolean isEnableVersioning() {
        return enableVersioning;
    }

    public void setEnableVersioning(boolean enableVersioning) {
        this.enableVersioning = enableVersioning;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public List<String> getObjectFields() {
        return objectFields;
    }

    public void setObjectFields(List<String> objectFields) {
        this.objectFields = objectFields;
    }

    public String getContentType() {
        return contentType;
    }

    public void setContentType(String contentType) {
        this.contentType = contentType;
    }
}
