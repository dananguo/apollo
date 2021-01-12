package com.ctrip.framework.apollo.openapi.dto;

public class OpenAppDTO extends BaseDTO {

    private String name;

    private String appId;

    private String orgId;

    private String orgName;

    private String ownerName;

    private String ownerEmail;

    public String getAppId() {
        return appId;
    }

    public void setAppId(String appId) {
        this.appId = appId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getOrgId() {
        return orgId;
    }

    public void setOrgId(String orgId) {
        this.orgId = orgId;
    }

    public String getOrgName() {
        return orgName;
    }

    public void setOrgName(String orgName) {
        this.orgName = orgName;
    }

    public String getOwnerEmail() {
        return ownerEmail;
    }

    public void setOwnerEmail(String ownerEmail) {
        this.ownerEmail = ownerEmail;
    }

    public String getOwnerName() {
        return ownerName;
    }

    public void setOwnerName(String ownerName) {
        this.ownerName = ownerName;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("OpenAppDTO{");
        sb.append("name='").append(name).append('\'');
        sb.append(", appId='").append(appId).append('\'');
        sb.append(", orgId='").append(orgId).append('\'');
        sb.append(", orgName='").append(orgName).append('\'');
        sb.append(", ownerName='").append(ownerName).append('\'');
        sb.append(", ownerEmail='").append(ownerEmail).append('\'');
        sb.append(", dataChangeCreatedBy='").append(dataChangeCreatedBy).append('\'');
        sb.append(", dataChangeLastModifiedBy='").append(dataChangeLastModifiedBy).append('\'');
        sb.append(", dataChangeCreatedTime=").append(dataChangeCreatedTime);
        sb.append(", dataChangeLastModifiedTime=").append(dataChangeLastModifiedTime);
        sb.append('}');
        return sb.toString();
    }
}
