package com.ctrip.framework.apollo.common.entity;

import com.google.common.base.MoreObjects;
import com.google.common.base.MoreObjects.ToStringHelper;

import javax.persistence.Column;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Inheritance;
import javax.persistence.InheritanceType;
import javax.persistence.MappedSuperclass;
import javax.persistence.PrePersist;
import javax.persistence.PreRemove;
import javax.persistence.PreUpdate;
import java.util.Date;

@MappedSuperclass
@Inheritance(strategy = InheritanceType.TABLE_PER_CLASS)
public abstract class BaseEntity {

    @Column(name = "IsDeleted", columnDefinition = "Bit default '0'")
    protected boolean isDeleted = false;
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "Id")
    private long id;
    @Column(name = "DataChange_CreatedBy", nullable = false)
    private String dataChangeCreatedBy;

    @Column(name = "DataChange_CreatedTime", nullable = false)
    private Date dataChangeCreatedTime;

    @Column(name = "DataChange_LastModifiedBy")
    private String dataChangeLastModifiedBy;

    @Column(name = "DataChange_LastTime")
    private Date dataChangeLastModifiedTime;

    public String getDataChangeCreatedBy() {
        return dataChangeCreatedBy;
    }

    public void setDataChangeCreatedBy(String dataChangeCreatedBy) {
        this.dataChangeCreatedBy = dataChangeCreatedBy;
    }

    public Date getDataChangeCreatedTime() {
        return dataChangeCreatedTime;
    }

    public void setDataChangeCreatedTime(Date dataChangeCreatedTime) {
        this.dataChangeCreatedTime = dataChangeCreatedTime;
    }

    public String getDataChangeLastModifiedBy() {
        return dataChangeLastModifiedBy;
    }

    public void setDataChangeLastModifiedBy(String dataChangeLastModifiedBy) {
        this.dataChangeLastModifiedBy = dataChangeLastModifiedBy;
    }

    public Date getDataChangeLastModifiedTime() {
        return dataChangeLastModifiedTime;
    }

    public void setDataChangeLastModifiedTime(Date dataChangeLastModifiedTime) {
        this.dataChangeLastModifiedTime = dataChangeLastModifiedTime;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public boolean isDeleted() {
        return isDeleted;
    }

    public void setDeleted(boolean deleted) {
        isDeleted = deleted;
    }

    @PrePersist
    protected void prePersist() {
        if (this.dataChangeCreatedTime == null) {
            dataChangeCreatedTime = new Date();
        }
        if (this.dataChangeLastModifiedTime == null) {
            dataChangeLastModifiedTime = new Date();
        }
    }

    @PreUpdate
    protected void preUpdate() {
        this.dataChangeLastModifiedTime = new Date();
    }

    @PreRemove
    protected void preRemove() {
        this.dataChangeLastModifiedTime = new Date();
    }

    protected ToStringHelper toStringHelper() {
        return MoreObjects.toStringHelper(this).omitNullValues().add("id", id)
                .add("dataChangeCreatedBy", dataChangeCreatedBy)
                .add("dataChangeCreatedTime", dataChangeCreatedTime)
                .add("dataChangeLastModifiedBy", dataChangeLastModifiedBy)
                .add("dataChangeLastModifiedTime", dataChangeLastModifiedTime);
    }

    public String toString() {
        return toStringHelper().toString();
    }
}
