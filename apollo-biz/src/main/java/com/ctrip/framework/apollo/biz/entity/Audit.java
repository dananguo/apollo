package com.ctrip.framework.apollo.biz.entity;

import com.ctrip.framework.apollo.common.entity.BaseEntity;
import org.hibernate.annotations.SQLDelete;
import org.hibernate.annotations.Where;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Table;

@Entity
@Table(name = "Audit")
@SQLDelete(sql = "Update Audit set isDeleted = 1 where id = ?")
@Where(clause = "isDeleted = 0")
public class Audit extends BaseEntity {

    @Column(name = "EntityName", nullable = false)
    private String entityName;
    @Column(name = "EntityId")
    private Long entityId;
    @Column(name = "OpName", nullable = false)
    private String opName;
    @Column(name = "Comment")
    private String comment;

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public Long getEntityId() {
        return entityId;
    }

    public void setEntityId(Long entityId) {
        this.entityId = entityId;
    }

    public String getEntityName() {
        return entityName;
    }

    public void setEntityName(String entityName) {
        this.entityName = entityName;
    }

    public String getOpName() {
        return opName;
    }

    public void setOpName(String opName) {
        this.opName = opName;
    }

    public String toString() {
        return toStringHelper().add("entityName", entityName).add("entityId", entityId)
                .add("opName", opName).add("comment", comment).toString();
    }

    public enum OP {
        INSERT, UPDATE, DELETE
    }
}
