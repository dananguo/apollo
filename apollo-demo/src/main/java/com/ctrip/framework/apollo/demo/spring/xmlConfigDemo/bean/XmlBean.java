package com.ctrip.framework.apollo.demo.spring.xmlConfigDemo.bean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Jason Song(song_s@ctrip.com)
 */
public class XmlBean {
    private static final Logger logger = LoggerFactory.getLogger(XmlBean.class);

    private int timeout;
    private int batch;

    public int getTimeout() {
        return timeout;
    }

    public void setTimeout(int timeout) {
        logger.info("updating timeout, old value: {}, new value: {}", this.timeout, timeout);
        this.timeout = timeout;
    }

    public int getBatch() {
        return batch;
    }

    public void setBatch(int batch) {
        logger.info("updating batch, old value: {}, new value: {}", this.batch, batch);
        this.batch = batch;
    }

    @Override
    public String toString() {
        return String.format("[XmlBean] timeout: %d, batch: %d", timeout, batch);
    }
}
