package com.ctrip.framework.apollo.portal.spi;

import com.ctrip.framework.apollo.portal.entity.bo.ReleaseHistoryBO;
import com.ctrip.framework.apollo.portal.environment.Env;

public interface MQService {

    void sendPublishMsg(Env env, ReleaseHistoryBO releaseHistory);

}
