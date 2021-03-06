package com.ctrip.framework.apollo.metaservice.service;

import com.ctrip.framework.apollo.common.condition.ConditionalOnMissingProfile;
import com.ctrip.framework.apollo.core.dto.ServiceDTO;
import com.ctrip.framework.apollo.tracer.Tracer;
import com.netflix.appinfo.InstanceInfo;
import com.netflix.discovery.EurekaClient;
import com.netflix.discovery.shared.Application;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Default discovery service for Eureka
 */
@Service
@ConditionalOnMissingProfile({"kubernetes", "nacos-discovery"})
public class DefaultDiscoveryService implements DiscoveryService {

    private static final Function<InstanceInfo, ServiceDTO> instanceInfoToServiceDTOFunc = instance -> {
        ServiceDTO service = new ServiceDTO();
        service.setAppName(instance.getAppName());
        service.setInstanceId(instance.getInstanceId());
        service.setHomepageUrl(instance.getHomePageUrl());
        return service;
    };
    private final EurekaClient eurekaClient;

    public DefaultDiscoveryService(final EurekaClient eurekaClient) {
        this.eurekaClient = eurekaClient;
    }

    @Override
    public List<ServiceDTO> getServiceInstances(String serviceId) {
        Application application = eurekaClient.getApplication(serviceId);
        if (application == null || CollectionUtils.isEmpty(application.getInstances())) {
            Tracer.logEvent("Apollo.Discovery.NotFound", serviceId);
            return Collections.emptyList();
        }
        return application.getInstances().stream().map(instanceInfoToServiceDTOFunc)
                .collect(Collectors.toList());
    }
}
