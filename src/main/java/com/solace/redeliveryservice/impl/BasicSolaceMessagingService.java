package com.solace.redeliveryservice.impl;

import com.solace.messaging.MessagingService;
import com.solace.messaging.config.SolaceProperties;
import com.solace.messaging.config.profile.ConfigurationProfile;
import com.solace.redeliveryservice.api.ISolaceMessagingService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.Properties;

/**
 * Class encapsulates Solace Connectivity using Basic Authentication.
 * Implement @ISolaceMessagingService for another form of Authentication
 * @author TKTheTechie
 */
@Component
public class BasicSolaceMessagingService extends ISolaceMessagingService {

    @Value("${solace.host}")
    private String host;

    @Value("${solace.user}")
    private String user;

    @Value("${solace.password}")
    private String password;


    /**
     * Reads in the properties and initializes the MessagingService
     */
    @PostConstruct
    public void init(){
        final Properties serviceConfiguration = new Properties();
        serviceConfiguration.setProperty(SolaceProperties.TransportLayerProperties.HOST,host);
        serviceConfiguration.setProperty(SolaceProperties.AuthenticationProperties.SCHEME_BASIC_USER_NAME, user);
        serviceConfiguration.setProperty(SolaceProperties.AuthenticationProperties.SCHEME_BASIC_PASSWORD, password);

        this.solaceMessagingService = MessagingService.builder(ConfigurationProfile.V1).fromProperties(serviceConfiguration).build().connect();
        super.init();
    }



}
