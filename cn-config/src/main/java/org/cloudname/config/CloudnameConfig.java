package org.cloudname.config;

import java.util.List;

/**
 * Main configuratio interface.
 */
public interface CloudnameConfig {
    Config createConfiguration(final String jsonString, final ConfigCoordinate coordinate, final String clientId);
    void addConfigurationListener(final ConfigCoordinate coordinate, final ConfigListener listener);
    void removeConfigListener(final ConfigListener listener);
    boolean removeConfiguration(final Config configuration);
    List<String> listClients(final Config configuration);
    List<Config> getConfigurations(final ConfigCoordinate coordinate);
}
