package org.cloudname.config;

public interface Config {
    String getRegion();
    String getTag();
    String getService();
    String getVersion();
    String getChecksum();
    String getConfig();
    long getTimestamp();
}
