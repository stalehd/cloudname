# Cloudname configuration

## Configuration settings

All configuration settings are written in JSON. The structure and semantics of
the configuration files is determined by the service itself.

Each configuration is automatically assigned a version and a time stamp. The version is a checksum
of the configuration and the time stamp is the time the configuration was created.

Services subscribe to a particular configuration. When the configuration is
updated the services reload the configuration. Services ignore configuration with time stamps
lower than the current configuration.

## Configuration coordinates
Configuration coordinates are similar to the service coordinates but with an
version added to it. The version is a checksum of the configuration and is appended automatically by
the configuration library when the configuration is saved. The coordinates contains four parts:
`<region>.<tag>.<service>.<version>`.
A typical coordinate might look like this: `local.r1401.web.45`

The version is manually assigned when the configuration is set.

Once the services pick up the new configuration they will indicate that they
are notified of the new configuration.

## Configuration file utility
Configurations are updated with the cnconfig utility. The utility supports
the following arguments:

```
  conconfig <command> <args>
    Commands is one of:
        * create               Create or updaet existing configuration
        * dump                 Dump configuration.
        * remove               Remove configuration
        * list                 List configuration versions
        * clients              List clients using configuration
        * purge-old             Purge old configuration not in use.
    Args are:
        --input <file name>     Input file. Required for
        --region <region>       Region. Required.
        --tag <tag>             Tag. Required.
        --service <service>     Service name. Required.
        --version <version<     Version to remove. Required if --remove is specified, ignored otherwise
```

## Interface
```java

public interface CloudnameService {
}
```
