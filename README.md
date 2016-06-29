# OpenStack Swift Object Storage file output plugin for Embulk

[Embulk](http://www.embulk.org/) file output plugin stores to [OpenStack](https://www.openstack.org/) [Object Storage(Swift)](http://swift.openstack.org/)
This plugin also may be usable for Swift compatible cloud storages, such as [Rackspace](https://www.rackspace.com/), and more.

This plugin uses Java OpenStack Storage(JOSS) library.

## Overview

* **Plugin type**: file output
* **Resume supported**: no
* **Cleanup supported**: no

## Configuration

- **username**: username for accessing the object storage (string, required)
- **password**: password for accessing the object storage (string, required)
- **auth_url**: authentication url for accessing the object storage (string, required)
- **auth_type**: Authentication type. you can choose "tempauth", "keystone", or "basic". (string, required)
- **tenant_id**: Tenant Id for keystone authentication (string, optional)
- **tenant_name**: Tenant name for keystone authentication (string, optional)
- **container**: Container name (string, required)
- **path_prefix**: prefix of target objects (string, required)
- **file_ext**: e.g, "csv.gz", "json.gz" (string, required)
- **sequence_format**: object name format for sequential objects (string, default: `"%03d.%02d"`)
- **max_connection_retry**: max retry count for connecting the object storage (integer, default: `10`)

## Example

```yaml
out:
  type: swift
  auth_type: tempauth
  username: test:tester
  password: testing
  auth_url: http://localhost:8080/auth/v1.0
  container: embulk_output
  path_prefix: data
  sequence_format: "%03d.%02d"
  file_ext: csv
  formatter:
    type: csv
```


## Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```
