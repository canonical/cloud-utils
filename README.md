# cloud-utils

A useful set of utilities for interacting with a cloud.

## Install

cloud-utils is available in Ubuntu and Debian via the `cloud-utils` metapackage:

```shell
sudo apt update
sudo apt install cloud-utils
```

Red Hat based distributions offer the `cloud-utils` package with a subset of binaries and `cloud-utils-growpart` for growpart.

The package is also available in Alpine and Arch as `cloud-utils`.

## Usage

* `cloud-localds` - create a disk for cloud-init to utilize the nocloud datasource
* `ec2metadata` - query and display AWS EC2 metadata
* `growpart` - rewrite partition table so that partition takes up all the space it can
* `mount-image-callback` - mount a file to a temporary mount point and then invoke the provided cmd with args
* `resize-part-image` - resize a partition image and contained filesystem to a new size
* `ubuntu-cloudimage-query` - get the latest Ubuntu AWS AMI meeting certain criteria
* `vcs-run` - obtain a repository and execute a command with arguments
* `write-mime-multipart` - utilty for creating mime-multipart files, likely for use via user data and cloud-init

## Getting help

If you find a bug or issue please report it on the upstream [Launchpad project](https://bugs.launchpad.net/cloud-utils/+filebug).

## License

Distributed under the GPLv3 License. See LICENSE for more information.
