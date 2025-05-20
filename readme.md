# Traefik S3 Provider Plugin

NOTE: This plugin was developed assuming [yaegi](https://github.com/traefik/yaegi) was up-to-date and robust. Unfortunately,
as of 5/20/2025 the yaegi project cannot process smithy types from the aws-sdk and the hope is to leverage the expected
sdk auth behavior.

This plugin allows you to source your traefik dynamic configuration from an S3 or S3-compatible object storage.

## How is this useful?

### Why object storage?

First of all, object storage is a widely available, managed service that adds a layer of resilliency to your lookup.
If you need to share information from a single location, object storage gives you the ability to update in one location
and then trust that within an interval, all clients of the storage will ahve read and updated from it.  This is preferrable to
some solutions where you might need to mount lots of drives, keep backups, etc. in a self-managed system.

### Why this library?

Per [this discussion](https://github.com/traefik/traefik/issues/7389), the only currently supported way of using S3 object
storage for dynamic configuration is to set up your s3 bucket as static http website and then use the http provider to access it.
This adds lots of unnecessary risk, since static site setup can accidentally expose this s3 bucket's contents to the public
internet.  If that were to happen and you had TLS certificates in the configuration, you would have a large security problem.

S3 object stores offers a lot more security without ever being set up as a static site though.  Through the use of AWS credentials
and bucket policies, you can lock down access to an S3 bucket to only a set of roles and their provisioned credentials.  This 
reduces complexity of setup and increases certainty by allowing you to use an object store that is "account+role" private and has
no additional static site setup.

## How to use this library

The prescribed way to use this library is like any other AWS service that you are setting up. That means that you can use environment
based credentials, stored credential files, etc.  By supplying this information on traefik start, the provider will use those credentials
via the aws s3 client sdk to retrieve your specified files.

# About credentials refresh

Important! This provider is currently just proxying the s3-client and aws credential retrieval mechanisms.  It is up to you to set up 
a mechanism for refreshing any credentials that may change.

That means that, if you use environment variables for something like an assumed role's session, after 1 hour your credentials will be invalid
because the session will have timed out.  In that case, you would need to restart the server since environment variables do not get changed
within processes.

On the flip side, if you set up an Web token file, the aws-sdk will automatically renew sessions as required.  See the AWS documentation for 
information.

### S3 compatible (Linode Object Storage)

If you are using Linode Object Storage, you can take advantage of it's s3 compatibility and modify a few configuration values:

* base endpoint url - your linode `https://<region>/linodeobjects.com` bucket url
* region - the linode datacenter region
* access key id - listed as `access key` when you create an object store api key
* secret access key - listed as `secret` when you create an object store api key

If you are not worried about having to force restart a traefik machine on api key rotation, you could make use of the AWS environment
variables and set them to something like:

```shell
AWS_ENDPOINT_URL=https://us-ord-1.linodeobjects.com
AWS_REGION=us-ord-1
AWS_ACCESS_KEY_ID=<access key>
AWS_SECRET_ACCESS_KEY=<secret>
```

# TODO - adding traefik configuration and files - not really work it until there is a viable plugin path

