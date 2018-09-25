[![Build Status](https://travis-ci.org/CDCgov/fdns-ms-object.svg?branch=master)](https://travis-ci.org/CDCgov/fdns-ms-object)

# FDNS Object Microservice
This repository contains the Object layer for the Data Lake. This is the mutable layer of the data lake.

## Running locally
Please read the following instructions carefully for information on how to build, run and test this microservice in your local environment.

### Before you start
You will need to have the following installed before getting up and running locally:

- Docker, [Installation guides](https://docs.docker.com/install/)
- Docker Compose, [Installation guides](https://docs.docker.com/compose/install/)
- **Windows Users**: This project uses `Make`, please see [Cygwin](http://www.cygwin.com/) for running commands in this README

### Build

First you'll need to build the image, to do so just run the following command:

```
make docker-build
```

### Run

Now you'll be able to run the image, you can easily do so by running the following command:

```
make docker-run
```

### Test

To check if the microservice is running, just open the following URL in your browser:

[http://127.0.0.1:8083/](http://127.0.0.1:8083/)

### Documentation

To access the Swagger documentation, just open the following URL in your browser:

[http://127.0.0.1:8083/swagger-ui.html](http://127.0.0.1:8083/swagger-ui.html)

### MongoDB Configuration

First, you need to configure these environment variables using Docker or the STS Launch Configuration:

* `OBJECT_MONGO_HOST`: This is the host for your MongoDB server
* `OBJECT_MONGO_PORT`: This is the port for your MongoDB server
* `OBJECT_MONGO_USER_DATABASE`: Contains where the user name and password are stored in MongoDB, by default it's `admin`
* `OBJECT_MONGO_USERNAME`: The user name that you want to use to authenticate
* `OBJECT_MONGO_PASSWORD`: The password that you want to use to authenticate

Be sure, that you have properly configured your MongoDB server. If you use the `docker-compose.yml` file in this project, after starting Mongo, you need to:

* Login to the MongoDB server (in this case, we connect to the `admin` database):

```
docker exec -it fdns-ms-object_mongo_1 mongo admin
```

* And create the user:

```
db.createUser({ user: 'admin', pwd: 'admin', roles: [ { role: "userAdminAnyDatabase", db: "admin" } ] });
```

* Exit the console, by typing:

```
exit
```

### OAuth 2 Configuration

This microservice is configurable to be secured with an OAuth 2 provider.

__Scopes__: This application uses the following scope: `object.database.collection`

Please see the following environment variables for configuring with your OAuth 2 provider:

* `OAUTH2_ACCESS_TOKEN_URI`: This is the introspection URL of your provider, ex: `https://hydra:4444/oauth2/introspect`
* `OAUTH2_PROTECTED_URIS`: This is a path for which routes are to be restricted, ex: `/api/1.0/**`
* `OAUTH2_CLIENT_ID`: This is your OAuth 2 client id with the provider
* `OAUTH2_CLIENT_SECRET`: This is your OAuth 2 client secret with the provider
* `SSL_VERIFYING_DISABLE`: This is an option to disable SSL verification, you can disable this when testing locally but this should be set to `false` for all production systems

### Miscellaneous Configurations

Here are other various configurations and their purposes:

* `OBJECT_PORT`: This is a configurable port the application is set to run on
* `OBJECT_FLUENTD_HOST`: This is the host of your [Fluentd](https://www.fluentd.org/)
* `OBJECT_FLUENTD_PORT`: This is the port of your [Fluentd](https://www.fluentd.org/)
* `OBJECT_PROXY_HOSTNAME`: This is the hostname of your environment for use with Swagger UI, ex: `api.my.org`
* `OBJECT_IMMUTABLE`: This is a `;` separated list of collections which are immutable collections
  
## Public Domain
This repository constitutes a work of the United States Government and is not
subject to domestic copyright protection under 17 USC § 105. This repository is in
the public domain within the United States, and copyright and related rights in
the work worldwide are waived through the [CC0 1.0 Universal public domain dedication](https://creativecommons.org/publicdomain/zero/1.0/).
All contributions to this repository will be released under the CC0 dedication. By
submitting a pull request you are agreeing to comply with this waiver of
copyright interest.

## License
The repository utilizes code licensed under the terms of the Apache Software
License and therefore is licensed under ASL v2 or later.

This source code in this repository is free: you can redistribute it and/or modify it under
the terms of the Apache Software License version 2, or (at your option) any
later version.

This source code in this repository is distributed in the hope that it will be useful, but WITHOUT ANY
WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
PARTICULAR PURPOSE. See the Apache Software License for more details.

You should have received a copy of the Apache Software License along with this
program. If not, see http://www.apache.org/licenses/LICENSE-2.0.html

The source code forked from other open source projects will inherit its license.


## Privacy
This repository contains only non-sensitive, publicly available data and
information. All material and community participation is covered by the
Surveillance Platform [Disclaimer](https://github.com/CDCgov/template/blob/master/DISCLAIMER.md)
and [Code of Conduct](https://github.com/CDCgov/template/blob/master/code-of-conduct.md).
For more information about CDC's privacy policy, please visit [http://www.cdc.gov/privacy.html](http://www.cdc.gov/privacy.html).

## Contributing
Anyone is encouraged to contribute to the repository by [forking](https://help.github.com/articles/fork-a-repo)
and submitting a pull request. (If you are new to GitHub, you might start with a
[basic tutorial](https://help.github.com/articles/set-up-git).) By contributing
to this project, you grant a world-wide, royalty-free, perpetual, irrevocable,
non-exclusive, transferable license to all users under the terms of the
[Apache Software License v2](http://www.apache.org/licenses/LICENSE-2.0.html) or
later.

All comments, messages, pull requests, and other submissions received through
CDC including this GitHub page are subject to the [Presidential Records Act](http://www.archives.gov/about/laws/presidential-records.html)
and may be archived. Learn more at [http://www.cdc.gov/other/privacy.html](http://www.cdc.gov/other/privacy.html).

## Records
This repository is not a source of government records, but is a copy to increase
collaboration and collaborative potential. All government records will be
published through the [CDC web site](http://www.cdc.gov).

## Notices
Please refer to [CDC's Template Repository](https://github.com/CDCgov/template)
for more information about [contributing to this repository](https://github.com/CDCgov/template/blob/master/CONTRIBUTING.md),
[public domain notices and disclaimers](https://github.com/CDCgov/template/blob/master/DISCLAIMER.md),
and [code of conduct](https://github.com/CDCgov/template/blob/master/code-of-conduct.md).
