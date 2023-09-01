<!--- Code generated with go generate ./... DO NOT EDIT. --->
# Configuration

## Environment Variables

The Service uses the following environment variables:

* `VOTE_PORT`: Port on which the service listen on. The default is `9013`.
* `MESSAGE_BUS_HOST`: Host of the redis server. The default is `localhost`.
* `MESSAGE_BUS_PORT`: Port of the redis server. The default is `6379`.
* `OPENSLIDES_DEVELOPMENT`: If set, the service uses the default secrets. The default is `false`.
* `DATABASE_PASSWORD_FILE`: Postgres Password. The default is `/run/secrets/postgres_password`.
* `DATABASE_USER`: Postgres Database. The default is `openslides`.
* `DATABASE_HOST`: Postgres Host. The default is `localhost`.
* `DATABASE_PORT`: Postgres Post. The default is `5432`.
* `DATABASE_NAME`: Postgres User. The default is `openslides`.
* `AUTH_PROTOCOL`: Protocol of the auth service. The default is `http`.
* `AUTH_HOST`: Host of the auth service. The default is `localhost`.
* `AUTH_PORT`: Port of the auth service. The default is `9004`.
* `AUTH_FAKE`: Use user id 1 for every request. Ignores all other auth environment variables. The default is `false`.
* `AUTH_TOKEN_KEY_FILE`: Key to sign the JWT auth tocken. The default is `/run/secrets/auth_token_key`.
* `AUTH_COOKIE_KEY_FILE`: Key to sign the JWT auth cookie. The default is `/run/secrets/auth_cookie_key`.
* `CACHE_HOST`: Host of the redis used for the fast backend. The default is `localhost`.
* `CACHE_PORT`: Port of the redis used for the fast backend. The default is `6379`.
* `VOTE_DATABASE_PASSWORD_FILE`: Password of the postgres database used for long polls. The default is `/run/secrets/postgres_password`.
* `VOTE_DATABASE_USER`: Databasename of the postgres database used for long polls. The default is `openslides`.
* `VOTE_DATABASE_HOST`: Host of the postgres database used for long polls. The default is `localhost`.
* `VOTE_DATABASE_PORT`: Port of the postgres database used for long polls. The default is `5432`.
* `VOTE_DATABASE_NAME`: Name of the database to save long running polls. The default is `openslides`.
* `VOTE_SINGLE_INSTANCE`: More performance if the serice is not scalled horizontally. The default is `false`.
