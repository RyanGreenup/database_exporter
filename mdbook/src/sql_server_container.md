# SQL Server Container

## Components
### Docker Compose

```yaml
version: '3.8'

services:
  sql-server:
    image: mcr.microsoft.com/mssql/server
    container_name: sql-server-container
    environment:
      SA_PASSWORD: ${SA_PASSWORD}
      ACCEPT_EULA: Y
    ports:
      - "1433:1433"
    # volumes:
    #   - "./Chinook_SqlServer.sql:/docker-entrypoint-initdb.d/1.sql"
```

### ENV


> [!NOTE]
> Microsoft software is highly interactive, which can be confusing inside containers, here is the Password requirements
>
> - Greater than 8 Characters
> - Upper Case
> - Lowercase
> - digits
> - Symbols
>
>
> ```
> 2025-01-29 04:01:41.14 spid54s     ERROR: Unable to set system administrator password: Password validation failed. The password does not meet SQL Server password policy requirements because it is not complex enough. The password must be at least 8 characters long and contain characters from three of the following four sets: Uppercase letters, Lowercase letters, Base 10 digits, and Symbols..
> 2025-01-29 04:01:41.15 spid54s     An error occurred during server setup. See previous errors for more information.
> ```


```sh
SA_PASSWORD=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx

# username is sa
# database is master
# https://medium.com/@seventechnologiescloud/local-sqlserver-database-via-docker-compose-the-ultimate-guide-f1d9f0ac1354
```

### Get Data

```sh
curl \
    https://github.com/lerocha/chinook-database/releases/download/v1.4.5/Chinook_SqlServer.sql \
    > Chinook_SqlServer.sql
```

## Usage

```
# Start the container
docker compose down
docker compose up -d
docker compose logs -f

# Import the Data
sqlcmd \
    -H localhost \
    -P '238923klsdklsdklDSDSDS@!!@' \
    -U 'sa' \
    -C  \
    -i Chinook_SqlServer.sql

```

