# Postgres Container

For development I used the following postgres container:


```yaml
version: '3.1'

services:

  db:
    image: postgres
    restart: unless-stopped
    environment:
      POSTGRES_PASSWORD: example
      POSTGRES_HOST_AUTH_METHOD: trust
      PGDATA: /var/lib/postgresql/data/pgdata
    volumes:
      - ./data/pgdata:/var/lib/postgresql/data/pgdata
    ports:
      - 5432:5432

  adminer:
    image: adminer
    restart: always
    ports:
      - 8787:8080
  pgadmin:
      container_name: pgadmin4_container
      image: dpage/pgadmin4
      restart: always
      environment:
        PGADMIN_DEFAULT_EMAIL: admin@admin.com
        PGADMIN_DEFAULT_PASSWORD: root
      volumes:
        - ./data/pgadmin:/var/lib/pgadmin
      ports:
        - "5050:80"
```

