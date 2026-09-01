FROM trinodb/trino:455

USER root

COPY --from=minio/mc:latest /usr/bin/mc /usr/bin/mc

RUN chmod +x /usr/bin/mc