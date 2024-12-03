# A Dockerfile which builds a Trino image with our UDFs.

# Use the official Trino image as the base image.
FROM trinodb/trino:443

# Copy ./java/trino-plugin/target/ to /usr/lib/trino/plugin/joinery.
ADD ./java/trino-plugin/target/ /usr/lib/trino/plugin/joinery
ADD ./sql/trino_compat.sql /etc/trino/
ADD ./docker/trino/install-udfs /usr/local/bin/