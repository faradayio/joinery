# A Dockerfile which builds a Trino image with our UDFs.

# Use the official Trino image as the base image.
#
# TODO: We'll eventually freeze this to a known-good version.
FROM trinodb/trino:latest

# Copy ./java/trino-plugin/target/ to /usr/lib/trino/plugin/joinery.
ADD ./java/trino-plugin/target/ /usr/lib/trino/plugin/joinery
ADD ./sql/trino_compat.sql /etc/trino/
ADD ./docker/trino/install-udfs /usr/local/bin/