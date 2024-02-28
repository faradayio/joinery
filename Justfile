# Build everything and run standalone tests.
default: trino-image check

# Use ASDF to install dev tools.
install-dev-tools:
	asdf plugin-add java https://github.com/halcyon/asdf-java.git
	asdf plugin-add maven https://github.com/halcyon/asdf-maven.git
	cd java/trino-plugin && asdf install

# Build our Trino plugin with our UDFs. This works on vanilla Trino, not
# AWS Athena 3, which uses Lambda-based UDFs.
trino-plugin:
	cd java/trino-plugin && mvn package

# Build our Trino image.
trino-image: trino-plugin
	docker build -f docker/trino/Dockerfile -t trino-joinery .

# Run our Trino image.
docker-run-trino: trino-image
	docker run --name trino-joinery -p 8080:8080 -d trino-joinery
	docker exec trino-joinery install-udfs

# Stop and delete our Trino container.
docker-rm-trino:
	docker rm -f trino-joinery

# Verify that our image works.
check:
	cargo clippy -- -D warnings
	cargo fmt -- --check
	# TODO: Set this up.
	#cargo deny check
	cargo test

# Check Trino. Assumes `docker-run-trino` has been run.
check-trino:
	cargo run -- sql-test --database "trino://anyone@localhost/memory/default" ./tests/sql/

# Access a Trino shell.
trino-shell:
	docker exec -it trino-joinery trino