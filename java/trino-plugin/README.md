# Experimental Trino Plugin

Install the [`asdf` version manager][asdf] and the plugins needed for Java:

```sh
asdf plugin-add java https://github.com/halcyon/asdf-java.git
asdf plugin-add maven https://github.com/halcyon/asdf-maven.git
```

Then install a Java environment using versions specified in `.tool-versions`:

```sh
asdf install
```

You should be able to "package" the plugin into `./target/` using:

```sh
mvn package
```

Then you can run Trino with the plugin using:

```sh
docker run --name trino -d --mount type=bind,source="$(pwd)"/target,target=/usr/lib/trino/plugin/joinery -p 8080:8080 trinodb/trino
```

This should give you a working `FARM_FINGERPRINT` function, which you can test
as follows:

```txt
❯ docker exec -it trino trino
trino> select farm_fingerprint('Hello');
        _col0         
----------------------
 -3042045079152025465 
```

[asdf]: https://asdf-vm.com/
