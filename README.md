PoC Trino connector for Phoenix using the thin client with the Phoenix Query Server.

Supports:
--------
- SELECT
- INSERT (UPSERT semantics)
- UPDATE
- DELETE

CREATE, etc, can be added later.

Integrated tests are currently not possible as this would require pulling
most of Hadoop.

Unzip this into the trino/plugin folder, and add trino-phoenix to Trino's toplevel pom.xml
(Known to work with Trion-473)

To build, first and install Trino:
./mvnw install -DskipTests -pl \!docs

The connector can then be build separately:
./mvnw package -DskipTests -pl plugin/trino-phoenix/

This will build a zip file in target that you unzip into the Trino
installation (again the plugin folder).

To use add a property file like this to the trino catalog:

connector.name=phoenix
connection-url=jdbc:phoenix:thin:url=http://localhost:8765;serialization=protobuf
insert.non-transactional-insert.enabled=true

And make sure that in addition to Hadoop, HBase, Zookeeper, you run the Phoenix Query Server.
(https://github.com/apache/phoenix-queryserver.git)
