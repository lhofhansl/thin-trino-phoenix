/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.phoenix;

import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ConfiguringConnectionFactory;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.DriverConnectionFactory;
import io.trino.plugin.jdbc.ForBaseJdbc;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcJoinPushdownSupportModule;
import io.trino.plugin.jdbc.JdbcStatisticsConfig;
import io.trino.plugin.jdbc.JdbcWriteConfig;
import io.trino.plugin.jdbc.QueryBuilder;
import io.trino.plugin.jdbc.credential.CredentialProvider;
import io.trino.plugin.jdbc.ptf.Query;
import io.trino.spi.function.table.ConnectorTableFunction;
import org.apache.phoenix.queryserver.client.Driver;

import java.util.Properties;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;

public final class PhoenixClientModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        binder.bind(JdbcClient.class).annotatedWith(ForBaseJdbc.class).to(PhoenixClient.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(JdbcStatisticsConfig.class);
        install(new JdbcJoinPushdownSupportModule());
        newSetBinder(binder, ConnectorTableFunction.class).addBinding().toProvider(Query.class).in(Scopes.SINGLETON);
        newOptionalBinder(binder, QueryBuilder.class).setBinding().to(UpsertQueryBuilder.class).in(Scopes.SINGLETON);

        // there is no advantage in Phoenix to insert into a separate table first
        // (and in fact until table creation works it fails anyway)
        configBinder(binder).bindConfigDefaults(JdbcWriteConfig.class, config -> config.setNonTransactionalInsert(true));
        configBinder(binder).bindConfigDefaults(JdbcWriteConfig.class, config -> config.setNonTransactionalMerge(true));
    }

    @Provides
    @Singleton
    @ForBaseJdbc
    public static ConnectionFactory connectionFactory(BaseJdbcConfig config, CredentialProvider credentialProvider, OpenTelemetry openTelemetry)
    {
        Properties connectionProperties = new Properties();

        return new ConfiguringConnectionFactory(
                DriverConnectionFactory.builder(
                            new Driver(),
                            config.getConnectionUrl(),
                            credentialProvider)
                    .setConnectionProperties(connectionProperties)
                    .setOpenTelemetry(openTelemetry)
                    .build(),
                connection -> {
                    // The seems PhoenixDriver to default autocommit to false (in violation of the JDBC spec)
                    connection.setAutoCommit(true);
                });
    }
}
