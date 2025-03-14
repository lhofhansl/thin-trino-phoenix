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

import com.google.common.base.Joiner;
import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.trino.plugin.jdbc.DefaultQueryBuilder;
import io.trino.plugin.jdbc.JdbcAssignmentItem;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcNamedRelationHandle;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.PreparedQuery;
import io.trino.plugin.jdbc.QueryParameter;
import io.trino.plugin.jdbc.WriteFunction;
import io.trino.plugin.jdbc.expression.ParameterizedExpression;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static java.lang.String.format;
import static java.util.stream.Collectors.joining;

public class UpsertQueryBuilder
        extends DefaultQueryBuilder
{
    @Inject
    public UpsertQueryBuilder(RemoteQueryModifier queryModifier)
    {
        super(queryModifier);
    }

/*
    Rewrite     UPDATE T SET c1 = expr1, c2 = expr2 WHERE ...
                    to
                UPSERT INTO T (keys, c1, c2) SELECT keys, expr1, expr2 FROM T WHERE ...
                (exprs have to be constant)
*/
    public PreparedQuery prepareUpdateQuery(
            JdbcClient client,
            ConnectorSession session,
            Connection connection,
            JdbcNamedRelationHandle baseRelation,
            TupleDomain<ColumnHandle> tupleDomain,
            Optional<ParameterizedExpression> additionalPredicate,
            List<JdbcAssignmentItem> assignments)
    {
        ImmutableList.Builder<String> conjuncts = ImmutableList.builder();
        ImmutableList.Builder<QueryParameter> accumulator = ImmutableList.builder();

        List<String> keys = new ArrayList<>();
        try (ResultSet rs = connection.getMetaData().getColumns("", "", baseRelation.getRemoteTableName().getTableName(), "")) {
            while (rs.next()) {
                if (rs.getString("KEY_SEQ") != null) {
                    keys.add(rs.getString("COLUMN_NAME"));
                }
            }
        }
        catch (SQLException x) {
            throw new RuntimeException(x);
        }

        String keyList = Joiner.on(", ").join(keys);
        // TODO: Check if columns contain any keys -> fail
        String columnList = assignments.stream().map(JdbcAssignmentItem::column).map(JdbcColumnHandle::getColumnName).collect(joining(", "));
        String exprList = assignments.stream()
                .map(JdbcAssignmentItem::column)
                .map(columnHandle -> {
                    String bindExpression = getWriteFunction(
                            client,
                            session,
                            connection,
                            columnHandle.getJdbcTypeHandle(),
                            columnHandle.getColumnType())
                            .getBindExpression();
                    return bindExpression;
                })
                .collect(joining(", "));

        String sql = "UPSERT INTO " + getRelation(client, baseRelation.getRemoteTableName());
        sql += " (" + keyList + ", " + columnList + ") ";
        sql += "SELECT " + keyList + ", " + exprList + " ";
        assignments.forEach(entry -> {
            JdbcColumnHandle columnHandle = entry.column();
            accumulator.add(
                    new QueryParameter(
                            columnHandle.getJdbcTypeHandle(),
                            columnHandle.getColumnType(),
                            entry.queryParameter().getValue()));
        });

        sql += getFrom(client, baseRelation, accumulator::add);

        toConjuncts(client, session, connection, tupleDomain, conjuncts, accumulator::add);
        additionalPredicate.ifPresent(predicate -> {
            conjuncts.add(predicate.expression());
            accumulator.addAll(predicate.parameters());
        });
        List<String> clauses = conjuncts.build();
        if (!clauses.isEmpty()) {
            sql += " WHERE " + Joiner.on(" AND ").join(clauses);
        }
        return new PreparedQuery(sql, accumulator.build());
    }

    private static WriteFunction getWriteFunction(JdbcClient client, ConnectorSession session, Connection connection, JdbcTypeHandle jdbcType, Type type)
    {
        WriteFunction writeFunction = client.toColumnMapping(session, connection, jdbcType)
                .orElseThrow(() -> new VerifyException(format("Unsupported type %s with handle %s", type, jdbcType)))
                .getWriteFunction();
        verify(writeFunction.getJavaType() == type.getJavaType(), "Java type mismatch: %s, %s", writeFunction, type);
        return writeFunction;
    }
}
