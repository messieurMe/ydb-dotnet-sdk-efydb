using System;
using System.Linq;
using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;

namespace EfCore.Ydb.Query.Internal;

public class YdbQuerySqlGenerator : QuerySqlGenerator
{
    protected readonly ISqlGenerationHelper SqlGenerationHelper;
    protected bool SkipAliases = false;

    public YdbQuerySqlGenerator(QuerySqlGeneratorDependencies dependencies) : base(dependencies)
    {
        SqlGenerationHelper = dependencies.SqlGenerationHelper;
    }

    protected override Expression VisitColumn(ColumnExpression columnExpression)
    {
        if (SkipAliases)
        {
            Sql.Append(SqlGenerationHelper.DelimitIdentifier(columnExpression.Name));
        }
        else
        {
            base.VisitColumn(columnExpression);
        }

        return columnExpression;
    }

    protected override Expression VisitTable(TableExpression tableExpression)
    {
        if (SkipAliases)
        {
            Sql.Append(SqlGenerationHelper.DelimitIdentifier(tableExpression.Name, tableExpression.Schema));
        }
        else
        {
            base.VisitTable(tableExpression);
        }

        return tableExpression;
    }

    protected override Expression VisitDelete(DeleteExpression deleteExpression)
    {
        Sql.Append("DELETE FROM ");

        SkipAliases = true;
        Visit(deleteExpression.Table);
        SkipAliases = false;

        var select = deleteExpression.SelectExpression;
        var table = deleteExpression.Table;

        var deleteWithFullSelect = select.Offset != null
                                   || select.Limit != null
                                   || select.Having != null
                                   || select.Orderings.Count > 0
                                   || select.GroupBy.Count > 0
                                   || select.Projection.Count > 0
                                   || select.Tables.Count > 1
                                   || !(select.Tables.Count == 1 && select.Tables[0].Equals(table));

        if (deleteWithFullSelect)
        {
            throw new NotImplementedException("Delete with select as subquery not implemented yet");
        }
        else
        {
            Sql.Append(" WHERE ");

            SkipAliases = true;
            Visit(select.Predicate);
            SkipAliases = false;
        }

        return deleteExpression;
    }

    protected override Expression VisitUpdate(UpdateExpression updateExpression)
    {
        var select = updateExpression.SelectExpression;

        var useFullySelectDelete = select.Offset != null
                                   || select.Limit != null
                                   || select.Having != null
                                   || select.Orderings.Count > 0
                                   || select.GroupBy.Count > 0
                                   || select.Projection.Count > 0
                                   || select.Tables.Count > 1
                                   || !(select.Tables.Count == 1 &&
                                        select.Tables[0].Equals(updateExpression.Table));

        Sql.Append("UPDATE ");

        if (!useFullySelectDelete)
        {
            SkipAliases = true;
            Visit(updateExpression.Table);
            SkipAliases = false;

            Sql.AppendLine()
                .Append("SET ")
                .Append(SqlGenerationHelper.DelimitIdentifier(updateExpression.ColumnValueSetters[0].Column.Name))
                .Append(" = ");

            SkipAliases = true;
            Visit(updateExpression.ColumnValueSetters[0].Value);
            SkipAliases = false;

            using (Sql.Indent())
            {
                foreach (var columnValueSetter in updateExpression.ColumnValueSetters.Skip(1))
                {
                    Sql
                        .AppendLine(",")
                        .Append($"{SqlGenerationHelper.DelimitIdentifier(columnValueSetter.Column.Name)} = ");

                    SkipAliases = true;
                    Visit(columnValueSetter.Value);
                    SkipAliases = false;
                }
            }

            var predicate = select.Predicate;
            if (predicate != null)
            {
                Sql.AppendLine().Append("WHERE ");

                SkipAliases = true;
                Visit(predicate);
                SkipAliases = false;
            }
        }
        else
        {
            throw new NotImplementedException("Update with select as subquery not implemented yet");
        }

        return updateExpression;
    }
}
