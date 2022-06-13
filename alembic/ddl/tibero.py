from __future__ import annotations

from typing import Any
from typing import Optional
from typing import TYPE_CHECKING
from typing import Union

from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql import sqltypes

from .base import AddColumn
from .base import alter_table
from .base import ColumnComment
from .base import ColumnDefault
from .base import ColumnName
from .base import ColumnNullable
from .base import ColumnType
from .base import format_column_name
from .base import format_server_default
from .base import format_table_name
from .base import format_type
from .base import IdentityColumnDefault
from .base import RenameTable
from .impl import DefaultImpl

if TYPE_CHECKING:
    from sqlalchemy.dialects.tibero.base import TiberoDDLCompiler
    from sqlalchemy.engine.cursor import CursorResult
    from sqlalchemy.engine.cursor import LegacyCursorResult
    from sqlalchemy.sql.schema import Column


class TiberoImpl(DefaultImpl):
    __dialect__ = "tibero"
    transactional_ddl = False
    batch_separator = "/"
    command_terminator = ""
    type_synonyms = DefaultImpl.type_synonyms + (
        {"VARCHAR", "VARCHAR2"},
        {"BIGINT", "INTEGER", "SMALLINT", "DECIMAL", "NUMERIC", "NUMBER"},
        {"DOUBLE", "FLOAT", "DOUBLE_PRECISION"},
    )
    identity_attrs_ignore = ()

    def __init__(self, *arg, **kw) -> None:
        super(TiberoImpl, self).__init__(*arg, **kw)
        self.batch_separator = self.context_opts.get(
            "tibero_batch_separator", self.batch_separator
        )

    def _exec(
        self, construct: Any, *args, **kw
    ) -> Optional[Union["LegacyCursorResult", "CursorResult"]]:
        result = super(TiberoImpl, self)._exec(construct, *args, **kw)
        if self.as_sql and self.batch_separator:
            self.static_output(self.batch_separator)
        return result

    def emit_begin(self) -> None:
        self._exec("SET TRANSACTION READ WRITE")

    def emit_commit(self) -> None:
        self._exec("COMMIT")


@compiles(AddColumn, "tibero")
def visit_add_column(
    element: "AddColumn", compiler: "TiberoDDLCompiler", **kw
) -> str:
    return "%s %s" % (
        alter_table(compiler, element.table_name, element.schema),
        add_column(compiler, element.column, **kw),
    )


@compiles(ColumnNullable, "tibero")
def visit_column_nullable(
    element: "ColumnNullable", compiler: "TiberoDDLCompiler", **kw
) -> str:
    return "%s %s %s" % (
        alter_table(compiler, element.table_name, element.schema),
        alter_column(compiler, element.column_name),
        "NULL" if element.nullable else "NOT NULL",
    )


@compiles(ColumnType, "tibero")
def visit_column_type(
    element: "ColumnType", compiler: "TiberoDDLCompiler", **kw
) -> str:
    return "%s %s %s" % (
        alter_table(compiler, element.table_name, element.schema),
        alter_column(compiler, element.column_name),
        "%s" % format_type(compiler, element.type_),
    )


@compiles(ColumnName, "tibero")
def visit_column_name(
    element: "ColumnName", compiler: "TiberoDDLCompiler", **kw
) -> str:
    return "%s RENAME COLUMN %s TO %s" % (
        alter_table(compiler, element.table_name, element.schema),
        format_column_name(compiler, element.column_name),
        format_column_name(compiler, element.newname),
    )


@compiles(ColumnDefault, "tibero")
def visit_column_default(
    element: "ColumnDefault", compiler: "TiberoDDLCompiler", **kw
) -> str:
    return "%s %s %s" % (
        alter_table(compiler, element.table_name, element.schema),
        alter_column(compiler, element.column_name),
        "DEFAULT %s" % format_server_default(compiler, element.default)
        if element.default is not None
        else "DEFAULT NULL",
    )


@compiles(ColumnComment, "tibero")
def visit_column_comment(
    element: "ColumnComment", compiler: "TiberoDDLCompiler", **kw
) -> str:
    ddl = "COMMENT ON COLUMN {table_name}.{column_name} IS {comment}"

    comment = compiler.sql_compiler.render_literal_value(
        (element.comment if element.comment is not None else ""),
        sqltypes.String(),
    )

    return ddl.format(
        table_name=element.table_name,
        column_name=element.column_name,
        comment=comment,
    )


@compiles(RenameTable, "tibero")
def visit_rename_table(
    element: "RenameTable", compiler: "TiberoDDLCompiler", **kw
) -> str:
    return "%s RENAME TO %s" % (
        alter_table(compiler, element.table_name, element.schema),
        format_table_name(compiler, element.new_table_name, None),
    )


def alter_column(compiler: "TiberoDDLCompiler", name: str) -> str:
    return "MODIFY %s" % format_column_name(compiler, name)


def add_column(compiler: "TiberoDDLCompiler", column: "Column", **kw) -> str:
    return "ADD %s" % compiler.get_column_specification(column, **kw)


@compiles(IdentityColumnDefault, "tibero")
def visit_identity_column(
    element: "IdentityColumnDefault", compiler: "TiberoDDLCompiler", **kw
):
    text = "%s %s " % (
        alter_table(compiler, element.table_name, element.schema),
        alter_column(compiler, element.column_name),
    )
    if element.default is None:
        # drop identity
        text += "DROP IDENTITY"
        return text
    else:
        text += compiler.visit_identity_column(element.default)
        return text
