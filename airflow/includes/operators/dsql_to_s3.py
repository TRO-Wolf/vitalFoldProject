"""
DSQLToS3Operator — ``SqlToS3Operator`` specialised for Amazon Aurora DSQL.

WHY THIS EXISTS
───────────────
``apache-airflow-providers-amazon`` 9.12.0's
:class:`~airflow.providers.amazon.aws.transfers.sql_to_s3.SqlToS3Operator`
has two properties we need to work around:

1. **No provider registration for DSQL** — its default ``_get_hook()``
   calls ``BaseHook.get_connection(sql_conn_id).get_hook()``, which
   only resolves for ``conn_type`` values registered by an installed
   Airflow provider package. Aurora DSQL isn't a stock provider, so we
   instantiate :class:`DSQLSqlHook` directly in ``_get_hook``.

2. **Dataframe serialization is pandas-only** — its ``execute()``
   hardcodes ``df_type="pandas"`` and then writes the frame via
   ``getattr(df, "to_parquet")(buf)`` / ``to_csv`` / ``to_json``. Those
   are pandas-only method names — modern polars DataFrames expose
   ``write_parquet`` / ``write_csv`` / ``write_json`` instead, and
   calling ``to_parquet`` on a polars frame raises ``AttributeError``.

To fix both issues in one place we override ``execute()`` completely.
The new body fetches the DataFrame in the requested ``df_type`` and
then dispatches on a single ``(df_type, file_format)`` matrix — so the
full cartesian product (pandas × {csv, parquet, json}, polars × {csv,
parquet, json}) is explicitly supported in one readable place.

SERIALIZATION MATRIX
────────────────────
                    csv                  parquet              json
    pandas   df.to_csv(text_buf)    df.to_parquet(buf)   df.to_json(text_buf)
    polars   df.write_csv(buf)      df.write_parquet(buf) df.write_json(buf)

USAGE
─────
.. code-block:: python

    from includes.operators.dsql_to_s3 import DSQLToS3Operator

    DSQLToS3Operator(
        task_id="appointment_extraction_task",
        sql_conn_id="vital_fold_dsql",
        aws_conn_id="vital_fold_aws",
        query="SELECT * FROM vital_fold.appointment WHERE appointment_datetime::date = %(ds)s",
        parameters={"ds": "{{ ds }}"},
        s3_bucket="vital-fold-bronze-bucket-v1",
        s3_key="bronze/appointment/{{ ds }}.parquet",
        file_format="parquet",
        df_type="polars",          # ← custom kwarg added by this subclass
        replace=True,
        sql_hook_params={
            "cluster_identifier": "xxxxxxxxxxxxxxxx.dsql.us-east-2.on.aws",
            "default_host":       "xxxxxxxxxxxxxxxx.dsql.us-east-2.on.aws",
            "database":           "postgres",
        },
    )

LIMITATIONS
───────────
Since we override ``execute()`` entirely, the parent's
``max_rows_per_file``, ``groupby_kwargs``, and ``pd_kwargs`` knobs are
**not** honoured — they apply only to the parent's partitioned
upload path. This operator always writes a single object per task. If
you need row-count or group-based partitioning, use the stock
``SqlToS3Operator`` (and stick to pandas) instead of this subclass.
``pd_kwargs`` is still forwarded to the pandas serializer for
compatibility with existing DAGs that use it to pass ``orient``,
``index``, etc., but for the polars path it's silently ignored.
"""

from __future__ import annotations

import io
from typing import TYPE_CHECKING, Any, Literal

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.transfers.sql_to_s3 import (
    FILE_FORMAT,
    SqlToS3Operator,
)

from includes.hooks.dsql import DSQLSqlHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class DSQLToS3Operator(SqlToS3Operator):
    """``SqlToS3Operator`` that sources rows from Amazon Aurora DSQL and
    supports both pandas and polars DataFrames as the in-memory
    serialization layer."""

    def __init__(
        self,
        *,
        sql_conn_id: str = "aws_dsql_default",
        sql_hook_params: dict[str, Any] | None = None,
        df_type: Literal["pandas", "polars"] = "pandas",
        **kwargs,
    ) -> None:
        # ``sql_conn_id`` is required by the parent constructor but we
        # never feed it to BaseHook.get_connection — see _get_hook below.
        super().__init__(
            sql_conn_id=sql_conn_id,
            sql_hook_params=sql_hook_params,
            **kwargs,
        )
        if df_type not in ("pandas", "polars"):
            raise AirflowException(
                f"df_type must be 'pandas' or 'polars', got: {df_type!r}"
            )
        self.df_type = df_type

    # ── Hook resolution ──────────────────────────────────────────────────

    def _get_hook(self) -> DSQLSqlHook:
        """
        Build a :class:`DSQLSqlHook` from ``sql_conn_id`` and
        ``sql_hook_params``.

        Bypasses the parent's ``BaseHook.get_connection(...).get_hook()``
        path, which only works for conn_types registered in an installed
        Airflow provider package.

        ``sql_conn_id`` is always forwarded as ``dsql_conn_id`` so the hook
        resolves host/cluster from the correct Airflow connection.  Any
        explicit keys in ``sql_hook_params`` take precedence.
        """
        params: dict[str, Any] = {
            "dsql_conn_id": self.sql_conn_id,
            "aws_conn_id": getattr(self, "aws_conn_id", None) or self.sql_conn_id,
        }
        params.update(self.sql_hook_params or {})

        self.log.debug(
            "Instantiating DSQLSqlHook (resolved params=%s)", params,
        )
        hook = DSQLSqlHook(**params)

        # Same duck-type check the parent does — guards against a future
        # refactor that removes ``get_df`` from ``DSQLSqlHook``.
        if not callable(getattr(hook, "get_df", None)):
            raise AirflowException(
                "DSQLSqlHook is missing a callable get_df method; "
                "DSQLToS3Operator cannot proceed."
            )
        return hook

    # ── Execution ────────────────────────────────────────────────────────

    def execute(self, context: Context) -> None:
        """Fetch → serialize → upload. One unified path covering all four
        ``(df_type, file_format)`` combinations."""
        hook = self._get_hook()

        df = hook.get_df(
            sql=self.query,
            parameters=self.parameters,
            df_type=self.df_type,
        )
        self.log.info(
            "Fetched DataFrame (df_type=%s, shape=%s) from DSQL",
            self.df_type,
            self._df_shape(df),
        )

        buf = self._serialize_df(df)
        self.log.info(
            "Uploading %s serialized as %s to s3://%s/%s",
            self.df_type,
            self.file_format.name.lower(),
            self.s3_bucket,
            self.s3_key,
        )
        s3 = S3Hook(aws_conn_id=self.aws_conn_id, verify=self.verify)
        s3.load_file_obj(
            file_obj=buf,
            key=self.s3_key,
            bucket_name=self.s3_bucket,
            replace=self.replace,
        )

    # ── Serialization helpers ────────────────────────────────────────────

    def _serialize_df(self, df) -> io.BytesIO:
        """Dispatch to the right serializer for the (df_type, file_format) pair."""
        buf = io.BytesIO()
        if self.df_type == "pandas":
            self._write_pandas(df, buf)
        else:  # polars
            self._write_polars(df, buf)
        buf.seek(0)
        return buf

    def _write_pandas(self, df, buf: io.BytesIO) -> None:
        """Serialize a pandas DataFrame into ``buf`` using pandas-native
        ``df.to_*`` methods.

        CSV and JSON take a different route than Parquet: pandas'
        ``to_csv`` / ``to_json`` return a ``str`` when called with no
        ``path_or_buf`` argument, so we encode that string to UTF-8
        bytes and write it to the caller's ``BytesIO``. Wrapping
        ``buf`` in an ``io.TextIOWrapper`` would be the obvious-looking
        alternative but the wrapper closes the underlying binary
        buffer when it gets garbage-collected — which happens as soon
        as this method returns, breaking the caller's ``buf.seek(0)``
        with ``ValueError: I/O operation on closed file``.
        """
        pd_kwargs = self.pd_kwargs or {}

        if self.file_format == FILE_FORMAT.PARQUET:
            df.to_parquet(buf, **pd_kwargs)
        elif self.file_format == FILE_FORMAT.CSV:
            buf.write(df.to_csv(**pd_kwargs).encode("utf-8"))
        elif self.file_format == FILE_FORMAT.JSON:
            buf.write(df.to_json(**pd_kwargs).encode("utf-8"))
        else:
            raise AirflowException(
                f"Unsupported file_format for pandas path: {self.file_format!r}"
            )

    @staticmethod
    def _cast_object_columns(df):
        """Cast any ``Object``-typed columns to ``Utf8`` so Parquet/CSV
        serialization succeeds (Polars cannot write ``Object`` dtype)."""
        import polars as pl

        casts = [
            pl.col(name).cast(pl.Utf8)
            for name, dtype in df.schema.items()
            if dtype == pl.Object
        ]
        return df.with_columns(casts) if casts else df

    def _write_polars(self, df, buf: io.BytesIO) -> None:
        """Serialize a polars DataFrame into ``buf`` using polars-native
        ``df.write_*`` methods. All three formats accept a binary
        buffer directly — polars handles encoding internally."""
        df = self._cast_object_columns(df)
        if self.file_format == FILE_FORMAT.PARQUET:
            df.write_parquet(buf)
        elif self.file_format == FILE_FORMAT.CSV:
            df.write_csv(buf)
        elif self.file_format == FILE_FORMAT.JSON:
            # write_json emits a single JSON array (matches pandas
            # to_json default orient="columns"-ish behaviour well enough
            # for downstream consumers). Use write_ndjson manually if
            # you need newline-delimited JSON.
            df.write_json(buf)
        else:
            raise AirflowException(
                f"Unsupported file_format for polars path: {self.file_format!r}"
            )

    @staticmethod
    def _df_shape(df) -> tuple[int, int]:
        """Return a ``(rows, cols)`` tuple for either a pandas or a polars frame."""
        # pandas uses .shape → (rows, cols); polars does too, but also
        # exposes .height / .width. Either way, .shape works on both.
        return tuple(df.shape)  # type: ignore[return-value]
