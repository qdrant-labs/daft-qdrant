"""Qdrant community extension for Daft.

Importing this module registers a ``write_qdrant`` method on ``daft.DataFrame``.

Example::

    import daft
    import daft_qdrant  # registers DataFrame.write_qdrant

    df = daft.from_pydict({
        "id": [1, 2, 3],
        "vector": [[0.1, 0.2], [0.3, 0.4], [0.5, 0.6]],
        "label": ["a", "b", "c"],
    })
    df.write_qdrant("my-collection", url="http://localhost:6333")
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import daft

from daft_qdrant.sink import QdrantDataSink

if TYPE_CHECKING:
    from daft.expressions import Expression


def _write_qdrant(
    self: daft.DataFrame,
    collection_name: str | Expression,
    url: str | None = None,
    api_key: str | None = None,
    id_column: str | None = None,
    vector_column: str | None = None,
    client_kwargs: dict[str, Any] | None = None,
    upsert_kwargs: dict[str, Any] | None = None,
) -> daft.DataFrame:
    """Write the DataFrame to a Qdrant collection.

    Each row becomes a Qdrant point. The DataFrame must have an ``id`` column
    (unsigned integer or UUID string) and a ``vector`` column (list of floats for
    a single vector, or dict for named vectors). All other columns become the
    point payload.

    Use ``id_column`` / ``vector_column`` to map differently named columns to
    the required ``id`` / ``vector`` roles.

    The target collection must already exist in Qdrant before writing.

    Args:
        collection_name: Collection to write to. Pass a string for a single
            collection or a Daft expression to route rows to different
            collections based on a column value.
        url: Qdrant server URL, e.g. ``"http://localhost:6333"``.
        api_key: Qdrant API key. Falls back to ``QDRANT_API_KEY`` env var.
        id_column: Column to use as the Qdrant point id (renamed to ``"id"``
            before writing).
        vector_column: Column to use as the Qdrant point vector (renamed to
            ``"vector"`` before writing).
        client_kwargs: Extra keyword arguments forwarded to
            ``qdrant_client.QdrantClient()``.  ``url`` and ``api_key`` are
            merged in automatically; passing them here again raises an error.
        upsert_kwargs: Extra keyword arguments forwarded to
            ``QdrantClient.upsert()``.

    Returns:
        A DataFrame with a single ``write_responses`` column containing one
        :class:`~daft.io.sink.WriteResult` per partition written.

    Examples:
        Write a DataFrame with explicit id / vector columns::

            import daft
            import daft_qdrant

            df = daft.from_pydict({
                "id": [1, 2, 3],
                "vector": [[0.1, 0.2], [0.3, 0.4], [0.5, 0.6]],
            })
            df.write_qdrant("my-collection", url="http://localhost:6333")

        Route rows to different collections via an expression::

            import daft
            import daft_qdrant

            df = daft.from_pydict({
                "id": [1, 2],
                "vector": [[0.1, 0.2], [0.3, 0.4]],
                "collection": ["col-a", "col-b"],
            })
            df.write_qdrant(daft.col("collection"), url="http://localhost:6333")
    """
    sink = QdrantDataSink(
        collection_name=collection_name,
        url=url,
        api_key=api_key,
        id_column=id_column,
        vector_column=vector_column,
        client_kwargs=client_kwargs,
        upsert_kwargs=upsert_kwargs,
    )
    return self.write_sink(sink)


daft.DataFrame.write_qdrant = _write_qdrant  # type: ignore[attr-defined]

__all__ = ["QdrantDataSink"]
