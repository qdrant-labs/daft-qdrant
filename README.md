# daft-qdrant

Community extension for [Daft](https://github.com/Eventual-Inc/Daft) to write vector embeddings and their payloads into [Qdrant](https://qdrant.tech/) collections.

## Installation

```bash
pip install daft-qdrant
```

## Quick start

```python
import daft
import daft_qdrant  # registers DataFrame.write_qdrant

df = daft.from_pydict({
    "id": [1, 2, 3],
    "vector": [[0.1, 0.2, 0.3], [0.4, 0.5, 0.6], [0.7, 0.8, 0.9]],
    "label": ["cat", "dog", "bird"],
})

df.write_qdrant("my-collection", url="http://localhost:6333")
```

The target collection must exist before writing. Each row becomes a Qdrant point:
- `id` column → point id (unsigned integer or UUID string)
- `vector` column → point vector (list of floats, or dict for named vectors)
- all other columns → point payload

## Embedding pipeline example

```python
import os
import daft
import daft_qdrant
from daft.functions import monotonically_increasing_id
from daft.functions.ai import embed_text
from qdrant_client import QdrantClient, models

url = "http://localhost:6333"
vector_size = 768

QdrantClient(url=url).create_collection(
    "daft-qdrant-example",
    vectors_config=models.VectorParams(size=vector_size, distance=models.Distance.COSINE),
)

(
    daft.read_huggingface("Open-Orca/OpenOrca")
    .limit(100)
    .with_column(
        "vector",
        embed_text(daft.col("response"), provider="sentence_transformers", model="BAAI/bge-base-en-v1.5"),
    )
    .with_column("id", monotonically_increasing_id())
    .write_qdrant("daft-qdrant-example", url=url)
)
```

## Routing rows to multiple collections

Pass a Daft expression as `collection_name` to route rows to different collections
based on a column value:

```python
df.write_qdrant(daft.col("collection"), url="http://localhost:6333")
```

## Column remapping

Use `id_column` and `vector_column` to map differently named columns to the
required `id` and `vector` roles:

```python
df.write_qdrant(
    "my-collection",
    url="http://localhost:6333",
    id_column="doc_id",
    vector_column="embedding",
)
```

## Links

- [Daft documentation](https://docs.daft.ai)
- [Community extensions](https://docs.daft.ai/en/latest/extensions/community/)
- [Qdrant documentation](https://qdrant.tech/documentation/)
