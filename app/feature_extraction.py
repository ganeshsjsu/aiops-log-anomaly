"""Utilities to transform semi-structured logs into ML friendly feature vectors."""
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Sequence

import pandas as pd
from sklearn.feature_extraction.text import HashingVectorizer


@dataclass
class FeatureConfig:
    """Configuration for the hashing vectorizer."""

    n_features: int = 2 ** 12
    alternate_sign: bool = False
    ngram_range: tuple[int, int] = (1, 2)


def build_vectorizer(config: FeatureConfig | None = None) -> HashingVectorizer:
    cfg = config or FeatureConfig()
    return HashingVectorizer(
        n_features=cfg.n_features,
        alternate_sign=cfg.alternate_sign,
        norm="l2",
        ngram_range=cfg.ngram_range,
    )


def _compose_text(record: dict | pd.Series) -> str:
    parts: List[str] = []
    for key in ("service", "level", "host", "trace_id", "message"):
        value = record.get(key)
        if value:
            parts.append(str(value))
    return " ".join(parts)


def corpus_from_records(records: Sequence[dict]) -> List[str]:
    return [_compose_text(record) for record in records]


def feature_matrix(
    vectorizer: HashingVectorizer, records: Sequence[dict]
) -> "scipy.sparse.csr_matrix":
    corpus = corpus_from_records(records)
    return vectorizer.transform(corpus)


def feature_matrix_from_df(
    vectorizer: HashingVectorizer, dataframe: pd.DataFrame
) -> "scipy.sparse.csr_matrix":
    return feature_matrix(vectorizer, dataframe.to_dict(orient="records"))
