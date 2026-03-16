"""Helper utilities for optional tqdm progress bars."""

from __future__ import annotations

from typing import Iterable, Iterator, TypeVar

try:  # noqa: SIM105 - optional dependency style
    from tqdm import tqdm
except Exception:  # pragma: no cover - tqdm always available via requirements
    tqdm = None  # type: ignore

T = TypeVar("T")


def iter_progress(
    iterable: Iterable[T],
    *,
    enabled: bool,
    desc: str | None = None,
    total: int | None = None,
) -> Iterator[T]:
    if not enabled or tqdm is None:
        for item in iterable:
            yield item
        return
    yield from tqdm(iterable, desc=desc, total=total)
