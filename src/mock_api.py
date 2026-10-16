import json
import math
import os
from pathlib import Path
import random
from typing import Annotated, Any, Dict, List, Optional

from fastapi import FastAPI, Query, Response
from fastapi.responses import JSONResponse
import uvicorn

from data.generate_data import generate_synthetic_data

app = FastAPI(title="Mock E-Commerce API", version="1.0.0")

DATA_DIR = Path(__file__).resolve().parent.parent / "data"


def load_dataset(filename: str, fallback_generator_idx: int) -> List[Dict[str, Any]]:
    path = DATA_DIR / filename
    if path.exists():
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    customers, orders = generate_synthetic_data()
    return customers if fallback_generator_idx == 0 else orders


CUSTOMERS_DATA: List[Dict[str, Any]] = load_dataset("customers.json", 0)
ORDERS_DATA: List[Dict[str, Any]] = load_dataset("orders.json", 1)


def maybe_inject_failure(fail_rate: float) -> Optional[Response]:
    if fail_rate > 0.0 and random.random() < fail_rate:
        if random.random() < 0.5:
            return JSONResponse(
                status_code=429,
                content={"detail": "Rate limit exceeded."},
                headers={"Retry-After": "1"},
            )
        return JSONResponse(
            status_code=500,
            content={"detail": "Simulated internal server error."},
        )
    return None


def paginate_and_filter(
    items: List[Dict[str, Any]],
    page: int,
    limit: int,
    updated_since: Optional[str],
) -> Dict[str, Any]:
    filtered = items
    if updated_since:
        filtered = [
            item for item in items
            if isinstance(item.get("updated_at"), str) and item["updated_at"] >= updated_since
        ]

    total = len(filtered)
    total_pages = max(1, math.ceil(total / limit))
    start_idx = (page - 1) * limit
    page_data = filtered[start_idx : start_idx + limit]

    return {
        "data": page_data,
        "page": page,
        "limit": limit,
        "total": total,
        "total_pages": total_pages,
        "has_next": page < total_pages,
    }


@app.get("/health")
def health() -> Dict[str, str]:
    return {"status": "ok"}


@app.get("/customers")
def get_customers(
    page: Annotated[int, Query(ge=1)] = 1,
    limit: Annotated[int, Query(ge=1, le=100)] = 20,
    updated_since: Annotated[Optional[str], Query()] = None,
    fail_rate: Annotated[float, Query(ge=0.0, le=1.0)] = 0.0,
):
    failure = maybe_inject_failure(fail_rate)
    if failure:
        return failure
    return paginate_and_filter(CUSTOMERS_DATA, page, limit, updated_since)


@app.get("/orders")
def get_orders(
    page: Annotated[int, Query(ge=1)] = 1,
    limit: Annotated[int, Query(ge=1, le=100)] = 20,
    updated_since: Annotated[Optional[str], Query()] = None,
    fail_rate: Annotated[float, Query(ge=0.0, le=1.0)] = 0.0,
):
    failure = maybe_inject_failure(fail_rate)
    if failure:
        return failure
    return paginate_and_filter(ORDERS_DATA, page, limit, updated_since)


if __name__ == "__main__":
    uvicorn.run("src.mock_api:app", host="127.0.0.1", port=8000, reload=True)
