from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
import json
from typing import Any, Dict, List, Optional, Tuple

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator


@dataclass
class RejectedRecord:
    run_id: str
    entity: str
    reason: str
    payload: Dict[str, Any]


@dataclass
class ValidationResult:
    valid_records: List[Dict[str, Any]]
    rejected_records: List[RejectedRecord]


class CustomerModel(BaseModel):
    model_config = ConfigDict(extra="ignore")

    customer_id: str = Field(min_length=1)
    name: str = Field(min_length=1)
    email: str = Field(min_length=3)
    country: str = Field(min_length=1)
    updated_at: datetime

    @field_validator("name", "email", "country", mode="before")
    @classmethod
    def validate_non_empty_string(cls, v: Any, info) -> str:
        if v is None:
            raise ValueError(f"Field '{info.field_name}' cannot be null")
        if not isinstance(v, str):
            raise TypeError(f"Field '{info.field_name}' must be a string, got {type(v).__name__}")
        stripped = v.strip()
        if not stripped:
            raise ValueError(f"Field '{info.field_name}' cannot be blank")
        return stripped


class OrderModel(BaseModel):
    model_config = ConfigDict(extra="ignore")

    order_id: str = Field(min_length=1)
    customer_id: str = Field(min_length=1)
    amount: Decimal = Field(gt=0)
    status: str = Field(min_length=1)
    order_ts: datetime
    updated_at: datetime

    @field_validator("customer_id", "status", mode="before")
    @classmethod
    def validate_non_empty_string(cls, v: Any, info) -> str:
        if v is None:
            raise ValueError(f"Field '{info.field_name}' cannot be null")
        if not isinstance(v, str):
            raise TypeError(f"Field '{info.field_name}' must be a string, got {type(v).__name__}")
        stripped = v.strip()
        if not stripped:
            raise ValueError(f"Field '{info.field_name}' cannot be blank")
        return stripped

    @field_validator("amount", mode="before")
    @classmethod
    def validate_amount(cls, v: Any) -> Decimal:
        if v is None:
            raise ValueError("Field 'amount' cannot be null")
        try:
            val = Decimal(str(v))
            if val <= 0:
                raise ValueError("Field 'amount' must be greater than zero")
            return round(val, 2)
        except Exception:
            raise ValueError(f"Field 'amount' is not a valid decimal: {v}")


def _format_validation_error(exc: ValidationError) -> str:
    messages: List[str] = []
    for err in exc.errors():
        field_name = ".".join(str(loc) for loc in err.get("loc", []))
        msg = err.get("msg", "Invalid value")
        messages.append(f"{field_name}: {msg}" if field_name else msg)
    return "; ".join(messages)


def validate_customer(record: Dict[str, Any], run_id: str = "N/A") -> Tuple[Optional[Dict[str, Any]], Optional[RejectedRecord]]:
    try:
        model = CustomerModel.model_validate(record)
        cleaned = {
            "customer_id": model.customer_id,
            "name": model.name,
            "email": model.email,
            "country": model.country,
            "updated_at": model.updated_at.isoformat(),
        }
        return cleaned, None
    except (ValidationError, Exception) as exc:
        reason = _format_validation_error(exc) if isinstance(exc, ValidationError) else str(exc)
        rejected = RejectedRecord(
            run_id=run_id,
            entity="customer",
            reason=reason,
            payload=record,
        )
        return None, rejected


def validate_order(record: Dict[str, Any], run_id: str = "N/A") -> Tuple[Optional[Dict[str, Any]], Optional[RejectedRecord]]:
    try:
        model = OrderModel.model_validate(record)
        cleaned = {
            "order_id": model.order_id,
            "customer_id": model.customer_id,
            "amount": float(model.amount),
            "status": model.status,
            "order_ts": model.order_ts.isoformat(),
            "updated_at": model.updated_at.isoformat(),
        }
        return cleaned, None
    except (ValidationError, Exception) as exc:
        reason = _format_validation_error(exc) if isinstance(exc, ValidationError) else str(exc)
        rejected = RejectedRecord(
            run_id=run_id,
            entity="order",
            reason=reason,
            payload=record,
        )
        return None, rejected


def validate_records(
    records: List[Dict[str, Any]],
    entity_type: str,
    run_id: str = "N/A",
) -> ValidationResult:
    clean_entity = entity_type.strip("/").lower()
    validator = validate_customer if "customer" in clean_entity else validate_order

    valid_list: List[Dict[str, Any]] = []
    rejected_list: List[RejectedRecord] = []

    for item in records:
        valid_item, rejected_item = validator(item, run_id=run_id)
        if valid_item is not None:
            valid_list.append(valid_item)
        elif rejected_item is not None:
            rejected_list.append(rejected_item)

    return ValidationResult(valid_records=valid_list, rejected_records=rejected_list)
