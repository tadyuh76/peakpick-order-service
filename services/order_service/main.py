from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import UTC, datetime
from uuid import uuid4

from fastapi import FastAPI, Request
from pydantic import BaseModel, Field

from shared.event_bus import build_event_bus
from shared.events import EventType, new_event
from shared.logging import configure_logging, log_event
from shared.settings import get_settings


settings = get_settings("order-service")
logger = configure_logging(settings.service_name)
carts: dict[str, dict[str, object]] = {}
orders: dict[str, dict[str, object]] = {}


class OrderItem(BaseModel):
    sku: str
    quantity: int = Field(gt=0)


class CartRequest(BaseModel):
    customer_name: str
    items: list[OrderItem]


class CheckoutRequest(CartRequest):
    pickup_window: str = Field(examples=["12:00-12:15"])


@asynccontextmanager
async def lifespan(app: FastAPI):
    event_bus = build_event_bus(settings)
    await event_bus.connect()
    app.state.event_bus = event_bus
    log_event(logger, settings.service_name, "event bus connected", bus=settings.event_bus)
    try:
        yield
    finally:
        await event_bus.close()


app = FastAPI(
    title="PeakPick Order Service",
    version="0.1.0",
    description="Cart, mock checkout, and order lifecycle events.",
    lifespan=lifespan,
)


@app.get("/health")
async def health(request: Request) -> dict[str, object]:
    return {
        "status": "ok",
        "service": settings.service_name,
        "event_bus_connected": request.app.state.event_bus.is_connected,
    }


@app.post("/carts", status_code=201)
async def create_cart(payload: CartRequest, request: Request) -> dict[str, object]:
    cart_id = f"cart-{uuid4()}"
    cart = {
        "cart_id": cart_id,
        "customer_name": payload.customer_name,
        "items": [item.model_dump() for item in payload.items],
        "status": "CartCreated",
    }
    carts[cart_id] = cart
    event = new_event(
        EventType.CART_CREATED,
        aggregate_id=cart_id,
        source=settings.service_name,
        payload=cart,
    )
    await request.app.state.event_bus.publish(event)
    return {"cart": cart, "correlation_id": event.correlation_id}


@app.post("/checkout", status_code=201)
async def checkout(payload: CheckoutRequest, request: Request) -> dict[str, object]:
    order_id = f"order-{uuid4()}"
    paid_at = datetime.now(UTC).isoformat()
    order = {
        "order_id": order_id,
        "customer_name": payload.customer_name,
        "items": [item.model_dump() for item in payload.items],
        "pickup_window": payload.pickup_window,
        "payment_status": "Paid",
        "order_status": "Paid",
        "paid_at": paid_at,
    }
    orders[order_id] = order
    event = new_event(
        EventType.ORDER_PAID,
        aggregate_id=order_id,
        source=settings.service_name,
        payload=order,
    )
    await request.app.state.event_bus.publish(event)
    log_event(logger, settings.service_name, "order paid", order_id=order_id, correlation_id=event.correlation_id)
    return {"order": order, "correlation_id": event.correlation_id}


@app.get("/orders")
async def list_orders() -> list[dict[str, object]]:
    return list(orders.values())


@app.get("/orders/{order_id}")
async def get_order(order_id: str) -> dict[str, object]:
    return orders[order_id]

