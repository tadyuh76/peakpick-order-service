from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from uuid import uuid4

from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel, Field, field_validator

from shared.event_bus import InMemoryEventBus, RabbitMQEventBus
from shared.event_bus import build_event_bus
from shared.events import EventEnvelope
from shared.events import EventType, new_event
from shared.logging import configure_logging, log_event
from shared.settings import get_settings


settings = get_settings("order-service")
logger = configure_logging(settings.service_name)
carts: dict[str, dict[str, object]] = {}
orders: dict[str, dict[str, object]] = {}
ALLOWED_PICKUP_WINDOWS = {"09:30-09:35", "12:00-12:15", "17:30-17:45"}
ORDER_STATUS_RANK = {
    "Paid": 0,
    "SlotAssigned": 1,
    "Preparing": 2,
    "PlacedInSlot": 3,
    "ReadyForPickup": 4,
    "Completed": 5,
    "Expired": 5,
    "InventoryShortage": 5,
    "SlotAssignmentFailed": 5,
}
TERMINAL_ORDER_STATUSES = {
    "Completed",
    "Expired",
    "InventoryShortage",
    "SlotAssignmentFailed",
}


class OrderItem(BaseModel):
    sku: str
    quantity: int = Field(gt=0)


class CartRequest(BaseModel):
    customer_name: str = Field(min_length=1)
    items: list[OrderItem] = Field(min_length=1)


class CheckoutRequest(CartRequest):
    pickup_window: str = Field(min_length=1, examples=["12:00-12:15"])

    @field_validator("pickup_window")
    @classmethod
    def pickup_window_must_be_supported(cls, value: str) -> str:
        if value not in ALLOWED_PICKUP_WINDOWS:
            raise ValueError("pickup_window must match one of the supported demo windows")
        return value


def _database_enabled() -> bool:
    return bool(settings.database_url)


async def _save_cart(cart: dict[str, object]) -> None:
    if not _database_enabled():
        return
    await asyncio.to_thread(_save_cart_sync, cart)


def _save_cart_sync(cart: dict[str, object]) -> None:
    import psycopg

    with psycopg.connect(settings.database_url) as conn:
        with conn.transaction():
            conn.execute(
                """
                INSERT INTO carts (cart_id, customer_name, status)
                VALUES (%s, %s, %s)
                ON CONFLICT (cart_id) DO UPDATE
                    SET customer_name = EXCLUDED.customer_name,
                        status = EXCLUDED.status
                """,
                (cart["cart_id"], cart["customer_name"], cart["status"]),
            )
            conn.execute("DELETE FROM cart_items WHERE cart_id = %s", (cart["cart_id"],))
            with conn.cursor() as cursor:
                cursor.executemany(
                    """
                    INSERT INTO cart_items (cart_id, sku, quantity)
                    VALUES (%s, %s, %s)
                    """,
                    [
                        (cart["cart_id"], item["sku"], item["quantity"])
                        for item in cart["items"]  # type: ignore[index]
                    ],
                )


async def _save_order(order: dict[str, object]) -> None:
    if not _database_enabled():
        return
    await asyncio.to_thread(_save_order_sync, order)


def _save_order_sync(order: dict[str, object]) -> None:
    import psycopg

    with psycopg.connect(settings.database_url) as conn:
        with conn.transaction():
            conn.execute(
                """
                INSERT INTO orders (
                    order_id, customer_name, pickup_window,
                    payment_status, order_status, paid_at
                )
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (order_id) DO UPDATE
                    SET customer_name = EXCLUDED.customer_name,
                        pickup_window = EXCLUDED.pickup_window,
                        payment_status = EXCLUDED.payment_status,
                        order_status = EXCLUDED.order_status,
                        paid_at = EXCLUDED.paid_at
                """,
                (
                    order["order_id"],
                    order["customer_name"],
                    order["pickup_window"],
                    order["payment_status"],
                    order["order_status"],
                    order["paid_at"],
                ),
            )
            conn.execute("DELETE FROM order_items WHERE order_id = %s", (order["order_id"],))
            with conn.cursor() as cursor:
                cursor.executemany(
                    """
                    INSERT INTO order_items (order_id, sku, quantity)
                    VALUES (%s, %s, %s)
                    """,
                    [
                        (order["order_id"], item["sku"], item["quantity"])
                        for item in order["items"]  # type: ignore[index]
                    ],
                )


async def _list_orders_from_db() -> list[dict[str, object]]:
    if not _database_enabled():
        return list(orders.values())
    return await asyncio.to_thread(_list_orders_from_db_sync)


def _list_orders_from_db_sync() -> list[dict[str, object]]:
    import psycopg
    from psycopg.rows import dict_row

    with psycopg.connect(settings.database_url, row_factory=dict_row) as conn:
        rows = conn.execute(
            """
            SELECT order_id, customer_name, pickup_window,
                   payment_status, order_status, paid_at
            FROM orders
            ORDER BY created_at DESC
            """
        ).fetchall()
        return [_hydrate_order_sync(conn, row) for row in rows]


async def _get_order_from_db(order_id: str) -> dict[str, object] | None:
    if not _database_enabled():
        return orders.get(order_id)
    return await asyncio.to_thread(_get_order_from_db_sync, order_id)


def _get_order_from_db_sync(order_id: str) -> dict[str, object] | None:
    import psycopg
    from psycopg.rows import dict_row

    with psycopg.connect(settings.database_url, row_factory=dict_row) as conn:
        row = conn.execute(
            """
            SELECT order_id, customer_name, pickup_window,
                   payment_status, order_status, paid_at
            FROM orders
            WHERE order_id = %s
            """,
            (order_id,),
        ).fetchone()
        if row is None:
            return None
        return _hydrate_order_sync(conn, row)


def _hydrate_order_sync(conn, row: dict[str, object]) -> dict[str, object]:
    items = conn.execute(
        """
        SELECT sku, quantity
        FROM order_items
        WHERE order_id = %s
        ORDER BY sku
        """,
        (row["order_id"],),
    ).fetchall()
    return {**dict(row), "items": [dict(item) for item in items]}


async def update_order_status(
    order_id: str,
    status: str,
    state: dict[str, dict[str, object]] = orders,
) -> None:
    if order_id in state:
        current_status = str(state[order_id].get("order_status", ""))
        if not _can_transition_order_status(current_status, status):
            return
        state[order_id]["order_status"] = status
    if _database_enabled():
        await asyncio.to_thread(_update_order_status_sync, order_id, status)


def _can_transition_order_status(current_status: str, next_status: str) -> bool:
    if current_status == next_status:
        return True
    if current_status in TERMINAL_ORDER_STATUSES:
        return False
    current_rank = ORDER_STATUS_RANK.get(current_status)
    next_rank = ORDER_STATUS_RANK.get(next_status)
    if current_rank is None or next_rank is None:
        return True
    return next_rank >= current_rank


def _update_order_status_sync(order_id: str, status: str) -> None:
    import psycopg

    with psycopg.connect(settings.database_url) as conn:
        with conn.transaction():
            row = conn.execute(
                """
                SELECT order_status
                FROM orders
                WHERE order_id = %s
                FOR UPDATE
                """,
                (order_id,),
            ).fetchone()
            if row is None or not _can_transition_order_status(str(row[0]), status):
                return
            conn.execute(
                """
                UPDATE orders
                SET order_status = %s
                WHERE order_id = %s
                """,
                (status, order_id),
            )


async def handle_order_lifecycle_event(
    event: EventEnvelope,
    state: dict[str, dict[str, object]] = orders,
) -> None:
    status_by_event = {
        EventType.PICKUP_SLOT_RESERVED: "SlotAssigned",
        EventType.PICKUP_SLOT_FULL: "SlotAssignmentFailed",
        EventType.INVENTORY_SHORTAGE_DETECTED: "InventoryShortage",
        EventType.ORDER_PREPARING: "Preparing",
        EventType.ORDER_PLACED_IN_SLOT: "PlacedInSlot",
        EventType.ORDER_READY: "ReadyForPickup",
        EventType.ORDER_PICKED_UP: "Completed",
        EventType.ORDER_EXPIRED: "Expired",
    }
    event_type = EventType(event.event_type)
    status = status_by_event.get(event_type)
    if status:
        await update_order_status(event.aggregate_id, status, state)


@asynccontextmanager
async def lifespan(app: FastAPI):
    event_bus = build_event_bus(settings)
    await event_bus.connect()
    for event_type in (
        EventType.PICKUP_SLOT_RESERVED,
        EventType.PICKUP_SLOT_FULL,
        EventType.INVENTORY_SHORTAGE_DETECTED,
        EventType.ORDER_PREPARING,
        EventType.ORDER_PLACED_IN_SLOT,
        EventType.ORDER_READY,
        EventType.ORDER_PICKED_UP,
        EventType.ORDER_EXPIRED,
    ):
        await event_bus.subscribe(
            event_type,
            handle_order_lifecycle_event,
            queue_name=f"{settings.service_name}.{event_type}",
        )
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
    await _save_cart(cart)
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
    await _save_order(order)
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
    return await _list_orders_from_db()


@app.get("/orders/{order_id}")
async def get_order(order_id: str) -> dict[str, object]:
    order = await _get_order_from_db(order_id)
    if order is None:
        raise HTTPException(status_code=404, detail="Order not found")
    return order
