# PeakPick Order Service

Owns cart, checkout, mock payment, and order lifecycle state. It publishes `CartCreated` and `OrderPaid`, then reacts to lifecycle events from other services.

Owned database tables:

- `carts`
- `cart_items`
- `orders`
- `order_items`
- local `event_log`

Run locally:

```bash
pip install -r requirements.txt
uvicorn services.order_service.main:app --reload --port 8002
```
