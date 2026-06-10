# PeakPick Order Service

Order Service là microservice quản lý giỏ hàng, checkout mock và trạng thái đơn hàng.

## Database Riêng

Service này sở hữu database `peakpick_order` với các bảng:

- `carts`
- `cart_items`
- `orders`
- `order_items`
- `event_log`

## Event

Phát event:

- `CartCreated`
- `OrderPaid`

Nhận event:

- Các event vòng đời đơn hàng từ Slot và Store Operations để cập nhật trạng thái đọc.

## Chạy Local

```bash
pip install -r requirements.txt
uvicorn services.order_service.main:app --reload --port 8002
```
