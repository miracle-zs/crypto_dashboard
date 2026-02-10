## 数据字典（核心表）

### trades
- `symbol`：币种（去 USDT 后缀）
- `side`：LONG/SHORT
- `entry_time` / `exit_time`：入场/出场时间（UTC+8 字符串）
- `entry_price` / `exit_price`
- `qty` / `entry_amount`
- `pnl_net` / `fees` / `pnl_before_fees`
- `entry_order_id` / `exit_order_id`

### open_positions
- `symbol` / `side`
- `entry_time` / `entry_price` / `qty` / `entry_amount`
- `order_id`：入场订单号

### balance_history
- `timestamp`：UTC 时间戳
- `balance`：保证金余额
- `wallet_balance`：钱包余额

### transfers
- `amount` / `type` / `description`
- `timestamp`

### ws_events
- `event_type`：WebSocket 事件类型
- `event_time`：事件时间（毫秒）
- `payload`：原始事件 JSON

### sync_status
- `last_sync_time`
- `last_entry_time`
- `total_trades`
- `status` / `error_message`

