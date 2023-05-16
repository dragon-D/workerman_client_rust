# workerman_client_rust
workerman的rust客户端版本

## 初始化
```rust
let dispatcher_service = run_dispatcher(vec!["127.0.0.1:1238".to_string()]).await;
```

## 查询分组
```rust
let _ = get_uid_list_group(dispatcher_service, vec!["group".to_string()]).await;
```

## 查询uid下的client_id
```rust
let client_id = get_client_id_by_uid(dispatcher_service, "uid1").await;
```

## client_id是否在线
```rust
let online = is_online(dispatcher_service, "7f0000010b5400000001").await;
```

## 广播uid
```rust
let _ = send_uid(
    dispatcher_service,
    "86490735033065472",
    "这是rust sdk发送的",
)
.await;
```

## 广播组
```rust
let _ = send_group(
    dispatcher_service.clone(),
    "呼叫group 1",
    "group",
    None,
    None,
)
.await;
```

## 加入组
```rust
let _ = cid_join_group(dispatcher_service.clone(), "7f0000010b540000000c", "group").await;
```

## 离开组
```rust
let _ = leave_group(dispatcher_service.clone(), "7f0000010b540000000c", "group").await;
```


## bind uid
```rust
let _ = bind_uid(dispatcher_service.clone(), "7f0000010b5400000002", "uid1").await;
```