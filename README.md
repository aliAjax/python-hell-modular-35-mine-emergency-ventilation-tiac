# 矿井应急避险与通风协调

这是一个只使用Python标准库和SQLite的模块化原型项目，默认端口为`8335`。领域对象包括矿井人员、气体传感、通风设备、逃生通道、避险硐室、事件和处置任务。`app.py`只负责参数解析、依赖组装和服务生命周期，业务状态机与约束集中在`src/rules.py`。

风量公共账（`src/air.py`）解决"各区域送风需求与在运行风机容量对不上、调度靠电话分风"的问题：区域按核定需求占用总容量，容量不足排队，报警级别高的应急申请可以从较低级别区域让风，让渡和收回全部落账可查。

## 模块结构

- `app.py`：命令行参数、依赖组装、启动和信号处理。
- `src/domain.py`：角色、领域异常、身份解析和实体数据结构。
- `src/rules.py`：状态机、角色权限、领域计算、冲突和跨对象校验。
- `src/repository.py`：SQLite建表、查询、事务、乐观锁、审计和幂等键。
- `src/service.py`：用例编排、离线记录合并、版本控制和审计写入。
- `src/http_api.py`：HTTP路由、JSON解析和统一错误响应。
- `src/air.py`：风量公共账——区域核定需求、排队、报警抢占、让渡/收回账、容量回填。纯函数分配器 + SQLite落账 + 用例编排。
- `src/audit.py`：实体操作审计时间线。
- `static/index.html`：最小演示页面。
- `tests/`：完整流程、规则和失败场景测试。

## 初始化与启动

```bash
python3 app.py --db ./data.db --port 8335
```

服务启动时自动建表。`--host`可修改监听地址，`--db`可指定其他SQLite文件。初始化服务不需要单独命令，首次启动即可访问：

```bash
curl http://127.0.0.1:8335/health
```

## 主要接口

- `GET /health`：健康检查。
- `GET /api/<kind>`：按对象类型查询，可用`?status=`过滤。
- `POST /api/<kind>`：创建对象；请求体为JSON。
- `GET /api/entities/<id>`：读取对象当前版本。
- `POST /api/entities/<id>/actions`：提交`{"action":"动作名","data":{...},"expected_version":数字}`。
- `GET /api/audit`：读取审计记录。

身份通过`X-User-Id`和`X-Role`请求头传入，角色和动作权限由规则引擎校验。

## 风量公共账

启动时自动按**在运行风机**（`ventilation`且`status=running`）的`capacity`之和回填总容量（首次无风机记 0，补录风机后再回填一次；已回填不覆盖，显式同步走`capacity`接口）。所有占用在每次变更后整体重算。

分账规则：

- 每个区域按`approved_demand`核定需求占用；容量不足的申请排队，顺序为报警级别（critical > alarm > warning > none）→ 等待时间（入队序号）→ 请求ID，报警变化后排队时间沿用首次入队时间。
- 高级别应急申请容量不足时，自动从**严格较低级别**区域让出，记一笔`active`让渡账（谁让给谁、让多少）。
- 调度可点名让渡：`POST /api/air/yields`（`donor_id`/`receiver_id`/`amount`），点名划转先于自然抢占执行，可带`Yield-Key`做幂等。
- 被让出区域**自己一报警立即收回**：让渡账置`recalled`并形成保护区，即使接收方级别更高，在捐赠方报警期间也拿不回这笔风；捐赠方报警解除后保护消失、重新参与分配。
- 区域报警等级一变，原占用立即作废（`served`清零、`epoch+1`），整体重算，使用方退回队列，旧让渡账自动关账。
- 两名调度同时提交同一笔让渡：同一(donor, receiver)只有一笔活动账，先落账者生效；版本冲突或库锁时按**原申请参数**重试（默认8次）；相同`Yield-Key`直接返回首笔账。

接口（均需对应角色，viewer只读）：

- `POST /api/air/zones`：登记区域 `{area_code, approved_demand}`。
- `PATCH`风格动作：`POST /api/air/zones/<id>/requests`（提交送风申请）、`POST /api/air/zones/<id>/alarm`（`{alarm}`变更报警级别）、`POST /api/air/zones/<id>`（`{approved_demand}`调整核定需求）。
- `POST /api/air/yields`：点名让渡。
- `POST /api/air/capacity`：显式同步在运行风机总容量（admin/safety）；`POST /api/air/capacity/backfill`：按运行风机回填。
- `GET /api/air/ledger`：公共账视图（容量/已占用/可用、排队顺序与每项占用、活动及收回中的让渡账）。
- `GET /api/air/loans`：全部让渡账（含`closed`历史）。

## 核心流程

创建矿井事件、人员和设备记录后，依次执行撤离、搜救、通风恢复和事件关闭。`POST /api/offline-records` 用于合并现场离线记录，`source_id + record_id` 相同会幂等返回原记录。

## 规则重点

- 活跃任务按 `dedupe_key` 防止重复派工。
- 气体读数按阈值计算`severity`。
- 事件关闭前必须没有失联或已定位人员、没有活跃任务，并且所有通风设备恢复运行。

## 测试

```bash
python3 -m unittest discover -s tests -v
```

## 局限

项目使用请求头模拟身份、SQLite单机持久化和简化状态机，适合原型演示和流程验证，不替代行业正式系统、设备控制系统或现场安全规程。
