# 旧通知线路退役中的承诺接续

这里保存一套可脱机使用的合成运维资料。两个事件文件各自从相同清册开始，模拟两次独立运行。

## 清册和事件

`samples/inventory.json` 记录演练开始时已知的资源、对象与约定。通知身份用 tenant 与 message 表示，订阅角色用 consumer 和 topic 表示。功能线路共用的资源列在 routes 中。tenant_contract 表示旧线路承诺的接续截止和新版确认需满的窗口长度。

`samples/shift-log.jsonl` 和 `samples/clean-delegate.jsonl` 每行是一个 JSON 对象。通用字段 `at` 是资料到达值班侧的演练秒，`kind` 标明来源记录的类型。同一秒内保持给定文件顺序。事实发生时间由记录自身字段提供。delegation 规定 from/until 区间。new_confirm 是消费方的迁移声明，覆盖具体 topic，需要相应回执作为证据。delivery_receipt.stage 的 received 和 applied 是分别观察的事实。line_action/line_effect 表示申请和实际资源动作。

文件可包括申请、已发指令、状态观测和外部回执。出现于记录中的申请或意图可能未获准，外部回执可能迟到或与此前观测冲突。

## 浏览资料

在仓库根目录用 Python 3.11 运行。只需标准库。

```sh
python3 sample_reader.py samples/shift-log.jsonl
python3 -m unittest discover -s tests -v
```

原始资料可按 `--kind` 筛选。程序按接收顺序显示记录。单元测试核对资料能够读取，以及事件夹具的基本结构。

## 退役协调能力

仓库现在包含一个仅使用 Python 3.11 标准库的 `retirement` 包和 `retire.py` 命令行入口。它把三类状态分开：

- **接收方/业务状态**：消费者、通知种类、代理委托、交付、业务处理回执、迁移确认和接续承诺。
- **退役决定状态**：分批订阅处置（转接新版或保留）、幂等命令、操作记录和回滚评估。
- **线路/资源观测状态**：`line_action` 只是申请；只有关联的 `line_effect` 才改变观测到的资源状态。合成接收端 `SyntheticReceiver` 与合成线路 `SyntheticLine` 也可独立观察。

核心原则是：探活、交付事件、`received` 回执、旧端停止轮询或访问量下降，都不能把业务承诺标记为完成；只有授权行为者的有效 `applied` 业务回执，或消费者在明确范围内的有效接续，才解除旧线路义务。

### 一次性分析样例

```sh
python3 -m retirement replay samples/inventory.json samples/shift-log.jsonl
python3 -m retirement replay samples/inventory.json samples/clean-delegate.jsonl
python3 -m retirement demo
```

输出中的关键字段：

- `backlog[].routing`：这份积压现在应送往消费者、有效代理、新版路由，还是进入人工/等待对账队列。
- `confirmations[]`：确认覆盖的消费者、topic、消息范围、证据和约定窗口；联调环境证据不会作为生产正式迁移。
- `resources[].blocking_claims`：资源仍被哪些租户、订阅或未完成义务支撑。
- `resources[].rollback`：资源已确认拆除后，哪些配置需要补建、哪些运行时/工单状态缺口无法由自动化弥补。

### 可重启的值班状态

```sh
STATE=/tmp/retirement/state.json

python3 -m retirement --state "$STATE" --inventory samples/inventory.json init
python3 -m retirement --state "$STATE" ingest samples/shift-log.jsonl

# 重复执行同一文件不会重复推进 revision。
python3 -m retirement --state "$STATE" ingest samples/shift-log.jsonl

python3 -m retirement --state "$STATE" backlog
python3 -m retirement --state "$STATE" plan --tenant T-b
python3 -m retirement --state "$STATE" trace --tenant T-b --message n8
python3 -m retirement --state "$STATE" route T-b n8
python3 -m retirement --state "$STATE" observe line
```

状态文件是原子替换写入的 JSON。进程重启后重新打开同一 `--state` 即可从已接收事件和已执行决定继续；相同事件 `id`/`event_id`/`report_id` 或完全相同的 JSON 内容会被幂等去重。

### 登记、迁移、停用和回撤

```sh
python3 -m retirement --state "$STATE" register-consumer \
  --consumer acme --tenant T-x --topics case,status --at 10

python3 -m retirement --state "$STATE" delegate \
  --consumer acme --proxy relay --topic status --start 10 --until 40 --at 10

python3 -m retirement --state "$STATE" confirm \
  --consumer acme --topic status --reference a01,a02 --at 12

# 预演；不加 --execute 时不写入决定。
python3 -m retirement --state "$STATE" migrate \
  --tenant T-x --consumer acme --topic status --messages a01,a02 --at 30

# 执行有确认范围依据的转接，并推进当前可安全自动处置的资源阶段。
python3 -m retirement --state "$STATE" advance --tenant T-x --execute --at 30

# 既有订阅也可显式保留到合同最后时刻。
python3 -m retirement --state "$STATE" subscription \
  --action retain --tenant T-x --consumer acme --topic status --at 30

# 已 removed 的资源不能普通 restore；该命令先给出补建和不可弥补缺口。
python3 -m retirement --state "$STATE" rollback --resource poll-legacy
python3 -m retirement --state "$STATE" rollback --resource poll-legacy --execute
```

`advance_decommission()` 在执行前会基于**最新到达的回执**重新计算义务，而不是沿用旧计划：

- 共享资源（例如同时属于 `legacy-voice` 和 `legacy-text` 的 `return-mail`）只要还有任一租户的未完成义务或合同期内订阅，就不会自动停用。
- 专用 poll 资源可先进入可回撤的暂停阶段，让客服通过审计/接管继续处理未完工单。
- 只有阻断性义务解除且观测状态为 `disabled` 后，才进入 `retire/remove`。
- 计划后到达的 `applied` 回执会缩小阻断范围；执行器只执行当前有证据/明确决定支撑的子集，其余进入 `deferred`。

### Python API

```python
from retirement import RetirementEngine, RetirementService, SyntheticLine, SyntheticReceiver
from retirement.service import load_inventory, load_jsonl

inventory = load_inventory("samples/inventory.json")
engine = RetirementEngine(inventory)
engine.ingest(load_jsonl("samples/shift-log.jsonl"))

engine.backlog()
engine.route_message("T-b", "n8", at=31)
engine.trace_ticket(tenant="T-b", message="n8")
engine.plan_retirement(at=50)
engine.advance_decommission(execute=True)

# 持久化、重复上报和重启：
service = RetirementService("state.json", inventory)
service.ingest_jsonl("samples/shift-log.jsonl")
```

### 已编码的关键判定

1. `received` 只证明送达/受理；只有规范化为 `applied/processed/completed` 的有效回执才能关闭业务义务。
2. 回执行为者必须是登记消费者本人，或在事实时刻持有匹配 topic 的有效委托。
3. 代理在委托有效期内已有效 `received/applied` 并建立托管（custody）后，委托到期不影响该已代收业务的后续处理和回执追溯；到期后才首次交付的消息不能再送往失效代理，应回到消费者主身份或等待人工处理。
4. 旧线路的有效 `applied` 回执覆盖同一 `(tenant, message)` 逻辑消息；即使新版重送，也应抑制重复业务处理。
5. `new_confirm` 只覆盖指定 `consumer + topic + reference(s)`；整 topic/全消息范围必须显式使用 `reference="*"`。
6. 正式迁移还要求新版路由的 `applied` 证据，并经过租户合同的 `confirm_seconds` 观察窗口；每个证据的窗口偏移和是否及时都会单独标出。联调/测试环境只作为 `integration_environment_validation_only`。
7. 租户取消只解除该租户自身的订阅合同主张；共享资源仍会计算其他租户、路由和未完成工单的主张。
8. `line_action` 与 `line_effect` 分离；没有实际 effect（或 effect 为 `still_active`）时，不会把线路状态当成已停用。
9. 已确认 `removed` 后的回撤返回 `rebuild_required` 与 `irreparable_gaps`，不会静默宣称回撤成功。

运行全部测试：

```sh
python3 -m unittest discover -s tests -v
```

