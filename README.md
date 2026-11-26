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
