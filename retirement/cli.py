"""Command line interface for the local legacy-line retirement capability."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from .errors import RetirementError
from .service import RetirementService, analyze_timeline, load_inventory


def print_json(value: Any) -> None:
    print(json.dumps(value, ensure_ascii=False, indent=2, default=str))


def service_from_args(args: argparse.Namespace) -> RetirementService:
    if not getattr(args, "state", None):
        raise RetirementError("此命令需要 --state state.json")
    inventory = None
    inventory_path = getattr(args, "inventory", None)
    if inventory_path:
        inventory = load_inventory(inventory_path)
    return RetirementService.create_or_open(args.state, inventory=inventory,
                                            inventory_path=None if inventory is not None else inventory_path)


def _at(args: argparse.Namespace) -> int | None:
    return getattr(args, "at", None)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="retire", description="旧通知线路退役协调器（仅标准库、本地状态）")
    parser.add_argument("--state", help="持久化 JSON 状态路径")
    parser.add_argument("--inventory", help="inventory.json 路径；新状态必需")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("init", help="从清册初始化状态")
    p = sub.add_parser("ingest", help="按到达顺序摄入 JSONL 事件；重复事件幂等")
    p.add_argument("timeline")

    p = sub.add_parser("replay", help="一次性分析 inventory + timeline，不写状态")
    p.add_argument("inventory_path")
    p.add_argument("timeline")
    p.add_argument("--at", type=int)

    for name in ("status", "plan"):
        p = sub.add_parser(name)
        p.add_argument("--tenant")
        p.add_argument("--at", type=int)

    p = sub.add_parser("backlog")
    p.add_argument("--tenant")
    p.add_argument("--at", type=int)
    p.add_argument("--include-completed", action="store_true")

    p = sub.add_parser("trace", help="从工单/租户消息追溯最后有效回执")
    p.add_argument("ticket", nargs="?")
    p.add_argument("--tenant")
    p.add_argument("--message")
    p.add_argument("--at", type=int)

    p = sub.add_parser("route", help="查询某逻辑消息现在应送往何处")
    p.add_argument("tenant")
    p.add_argument("message")
    p.add_argument("--consumer")
    p.add_argument("--topic")
    p.add_argument("--at", type=int)

    p = sub.add_parser("register-consumer")
    p.add_argument("--consumer", required=True)
    p.add_argument("--tenant", required=True)
    p.add_argument("--topics", default="", help="逗号分隔的通知种类")
    p.add_argument("--at", type=int)

    p = sub.add_parser("delegate")
    p.add_argument("--consumer", required=True)
    p.add_argument("--proxy", required=True)
    p.add_argument("--topic", default="*")
    p.add_argument("--start", type=int)
    p.add_argument("--until", type=int)
    p.add_argument("--at", type=int)
    p.add_argument("--delegation-id")

    p = sub.add_parser("revoke-delegation")
    p.add_argument("--delegation-id")
    p.add_argument("--consumer")
    p.add_argument("--proxy")
    p.add_argument("--topic")
    p.add_argument("--at", type=int)

    p = sub.add_parser("deliver")
    p.add_argument("--tenant", required=True)
    p.add_argument("--message", required=True)
    p.add_argument("--consumer", required=True)
    p.add_argument("--topic")
    p.add_argument("--route")
    p.add_argument("--at", type=int)

    p = sub.add_parser("receipt")
    p.add_argument("--tenant", required=True)
    p.add_argument("--message", required=True)
    p.add_argument("--actor", required=True)
    p.add_argument("--stage", required=True, choices=["received", "applied"])
    p.add_argument("--route")
    p.add_argument("--at", type=int)
    p.add_argument("--receipt-id")

    p = sub.add_parser("confirm")
    p.add_argument("--consumer", required=True)
    p.add_argument("--topic", required=True)
    p.add_argument("--reference", required=True, help="消息 ID、逗号分隔列表，或 *")
    p.add_argument("--route", default="next")
    p.add_argument("--tenant")
    p.add_argument("--at", type=int)

    p = sub.add_parser("assume")
    p.add_argument("--consumer", required=True)
    p.add_argument("--topic", required=True)
    p.add_argument("--reference", required=True)
    p.add_argument("--tenant")
    p.add_argument("--to-route", default="next")
    p.add_argument("--at", type=int)

    p = sub.add_parser("subscription", help="对既有订阅执行 transfer/retain/stop/close_intake 批次动作")
    p.add_argument("--action", required=True,
                   choices=["transfer", "redirect", "migrate", "retain", "stop", "close_intake"])
    p.add_argument("--tenant")
    p.add_argument("--consumer")
    p.add_argument("--topic")
    p.add_argument("--batch-id")
    p.add_argument("--target-route", default="next")
    p.add_argument("--at", type=int)

    p = sub.add_parser("migrate", help="推进一次订阅迁移；默认只预演，加 --execute 落决定")
    p.add_argument("--tenant")
    p.add_argument("--consumer")
    p.add_argument("--topic")
    p.add_argument("--messages", help="逗号分隔消息范围；省略且全量确认存在时迁移整订阅")
    p.add_argument("--batch-id")
    p.add_argument("--at", type=int)
    p.add_argument("--execute", action="store_true")

    p = sub.add_parser("decommission", help="按最新回执推进可自动处置的资源停用")
    p.add_argument("--tenant")
    p.add_argument("--resource", action="append", dest="resources")
    p.add_argument("--at", type=int)
    p.add_argument("--execute", action="store_true")
    p.add_argument("--no-simulate-line", action="store_true")

    p = sub.add_parser("advance", help="先迁移订阅，再停用可安全停用的资源")
    p.add_argument("--tenant")
    p.add_argument("--consumer")
    p.add_argument("--topic")
    p.add_argument("--messages")
    p.add_argument("--resource", action="append", dest="resources")
    p.add_argument("--batch-id")
    p.add_argument("--at", type=int)
    p.add_argument("--execute", action="store_true")
    p.add_argument("--no-simulate-line", action="store_true")

    p = sub.add_parser("line-action")
    p.add_argument("--resource", required=True)
    p.add_argument("--action", required=True)
    p.add_argument("--operation")
    p.add_argument("--tenant")
    p.add_argument("--at", type=int)
    p.add_argument("--simulate", action="store_true", help="由内置合成线路立即产生 effect")
    p.add_argument("--force-fail", action="store_true")

    p = sub.add_parser("line-effect")
    p.add_argument("--resource", required=True)
    p.add_argument("--state", required=True)
    p.add_argument("--operation")
    p.add_argument("--at", type=int)

    p = sub.add_parser("rollback")
    p.add_argument("--resource", required=True)
    p.add_argument("--at", type=int)
    p.add_argument("--execute", action="store_true")
    p.add_argument("--no-simulate-line", action="store_true")

    p = sub.add_parser("takeover")
    p.add_argument("ticket")
    p.add_argument("--agent", required=True)
    p.add_argument("--reason")
    p.add_argument("--at", type=int)

    p = sub.add_parser("observe", help="独立查看 receiver 或 line 状态")
    p.add_argument("component", choices=["receiver", "line", "all"])

    sub.add_parser("demo", help="运行内置端到端合成演示")
    # Accept the persistent global options both before and after the command
    # name.  SUPPRESS ensures an absent subcommand-level option does not erase
    # a value already parsed before the subcommand.
    for action in parser._actions:  # noqa: SLF001 - argparse extension point
        if isinstance(action, argparse._SubParsersAction):
            for subparser in action.choices.values():
                try:
                    subparser.add_argument("--state", default=argparse.SUPPRESS,
                                           help="persistent JSON state path")
                    subparser.add_argument("--inventory", default=argparse.SUPPRESS,
                                           help="inventory.json path for a new state")
                except argparse.ArgumentError:
                    # Already present on this parser (for example in embedders
                    # that construct their own compatible CLI).
                    pass
    return parser


def split_list(value: str | None) -> list[str] | None:
    if value is None:
        return None
    if value.strip() == "*":
        return "*"  # type: ignore[return-value]
    return [x.strip() for x in value.split(",") if x.strip()]


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        if args.command == "replay":
            inventory = load_inventory(args.inventory_path)
            from .service import load_jsonl
            result = analyze_timeline(inventory, load_jsonl(args.timeline), at=args.at)
            print_json(result)
            return 0

        if args.command == "demo":
            from .demo import run_demo
            print_json(run_demo())
            return 0

        service = service_from_args(args)

        if args.command == "init":
            print_json({"initialized": str(service.state_path), "revision": service.engine.state["revision"]})
        elif args.command == "ingest":
            results = service.ingest_jsonl(args.timeline)
            print_json({"state": str(service.state_path), "results": results,
                        "revision": service.engine.state["revision"]})
        elif args.command == "status":
            print_json(service.engine.plan_retirement(tenant=args.tenant, at=_at(args)))
        elif args.command == "plan":
            print_json(service.engine.plan_retirement(tenant=args.tenant, at=_at(args)))
        elif args.command == "backlog":
            print_json(service.engine.backlog(tenant=args.tenant, at=_at(args),
                                              include_completed=args.include_completed))
        elif args.command == "trace":
            print_json(service.engine.trace_ticket(args.ticket, tenant=args.tenant,
                                                   message=args.message, at=_at(args)))
        elif args.command == "route":
            print_json(service.engine.route_message(args.tenant, args.message,
                                                    consumer=args.consumer, topic=args.topic, at=args.at))
        elif args.command == "register-consumer":
            topics = [x.strip() for x in args.topics.split(",") if x.strip()]
            print_json(service.register_consumer(args.consumer, args.tenant, topics, at=args.at))
        elif args.command == "delegate":
            print_json(service.delegate(args.consumer, args.proxy, topic=args.topic,
                                        start=args.start, until=args.until, at=args.at,
                                        delegation_id=args.delegation_id))
        elif args.command == "revoke-delegation":
            print_json(service.revoke_delegation(delegation_id=args.delegation_id,
                                                  consumer=args.consumer, proxy=args.proxy,
                                                  topic=args.topic, at=args.at))
        elif args.command == "deliver":
            print_json(service.deliver(args.tenant, args.message, args.consumer, topic=args.topic,
                                       route=args.route, at=args.at))
        elif args.command == "receipt":
            extra = {"receipt_id": args.receipt_id} if args.receipt_id else {}
            print_json(service.receipt(args.tenant, args.message, args.actor, args.stage,
                                       route=args.route, at=args.at, **extra))
        elif args.command == "confirm":
            reference: Any = "*" if args.reference == "*" else split_list(args.reference)
            if isinstance(reference, list) and len(reference) == 1:
                reference = reference[0]
            print_json(service.confirm(args.consumer, args.topic, reference, route=args.route,
                                       tenant=args.tenant, at=args.at))
        elif args.command == "assume":
            reference = split_list(args.reference)
            ref: Any = "*" if reference == "*" else reference
            if isinstance(ref, list) and len(ref) == 1:
                ref = ref[0]
            print_json(service.assume(args.consumer, args.topic, ref, tenant=args.tenant,
                                      to_route=args.to_route, at=args.at))
        elif args.command == "subscription":
            print_json(service.transition_subscriptions(
                args.action, tenant=args.tenant, consumer=args.consumer, topic=args.topic,
                batch_id=args.batch_id, at=args.at, target_route=args.target_route))
        elif args.command == "migrate":
            messages = split_list(args.messages)
            print_json(service.engine.advance_migration(
                tenant=args.tenant, consumer=args.consumer, topic=args.topic,
                messages=messages, batch_id=args.batch_id, at=args.at, execute=args.execute))
            service.save()
        elif args.command == "decommission":
            print_json(service.engine.advance_decommission(
                tenant=args.tenant, resources=args.resources, at=args.at,
                execute=args.execute, simulate_line=not args.no_simulate_line))
            service.save()
        elif args.command == "advance":
            messages = split_list(args.messages)
            migration = service.engine.advance_migration(
                tenant=args.tenant, consumer=args.consumer, topic=args.topic,
                messages=messages, batch_id=args.batch_id, at=args.at, execute=args.execute)
            decommission = service.engine.advance_decommission(
                tenant=args.tenant, resources=args.resources, at=args.at,
                execute=args.execute, simulate_line=not args.no_simulate_line)
            service.save()
            print_json({"migration": migration, "decommission": decommission})
        elif args.command == "line-action":
            print_json(service.line_action(args.resource, args.action, operation=args.operation,
                                           tenant=args.tenant, at=args.at, simulate=args.simulate,
                                           force_fail=args.force_fail if args.force_fail else None))
        elif args.command == "line-effect":
            print_json(service.line_effect(args.resource, args.state, operation=args.operation, at=args.at))
        elif args.command == "rollback":
            print_json(service.engine.rollback_resource(
                args.resource, at=args.at, execute=args.execute,
                simulate_line=not args.no_simulate_line))
            service.save()
        elif args.command == "takeover":
            print_json(service.take_over(args.ticket, args.agent, at=args.at, reason=args.reason))
        elif args.command == "observe":
            if args.component in {"receiver", "all"}:
                print_json(service.engine.state["receiver"])
            if args.component in {"line", "all"}:
                print_json(service.engine.state["line"])
        return 0
    except RetirementError as error:
        print(json.dumps({"error": type(error).__name__, "message": str(error)}, ensure_ascii=False),
              file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
