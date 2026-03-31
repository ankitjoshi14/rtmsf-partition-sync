"""
sim_renderers.py — All Rich panel rendering for RTMSF simulation.
"""

import time

from rich.columns import Columns
from rich.console import Console, Group
from rich.panel import Panel
from rich.rule import Rule
from rich.table import Table
from rich.text import Text
from rich import box as rich_box

from mock_kafka import KafkaTopic
from rtmsf_worker import RTMSFWorker, TABLE_ROOT
from mock_storage import MockStorage
from mock_catalog import MockCatalog

from sim_scenarios import EPOCH_SCENARIOS


# ------------------------------------------------------------------ helpers

def _ts() -> str:
    return time.strftime("%H:%M:%S")


def _short(s: str, width: int = 30) -> str:
    return (s[:width - 2] + "..") if len(s) > width else s


# ------------------------------------------------------------------ renderers

def render_header(W: float, step_mode: bool, epoch_num: int) -> Panel:
    t = Text(justify="center")
    t.append("  Cloud Storage ", style="bold cyan")
    t.append("──▶ ", style="dim")
    t.append("Kafka ", style="bold blue")
    t.append("──▶ ", style="dim")
    t.append("Normalizer ", style="bold magenta")
    t.append("──▶ ", style="dim")
    t.append(f"WOERA x2 (W={W}s) ", style="bold yellow")
    t.append("──▶ ", style="dim")
    t.append("PMCP + Catalog\n", style="bold green")
    if step_mode:
        scenario = EPOCH_SCENARIOS[epoch_num % len(EPOCH_SCENARIOS)]
        t.append(
            f"  Epoch {epoch_num + 1}/{len(EPOCH_SCENARIOS)}: {scenario['title']} — {scenario['tagline']}",
            style="bold white",
        )
    else:
        t.append(
            "  Files land in object store  →  Kafka queues everything (dups + OOO included)  "
            "→  WOERA coalesces  →  PMCP storage-verified catalog write",
            style="dim italic",
        )
    return Panel(t, border_style="dim", padding=(0, 1))


def build_events_panel(state, topic: KafkaTopic, step_mode: bool = False) -> Panel:
    """
    Combined panel: Object Store (top half) + Kafka Topic (bottom half).
    Replaces the two separate panels from the previous 4-panel layout.
    """
    from simulate import MAX_LOG

    # ---- Object Store section ----
    obj_table = Table(box=None, show_header=False, padding=(0, 0), expand=True)
    obj_table.add_column("ts",   style="dim", width=9,  no_wrap=True)
    obj_table.add_column("body", ratio=1)

    display_limit = 20 if step_mode else 10
    for (ts, op, pkey, fname, prev_count) in list(state.event_log)[:display_limit]:
        if op == "OBJECT_DELETED":
            icon  = "[red]🗑 [/red]"
            op_label = "[red]OBJECT_DELETED[/red]"
        else:
            icon  = "[green]🔔[/green]"
            op_label = "[bold]OBJECT_CREATED[/bold]"

        obj_table.add_row(
            f"[dim]{ts}[/dim]",
            f"{icon} {op_label}  [cyan]{TABLE_ROOT}{pkey}/{fname}[/cyan]",
        )

    obj_stats = Text.from_markup(
        f"[green]Notifs: {state.produced}[/green]"
    )

    # ---- Kafka + Normalizer (merged) ----
    kfk_table = Table(
        box=None, show_header=True,
        padding=(0, 1), expand=True, header_style="dim",
    )
    kfk_table.add_column("Offset", style="dim", width=6, no_wrap=True)
    kfk_table.add_column("Key", ratio=1)
    kfk_table.add_column("Partition", width=12, no_wrap=True)

    # Read recent messages from all partitions for display
    recent_msgs = []
    for p in topic.partitions:
        start = state._kafka_display_start.get(p.partition_id, 0)
        for msg in p.messages[start:]:
            recent_msgs.append(msg)
    # Sort by partition and offset (most recent first for display)
    recent_msgs.sort(key=lambda m: (m.partition, m.offset), reverse=True)

    for msg in recent_msgs[:display_limit]:
        pidx = msg.partition
        color = "magenta" if pidx == 0 else "blue"
        kfk_table.add_row(
            f"#{msg.offset}",
            f"[cyan]{msg.key}[/cyan]",
            f"[{color}]partition-{pidx}[/{color}]",
        )

    lag = topic.total_lag()
    lag_color = "red" if lag > 20 else "yellow" if lag > 5 else "green"
    kfk_stats = Text.from_markup(
        f"[{lag_color}]Lag: {lag}[/{lag_color}]  "
        f"[blue]Produced: {state.produced}[/blue]  "
        f"[dim]Consumed: {state.consumed}[/dim]"
    )

    body = Group(
        Text.from_markup("[bold dim]▸ Object Store[/bold dim]"),
        obj_table,
        obj_stats,
        Rule("Keyed Kafka Topic (partition-key affinity)", style="dim magenta"),
        kfk_table,
        kfk_stats,
    )

    return Panel(
        body,
        title="[bold cyan]STORAGE EVENTS → EVENT BUS[/bold cyan]",
        border_style="cyan",
        padding=(0, 1),
    )


def _render_buffer_table(snap: dict, flushed_keys: list = None) -> Table:
    """Render a single WOERA buffer snapshot as a Table."""
    buf_table = Table(
        box=None, show_header=True,
        padding=(0, 1), expand=True, header_style="dim",
    )
    buf_table.add_column("Table", width=16)
    buf_table.add_column("Partition Key", ratio=1)
    buf_table.add_column("Files", justify="right", width=5)
    buf_table.add_column("Flush in", justify="right", width=8)
    buf_table.add_column("Intent", justify="right", width=8)

    if snap:
        # Active buffer — show countdown
        for key, info in list(snap.items()):
            pkey = key[2] if isinstance(key, tuple) else str(key)
            flush_in = info["flush_in"]
            flush_color = "red" if flush_in < 1.0 else "yellow" if flush_in < 4.0 else "green"
            buf_table.add_row(
                "[cyan]warehouse.sales[/cyan]",
                f"[cyan]{pkey}[/cyan]",
                str(info["count"]),
                f"[{flush_color}]{flush_in:.1f}s[/{flush_color}]",
                "[dim]pending[/dim]",
            )
    elif flushed_keys:
        # Buffer empty after flush — show emitted intents
        for item in flushed_keys:
            kind = item["kind"]
            if kind == "ADD":
                intent_label = "[green]ADD[/green]"
            elif kind == "DROP":
                intent_label = "[red]DROP[/red]"
            else:
                intent_label = "[dim]—[/dim]"
            buf_table.add_row(
                "[cyan]warehouse.sales[/cyan]",
                f"[cyan]{item['pkey']}[/cyan]",
                str(item["count"]),
                "[dim]—[/dim]",
                intent_label,
            )

    return buf_table


def render_woera_buffer(state, worker_a: RTMSFWorker, worker_b: RTMSFWorker) -> Panel:
    # Worker-0 buffer
    buf_table_a = _render_buffer_table(state.buffer_snap_a, state.flushed_keys_a)
    # Worker-1 buffer
    buf_table_b = _render_buffer_table(state.buffer_snap_b, state.flushed_keys_b)

    w0_label = Table(box=None, show_header=False, padding=(0, 0), expand=True)
    w0_label.add_column(ratio=1)
    w0_label.add_row(f"[magenta]Worker-0 (partition-0)[/magenta]")

    w1_label = Table(box=None, show_header=False, padding=(0, 0), expand=True)
    w1_label.add_column(ratio=1)
    w1_label.add_row(f"[blue]Worker-1 (partition-1)[/blue]")

    body = Group(
        w0_label,
        buf_table_a,
        Rule(style="dim"),
        w1_label,
        buf_table_b,
    )

    return Panel(
        body,
        title="[bold yellow]WOERA BUFFER[/bold yellow]",
        subtitle="[dim]Quiet-window coalescing[/dim]",
        border_style="yellow",
        padding=(0, 1),
    )


def render_pmcp_catalog(state, worker_a: RTMSFWorker, worker_b: RTMSFWorker, step_mode: bool = False) -> Panel:
    t = Table(box=None, show_header=False, padding=(0, 0), expand=True)
    t.add_column("ts",   style="dim", width=9,  no_wrap=True)
    t.add_column("body", ratio=1)

    pmcp_limit = 20 if step_mode else 10
    for entry in list(state.pmcp_log)[:pmcp_limit]:
        ts, kind, pkey, status, head_ok, list_used, note, wlabel = entry
        if status == "APPLIED" and kind == "DROP":
            verify = "[red]✓ DROP APPLIED[/red]  [blue]LIST ✓[/blue]  [dim]partition removed[/dim]"
        elif status == "APPLIED" and note == "retry_resolved":
            verify = "[green]✓ APPLIED[/green]  [dim](retried)[/dim]"
        elif status == "APPLIED":
            if head_ok:
                verify = "[green]✓ APPLIED[/green]  [cyan]HEAD ✓[/cyan]  [dim]storage verified[/dim]"
            else:
                verify = (
                    "[green]✓ APPLIED[/green]  "
                    "[yellow]HEAD ✗[/yellow]  [blue]LIST ✓[/blue]  "
                    "[dim](visibility jitter)[/dim]"
                )
        elif status == "RETRY" and note == "stale_catalog_entry_cleared":
            verify = "[yellow]⚠ STALE ENTRY CLEARED[/yellow]  [blue]LIST ✓ empty[/blue]"
        elif status == "RETRY" and note == "not_ready":
            verify = "[red]↺ RETRY[/red]  [dim]re-queued[/dim]"
        elif status == "NOOP" and note == "already_exists_verified":
            verify = "[dim]~ NOOP — partition exists  [blue]LIST ✓[/blue]  files confirmed[/dim]"
        elif status == "NOOP" and kind == "DROP":
            verify = "[dim]~ NOOP — not in catalog[/dim]"
        elif status == "NOOP":
            verify = "[dim]~ NOOP — already committed[/dim]"
        else:
            verify = f"[yellow]{status}[/yellow]"

        t.add_row(
            f"[dim]{ts}[/dim]",
            f"{verify}  [cyan]{TABLE_ROOT}{pkey}[/cyan]",
        )

    # Catalog state section
    partitions = worker_a.catalog._partitions
    cat_table = Table(box=None, show_header=True, padding=(0, 1), expand=True, header_style="dim")
    cat_table.add_column("Table", width=18)
    cat_table.add_column("Partition Key", ratio=1)
    if partitions:
        for key in partitions:
            table_fqn = key[1] if isinstance(key, tuple) else "warehouse.sales"
            pkey_str = key[2] if isinstance(key, tuple) else str(key)
            cat_table.add_row(f"[cyan]{table_fqn}[/cyan]", f"[cyan]{pkey_str}[/cyan]")
    else:
        cat_table.add_row("[dim italic]No partitions committed yet[/dim italic]", "")

    p_a, p_b = worker_a.pmcp, worker_b.pmcp
    total_applied = p_a.applied + p_b.applied
    total_retries = state.retry_count
    total_head = worker_a.storage.head_requests  # shared storage
    total_list = worker_a.storage.list_requests  # shared storage
    body = Group(
        t,
        Rule("Catalog State", style="dim green"),
        cat_table,
    )
    return Panel(
        body,
        title="[bold green]PMCP + CATALOG[/bold green]",
        subtitle=(
            f"[dim]HEAD proof → catalog write  |  "
            f"[green]Applied: {total_applied}[/green]  "
            f"[red]Retries: {total_retries}[/red]  "
            f"[cyan]HEAD: {total_head}[/cyan]  "
            f"[blue]LIST: {total_list}[/blue][/dim]"
        ),
        border_style="green",
        padding=(0, 1),
    )


def show_epoch_summary(
    console: Console,
    state,
    worker_a: RTMSFWorker,
    worker_b: RTMSFWorker,
    storage: MockStorage,
    catalog: MockCatalog,
    W: float,
) -> bool:
    """Prompt for next epoch or show final results. Returns False if user quits."""
    ep_display  = state.epoch_num + 1

    # Hint for next epoch or end
    if ep_display < len(EPOCH_SCENARIOS):
        next_scenario = EPOCH_SCENARIOS[(state.epoch_num + 1) % len(EPOCH_SCENARIOS)]
        try:
            user_input = input(f"  Next: Epoch {ep_display + 1} — {next_scenario['title']}  [Enter] or 'q': ")
            return user_input.strip().lower() != "q"
        except (KeyboardInterrupt, EOFError):
            return False
    else:
        return False
