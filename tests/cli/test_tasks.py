"""Tests for task operation commands."""

from __future__ import annotations

from snowmin.operations.tasks import (
    list_tasks_command,
    resume_task_command,
    suspend_task_command,
)


class FakeCursor:
    def __init__(self, description=None, rows=None):
        self.description = description or []
        self._rows = rows or []

    def fetchall(self):
        return self._rows

    def close(self):
        pass


TASK_DESCRIPTION = [
    ("name",),
    ("state",),
    ("database_name",),
    ("schema_name",),
]

TASK_GRAPH_DESCRIPTION = TASK_DESCRIPTION + [("predecessors",)]
TASK_RELATIONS_DESCRIPTION = TASK_DESCRIPTION + [("task_relations",)]


def _ctx(mocker):
    ctx = mocker.Mock()
    ctx.obj = {"settings": mocker.Mock(), "cli_overrides": {}}
    return ctx


def _patch_config(mocker):
    mocker.patch(
        "snowmin.operations.tasks.get_merged_connection_config",
        return_value={"database": "RAP_DEV_ANALYTICS"},
    )


def test_suspend_all_only_processes_started_tasks(mocker):
    _patch_config(mocker)
    mocker.patch("snowmin.operations.tasks.click.confirm", return_value=True)
    execute = mocker.patch("snowmin.operations.tasks.ConnectionManager.execute")

    def execute_side_effect(query, conn_config):
        if query == "SHOW TASKS IN SCHEMA RAP_DEV_ANALYTICS.MART":
            return FakeCursor(
                description=TASK_DESCRIPTION,
                rows=[
                    ("LOAD_CUSTOMERS", "started", "RAP_DEV_ANALYTICS", "MART"),
                    ("LOAD_ORDERS", "suspended", "RAP_DEV_ANALYTICS", "MART"),
                ],
            )
        return FakeCursor()

    execute.side_effect = execute_side_effect

    suspend_task_command(
        _ctx(mocker),
        task_name=None,
        all=True,
        pattern=None,
        schema="RAP_DEV_ANALYTICS.MART",
    )

    executed_sql = [call.args[0] for call in execute.call_args_list]
    assert "ALTER TASK RAP_DEV_ANALYTICS.MART.LOAD_CUSTOMERS SUSPEND" in executed_sql
    assert "ALTER TASK RAP_DEV_ANALYTICS.MART.LOAD_ORDERS SUSPEND" not in executed_sql


def test_resume_pattern_only_processes_suspended_tasks(mocker):
    _patch_config(mocker)
    mocker.patch("snowmin.operations.tasks.click.confirm", return_value=True)
    execute = mocker.patch("snowmin.operations.tasks.ConnectionManager.execute")

    def execute_side_effect(query, conn_config):
        if query == "SHOW TASKS IN SCHEMA RAP_DEV_ANALYTICS.MART":
            return FakeCursor(
                description=TASK_DESCRIPTION,
                rows=[
                    ("LOAD_CUSTOMERS", "started", "RAP_DEV_ANALYTICS", "MART"),
                    ("LOAD_ORDERS", "suspended", "RAP_DEV_ANALYTICS", "MART"),
                ],
            )
        return FakeCursor()

    execute.side_effect = execute_side_effect

    resume_task_command(
        _ctx(mocker),
        task_name=None,
        all=False,
        pattern="^LOAD_",
        schema="RAP_DEV_ANALYTICS.MART",
    )

    executed_sql = [call.args[0] for call in execute.call_args_list]
    assert "ALTER TASK RAP_DEV_ANALYTICS.MART.LOAD_CUSTOMERS RESUME" not in executed_sql
    assert "ALTER TASK RAP_DEV_ANALYTICS.MART.LOAD_ORDERS RESUME" in executed_sql


def test_list_tasks_queries_comma_separated_schemas(mocker):
    _patch_config(mocker)
    execute = mocker.patch("snowmin.operations.tasks.ConnectionManager.execute")

    def execute_side_effect(query, conn_config):
        if query == "SHOW TASKS IN SCHEMA RAP_DEV_ANALYTICS.SILVER":
            return FakeCursor(
                description=TASK_DESCRIPTION,
                rows=[("LOAD_CUSTOMERS", "started", "RAP_DEV_ANALYTICS", "SILVER")],
            )
        if query == "SHOW TASKS IN SCHEMA RAP_DEV_ANALYTICS.GOLD":
            return FakeCursor(
                description=TASK_DESCRIPTION,
                rows=[("LOAD_ORDERS", "suspended", "RAP_DEV_ANALYTICS", "GOLD")],
            )
        return FakeCursor()

    execute.side_effect = execute_side_effect

    list_tasks_command(_ctx(mocker), schema="SILVER,GOLD")

    executed_sql = [call.args[0] for call in execute.call_args_list]
    assert executed_sql == [
        "SHOW TASKS IN SCHEMA RAP_DEV_ANALYTICS.SILVER",
        "SHOW TASKS IN SCHEMA RAP_DEV_ANALYTICS.GOLD",
    ]


def test_suspend_all_queries_comma_separated_schemas(mocker):
    _patch_config(mocker)
    mocker.patch("snowmin.operations.tasks.click.confirm", return_value=True)
    execute = mocker.patch("snowmin.operations.tasks.ConnectionManager.execute")

    def execute_side_effect(query, conn_config):
        if query == "SHOW TASKS IN SCHEMA RAP_DEV_ANALYTICS.SILVER":
            return FakeCursor(
                description=TASK_DESCRIPTION,
                rows=[("LOAD_CUSTOMERS", "started", "RAP_DEV_ANALYTICS", "SILVER")],
            )
        if query == "SHOW TASKS IN SCHEMA RAP_DEV_ANALYTICS.GOLD":
            return FakeCursor(
                description=TASK_DESCRIPTION,
                rows=[("LOAD_ORDERS", "started", "RAP_DEV_ANALYTICS", "GOLD")],
            )
        return FakeCursor()

    execute.side_effect = execute_side_effect

    suspend_task_command(
        _ctx(mocker),
        task_name=None,
        all=True,
        pattern=None,
        schema="SILVER,GOLD",
    )

    executed_sql = [call.args[0] for call in execute.call_args_list]
    assert "ALTER TASK RAP_DEV_ANALYTICS.SILVER.LOAD_CUSTOMERS SUSPEND" in executed_sql
    assert "ALTER TASK RAP_DEV_ANALYTICS.GOLD.LOAD_ORDERS SUSPEND" in executed_sql


def test_single_suspend_skips_task_that_is_not_started(mocker):
    _patch_config(mocker)
    execute = mocker.patch("snowmin.operations.tasks.ConnectionManager.execute")

    execute.return_value = FakeCursor(
        description=TASK_DESCRIPTION,
        rows=[("LOAD_CUSTOMERS", "suspended", "RAP_DEV_ANALYTICS", "MART")],
    )

    suspend_task_command(
        _ctx(mocker),
        task_name="LOAD_CUSTOMERS",
        all=False,
        pattern=None,
        schema="RAP_DEV_ANALYTICS.MART",
    )

    executed_sql = [call.args[0] for call in execute.call_args_list]
    assert executed_sql == [
        "SHOW TASKS LIKE 'LOAD_CUSTOMERS' IN SCHEMA RAP_DEV_ANALYTICS.MART"
    ]


def test_single_suspend_non_graph_task_executes_once(mocker):
    _patch_config(mocker)
    execute = mocker.patch("snowmin.operations.tasks.ConnectionManager.execute")

    def execute_side_effect(query, conn_config):
        if query == "SHOW TASKS LIKE 'LOAD_CUSTOMERS' IN SCHEMA RAP_DEV_ANALYTICS.MART":
            return FakeCursor(
                description=TASK_DESCRIPTION,
                rows=[("LOAD_CUSTOMERS", "started", "RAP_DEV_ANALYTICS", "MART")],
            )
        return FakeCursor()

    execute.side_effect = execute_side_effect

    suspend_task_command(
        _ctx(mocker),
        task_name="LOAD_CUSTOMERS",
        all=False,
        pattern=None,
        schema="RAP_DEV_ANALYTICS.MART",
    )

    executed_sql = [call.args[0] for call in execute.call_args_list]
    assert executed_sql == [
        "SHOW TASKS LIKE 'LOAD_CUSTOMERS' IN SCHEMA RAP_DEV_ANALYTICS.MART",
        "ALTER TASK RAP_DEV_ANALYTICS.MART.LOAD_CUSTOMERS SUSPEND",
    ]


def test_suspend_all_suspends_graph_root_before_children(mocker):
    _patch_config(mocker)
    mocker.patch("snowmin.operations.tasks.click.confirm", return_value=True)
    execute = mocker.patch("snowmin.operations.tasks.ConnectionManager.execute")

    def execute_side_effect(query, conn_config):
        if query == "SHOW TASKS IN SCHEMA RAP_DEV_ANALYTICS.MART":
            return FakeCursor(
                description=TASK_GRAPH_DESCRIPTION,
                rows=[
                    ("ROOT_TASK", "started", "RAP_DEV_ANALYTICS", "MART", "[]"),
                    (
                        "CHILD_A",
                        "started",
                        "RAP_DEV_ANALYTICS",
                        "MART",
                        '["RAP_DEV_ANALYTICS.MART.ROOT_TASK"]',
                    ),
                    (
                        "CHILD_B",
                        "started",
                        "RAP_DEV_ANALYTICS",
                        "MART",
                        '["RAP_DEV_ANALYTICS.MART.ROOT_TASK"]',
                    ),
                ],
            )
        return FakeCursor()

    execute.side_effect = execute_side_effect

    suspend_task_command(
        _ctx(mocker),
        task_name=None,
        all=True,
        pattern=None,
        schema="RAP_DEV_ANALYTICS.MART",
    )

    executed_sql = [call.args[0] for call in execute.call_args_list]
    root_sql = "ALTER TASK RAP_DEV_ANALYTICS.MART.ROOT_TASK SUSPEND"
    child_a_sql = "ALTER TASK RAP_DEV_ANALYTICS.MART.CHILD_A SUSPEND"
    child_b_sql = "ALTER TASK RAP_DEV_ANALYTICS.MART.CHILD_B SUSPEND"
    assert executed_sql.count(root_sql) == 1
    assert executed_sql.index(root_sql) < executed_sql.index(child_a_sql)
    assert executed_sql.index(root_sql) < executed_sql.index(child_b_sql)


def test_single_suspend_graph_child_suspends_root_first(mocker):
    _patch_config(mocker)
    execute = mocker.patch("snowmin.operations.tasks.ConnectionManager.execute")

    def execute_side_effect(query, conn_config):
        if query == "SHOW TASKS LIKE 'CHILD_TASK' IN SCHEMA RAP_DEV_ANALYTICS.MART":
            return FakeCursor(
                description=TASK_GRAPH_DESCRIPTION,
                rows=[
                    (
                        "CHILD_TASK",
                        "started",
                        "RAP_DEV_ANALYTICS",
                        "MART",
                        '["RAP_DEV_ANALYTICS.MART.ROOT_TASK"]',
                    ),
                ],
            )
        if query == "SHOW TASKS IN SCHEMA RAP_DEV_ANALYTICS.MART":
            return FakeCursor(
                description=TASK_GRAPH_DESCRIPTION,
                rows=[
                    ("ROOT_TASK", "started", "RAP_DEV_ANALYTICS", "MART", "[]"),
                    (
                        "CHILD_TASK",
                        "started",
                        "RAP_DEV_ANALYTICS",
                        "MART",
                        '["RAP_DEV_ANALYTICS.MART.ROOT_TASK"]',
                    ),
                ],
            )
        return FakeCursor()

    execute.side_effect = execute_side_effect

    suspend_task_command(
        _ctx(mocker),
        task_name="CHILD_TASK",
        all=False,
        pattern=None,
        schema="RAP_DEV_ANALYTICS.MART",
    )

    executed_sql = [call.args[0] for call in execute.call_args_list]
    root_sql = "ALTER TASK RAP_DEV_ANALYTICS.MART.ROOT_TASK SUSPEND"
    child_sql = "ALTER TASK RAP_DEV_ANALYTICS.MART.CHILD_TASK SUSPEND"
    assert root_sql in executed_sql
    assert child_sql in executed_sql
    assert executed_sql.index(root_sql) < executed_sql.index(child_sql)


def test_single_resume_graph_root_uses_task_dependents_enable(mocker):
    _patch_config(mocker)
    execute = mocker.patch("snowmin.operations.tasks.ConnectionManager.execute")

    def execute_side_effect(query, conn_config):
        if query == "SHOW TASKS LIKE 'ROOT_TASK' IN SCHEMA RAP_DEV_ANALYTICS.MART":
            return FakeCursor(
                description=TASK_GRAPH_DESCRIPTION,
                rows=[
                    ("ROOT_TASK", "suspended", "RAP_DEV_ANALYTICS", "MART", "[]"),
                ],
            )
        if query == "SHOW TASKS IN SCHEMA RAP_DEV_ANALYTICS.MART":
            return FakeCursor(
                description=TASK_GRAPH_DESCRIPTION,
                rows=[
                    ("ROOT_TASK", "suspended", "RAP_DEV_ANALYTICS", "MART", "[]"),
                    (
                        "CHILD_TASK",
                        "suspended",
                        "RAP_DEV_ANALYTICS",
                        "MART",
                        '["RAP_DEV_ANALYTICS.MART.ROOT_TASK"]',
                    ),
                ],
            )
        return FakeCursor()

    execute.side_effect = execute_side_effect

    resume_task_command(
        _ctx(mocker),
        task_name="ROOT_TASK",
        all=False,
        pattern=None,
        schema="RAP_DEV_ANALYTICS.MART",
    )

    executed_sql = [call.args[0] for call in execute.call_args_list]
    assert "ALTER TASK RAP_DEV_ANALYTICS.MART.ROOT_TASK RESUME" not in executed_sql
    assert "ALTER TASK RAP_DEV_ANALYTICS.MART.ROOT_TASK SUSPEND" in executed_sql
    assert (
        "SELECT SYSTEM$TASK_DEPENDENTS_ENABLE('RAP_DEV_ANALYTICS.MART.ROOT_TASK')"
        in executed_sql
    )


def test_single_resume_graph_child_resumes_root_graph(mocker):
    _patch_config(mocker)
    execute = mocker.patch("snowmin.operations.tasks.ConnectionManager.execute")

    def execute_side_effect(query, conn_config):
        if query == "SHOW TASKS LIKE 'CHILD_TASK' IN SCHEMA RAP_DEV_ANALYTICS.MART":
            return FakeCursor(
                description=TASK_GRAPH_DESCRIPTION,
                rows=[
                    (
                        "CHILD_TASK",
                        "suspended",
                        "RAP_DEV_ANALYTICS",
                        "MART",
                        '["RAP_DEV_ANALYTICS.MART.ROOT_TASK"]',
                    ),
                ],
            )
        if query == "SHOW TASKS IN SCHEMA RAP_DEV_ANALYTICS.MART":
            return FakeCursor(
                description=TASK_GRAPH_DESCRIPTION,
                rows=[
                    ("ROOT_TASK", "suspended", "RAP_DEV_ANALYTICS", "MART", "[]"),
                    (
                        "CHILD_TASK",
                        "suspended",
                        "RAP_DEV_ANALYTICS",
                        "MART",
                        '["RAP_DEV_ANALYTICS.MART.ROOT_TASK"]',
                    ),
                ],
            )
        return FakeCursor()

    execute.side_effect = execute_side_effect

    resume_task_command(
        _ctx(mocker),
        task_name="CHILD_TASK",
        all=False,
        pattern=None,
        schema="RAP_DEV_ANALYTICS.MART",
    )

    executed_sql = [call.args[0] for call in execute.call_args_list]
    assert "ALTER TASK RAP_DEV_ANALYTICS.MART.CHILD_TASK RESUME" not in executed_sql
    assert "ALTER TASK RAP_DEV_ANALYTICS.MART.ROOT_TASK SUSPEND" in executed_sql
    assert (
        "SELECT SYSTEM$TASK_DEPENDENTS_ENABLE('RAP_DEV_ANALYTICS.MART.ROOT_TASK')"
        in executed_sql
    )


def test_single_resume_finalizer_resumes_root_graph(mocker):
    _patch_config(mocker)
    execute = mocker.patch("snowmin.operations.tasks.ConnectionManager.execute")

    def execute_side_effect(query, conn_config):
        if (
            query
            == "SHOW TASKS LIKE 'ROOT_TASK_FINALIZER' IN SCHEMA RAP_DEV_ANALYTICS.MART"
        ):
            return FakeCursor(
                description=TASK_RELATIONS_DESCRIPTION,
                rows=[
                    (
                        "ROOT_TASK_FINALIZER",
                        "suspended",
                        "RAP_DEV_ANALYTICS",
                        "MART",
                        '{"FinalizedRootTask":"RAP_DEV_ANALYTICS.MART.ROOT_TASK","Predecessors":[]}',
                    ),
                ],
            )
        if query == "SHOW TASKS IN SCHEMA RAP_DEV_ANALYTICS.MART":
            return FakeCursor(
                description=TASK_RELATIONS_DESCRIPTION,
                rows=[
                    (
                        "ROOT_TASK",
                        "suspended",
                        "RAP_DEV_ANALYTICS",
                        "MART",
                        '{"Predecessors":[],"FinalizerTask":"RAP_DEV_ANALYTICS.MART.ROOT_TASK_FINALIZER"}',
                    ),
                    (
                        "ROOT_TASK_FINALIZER",
                        "suspended",
                        "RAP_DEV_ANALYTICS",
                        "MART",
                        '{"FinalizedRootTask":"RAP_DEV_ANALYTICS.MART.ROOT_TASK","Predecessors":[]}',
                    ),
                ],
            )
        return FakeCursor()

    execute.side_effect = execute_side_effect

    resume_task_command(
        _ctx(mocker),
        task_name="ROOT_TASK_FINALIZER",
        all=False,
        pattern=None,
        schema="RAP_DEV_ANALYTICS.MART",
    )

    executed_sql = [call.args[0] for call in execute.call_args_list]
    assert (
        "ALTER TASK RAP_DEV_ANALYTICS.MART.ROOT_TASK_FINALIZER RESUME"
        not in executed_sql
    )
    assert "ALTER TASK RAP_DEV_ANALYTICS.MART.ROOT_TASK SUSPEND" in executed_sql
    assert (
        "SELECT SYSTEM$TASK_DEPENDENTS_ENABLE('RAP_DEV_ANALYTICS.MART.ROOT_TASK')"
        in executed_sql
    )


def test_single_resume_uses_bare_name_when_task_is_qualified(mocker):
    _patch_config(mocker)
    execute = mocker.patch("snowmin.operations.tasks.ConnectionManager.execute")

    def execute_side_effect(query, conn_config):
        if query == "SHOW TASKS LIKE 'LOAD_CUSTOMERS' IN SCHEMA DB1.SCHEMA1":
            return FakeCursor(
                description=TASK_DESCRIPTION,
                rows=[("LOAD_CUSTOMERS", "suspended", "DB1", "SCHEMA1")],
            )
        return FakeCursor()

    execute.side_effect = execute_side_effect

    resume_task_command(
        _ctx(mocker),
        task_name="DB1.SCHEMA1.LOAD_CUSTOMERS",
        all=False,
        pattern=None,
        schema=None,
    )

    executed_sql = [call.args[0] for call in execute.call_args_list]
    assert executed_sql == [
        "SHOW TASKS LIKE 'LOAD_CUSTOMERS' IN SCHEMA DB1.SCHEMA1",
        "ALTER TASK DB1.SCHEMA1.LOAD_CUSTOMERS RESUME",
    ]
