import logging
import azure.functions as func

# Why: We define a Blueprint so this file follows the same pattern as every
# other Monica function file (task_morning.py, task_guardian.py, etc.).
# A Blueprint is a self-contained collection of triggers that function_app.py
# imports and registers. This means one broken file can never take down the
# others.
bp_pre_warm = func.Blueprint()


@bp_pre_warm.timer_trigger(
    schedule="0 45 4 * * *",  # 04:45 UTC every day
    arg_name="timer",
    run_on_startup=False,
    use_monitor=False,
)
def preWarm(timer: func.TimerRequest) -> None:
    """
    Pre-warm check. Fires at 04:45 UTC — 15 minutes before the critical
    05:00 task creation window.

    Why this exists
    ---------------
    Azure Consumption plan hosts can be recycled by the platform at any time.
    keepAlive (every 4 minutes) prevents cold starts caused by *inactivity*,
    but cannot prevent a platform-initiated recycle. If the host is recycled
    overnight, keepAlive will eventually wake it — but there is no guarantee
    it will be fully warm by 05:00.

    preWarm solves this with an explicit pre-flight check:
      - It fires at 04:45, waking the host if it has been recycled.
      - It logs a clearly labelled Application Insights entry.
      - If the 04:45 entry is present in the logs, you have confirmation the
        host was alive before 05:00.
      - If it is ABSENT, that absence is the diagnostic signal — the host was
        still cold at 04:45, meaning the 05:00 window was at risk.

    This pairs with taskGuardian (05:15) to give belt-and-braces coverage:
      04:45  preWarm      → explicit wake-up and evidence of life
      05:00  task creation → morning tasks created (should now reliably fire)
      05:15  taskGuardian → verifies tasks are present; creates any that are
                            missing and logs a recovery warning
    """

    # Why use past_due: Azure tells us whether this trigger fired late.
    # If the host was recycled and the 04:45 slot was missed, Azure will
    # fire it as soon as the host recovers, setting past_due=True.
    # We log this explicitly so it is visible in Application Insights.
    if timer.past_due:
        logging.warning(
            "PRE_WARM_LATE: preWarm fired past its scheduled time. "
            "Host was not alive at 04:45 UTC. The 05:00 task window may "
            "have been at risk. Check createMorningTasks and taskGuardian logs."
        )
    else:
        logging.info(
            "PRE_WARM_OK: Host confirmed alive at 04:45 UTC. "
            "05:00 task creation window is covered."
        )
