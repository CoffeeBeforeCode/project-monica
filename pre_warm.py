import logging
import azure.functions as func

bp_pre_warm = func.Blueprint()

@bp_pre_warm.timer_trigger(
    schedule="0 45 3 * * *",  # 03:45 UTC every day — 15 minutes before 04:00 UTC (05:00 BST)
    arg_name="timer",
    run_on_startup=False,
    use_monitor=False,
)
def preWarm(timer: func.TimerRequest) -> None:
    """
    Pre-warm check. Fires at 03:45 UTC — 15 minutes before the critical
    04:00 UTC task creation and digest window (05:00 BST year-round).

    Why this exists
    ---------------
    The Function App runs on a B1 Basic App Service Plan (always-on), but
    the platform can still recycle the host overnight. If the host is recycled,
    the Managed Identity sidecar at 169.254.129.3:8081 may not be ready when
    the first functions fire at 04:00 UTC, causing token acquisition to time
    out and all 04:00 UTC functions to exit silently with no tasks or digest.

    preWarm solves this by firing at 03:45 UTC — 15 minutes early — so the
    host and sidecar are fully initialised before the 04:00 UTC window.

    Why 03:45 UTC and not 04:45 UTC
    --------------------------------
    During GMT, 05:00 London = 05:00 UTC. During BST, 05:00 London = 04:00 UTC.
    The previous schedule of 04:45 UTC was 15 minutes early in GMT but
    45 minutes late in BST — arriving after the functions it was meant to
    protect had already failed. 03:45 UTC is 15 minutes early in both seasons.

    Observability
    -------------
    PRE_WARM_OK  — host was alive and warm at 03:45 UTC
    PRE_WARM_LATE — host was recycled; this fired past its scheduled time
    """
    if timer.past_due:
        logging.warning(
            "PRE_WARM_LATE: preWarm fired past its scheduled time. "
            "Host was not alive at 03:45 UTC. The 04:00 UTC task window may "
            "have been at risk. Check createMorningTasks and emailDigest0500 logs."
        )
    else:
        logging.info(
            "PRE_WARM_OK: Host confirmed alive at 03:45 UTC. "
            "04:00 UTC task creation and digest window is covered."
        )
