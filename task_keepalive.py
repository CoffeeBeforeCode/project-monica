# task_keepalive.py
# Why: The Azure Consumption plan shuts down the host after a period of
# inactivity. When the host is cold at the moment a Timer Trigger is
# scheduled to fire, the invocation is silently lost. This keepAlive
# trigger fires every 4 minutes to keep the host warm, ensuring all
# scheduled triggers fire reliably. It does nothing except log a
# heartbeat — no Graph calls, no task creation, no cost beyond the
# minimal execution time.

import azure.functions as func
import logging

bp = func.Blueprint()


@bp.timer_trigger(
    schedule="0 */4 * * * *",
    arg_name="timer",
    run_on_startup=False
)
def keepAlive(timer: func.TimerRequest) -> None:
    logging.info("Monica keepAlive heartbeat.")
