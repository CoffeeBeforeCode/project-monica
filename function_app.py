"""
function_app.py — Monica Function App entry point
WHY this file exists:
  The Azure Functions Python v2 programming model requires a single entry
  point that imports and registers all Blueprint objects. This file is
  intentionally minimal — it contains only the ping health-check and the
  blueprint registrations. All business logic lives in the individual
  Blueprint files.
  Keeping this file small means that a syntax error in any Blueprint file
  crashes only that Blueprint. If this file itself crashes, all functions
  go down — so it must stay simple and free of logic.
Current function inventory (19 timer triggers + 3 HTTP triggers = 22 total):
  File                     Function                    Type
  ─────────────────────────────────────────────────────────────────────────
  function_app.py          ping                        HTTP Trigger
  task_chain.py            taskChain                   HTTP Trigger
  messages.py              messages                    HTTP Trigger
  email_digest_0500.py     emailDigest0500             Timer — daily 05:00
  email_digest_0700.py     emailDigest0700             Timer — daily 07:00
  email_digest_0900.py     emailDigest0900             Timer — daily 09:00
  email_digest_1100.py     emailDigest1100             Timer — daily 11:00
  email_digest_1300.py     emailDigest1300             Timer — daily 13:00
  email_digest_1500.py     emailDigest1500             Timer — daily 15:00
  email_digest_1700.py     emailDigest1700             Timer — daily 17:00
  email_digest_1900.py     emailDigest1900             Timer — daily 19:00
  webhook_renewal.py       renewWebhookSubscriptions   Timer — daily 07:00
  task_morning.py          createMorningTasks          Timer — daily 05:00
  task_evening.py          createEveningTasks          Timer — daily 17:00
  task_monday.py           createMondayTasks           Timer — Monday 05:00
  task_tuesday.py          createTuesdayTasks          Timer — Tuesday 05:00
  task_wednesday.py        createWednesdayTasks        Timer — Wednesday 05:00
  task_thursday.py         createThursdayTasks         Timer — Thursday 05:00
  task_friday.py           createFridayTasks           Timer — Friday 05:00
  task_sunday.py           createSundayTasks           Timer — Sunday 17:00
  task_monthly.py          createMonthlyTasks          Timer — 1st of month 05:00
  task_guardian.py         taskGuardian                Timer — daily 08:00
  Session 33 change:
    email_digest.py (single file, single timer trigger covering all slots
    via runtime conditional logic) replaced by eight independent slot files
    — one per time slot, each with its own cron expression and Blueprint.
    digest_shared.py added as a shared helper module (not a Blueprint —
    it exports no timer trigger and requires no registration here).
    Sunday 05:00 suppression removed. All eight slots fire seven days a week.
  Note: task_keepalive.py removed in Session 30. keepAlive was introduced
  to prevent the Consumption plan host going cold. The project is now on
  the B1 Basic App Service Plan, which is always-on. keepAlive serves
  no purpose on B1 and has been deleted.
"""
import azure.functions as func
# ── Blueprint imports ─────────────────────────────────────────────────────────
# WHY import as aliases (bp_xxx):
#   Every Blueprint file exports a variable named `bp`. If we imported them
#   all as `bp`, each import would overwrite the previous. Aliasing makes
#   each one distinct and makes the registration list below self-documenting.
from task_chain          import bp as bp_task_chain
from messages            import bp as bp_messages
from email_digest_0500   import bp as bp_digest_0500
from email_digest_0700   import bp as bp_digest_0700
from email_digest_0900   import bp as bp_digest_0900
from email_digest_1100   import bp as bp_digest_1100
from email_digest_1300   import bp as bp_digest_1300
from email_digest_1500   import bp as bp_digest_1500
from email_digest_1700   import bp as bp_digest_1700
from email_digest_1900   import bp as bp_digest_1900
from webhook_renewal     import bp as bp_renewal
from task_morning        import bp as bp_morning
from task_evening        import bp as bp_evening
from task_monday         import bp as bp_monday
from task_tuesday        import bp as bp_tuesday
from task_wednesday      import bp as bp_wednesday
from task_thursday       import bp as bp_thursday
from task_friday         import bp as bp_friday
from task_sunday         import bp as bp_sunday
from task_monthly        import bp as bp_monthly
from task_guardian       import bp as bp_guardian
# ── App initialisation ────────────────────────────────────────────────────────
app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)
# ── Blueprint registration ────────────────────────────────────────────────────
# WHY register_blueprint:
#   This is how the v2 model discovers functions defined in other files.
#   The runtime calls into each registered Blueprint and finds the decorated
#   functions inside it. Without registration, a Blueprint file is ignored.
# WHY digest_shared.py is not registered here:
#   digest_shared.py is a plain Python module containing shared helpers and
#   constants. It exports no Blueprint and defines no timer trigger. It is
#   imported directly by the eight slot files — not by function_app.py.
app.register_blueprint(bp_task_chain)
app.register_blueprint(bp_messages)
app.register_blueprint(bp_digest_0500)
app.register_blueprint(bp_digest_0700)
app.register_blueprint(bp_digest_0900)
app.register_blueprint(bp_digest_1100)
app.register_blueprint(bp_digest_1300)
app.register_blueprint(bp_digest_1500)
app.register_blueprint(bp_digest_1700)
app.register_blueprint(bp_digest_1900)
app.register_blueprint(bp_renewal)
app.register_blueprint(bp_morning)
app.register_blueprint(bp_evening)
app.register_blueprint(bp_monday)
app.register_blueprint(bp_tuesday)
app.register_blueprint(bp_wednesday)
app.register_blueprint(bp_thursday)
app.register_blueprint(bp_friday)
app.register_blueprint(bp_sunday)
app.register_blueprint(bp_monthly)
app.register_blueprint(bp_guardian)
# ── Ping health-check ─────────────────────────────────────────────────────────
@app.route(route="ping", methods=["GET"])
def ping(req: func.HttpRequest) -> func.HttpResponse:
    """
    WHY ping lives here and not in its own file:
      It is a one-liner with no dependencies. Moving it to a Blueprint file
      would add a file with no benefit. Ping's only job is to confirm the
      Function App is alive — it should be the last thing standing if
      everything else fails, so keeping it in the entry point is correct.
    """
    return func.HttpResponse("Monica is alive.", status_code=200)
