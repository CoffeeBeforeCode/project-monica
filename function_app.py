# function_app.py
# Why: Minimal entry point. Each function lives in its own file as a Blueprint.
# Registering them here means function_app.py never contains business logic —
# it is only a registry. A broken task file cannot take down other functions.

import azure.functions as func

from task_chain      import bp as bp_task_chain
from webhook_renewal import bp as bp_renewal
from task_morning    import bp as bp_morning
from task_evening    import bp as bp_evening
from task_monday     import bp as bp_monday
from task_tuesday    import bp as bp_tuesday
from task_wednesday  import bp as bp_wednesday
from task_thursday   import bp as bp_thursday
from task_friday     import bp as bp_friday
from task_sunday     import bp as bp_sunday
from task_monthly    import bp as bp_monthly
from task_keepalive  import bp as bp_keepalive

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

app.register_functions(bp_task_chain)
app.register_functions(bp_renewal)
app.register_functions(bp_morning)
app.register_functions(bp_evening)
app.register_functions(bp_monday)
app.register_functions(bp_tuesday)
app.register_functions(bp_wednesday)
app.register_functions(bp_thursday)
app.register_functions(bp_friday)
app.register_functions(bp_sunday)
app.register_functions(bp_monthly)
app.register_functions(bp_keepalive)


@app.route(route="ping", auth_level=func.AuthLevel.ANONYMOUS)
def ping(req: func.HttpRequest) -> func.HttpResponse:
    return func.HttpResponse("Monica is online.", status_code=200)
