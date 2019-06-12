import asyncio
import functools
import os
import signal

def ask_exit(signame):
    logger.debug("got signal %s: exit" % signame)
    loop.stop()

loop = asyncio.get_event_loop()
for signame in ('SIGINT', 'SIGTERM'):
    loop.add_signal_handler(getattr(signal, signame),
                            functools.partial(ask_exit, signame))

logger.debug("Event loop running forever, press Ctrl+C to interrupt.")
logger.debug("pid %s: send SIGINT or SIGTERM to exit." % os.getpid())
try:
    loop.run_forever()
finally:
    loop.close()
    
