import signal
import os
import sys
import logging
import tornado
from src.katportal_server import BLKATPortalClient

log = logging.getLogger("BLUSE.interface")

def on_shutdown():
    # TODO: uncomment when you deploy
    # notify_slack("KATPortal module at MeerKAT has halted. Might want to check that!")
    log.info("Shutting Down Katportal Clients")
    sys.exit()

if __name__ == '__main__':
    
    FORMAT = "[ %(levelname)s - %(asctime)s - %(filename)s:%(lineno)s] %(message)s"
    # logger = logging.getLogger('reynard')
    logging.basicConfig(format=FORMAT)
    log.setLevel(logging.DEBUG)
    log.info("Starting Katportal Client")
    syslog_addr = '/dev/log' if os.path.exists('/dev/log') else '/var/run/syslog'
    handler = logging.handlers.SysLogHandler(address=syslog_addr) 
    log.addHandler(handler)

    client = BLKATPortalClient()
    signal.signal(signal.SIGINT, lambda sig, frame: on_shutdown())
    client.start()