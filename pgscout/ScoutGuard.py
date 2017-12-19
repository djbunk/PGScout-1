import logging
import sys
import time

from pgscout.Scout import Scout
from pgscout.config import use_pgpool, cfg_get
from pgscout.utils import load_pgpool_accounts

log = logging.getLogger(__name__)


class ScoutGuard(object):

    def __init__(self, auth, username, password, job_queue):
        self.job_queue = job_queue
        self.active = False

        # Set up initial account
        initial_account = {
            'auth_service': auth,
            'username': username,
            'password': password
        }
        if not username and use_pgpool():
            for x in range(0,cfg_get('pgpool-acct-multiplier')):
                initial_account = load_pgpool_accounts(1, reuse=True)
        self.acc = self.init_scout(initial_account)
        self.active = True

    def init_scout(self, acc_data):
        return Scout(acc_data['auth_service'], acc_data['username'], acc_data['password'], self.job_queue)

    def run(self):
        while True:
            self.active = True
            self.acc.run()
            self.active = False

            # Scout disabled, probably (shadow)banned.
            if use_pgpool():
                self.swap_account()
            else:
                # We don't have a replacement account, so just wait a veeeery long time.
                time.sleep(60*60*24*1000)
                break

    def swap_account(self):
        self.acc.release(reason=self.acc.last_msg)
        while True:
            new_acc = load_pgpool_accounts(1)
            if new_acc:
                log.info("Swapping bad account {} with new account {}".format(self.acc.username, new_acc['username']))
                self.acc = self.init_scout(new_acc)
                break
            log.warning("Could not request new account from PGPool. Out of accounts? Retrying in 1 minute.")
            time.sleep(60)
