import time
import logging
import sys
import time

from pgscout.Scout import Scout
from pgscout.config import use_pgpool, cfg_get
from pgscout.utils import load_pgpool_accounts
import pgscout.Scout

log = logging.getLogger(__name__)

def update_multiplier_accounts(username, password, acctinfo):
    for scout in pgscout.Scout.scouts:
        if (scout.acc.username == username) and (scout.acc.password == password):
            if (scout.duplicate == 2):
                log.info("ERROR! Wrong account status detected {} {} {}".format(username,password,scout.duplicate))
            elif (scout.duplicate == 1):           
                scout.acc.release(reason="removing multiplier account")
                scout.acc = scout.init_scout(acctinfo)
                scout.duplicate = 2

class ScoutGuard(object):

    def __init__(self, auth, username, password, job_queue, duplicate, index):
        self.job_queue = job_queue
        self.active = False
        self.duplicate = duplicate  # 0 = master account, 1 = duplicate account, 2 = release duplicate account from semaphore loop 
        self.index = index          # possibly to be used for staggering account logins

        # Set up initial account
        initial_account = {
            'auth_service': auth,
            'username': username,
            'password': password
        }
        if not username and use_pgpool():
#            for x in range(0,cfg_get('pgpool_acct_multiplier')):
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

            # if duplicate wait for master account to reconfigure this account to new login info
            if self.duplicate == 1:
                while self.duplicate == 1:     # wait for master account to move duplicate status from 1 to 2
    #                log.info("sempahore loop {}".format(self.acc))
                    time.sleep(1)
                time.sleep(10*(self.index+1))  # time delay to stagger logins after account change (unproven)
                
            # Scout disabled, probably (shadow)banned.
            if self.duplicate == 0:
                if use_pgpool():
                    self.swap_account()
                else:
                    # We don't have a replacement account, so just wait a veeeery long time.
                    time.sleep(60*60*24*1000)
                    break
            else:
                self.duplicate = 1;

    def swap_account(self):
        username = self.acc.username
        password = self.acc.password
        self.acc.release(reason=self.acc.last_msg)
        while True:
            new_acc = load_pgpool_accounts(1)
            if new_acc:
                log.info("Swapping bad account {} with new account {}".format(self.acc.username, new_acc['username']))
                self.acc = self.init_scout(new_acc)
                update_multiplier_accounts(username,password,new_acc)
                break
            log.warning("Could not request new account from PGPool. Out of accounts? Retrying in 1 minute.")
            time.sleep(60)
