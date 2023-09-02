import threading
import time
import pandas as pd
import pandas_gbq
from collections import defaultdict


class BqUploader:
    def __init__(self, logger, project_id, health_check_ping):
        self.project_id = project_id
        self.logger = logger
        self.health_check_ping = health_check_ping
        self.lock = threading.Lock()
        self.queue = defaultdict(list)
        self.terminated = False
        self.thread = threading.Thread(target=self._run)
        self.thread.start()

    def join(self):
        self.logger.info('join')
        self.terminated = True
        self.thread.join()

    def add(self, table_id, df):
        with self.lock:
            self.queue[table_id].append(df)

    def _run(self):
        while not self.terminated:
            try:
                table_id = None
                q = []
                with self.lock:
                    for key in self.queue.keys():
                        if len(self.queue[key]) > len(q):
                            table_id = key
                            q = self.queue[key][:]

                if table_id is None:
                    time.sleep(5)
                    continue

                df = pd.concat(q).reset_index(drop=True)
                pandas_gbq.to_gbq(
                    df,
                    table_id,
                    project_id=self.project_id,
                    if_exists='append'
                )
                self.logger.info(f'upload {table_id} {df.shape}')
                self.health_check_ping()

                with self.lock:
                    self.queue[table_id] = self.queue[table_id][len(q):]
            except Exception as e:
                self.logger.error(e, exc_info=True)
                time.sleep(5)
