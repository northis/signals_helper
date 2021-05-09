#!/usr/bin/python3.9

import threading
import asyncio
import logging
import classes
import forwarder
import db_stats
import db_poll


poll_event_sync = threading.Event()
stop_flag = classes.StopFlag()


logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    filename='main.log',
                    encoding='utf-8',
                    level=logging.INFO)


async def forwarder_event():
    await forwarder.main_exec(stop_flag)


def forwarder_sync():
    asyncio.run(forwarder_event())


if __name__ == "__main__":

    db_poll_thread = threading.Thread(target=db_poll.main_exec,
                                      args=[poll_event_sync], daemon=True)
    db_poll_thread.start()

    db_poll_forwarder = threading.Thread(target=forwarder_sync, daemon=True)
    # db_poll_forwarder.start()

    db_stats.WAIT_EVENT_OUTER = poll_event_sync
    history_downloader = threading.Thread(
        target=db_stats.main_exec, daemon=True)
    history_downloader.start()

    print("Press any key to exit")
    input()
    poll_event_sync.set()
    stop_flag.Value = True

    db_poll_thread.join()
    # db_poll_forwarder.join()
    history_downloader.join()
