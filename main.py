import threading
import asyncio

import db_poll
import forwarder

poll_event_sync = threading.Event()
forwarder_clients = list()


async def forwarder_event():
    await forwarder.main_exec(forwarder_clients)


def forwarder_sync():
    asyncio.run(forwarder_event())


if __name__ == "__main__":

    db_poll_thread = threading.Thread(target=db_poll.main_exec,
                                      args=[poll_event_sync], daemon=True)
    db_poll_thread.start()

    db_poll_forwarder = threading.Thread(target=forwarder_sync, daemon=True)
    db_poll_forwarder.start()

    print("Press any key to exit")
    input()
    poll_event_sync.set()
    asyncio.run(forwarder_clients[0].disconnect())
    db_poll_thread.join()
    db_poll_forwarder.join()
