#!/usr/bin/env python

import asyncio

import websockets

heartbeat_timeout = 2
loop = asyncio.get_event_loop()


async def heartbeat(socket):
    await asyncio.sleep(heartbeat_timeout)
    str = r'["{\"_event\":\"heartbeat\",\"data\":\"h\"}"]'
    print("sending ", str)
    await socket.send(str)
    await heartbeat(socket)


async def main():
    uri = "wss://stream230.forexpros.com:443/echo/819/srvbrq0k/websocket"
    async with websockets.connect(uri) as websocket:
        subStr = r'["{\"_event\":\"bulk-subscribe\",\"tzID\":8,\"message\":\"pid-1:%%pid-3:%%pid-2:%%pid-5:%%pid-9:%%pid-7:%%pid-10:%%pidTechSumm-1:%%pidTechSumm-3:%%pidTechSumm-2:%%pidTechSumm-5:%%pidTechSumm-9:%%pidTechSumm-7:%%pidTechSumm-10:%%pid-8874:%%pid-8873:%%pid-8839:%%pid-169:%%pid-166:%%pid-14958:%%pid-44336:%%pid-8827:%%pidExt-8874:%%isOpenExch-1:%%isOpenExch-2:%%isOpenPair-8874:%%isOpenPair-8873:%%isOpenPair-8839:%%isOpenPair-44336:%%isOpenPair-8827:%%cmt-1-5-8874:%%domain-1:\"}"]'
        await websocket.send(subStr)
        eventStr = r'["{\"_event\":\"UID\",\"UID\":0}"]'
        await websocket.send(eventStr)
        loop.create_task(heartbeat(websocket))
        while True:
            print("received: ", await websocket.recv())


# basically loop forever
loop.run_until_complete(main())
