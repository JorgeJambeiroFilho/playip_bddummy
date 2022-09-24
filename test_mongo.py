import asyncio

from playipappcommons.playipchatmongo import getBotMongoDB


async def tmongo():
    mdb = getBotMongoDB()
    r = await mdb.control.find_one({})
    print(r)

loop = asyncio.new_event_loop()
loop.run_until_complete(tmongo())
