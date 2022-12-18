import asyncio
import os

from playip.bddummy.import_contracts_tickets import importAllContratoPacoteServicoTicket

if __name__ == "__main__":
    print(os.getcwd())
    loop = asyncio.new_event_loop()
    loop.run_until_complete(importAllContratoPacoteServicoTicket())
    # loop.run_until_complete(importAddresses("dummy1", "Itapevi"))
    # loop.run_until_complete(importAddresses("dummy1", "Cotia"))
    # loop.run_until_complete(importAddresses("dummy1", "Jandira"))
    # loop.run_until_complete(importAddresses("dummy1", "Barueri"))
    # loop.run_until_complete(importAddresses("dummy1", "Carapicuiba"))
    #asyncio.create_task(importAddresses("dummy1", 0))