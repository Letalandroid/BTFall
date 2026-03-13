# Worker Fall Detection v0.2 (BLE modern)
# Adapted for Raspberry Pi Python 3.13

import asyncio
from bleak import BleakScanner
from termcolor import colored
import sqlite3 as sl
import os
import time
import string
import re

con = sl.connect('fall.db')
cursor = con.cursor()

found = 0
iterations = 0

os.system('clear')

print(colored('Worker Fall Detection v0.2', 'green'))
print("Roni Bandini - Argentina - Powered by Edge Impulse")
print("")
print("Scanning...")

async def scan_loop():
    global found
    global iterations

    while True:

        print("#" + str(iterations))

        devices = await BleakScanner.discover(timeout=2)

        for d in devices:

            name = d.name
            address = d.address

            if name:

                print(colored(f"    {name}, {address}", 'yellow'))

                if "Fall" in name:

                    found = 1
                    print(colored('Fall detected', 'red'))

                    pattern = r'[' + string.punctuation + ']'
                    nameDb = name

                    sql = "SELECT * from FALL WHERE name='" + nameDb + "'"
                    print(sql)

                    cursor.execute(sql)
                    records = cursor.fetchall()

                    if len(records) == 0:

                        print('Adding record: ' + name)

                        fieldArray = name.split("-")

                        sql = "INSERT INTO FALL (name, worker) values ('" + name + "','" + fieldArray[1] + "')"

                        with con:
                            con.execute(sql)

                        await asyncio.sleep(5)

                    else:
                        print(colored('This fall was already in the database', 'green'))

        if found == 0:
            print("")
            print("No falls detected")

        iterations += 1

        await asyncio.sleep(1)


asyncio.run(scan_loop())
