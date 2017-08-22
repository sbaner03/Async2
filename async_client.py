import aiohttp
import asyncio
import async_timeout
from datetime import datetime
import pickle
import pandas as pd
import json




async def fetch(session, url,data):
    with async_timeout.timeout(0):
        async with session.get(url, data = data) as response:
            return await response.json()

async def main(requests):
    async with aiohttp.ClientSession() as session:
        start = datetime.now()
        responses = [await fetch(session, req[0],req[1]) for req in requests]
        mid = datetime.now()
        end = datetime.now()
        total = (end-start).total_seconds()
        return total,responses


df = pd.read_excel('http://spoton.co.in/downloads/HTR_1HR/HTR_1HR.xls')
df['TIMESTAMP'] = df.apply(lambda x: datetime.strftime(x['TIMESTAMP'],"%Y-%m-%d %H:%M:%S"),axis=1)
condict = zip(df['Con Number'],df['Origin Branch'],df['Destn Branch'],df['Hub SC Location'],df['TIMESTAMP'])

jsondata = [json.dumps({'con': str(i[0]),'origin': i[1], 'destination': i[2],'location': i[3],'arratloc': i[4]}) for i in condict]
requests = [['http://localhost:50000/',i] for i in jsondata]
loop = asyncio.get_event_loop()
timetaken,coneta = loop.run_until_complete(main(requests))
retdf = pd.DataFrame(coneta)
retdf['Con Number'] = retdf.apply(lambda x: int(x['Con Number']), axis=1)
df = pd.merge(df,retdf, on = 'Con Number', how = 'left')
print (timetaken)
print (len(df))


