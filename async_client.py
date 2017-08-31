import aiohttp
import asyncio
import async_timeout
from datetime import datetime
import pickle
import pandas as pd
import json
import numpy as np

async def fetch(session, url,data):
    with async_timeout.timeout(0): 
        async with session.get(url, data = data) as response:
            return await response.json() #1. while you await response, get from url by passing data

async def main(requests): #2. for computing time
    async with aiohttp.ClientSession() as session:
        start = datetime.now()
        responses = [await fetch(session, req[0],req[1]) for req in requests]
        mid = datetime.now()
        end = datetime.now()
        total = (end-start).total_seconds()
        return total,responses

df1 = pd.read_excel('http://spoton.co.in/downloads/HTR_1HR/HTR_1HR.xls').loc[:,['Con Number',"TIMESTAMP",'Origin Branch','Destn Branch','Hub SC Location']]
df1['TIMESTAMP'] = df1['TIMESTAMP'].map(lambda x: x.strftime("%Y-%m-%d %H:%M:%S"))
df2 = pd.read_excel('http://spoton.co.in/downloads/TCR_UND_2HRS/TCR_UND_ISDEPART_YES_2HRS_NEXT_TO_NEXT.xls').loc[:,['DOCKNO',"DEPARTURE TIME FRM CURLOC",'ORIGIN BRCODE','DESTCD','CURR BRANCHCODE',"THC ETA","DEPARTED FRM CURRLOC THCNO"]]
df2['DEPARTURE TIME FRM CURLOC'] = df2['DEPARTURE TIME FRM CURLOC'].map(lambda x: x.strftime("%Y-%m-%d %H:%M:%S"))
df2['THC ETA'] = df2.apply(lambda x: 'abc' if pd.isnull(x["THC ETA"]) else datetime.strftime(x["THC ETA"],"%Y-%m-%d %H:%M:%S"),axis=1)
df2.columns=['Con Number',"TIMESTAMP",'Origin Branch','Destn Branch','Hub SC Location','THC ETA','THC NO']
df = df1.append(df2)
df = df.fillna('abc')
condict = zip(df['Con Number'],df['Origin Branch'],df['Destn Branch'],df['Hub SC Location'],df['TIMESTAMP'],df['THC ETA']) #3. docknodict format from inventory (not actually a dict)
jsondata = [json.dumps({'con': str(i[0]),'origin': i[1], 'destination': i[2],'location': i[3],'arratloc': i[4],'thceta': str(i[5])}) for i in condict] #4. json conversion for each con; list output
requests = [['http://localhost:50000/',i] for i in jsondata] #5. request format [url,json_per_con]
loop = asyncio.get_event_loop()
timetaken,coneta = loop.run_until_complete(main(requests)) #6. pass to main and get time,resposnse as list of tuples
retdf = pd.DataFrame(coneta) #7. dataframe created from it
retdf['Con Number'] = retdf.apply(lambda x: int(x['Con Number']), axis=1)
df = pd.merge(df,retdf, on = 'Con Number', how = 'left') #8. merging the responses df to inv df
print (timetaken)
print (len(df))
df.to_csv("abc.csv")


