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
            return await response.text()

async def main(websites):
    async with aiohttp.ClientSession() as session:
        start = datetime.now()
        htmls = [await fetch(session, html[0],html[1]) for html in websites]
        mid = datetime.now()
        end = datetime.now()
        total = (end-start).total_seconds()
        return (total,htmls)


checklist = pickle.load( open("C:/Users/Ankit Goel/Dropbox/Python/testrest/Async2/Async2/checklist.pkl", "rb" ) )
checklist = list(pd.unique(checklist))[0:20000]
jsondata = [json.dumps({'con': '10000','origin': i[0], 'destination': i[1],'location': i[0],'arratloc': '2017-07-08 17:30:00'}) for i in checklist]
websites = [['http://localhost:50000/',jsondata[ix]] for ix,i in enumerate(checklist)]
loop = asyncio.get_event_loop()
stats = loop.run_until_complete(main(websites))
print (stats[0],stats[1])
# errorcount = len([i for i in stats[1] if 'Error' in i[0]])
# newstats ={datetime.strftime(datetime.now(),'%Y-%m-%d %H:%s'):[stats[0],errorcount,len(checklist)]}
# basestats = {'2017-07-29 0:0': [0, 0,0]}
# try:
#     df = pd.read_csv('results.csv')
#     df.columns = ['Time',0,1,2]
#     df = df.set_index('Time')

# except:
#     df = pd.DataFrame.from_dict(basestats, orient='index')
#     df.columns = [0,1,2]
# resdf = df.append(pd.DataFrame.from_dict(newstats, orient='index'))
# resdf.to_csv('results.csv')
# print (resdf)
