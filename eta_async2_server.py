from aiohttp import web
from pymongo import MongoClient
from motor.motor_asyncio import AsyncIOMotorClient
import async_timeout
from datetime import datetime, timedelta
import json
import asyncio


client = AsyncIOMotorClient("mongodb://localhost:27017")
db = client.spotonv4
handling = 3

class Con:
    def __init__(self,docknodict,condata):
        self.docno = docknodict['con']
        self.currtime = datetime.strptime(docknodict['arratloc'],"%Y-%m-%d %H:%M:%S")
        self.currloc=docknodict['location']
        self.destination=docknodict['destination']
        self.a=condata

    def func(self,options,currtime,etatype="schedule"):
        if options:
            currdate = datetime.combine(currtime.date(), datetime.min.time())
            opts = [(currdate+timedelta(hours = int(i[0].split(':')[0])+int(i[0].split(':')[1])*1.0/60)) for i in options]
            opts = [i if i>currtime else i+timedelta(hours=24) for i in opts]
            if etatype=="schedule":
                deptime = min(opts)
            else:
                deptime = currtime + timedelta(hours=3)
            tt = options[opts.index(deptime)][1]+handling
            arratdesttime = deptime+timedelta(hours = tt)
            availableatdesttime = arratdesttime+timedelta(hours = handling)
        else:
            deptime = currtime
            arratdesttime = currtime+timedelta(hours=3)
            availableatdesttime = arratdesttime
        return (deptime,arratdesttime,availableatdesttime)

    def geteta(self,etatype="schedule"):
        [self.a["legdata"].get(str(i)).append({'depdetails':self.func(self.a["legdata"].get(str(i))[2],self.currtime,etatype)}) if i==1 else self.a["legdata"].get(str(i)).append({'depdetails':self.func(self.a["legdata"].get(str(i))[2],self.a['legdata'].get(str(i-1))[3]['depdetails'][2])}) for i in range(1,len(self.a['legdata'])+1)]
        eta = self.a["legdata"].get(str(len(self.a["legdata"])))[3]['depdetails'][2]
        return eta
    
    def getjourney(self,etatype="schedule"):
        [self.a["legdata"].get(str(i)).append({'depdetails':self.func(self.a["legdata"].get(str(i))[2],self.currtime,etatype)}) if i==1 else self.a["legdata"].get(str(i)).append({'depdetails':self.func(self.a["legdata"].get(str(i))[2],self.a['legdata'].get(str(i-1))[3]['depdetails'][2])}) for i in range(1,len(self.a['legdata'])+1)]
        return self.a['legdata']

async def handle(request):
    data = await request.json()

    db = request.app['db']
    condata= await db.master.find_one({'origin':data['origin'],'destination': data['destination']})
    try:
        res = Con(data,condata).geteta()
    except:
        res = 'Error'+str(data)
    return web.Response(text=str(res))



app = web.Application()
app['db'] = db
app.router.add_get('/', handle)


web.run_app(app, host='localhost', port=50000)
