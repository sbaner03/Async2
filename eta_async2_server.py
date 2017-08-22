from aiohttp import web
from pymongo import MongoClient
from motor.motor_asyncio import AsyncIOMotorClient
import async_timeout
from datetime import datetime, timedelta
import json
import asyncio


#client = AsyncIOMotorClient("mongodb://ankitgoel888:iep54321@cluster0-shard-00-00-ilsaa.mongodb.net:27017,cluster0-shard-00-01-ilsaa.mongodb.net:27017,cluster0-shard-00-02-ilsaa.mongodb.net:27017/<DATABASE>?ssl=true&replicaSet=Cluster0-shard-0&authSource=admin")
#"mongodb://localhost:27017"
client = AsyncIOMotorClient('mongodb://localhost:27017')
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
        return {'Con Number': str(self.docno),'ETA': datetime.strftime(eta,"%Y-%m-%d %H:%M:%S")}
    # pre-if code: for first leg please use currtime
    # post-if code: for subsequent legs please use the available time already updated in penultimate leg since the list comprehension is based on a range
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
        res = {'Con Number': str(data['con']),'ETA': 'Error'}
    return web.json_response(res)



app = web.Application()
app['db'] = db
app.router.add_get('/', handle)


web.run_app(app, host='localhost', port=50000)
