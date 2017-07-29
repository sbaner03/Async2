from aiohttp import web
from pymongo import MongoClient
from motor.motor_asyncio import AsyncIOMotorClient
import async_timeout
from datetime import datetime, timedelta
import collections
import json
from operator import itemgetter
import asyncio


client = AsyncIOMotorClient("mongodb://localhost:27017")
db = client.spotonv4


class Con:
    def __init__(self,docknodict,condata):
        self.docno = docknodict['con']
        self.arratloc = datetime.strptime(docknodict['arratloc'],"%Y-%m-%d %H:%M:%S")
        self.origin = docknodict['origin']
        self.location=docknodict['location']
        self.destination=docknodict['destination']
        conpath=condata['conpath']
        scheduledepsdict={}
        for i in condata["legdata"].values():
            scheduledepsdict.update({(i[0],i[1]):i[2]})
        self.conpath = conpath
        self.scheduledepsdict = scheduledepsdict

    def returndata(self):
        eta = str(self.geteta(self, journey='no'))
        return {self.docno:eta}

    def nextdepfunc(self,org,dest,arratloc): #check if self required
        timelist=[]
        scheduledepsdict=self.scheduledepsdict

        if scheduledepsdict.get((org,dest))!=None:
            for i in scheduledepsdict.get((org,dest)):
                nextdep1 = arratloc.replace(hour=int(i[0].split(":")[0]),minute=int(i[0].split(":")[1]),second=0,microsecond=0)
                nextarr1 = nextdep1+timedelta(hours=i[1])
                nextdep2 = arratloc.replace(hour=int(i[0].split(":")[0]),minute=int(i[0].split(":")[1]),second=0,microsecond=0)+timedelta(days=1)
                nextarr2 = nextdep2+timedelta(hours=i[1])
                if nextdep1>arratloc:
                    timelist.append([nextdep1,nextarr1])
                else:
                    timelist.append([nextdep2,nextarr2])
            return min(timelist,key=itemgetter(0))
        else:
            return [arratloc+timedelta(hours=3),arratloc+timedelta(hours=3)]  #for market and vb movements  --- con can never fail here


    def geteta(self,etatype='schedule',journey="yes"):
        arratloc = self.arratloc
        conpath = self.conpath
        etadict = collections.OrderedDict()
        for i in range (1,len(conpath)):
            org = conpath[i-1]
            dest = conpath[i]
            if conpath[i-1]==conpath[0] and etatype=="market":
                nextdep = datetime.today()+timedelta(hours=3)
                etadict.update({(org,dest):(arratloc,nextdep)})
                arratloc = self.nextdepfunc(org,dest,arratloc)[1]+timedelta(seconds=(self.nextdepfunc(org,dest,arratloc)[1]-self.nextdepfunc(org,dest,arratloc)[0]).total_seconds()*0.1)+timedelta(hours=5)  ##market penalty & unloading and loading hours
            else:
                nextdep = self.nextdepfunc(org,dest,arratloc)[0]
                etadict.update({(org,dest):(arratloc,nextdep)})
                arratloc = self.nextdepfunc(org,dest,arratloc)[1]+timedelta(hours=5) #unloading hours & loading hours
        etadict.update({(next(reversed(etadict))[1],next(reversed(etadict))[1]):(arratloc,arratloc)})
        if journey=="yes":
            return etadict.items()
        else:
            try:
                return max(max(etadict.values()))
            except:
                return None




async def handle(request):
    data = await request.json()

    db = request.app['db']
    condata= await db.master.find_one({'origin':data['origin'],'destination': data['destination']})
    try:
        res = Con(data,condata).returndata()
    except:
        res = 'Error'+str(data)
    return web.Response(text=str(res))



app = web.Application()
app['db'] = db
app.router.add_get('/', handle)


web.run_app(app, host='localhost', port=50000)
