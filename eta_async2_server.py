
from aiohttp import web
from pymongo import MongoClient
from motor.motor_asyncio import AsyncIOMotorClient
import async_timeout
from datetime import datetime, timedelta
import json
import asyncio
import numpy as np
import pandas as pd
import ast


#client = AsyncIOMotorClient("mongodb://ankitgoel888:iep54321@cluster0-shard-00-00-ilsaa.mongodb.net:27017,cluster0-shard-00-01-ilsaa.mongodb.net:27017,cluster0-shard-00-02-ilsaa.mongodb.net:27017/<DATABASE>?ssl=true&replicaSet=Cluster0-shard-0&authSource=admin")
#"mongodb://localhost:27017"
client = AsyncIOMotorClient('mongodb://localhost:27017')
db = client.spotonv4
handling = 3

class Con:
    def __init__(self,docknodict,condata): #1. docknodict and condata --> self (i)docno,(ii)currtime/arratloc,(iii) currloc,destn,(iv)condata
        self.docno = docknodict['con']
        self.currtime = datetime.strptime(docknodict['arratloc'],"%Y-%m-%d %H:%M:%S")
        self.currloc=docknodict['location']
        self.destination=docknodict['destination']
        self.thceta=docknodict['thceta']
        self.a=condata

    def func(self,options,currtime,etatype="schedule",thceta='abc'):
        if options:
            currdate = datetime.combine(currtime.date(), datetime.min.time()) #2. thisgives 12 midnight of currdate
            opts = [(currdate+timedelta(hours = int(i[0].split(':')[0])+int(i[0].split(':')[1])*1.0/60)) for i in options] #3. today's schedules
            opts = [i if i>currtime else i+timedelta(hours=24) for i in opts] #4. forward options
            if etatype=="schedule": #5. if market
                deptime = min(opts) 
            else:
                deptime = currtime + timedelta(hours=3)
            tt = options[opts.index(deptime)][1]+handling #6. select the shortlisted options tt and add handling
            arratdesttime = deptime+timedelta(hours = tt) if thceta=='abc' else datetime.strptime(self.thceta,"%Y-%m-%d %H:%M:%S") #7. arrival
            availableatdesttime = arratdesttime+timedelta(hours = handling) #8. unloaded
        else:
            deptime = currtime #9. not in schedule file so VB - start now
            arratdesttime = currtime+timedelta(hours=3) #10. vb completion
            availableatdesttime = arratdesttime #11. VB unloading is zero
        return (deptime,arratdesttime,availableatdesttime)

    async def geteta(self,etatype="schedule"):
        [self.a["legdata"].get(str(i)).append({'depdetails':self.func(self.a["legdata"].get(str(i))[2],self.currtime,etatype,self.thceta)}) if i==1 else self.a["legdata"].get(str(i)).append({'depdetails':self.func(self.a["legdata"].get(str(i))[2],self.a['legdata'].get(str(i-1))[3]['depdetails'][2])}) for i in range(1,len(self.a['legdata'])+1)]
        #12. (i) i = serial of each leg of legdata (ii) getting the current content and appending depdetails (deptime,arrtime,availabletime) (iii) depdetails arrived at via current content[2]-options of legdata and conditional time (iv) conditional time is current of 1st leg else get the appended frame and take its [2] i.e.availabletime
        eta = self.a["legdata"].get(str(len(self.a["legdata"])))[3]['depdetails'][2] #13. eta is last items depdetails [2] (available)
        return {'Con Number': str(self.docno),'ETA': datetime.strftime(eta,"%Y-%m-%d %H:%M:%S")} #14. return conno, strtime eta as a dict
    
    async def getjourney(self,etatype="schedule"):#15. journey: get entire legdata
        [self.a["legdata"].get(str(i)).append({'depdetails':self.func(self.a["legdata"].get(str(i))[2],self.currtime,etatype,self.thceta)}) if i==1 else self.a["legdata"].get(str(i)).append({'depdetails':self.func(self.a["legdata"].get(str(i))[2],self.a['legdata'].get(str(i-1))[3]['depdetails'][2])}) for i in range(1,len(self.a['legdata'])+1)]
        return self.a['legdata']
        

async def handle(request): #16. asynced the handle func
    try:
    	data = await request.json() #17. while you await response json
    except:
    	data2 = await request.text()
    	data = ast.literal_eval(data2)
    db = request.app['db'] #17 or you wait db request
    condata= await db.master.find_one({'origin':data['origin'],'destination': data['destination']}) #or you wait the db find
    try:
        res = await Con(data,condata).geteta() #18. start another process
    except:
        res = {'Con Number': str(data['con']),'ETA': 'Error'} #18. error handler
    return web.json_response(res)
    #return web.json_response(Con(data,condata).geteta())

app = web.Application()
app['db'] = db
app.router.add_get('/', handle)

web.run_app(app, host='localhost', port=50000)
