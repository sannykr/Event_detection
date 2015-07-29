import gzip
from datetime import datetime
import dateutil
from dateutil import parser
st=datetime.now()                   # starting time
f=open('output.txt','r')            # opening the output.txt file which contains the events sorted according to G score
event={}
x=0

# Now, I am trying to extract all dates on which that event was significant
for line in f:
    x=x+1
    item=line.strip().split('\t')    
    if not item[0] in event.keys():                  
        event[item[0]]=[]                # list to store dates
        event[item[0]].append(item[1])   # appending date to the list
    else:
        event[item[0]].append(item[1])
    if x==10000:
        break




f=gzip.open('/data/temporal_stream_ner_temp_event.gz', 'rb')

tweet=open("tweet.txt","w")

for line in f:
    
    
    try:
        t=line.strip().split('\t')
        NE=t[5]                      # Named Entity
        time=parser.parse(t[3])
        date=time.replace(tzinfo=None)
        date=date.date()
        if (NE in event.keys()) and (abs(parser.parse(event[NE][0]).date()-date).days<=7):
            x=len(event[NE])
            for i in range(0,x):
                tweet.write(event[NE][i]+'\t'+str(date)+'\t'+t[4]+'\t'+t[5]+'\t'+t[7]+'\n')
    except:
        pass
    

tweet.close()         
  
et=datetime.now()                                # end time
print 'time of execution'+ ' ' +  str(et-st)     # printing time of execution

          
