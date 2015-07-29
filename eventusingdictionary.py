import datetime
from datetime import datetime
import math                          
import dateutil
from dateutil import parser


def G2LLR(etCount, eCount, tCount, N):              #copied from bitbucket
    (etCount, eCount, tCount, N) = (float(etCount), float(eCount) + 1.0, float(tCount) + 1.0, float(N) + 2.0)

    if etCount < (eCount / N) * (tCount / N) * N:
        return 0

    try:
        return(etCount                         * math.log( (etCount / eCount) / (tCount / N) ) +
               (tCount - etCount)              * math.log( ((tCount - etCount) / (N - eCount)) / (tCount / N) ) +
               (eCount - etCount)              * math.log( ((eCount - etCount) / eCount) / ((N - tCount) / N) ) +
               (N - tCount - eCount + etCount) * math.log( ((N - tCount - eCount + etCount) / (N - eCount) / ((N - tCount) / N)) ))
    except ValueError:
        return 0



#no_of_tweets=input("Enter the number of tweets: ")                           # inputting the number of tweets that should be used
#n=input("Enter the no of events to be chosen, it should be fairly smaller than the number of tweets: ")


start_time=datetime.now()
import gzip
f = gzip.open('/data/temporal_stream_ner_temp_event.gz', 'rb')




x=0
date_count={}                # Dictionary for date count
NE_count={}                  # Dictionary for NE count
NEdate_count={}              # Dictionary for NE-date pair count

for line in f:
    try:
        splitted_line=line.strip().split("\t")      
        time=parser.parse(splitted_line[3])          # getting time of tweet
        time=time.replace(tzinfo=None)               # removing tz info
        date=time.date()
        tweet=splitted_line[7]                       # getting tweet
        NE=splitted_line[5]                          # getting NE
    
        if not date in date_count:
            date_count[date]=1                 
        else: 
            date_count[date]+=1
        if not NE in NE_count:
            NE_count[NE]=1
        else:
            NE_count[NE]+=1
        if not (NE,date) in NEdate_count:
            NEdate_count[(NE, date)]=1
        else:
            NEdate_count[(NE, date)]+=1
   
        x=x+1
#        if x==no_of_tweets:
#            break
    except:
        pass
f.close()



NEdateG=[]                     # declaring a 2D list for NE, date and Gscore 
for item in NEdate_count.keys():
    Gscore = G2LLR(NEdate_count[item], NE_count[item[0]], date_count[item[1]],x)     # calculating G score 
    NEdateG.append([item[0],item[1], Gscore])


NEdateG.sort(key=lambda x: x[2], reverse=True)      # sorting NEdateG List according to G score
Events=NEdateG[:]                                  # Extracting the top events

with open('output.txt', 'w') as file:                                      # witing the Events-date-G score  to a txt file
    file.writelines('\t'.join(str(j) for j in i) + '\n' for i in Events)


end_time=datetime.now()                           # end time of the code

timeofexecution=end_time-start_time               # calculating the time of execution

print timeofexecution 




