import datetime
import dateutil
from dateutil import parser
from datetime import datetime

start_time=datetime.now()
import gzip

f1=open('output.txt','r')

p=0


list_relevant_tweets=[]

for event in f1:
    f=gzip.open('temporal_stream_ner_temp_event.gz','rb')         #open zip file
    p=p+1
    event_list=event.strip().split('\t')
    event_date=parser.parse(event_list[0])
    event_date=event_date.replace(tzinfo=None)
    q=0
    print('tweets in 7 days window of event "%s"'%event_list[1])
    for tweet in f:
        q=q+1
        tweet_list=tweet.strip().split('\t')        
        tweet_date=parser.parse(tweet_list[3])
        tweet_date=tweet_date.replace(tzinfo=None)         
        diff=(tweet_date-event_date)
        if abs(diff.total_seconds())<7*24*3600  and  tweet_list[5]==event_list[1]:
            #print tweet
            #print diff
            list_relevant_tweets.append([event_list[1], event_list[0], tweet_list[7], tweet_list[3], diff.days])
        if q==100000:         
            break 
        

f1.close()
        
for item in list_relevant_tweets:
    print item

with open('subtract_output.txt', 'w') as file:
    file.writelines('\t'.join(str(j) for j in i) + '\n' for i in list_relevant_tweets)
    

end_time=datetime.now()
timeofexecution=end_time-start_time
print timeofexecution
        


    
