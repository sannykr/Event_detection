
import datetime
from datetime import datetime
start_time=datetime.now()
import math

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




def month(name_of_month):
    if name_of_month=="Jan":  return '01'
    elif name_of_month=="Feb": return '02'
    elif name_of_month=="Mar": return '03'
    elif name_of_month=="Apr": return '04'
    elif name_of_month=="May": return '05'
    elif name_of_month=="Jun": return '06'
    elif name_of_month=="Jul": return '07'
    elif name_of_month=="Aug": return '08'
    elif name_of_month=="Sep": return '09'
    elif name_of_month=="Oct": return '10'
    elif name_of_month=="Nov": return '11'
    else: return '12' 



def change_date_format(list1):
    modified_date=[]
    list1=list1.split(" ")
    modified_date=[list1[5],'-',month(list1[1]),'-',list1[2]]
    return "".join(modified_date)


no_of_tweets=input("Enter the number of tweets: ")                           # inputting the number of tweets that should be used
n=input("Enter the no of events to be chosen, it should be fairly smaller than the number of tweets: ")

import gzip
f = gzip.open('temporal_stream_ner_temp_event.gz', 'rb')
list1=[]
x=0
for line in f:
    bag_of_words=line.strip().split("\t")
    time=bag_of_words[3]
    if time[0]!="2":
        time=change_date_format(time)
    else: 
        temp=time.split(" ")
        time=temp[0]  
     
    list1.append((time,bag_of_words[7], bag_of_words[5]))
    x=x+1
    if x==no_of_tweets:
        break
f.close()

num=0
DatetweetNE=list(set(list1))                 # making the list of set of time, tweets and Named-Entity
DatetweetNE.sort(key=lambda x: x[0])         # sorting based on date

NEandDate=[]
for item in list1:
    NEandDate.append((item[0],item[2]))      # getting the NE and date pair


set_of_NEandDate=list(set(NEandDate))

DateNEGscore=[]                              # declaring the list  Date, NE and and Gscore 

for item in set_of_NEandDate:                # Getting the arguments for G test
    NEandDatecount=0
    NEcount=0
    Datecount=0
    for item1 in NEandDate:
        if item==item1:
            NEandDatecount=NEandDatecount+1
        if item[1]==item1[1]:
            NEcount=NEcount+1
        if item[0]==item1[0]:
            Datecount=Datecount+1
        Gscore=G2LLR(NEandDatecount, NEcount, Datecount,no_of_tweets)         #calculating the G score by calling the function G2LLR
    DateNEGscore.append([item[0],item[1],Gscore])                             # making a list of Date, NE and G score 
DateNEGscore.sort(key=lambda x: x[2], reverse=True)                                         # sorting the 2D list according to Gscore 


Events=DateNEGscore[:n]                                                                             #Extracting the events



with open('output.txt', 'w') as file:
    file.writelines('\t'.join(str(j) for j in i) + '\n' for i in Events)


end_time=datetime.now()

timeofexecution=end_time-start_time

print timeofexecution
                                                                
                                                                              
                                                                                      

         
            









