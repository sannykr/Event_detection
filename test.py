import gzip
f=gzip.open('temporal_stream_ner_temp_event.gz','rb')
count=0
y=0
for line in f:
    y=y+1
    x=line.strip().split('\t')
    if x[5]=='hi':
        count=count+1
    if y==1000:
        break


print count
