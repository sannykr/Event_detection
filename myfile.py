l1=['hi','hello','welcome']

f=open('f1.txt','w')
for ele in l1:
    f.write(ele+'\n')

f.close()
