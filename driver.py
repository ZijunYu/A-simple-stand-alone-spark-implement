import subprocess
from rdd import *
if __name__ == "__main__":
    result=[]
    for i in range(2,6):
        str1='ssh 2018210904@thumm0%s "cd /data/dsjxtjc/2018210904;rm result"' % (i)
        p1=subprocess.Popen(str1,shell=True)
        p1.wait()
    p=[]
    for i in range(2,6):
        os.system('scp executor.py 2018210904@thumm0%s:/data/dsjxtjc/2018210904' % (i))
        str2='ssh 2018210904@thumm0%s "cd /data/dsjxtjc/2018210904;python executor.py"' % (i)
        p.append(subprocess.Popen(str2,shell=True))
    for i in p:
        i.wait()
    sc=Context()
    for root,dirs,files in os.walk(".", topdown=False): 
        r=sc.parallelize(files,1)
    filenames=','.join(r.filt(lambda x:x[:6]=='result').collect())
    res=sc.pickleFile(filenames).collect()
    count=[value for key,value in enumerate(res) if key%3==0]
    mean_sum=[value for key,value in enumerate(res) if key%3==1]
    mean_square_sum=[value for key,value in enumerate(res) if key%3==2]
    mean=sum(mean_sum)/sum(count)
    result.append(mean)
    variance=sum(mean_square_sum)/sum(count)-mean**2
    result.append(variance)
    with open('final_result','w') as fw:
        fw.write(str(result))
