from rdd import *
import random
def load_text(filename,encoding='utf8'):
    with io.open(filename, 'r',encoding=encoding) as f:
        return io.StringIO(f.read())
def convert(x):
    try:
        return float(x)
    except:
        return None
    
if __name__=='__main__':
    sc=Context()
    for root,dirs,files in os.walk(".", topdown=False): 
        r=sc.parallelize(files,len(files))
    filenames=r.filt(lambda x:x[0]=='p')
    data=filenames.flatMap(lambda filename:load_text(filename).read().splitlines())
    res=[]
    data_filt=data.Map(convert).filt(lambda x:x is not None)
    res.append(data_filt.getLength())
    res.append(data_filt.summation())
    res.append(data_filt.Map(lambda x:x**2).summation())
    sc.parallelize(res,1).saveAsPicklefile('result')
    os.system('scp result 2018210904@thumm01:/home/dsjxtjc/2018210904/result%s'%(random.randint(1,500)))
