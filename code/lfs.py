import pandas as pd
import numpy as np
import sys,getopt
from rdflib.plugins.sparql.parser import parseQuery
from SPARQLWrapper import SPARQLWrapper, JSON;

import time

#https://yago-knowledge.org/sparql/query

from rdflib import Graph

np.random.seed(100)

sta=0
#
# df = pd.DataFrame(np.random.random((10,5)), columns=list('ABCDE'))
# df.index = df.index * 10
#
# df = pd.read_fwf('YAGODATAansweredv2.txt',encoding="utf-8",colspecs=[(0,4500)],names=['quer'])
argv=sys.argv[1:]
fla=0
#df2 = pd.read_fwf('YAGOgoodqueriesv2R.txt',encoding="utf-8",colspecs=[(0,4500)],names=['quer'])
if len(argv)<3:
    
    print("You need to provide two INPUT files (lfs_summary_filename lfs_queries_for_summary) and one filename for OUTPUT  and finally address_of_endpoint{OPTIONAL} ")
    print("USAGE: python lfs orig_summary_filename queries_for_summary LFS_summary_output {url of endpoint-optional) ")
    exit(1)
else:

    print ('Summary file is ',sys.argv[1])
    print ('Queries file is ',sys. argv[2])
    if len(argv)==4:
        print(" ADDRESSING ENDPOINT")
        fla=1
    else:
        print("NOT ADDRESSING ENDPOINT ")
df = pd.DataFrame(np.random.random((10,5)), columns=list('ABCDE'))
df.index = df.index * 10

#df = pd.read_fwf('answeredWiki3.txt',encoding="utf-8",colspecs=[(0,4500)],names=['quer'])

df = pd.read_fwf(sys.argv[1],encoding="utf-8",colspecs=[(0,4500)],names=['quer'])

df2 = pd.read_fwf(sys.argv[2],encoding="utf-8",colspecs=[(0,4500)],names=['quer'])

testlist=[]
#print (df)

from sklearn.model_selection import KFold

#added some parameters
kf = KFold(n_splits = 5, shuffle = True, random_state = 2)
#result = next(kf.split(df), None)

reslist=list(kf.split(df))
#print (result)
#(array([0, 2, 3, 5, 6, 7, 8, 9]), array([1, 4]))
co=1

print("\n\n PROGRAM STARTS....")
print("Spliting the dataset to train/test")
for result in reslist:
    fname=sys.argv[3]
    
    ff= open(fname, 'w',encoding="utf-8") 
    # print(fname)
  
    train = df.iloc[result[0]]
    test =  df.iloc[result[1]]
    print("Splitting ends. train.txt and test.txt Created...")
    train.to_csv("train.txt", header=None, index=None, sep=' ', mode='w')
    #:N5df28f1d1ab34a839ad1699f13055bce
     
    test.to_csv("test.txt", header=None, index=None, sep=' ', mode='w')
    co=co+1
    #print("info")
    #print (train)
    #print(test)
    su=[]
    ob=[]
    pr=[]
    
    exa=0
    exa2=0
    # print(test)
    
    testlist=test.index
    #testlist=list(test.index.values)
    
     # an ndarray method, you probably shouldn't depend on this
    #print("KONOS")
    #print(testlist)
    
    print("Spliting the data in train.txt, to autonomous triples....")
    print("...Adding them to final summary file "+sys.argv[3])
    for index, row in train.iterrows():
        #print("queryno "+queryno))
        #queryno=queryno+1
        que=row['quer']
        
        # print("QUE "+que)
        #print("queryno "+que)
        if "2023-01-31T01:09:43Z" not in que and "<<" not in que and "math" not in que and "<application/x-httpd-php>" not in que and '10766787-n' not in que and "1848831457" not in que:
            spla=que.split('\t')
    
            for node in spla:
                #print("NODAS "+node)
                
                try:
                    
                    if node[0]==" ":
                        node=node[1:]
                    if node[len(node)-1]==" ":
                        node=node[:-1]
                    
                    nnodes=node.split(' ')
      
                    sub=nnodes[0]
                    pre=nnodes[1]
                    obj=nnodes[2]
                    #@ print(node)
                    
                    #print("---------"+node+"+-len "+str(len(nnodes)))
    
                    if 1==1:
                        #print(nnodes)
                        sub=sub.replace("*","").replace("“","").replace("„","").replace("“","")
                        obj=obj.replace("*","").replace("“","").replace("„","").replace("“","")
                        pre=pre.replace("*","").replace("“","").replace("„","").replace("“","")
                        
                        
                        if '"' in sub:
                            sub=sub.replace('"',"")
                            sub="_:"+sub
                        if 'http' not in sub and sub[0]=='<':
                            sub=sub.replace('"','')
                        if 'http' not in pre and pre[0]=='<':
                            pre=pre.replace('"','')
                            
                        if 'http' not in obj and obj[0]=='<':
                            obj=obj.replace('"','')        
                        # if 'http' not in sub and  sub[0]=='<':
                        #     sub=sub.replace('"',"").replace('<',"").replace('>',"")
                        #     sub='"'+sub+'"'
                            #sub=sub.replace("<",'"').replace(">",'"')
                            #sub=sub+'"'
                        if 'http' not in pre and pre[0]=='<':
                            
                            pre=pre.replace('"',"").replace('<',"").replace('>',"")
                            pre='"'+pre+'"'
                            #pre=pre.replace("<",'"').replace(">",'"')   
                           # pre=pre+'"'
                        if 'http' not in obj and obj[0]=='<':
                            
                            obj=obj.replace('"',"").replace('<',"").replace('>',"")
                            obj='"'+obj+'"'
                            #obj=obj.replace("<",'"').replace(">",'"')
                        
                        # if '<' in sub and '>' not in sub:
                        #     sub=sub+">"
                        # if '<' in obj and '>' not in obj:
                        #     obj=obj+">"    
                        #
                        # if '<' in pre and '>' not in pre:
                        #     pre=pre+">"    
                            #obj=obj+'"'  
                        # obj=obj.replace('""','"')
                        # sub=sub.replace('""','"')
                        # pre=pre.replace('""','"')
                        #
    
                        from rdflib import URIRef, Graph, RDF, SDO
                        #print("res "+sub+" "+pre+" "+obj+" . \n")
                        resa=sub+" "+pre+" "+obj+" . \n"
                        ress2=sub+" "+pre+" "+obj+" . \n"
                        
                        
                        if resa!='" .\n' and ress2!=" > . \n" and "<<" not in ress2 and "math" not in resa and len(ress2)>10:
    
                            
                            if sta==0:
                                ff.write(sub+" "+pre+" "+obj+" .")
                            else:
                                ff.write("\n"+sub+" "+pre+" "+obj+" .")
                            sta=sta+1
                        
                        #f.write(node+' .\n')
                 
                except:
                    #print('e')
                    exa=exa+1
    
    ff.close()
    
    print("Final summary nt file -"+sys.argv[3]+"- Created ...")
    print("Starting quering the nt file with the the testing queries....")
    g = Graph()
    g.parse(fname)
    noq=0
    ansq=0
    noall=0
    fals=0
    validlist=[]
    
    
    start = time.time()
    occu=0
    for index, row in df2.iterrows():

        
        if index in testlist:
            ready=0
           # print(str(noall)+" "+str(index))
            #print("LOCAL")
            noall=noall+1
            que=row['quer']
    
           
       
            #print("\nKALA "+que+ " INDEX "+str(index))
    
            noda=que.split(',')
            #print(noda)
            nodi=''
            ex=0
            for nodas in noda:
                if nodas.count("?")==3:
                    #print(nodas)
                    ex=1;
                    print("X",end="")
                    #break;
                nodi=nodi+nodas+" ."
                #print("noda "+nodas)
            know = " SELECT * WHERE { "+nodi+"  } LIMIT 1"
            sta=0
            #print(know)
            try:
                #print(" len "+str(len(know)))
                if  len(know)<700 and ex==0 and know.count("?")<25 :
                    qres = g.query(know)
                    if len(qres)==0:
                        sta=1
                    ready=1
                    validlist.append(index)
                else:
                    print("!",end="") 
                    if (ex==0):
                        A=3
                        #print("KOKO "+know)
                    
                    # qres=[]
                    fals=fals+1
                    noall=noall-1
                #print(know)
                #print("-"+str(noall)+" "+str(fals)+" - count |",end="")
                #print("-"+str(noall),end="")
                if noall%1000==0:
                    print("Queried :"+str(noall),end=" ")
                    print("WORKING...")
                
                
            except:
                print("x",end="\n")
                qres=[]
                noall=noall-1
                #print(str(qres.vars))
                
                
            
            lena=len(qres)
            #print("lena "+str(lena));
            if ready==1 and sta==0:
                ansq=ansq+1
                coc=0
                # for row in qres:
                #     print("ROW "+str(row))
                #     coc=coc+1
                #     if coc==5:
                #         break
                    #oc=coc+1
                # else:
                #     print("")
                #print("LEN  "+str(len(qres)))
                
                re=0
            elif sta==1 and fla==1:
                #print("END")
                know = " SELECT * WHERE { "+nodi+"  } LIMIT 1"
                #print(know)
                
                
                sparql = SPARQLWrapper(sys.argv[4])
                sparql.setReturnFormat(JSON)
                sparql.setQuery(know)
                try:
                    sta1=time.time()
                    #qq=sparql.onlyConneg
                    qres= sparql.queryAndConvert()
                    end2=time.time();
                    occu=occu+end2-sta1
                    #print(qres)
    
    
                    #print(str(qres.vars))
                except Exception as s:
                    print(s,end=" ")    
    end = time.time()
    print('Elapsed time for LFS ',str(end - start-occu))
    print("Elapsed time for ENDPOINT",str(occu))
    #print("NOALL "+str(noall)+
          
    print("")
    print("COVERAGE FOR LFS "+str(float(ansq)/float(noall)))
    exit(1)
   
