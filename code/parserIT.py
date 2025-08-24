import sys
from rdflib.plugins.sparql.parser import parseQuery
from SPARQLWrapper import SPARQLWrapper, JSON;


def most_frequent(List):
    counter = 0
    num = List[0]
     
    for i in List:
        curr_frequency = List.count(i)
        if(curr_frequency> counter):
            counter = curr_frequency
            num = i
 
    return num


argv=sys.argv[1:]

lista=[]
import time
fla=0

if len(argv)<5:
    print("USAGE queryfile {flag0/1 mf or non mf}  {basefilename} {limit} {urlendpoint}")
    exit(1)
else:

    queryfile=sys.argv[1]
    mfflag=int(sys.argv[2])
    basefilename=sys.argv[3]
    limit=int(sys.argv[4])
    urlend=sys.argv[5]
#df2 = pd.read_fwf('YAGOgoodqueriesv2R.txt',encoding="utf-8",colspecs=[(0,4500)],names=['quer'])

    print("QUERY LOG: "+queryfile)
    print("MF FLAG: "+str(mfflag))
    print("BASEFILENAME: "+basefilename)
    print("LIMIT :"+str(limit))
    print("ENDPOINT URL: "+urlend)    
    
lista.append(limit)



for ll in lista:
    print(ll)
    start=time.time()
    start2=time.time()

    count1 = 0
    count2 = 0
    count3 = 0
    no=0
    co=0

    
    basefilename=basefilename+"_"+str(mfflag)+"_"+str(limit)
    
    
    ff10= open("1"+basefilename+'.txt', 'w', encoding="utf-8") 

    ff0= open("10"+basefilename+'.txt', 'w', encoding="utf-8") 

    ff= open("5"+basefilename+'.txt', 'w', encoding="utf-8") 
    ff2= open('Queries_'+basefilename+'.txt', 'w', encoding="utf-8") 
       
    # f= open('output2.txt')    
    f= open(queryfile)    

    total=0
    tor=0
    tor1=0
          
    while True:
        no=no+1
        if no%1000==0:
            print(str(no))
        # if no%10==0:
        #     break 
        line = f.readline()
        if not line:
            break
                     
        triples = line.split(',') 
    
        query = 'SELECT * WHERE { ' # initialize query
            
        for triple in triples: # loop all triples in current line and add them on the same query
            if triple == "\n":
                continue
    
            split_triple = triple.split(' ')
    
            if not len(split_triple) >= 3:
                continue
            
            query = f'{query} {split_triple[0]} {split_triple[1]} {split_triple[2]} . ' # extend query with triples
    
        query = query + ' } limit '+str(limit)+' ' # close query
        #
        # if "http://schema.org" in query:
        #
        #     query=query.replace("http://schema.org","https://schema.org")
        #
        #

        #sparql = SPARQLWrapper("https://yago-knowledge.org/sparql/query")
        sparql = SPARQLWrapper(urlend)

        sparql.setReturnFormat(JSON)
        sparql.setQuery(query)
    
        try:
            #print(query)
            ret = sparql.queryAndConvert()
            
            #print(ret)
            index=0
            trippi=''
            vari=''
            first=''
            alllist=[]
            #print("KEBIS "+str(len(ret)))
            for r in ret["results"]["bindings"]:
                accou=''
    
                #print("RARAR ",r)
                new_triple = ""
                #print("A")
                for triple in triples: # loop all triples in current line and add them on the same query
                    #print("B")
                    #new_triple=new_triple.replace(" ","")
                    #new_triple=new_triple+"A"
                    trippi=trippi+'\t'+new_triple
                    new_triple=""
                    if triple == "\n":
                        continue
    
                    count1 += 1
                    split_triple = triple.split(' ')
    
                    if not len(split_triple) >= 3:
                        continue
                    
                    for var in split_triple:
                        if "?" in var:
                            new_var = var[1:]
                            
                            if len(ret["results"]["bindings"]) == 0 or not ret["results"]["bindings"][0]:
                                continue
    
                            #print("INDA "+str(index))
                            if ret["results"]["bindings"][index][new_var]["value"]:
                                
                                new_triple = new_triple + " " + '<' + ret["results"]["bindings"][index][new_var]["value"].replace('\t',"").replace(" ","_") + '>'
                                count2+=1
                        else:
                            var=var.replace('\t',"").replace(" ","_")
                   
                            new_triple = new_triple + " " + var 
                         
                            count3+=1
                    new_triple=new_triple.lstrip()
                    new_triple=new_triple.strip()
                    accou=accou+new_triple+"\t"
                    #accou=accou.strip()
                    if index==0:
                        #accou=accou.strip()
                        first=accou
                    #print("PRETOR -"+new_triple,end=']')
                    new_triple=new_triple.replace("\t",'')
                    vari=vari+new_triple+"\t"
                    #new_triple=new_triple+"\t"
                #print("TRYPOS "+new_triple)
                #new_triple = new_triple + "\t"
                   # print("tripas ",new_triple)
                #new_triple=new_triple+"\t"
                #print("TOR" ,trippi)
    
                #trippi=trippi+"\t"+new_triple
                #accou=accou.strip()
                alllist.append(accou)
    
                index=index+1
                #print("accu ",accou)
    
            #print("start")
            # print("tripas ",trippi)
    
            #print(str(no),'total ',str(total))
            #print("VARI "+vari)
            
            #print('first '+first,end='}')
            #print("list")
            finalstring=''
            from collections import Counter
    
           #print("The original list : " + str(alllist))
     
            # using Counter.most_common() + list comprehension
            # sorting and removal of duplicates
            res = [key for key, value in Counter(alllist).most_common()]
            
            
            if (mfflag==1):
                got1=""
                for ii in range (0,1):
                    
                    try:    
                        got1=got1+res[ii]
                    except:
                        are=1
                got5=""
                for ii in range (0,4):
                    try:    
                        got5=got5+res[ii]
                    except:
                        are=1
                        #print("")
                got10=""
                for ii in range (0,9):
                    try:    
                        got10=got10+res[ii]
                    except:
                        #print("")
                        are=1
            elif mfflag==0:
                got1=""
                for ii in range (0,1):
                    
                    try:    
                        got1=got1+alllist[ii]
                    except:
                        are=1
                got5=""
                for ii in range (0,4):
                    try:    
                        got5=got5+alllist[ii]
                    except:
                        are=1
                        #print("")
                got10=""
                for ii in range (0,9):
                    try:    
                        got10=got10+alllist[ii]
                    except:
                        #print("")
                        are=1
            # print result
            #print("The list after sorting and removal : " + str(got))
            # mf=most_frequent(alllist)
            # if mf==first:
            #     #print("OK EINAI ")
            #     finalstring=first
            # else:
            #     finalstring=mf
            #     total=total+1
                # print("ALLO !")
            lin=line.strip()
            new_triple2=vari
            new_tripl=trippi.strip()
            
            line2=lin.replace(',\n','\n').replace("\n","").replace('\r', '')
            line2=line2.rstrip(",")

            #line2=line2.replace("http://","https://")
            new_triple2=got5.replace(',\n','\n').replace("\n","").replace('\r', '')
            new_triple22=got10.replace(',\n','\n').replace("\n","").replace('\r', '')
            new_triple222=got1.replace(',\n','\n').replace("\n","").replace('\r', '')

            if line2!="?s ?p ?o ," and new_triple22!="" and new_triple2!="" :
    
                #print("! -"+line2)
                ff2.write(line2+"\n")
    
                #print("# ="+new_triple2)
                ff.write(new_triple2+"\n")
                ff0.write(new_triple22+"\n")
                ff10.write(new_triple222+"\n")

                #print(new_triple2)
                #print(new_triple22)

                #print("writen")
                co=co+1
                tor1=tor1+1
             
            else:
                tor=tor+1
                #print("S "+str(tor1)+ " "+str(tor)+"  tot "+str(tor+tor1))
        except Exception as e:
            import traceback
            #print("EXa ",traceback.format_exc())
        #sys.stdout = old_stdout
    endd=time.time();
    print("TIME start-end ",str(endd-start))
    ff.close()
    ff2.close()
    ff0.close()
    ff10.close()

    print(count1)
    print(count2)
    print(count3)
    
