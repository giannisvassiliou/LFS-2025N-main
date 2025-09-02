# -*- coding: utf-8 -*-


import pandas as pd
import numpy as np
import sys,getopt
from rdflib.plugins.sparql.parser import parseQuery
from SPARQLWrapper import SPARQLWrapper, JSON;
import traceback
import time
from rdflib import URIRef, Graph, RDF, SDO

def fix_triple(triple):
    parts = re.findall(r'<[^>]*>', triple)
    if len(parts) != 3:
        return triple  # skip if not a simple triple
    subject, predicate, obj = parts
    if not 'http' in obj:
        obj_content = obj[1:-1]  # remove < and >
        obj = f'"{obj_content}"'
    return f"{subject} {predicate} {obj}"
np.random.seed(100)
def split_list(lst, n):
    k, m = divmod(len(lst), n)
    return [lst[i*(k) + min(i, m):(i+1)*(k) + min(i+1, m)] for i in range(n)]

# Example
sta=0
#
# df = pd.DataFrame(np.random.random((10,5)), columns=list('ABCDE'))
# df.index = df.index * 10
#
# df = pd.read_fwf('YAGODATAansweredv2.txt',encoding="utf-8",colspecs=[(0,4500)],names=['quer'])
argv=sys.argv[1:]
fla=0

dtt={}
dtt["newyago5limit1000.txt"]="YAGO_1_quer.txt"
#https://yago-knowledge.org/sparql/query

from rdflib import Graph
import re
def remove_non_english(text):
    ta=re.sub(r'[^A-Za-z0-9\s.,!?]', '', text)
    ta2='"'+ta+'"'
    return ta2
def is_valid(subject, predicate, obj):
    # Define regex patterns
    uri_pattern = r"^<https?://[^\s>]+>$"  # Matches <http://example.org/...>
    literal_pattern = r"^\".*\"(?:\^\^<https?://[^\s>]+>)?$"  # Matches "text" or "text"^^<datatype>
    blank_node_pattern = r"^_:([A-Za-z][A-Za-z0-9]*)$"  # Matches _:blankNode
    
    # Subject must be a URI or a blank node
    if not (re.match(uri_pattern, subject) or re.match(blank_node_pattern, subject)):
        return False
    
    # Predicate must be a URI
    if not re.match(uri_pattern, predicate):
        return False
    
    # Object can be a URI, a blank node, or a literal
    if not (re.match(uri_pattern, obj) or re.match(blank_node_pattern, obj) or re.match(literal_pattern, obj)):
        return False
    
    return True

#df2 = pd.read_fwf('YAGOgoodqueriesv2R.txt',encoding="utf-8",colspecs=[(0,4500)],names=['quer'])
if len(argv)<4:
    
    print("You need to provide two INPUT files (lfs_summary_filename lfs_queries_for_summary) and one filename for OUTPUT  and finally address_of_endpoint{OPTIONAL} ")
    print("USAGE: python lfs orig_summary_filename queries_for_summary LFS_summary_output percent {url of endpoint-optional)  ")
    exit(1)
else:
    #sys.argv[1]="newyago5limit1000.txt"
    #sys.argv[2]=dtt['newyago5limit1000.txt']
    print ('Summary file is ',sys.argv[1])
    print ('Queries file is ',sys. argv[2])
    print ('percent ',sys. argv[4])

    if len(argv)==5:
        print(" ADDRESSING ENDPOINT")
        fla=1
    else:
        print("NOT ADDRESSING ENDPOINT ")
        
filea = open("output.wiki10.txt", "w", encoding="utf-8")
      
for value in np.arange(0.2, 1.1, 0.2):  # 1.1 to include 1.0 due to floating point precision
    trips={}
    
    tripspre={}
    indextrip={}
    np.random.seed(100)
    fla=0
    sta=0
    #
    # df = pd.DataFrame(np.random.random((10,5)), columns=list('ABCDE'))
    # df.index = df.index * 10
    #
    # df = pd.read_fwf('YAGODATAansweredv2.txt',encoding="utf-8",colspecs=[(0,4500)],names=['quer'])
    argv=sys.argv[1:]
  

    
    print(round(value, 2))
    possa=round(value,2)
    filea.write("\n"+str(possa)+"\n")
    df = pd.DataFrame(np.random.random((10,5)), columns=list('ABCDE'))
    df.index = df.index * 10
    
    #df = pd.read_fwf('answeredWiki3.txt',encoding="utf-8",colspecs=[(0,4500)],names=['quer'])
    
    df = pd.read_fwf(sys.argv[1],encoding="utf-8",colspecs=[(0,444500)],names=['quer'])
    df5 = pd.read_fwf("5dbpedia_0_1000.txt",encoding="utf-8",colspecs=[(0,144500)],names=['quer'])

    df2 = pd.read_fwf(sys.argv[2],encoding="utf-8",colspecs=[(0,55500)],names=['quer'])
    qlines=[]
    with open(sys.argv[2], 'r') as file:
        qlines = file.readlines()
    testlist=[]
    #print (df)
    #df5['quer'] = df5['quer'].apply(fix_triple)
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
    for result in reslist[:1]:
        fname=sys.argv[3]
        ff= open(fname, 'w',encoding="utf-8") 
        #ffor= open(fname+"_or", 'w',encoding="utf-8") 

       
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
        grosstriples=0
        for index, row in train.iterrows():
            #print("queryno "+queryno))
            #queryno=queryno+1
            que=row['quer']
            #print("ROW -"+row+"-\n\n")
            #print("QUR -"+que+"-\n\n")
            # print("QUE "+que)
            #print("queryno "+que)
            if "2023-01-31T01:09:43Z" not in que and "<<" not in que and "math" not in que and "<application/x-httpd-php>" not in que and '10766787-n' not in que and "1848831457" not in que:
                spla=que.split('\t')
                
                # count = spla.count("<")
                # if (len(spla)>3):
                #     print("SPLA "+str(spla))
                #     exit(1)

                accu=""
                paso=0
                for node in spla:
                    #print("NODAS "+node)
                    grosstriples+=1
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
        
                            #print("res "+sub+" "+pre+" "+obj+" . \n")
                            resa=sub+" "+pre+" "+obj+" . \n"
                            ress2=sub+" "+pre+" "+obj+" . \n"
                            
                            
                            if resa!='" .\n' and ress2!=" > . \n" and "<<" not in ress2 and "math" not in resa and len(ress2)>10:
        
                                trip=sub+" "+pre+" "+obj+" ."
                                accu=accu+"\t"+trip
                                # if sta==0:
                                #     ff.write(sub+" "+pre+" "+obj+" .")
                                # else:
                                #     ff.write("\n"+sub+" "+pre+" "+obj+" .")
                                sta=sta+1
                                aaa=0
                                # try:
                                #
                                #    aaa=len(tripspre[trip])
                                #
                                # except:
                                #     aaa=0
                                #
                                # if aaa==0:
                                #     tripa=set()
                                #
                                #     gg=accu.split("\t")
                                #     for g in gg:
                                #         tripa.add(g)
                                #     tripspre[trip]=tripa
                                #

                                if trip not in trips:
                                    trips[trip]=1
                                else:
                                    trips[trip]=trips[trip]+1
                                paso=1
                            
                            #f.write(node+' .\n')
                     
                    except Exception as e:
                        kj=0
                        #print("Exception details:")
                        #raceback.print_exc()
                
                
                # # aaa=0
                # # try:
                # #    aaa=len(tripspre[trip])
                # #
                # # except:
                # #     print(OKOS)
                # #     aaa=0
                # #
                #
                # if aaa==0:
                #     tripa=set()
                #
                if paso==1:
                    gg=accu.split("\t")
                    for ga in gg:
                        aaaa=0
                        try:
                            aaaa=len(indextrip[ga])
                        except:
                            aaaa=0
                            
                            
                        if aaaa==0:
                            triin=set()
                            triin.add(index)
                            indextrip[ga]=triin
                        else:
                            indextrip[ga].add(index)
                            
                    #print("ACCU "+accu)
                   #print("LINES "+qlines[index])
                    countcom = qlines[index].count(",")
                    comma=countcom+1
                    gga=[]
                    if comma>1:
                        #print("COMMA",str(comma))
                        gga=split_list(gg,comma)
                        
                        
                        for gg in gga:
                        
                        
                            for g1 in gg:
                            
                                 
                                #print("GG "+g1)
            
                                aaa=0
                                try:
                                    aaa=len(tripspre[g1])
                                except:
                                    aaa=0
                                    #print("EXOS")
                                
                                
                               
                                if aaa==0:
                                    tripa=set()
                                    for g in gg:
                                        if g!=g1:
                                            tripa.add(g)
                                            #print("SKATA "+g)
                                            tripspre[g1]=tripa
                                            
                                   
                                    
                                else:
                                 
                                        
                                    for g in gg:
                                        if g!=g1:
                                        
                                            #print(g1+" "+g)
                                            tripspre[g1].add(g)
                                                
                    else:
                        ggac=accu.split("\t")    
                        
                        gga = [[item] for item in ggac]
                        #print(gga)

#                        print(split_lists)
                                            # print(gga)
                    #print("NO "+str(countcom))
    
                    #print("slioto "+accu)
    
                        
                        for gg in gga:
                            for g1 in gg:
                            
                                 
                                #print("GG "+g1)
            
                                aaa=0
                                try:
                                    aaa=len(tripspre[g1])
                                except:
                                    aaa=0
                                    #print("EXOS")
                                
                                
                               
                                if aaa==0:
                                    tripa=set()
                                    for g in gg:
                                        #if g!=g1 and g1!='':
                                        tripa.add(g)
                                        #print("SKATA "+g)
                                        tripspre[g1]=tripa
                                        
                                   
                                    
                                else:
                                 
                                        
                                    for g in gg:
                                        #if g!=g1 and g1!='':
                                        
                                            #print(g1+" "+g)
                                        tripspre[g1].add(g)
                                                
                                #tripspre[trip].add(accu)
                                                
        print("kaos4 "+str(len(tripspre)))
        coa=0
        for h in tripspre:
            #print("H "+h)
            coa+=len(tripspre[h])
            
        print("XOAS "+str(coa)+" KEYS "+str(len(tripspre.keys())))
        sorted_trips = dict(sorted(trips.items(), key=lambda item: item[1], reverse=True))
    
        poss=float(argv[3])
        print("POSA "+str(poss)+" "+sys.argv[4])
        poss=possa
        tt=1
        tsf=0
        tsf2=0
        for k in sorted_trips:
            #print(k+" value "+str(sorted_trips[k]))
            tt=tt+1
        
        filea.write("GROSS Triples "+str(grosstriples))
        filea.write("DISTINCT TOTAL TRIPLES "+str(tt))
       
        print("GROSS Triples "+str(grosstriples))
        print("DISTINCT TOTAL TRIPLES "+str(tt))
        tt=37804
        #tt=37804
        notr=int(poss*tt)
        noins=0
        filea.write("\nTOTAL distinct selected "+str(notr))   

        print("TOTAL distinct selected "+str(notr))
        
        #aej
        bad=0
        used=set()
        arra=set()
        nodis=0
        noexp=0
        # for k in sorted_trips:
        #     noins=noins+1
        #     tripcheck=k;
        #     tc=k.split()
        #     #print("tc "+tc[0]+" "+tc[1]+" "+tc[2])
        #     if is_valid(tc[0],tc[1],tc[2]):
        #         tc[0]=tc[0].replace('/n',"")
        #         tc[1]=tc[1].replace('/n',"")
        #         tc[2]=tc[2].replace('/n',"")
        #         lit=remove_non_english(tc[2])
        #         lit=tc[2]
        #         k2=tc[0]+" "+tc[1]+" "+" "+lit+" \t.";
        #         #k2 =remove_non_english(k)
        #         k2= k2.replace("^","")
        #         ffor.write(k2+"\n")
        #
        #         if  noins>=tt:
        #             break
        #     else:
        #         bad=bad+1
        #         #print("k",end="")
        # filea.write("\nTRIPLES IN SELECTED PORTION expanded:"+str(len(arra))+" EXPA "+str(noexp) +" DISTSEL "+str(nodis)+" athr "+str(nodis+noexp)+" gross from from sorted and expa : "+str(tsf2)+" \n")
        # print("\nTRIPLES IN SELECTED PORTION expanded:"+str(len(arra))+" EXPA "+str(noexp) +" DISTSEL "+str(nodis)+" athr "+str(nodis+noexp)+" gross from from sorted and expa : "+str(tsf2)+" \n")
        # print("bad "+str(bad)) 
        # ffor.close()
        #ole
        bad=0
        used=set()
        arra=set()
        tripl=set()
        nodis=0
        noexp=0
        noplus5=0
        eros=0
        noins=0
        for k in sorted_trips:
            #noins=noins+1
            tripcheck=k;
            tc=k.split()
            #print("tc "+tc[0]+" "+tc[1]+" "+tc[2])
            if is_valid(tc[0],tc[1],tc[2]):
                tc[0]=tc[0].replace('/n',"")
                tc[1]=tc[1].replace('/n',"")
                tc[2]=tc[2].replace('/n',"")
                lit=remove_non_english(tc[2])
                lit=tc[2]
                k2=tc[0]+" "+tc[1]+" "+" "+lit+" \t.";
                #k2 =remove_non_english(k)
                k2= k2.replace("^","")
                #print("KA{A "+k)
                tsf+=sorted_trips[k]
                
           
                            
                if k not in arra and k not in tripl :
                    nodis+=1
                    tsf2+=sorted_trips[k]
                    ff.write(k2+"\n")
                    #print("USE K"+k+"-")
                    used.add(k)
                    
                    
                        
                            # else:
                            #     if itta in tripl:
                            #         print("2tr")
                            #     if itta in used:
                            #         print("2us")
                            #     if itta in arra:
                            #         print("2ara")    #exit(1)
                    #$print("JHA")
                    # try:
                    #     ita=tripspre[k]
                    #     #print("USA "+str(ita))
                    #     #print(ita)
                    #     for itta in ita:
                    #         #print(itta)
                    #
                    #         ##rint("ITAS -"+itta+"-")
                    #         if itta not in arra and itta not in used and itta not in tripl:
                    #             #noins=noins+1
                    #             if itta!='':
                    #                 #print("OK")
                    #
                    #                 arra.add(itta)
                    #
                    #                 tc=itta.split()
                    #                 noexp+=1
                    #                 tc[0]=tc[0].replace('/n',"")
                    #                 tc[1]=tc[1].replace('/n',"")
                    #                 tc[2]=tc[2].replace('/n',"")
                    #                 lit=remove_non_english(tc[2])
                    #                 lit=tc[2]
                    #                 k22=tc[0]+" "+tc[1]+" "+" "+lit+" \t.";
                    #                 #k2 =remove_non_english(k)
                    #                 k22= k22.replace("^","")
                    #                 #print("ITA-"+itta+"-")
                    #                 #print("J22-"+k22+"-")
                    #                 ff.write(k22+"\n")
                    #                 # print("ITA4 "+itta)
                    #                 tsf2+=sorted_trips[itta]
                    #                 used.add(itta)
                    #         else:
                    #             gh=2
                    #             #print(itta)
                    #
                    #             #if itta in used:
                    #             #    print("US")
                    #             # if itta in tripl:
                    #             #     print("TR")
                    #             #if itta in arra:
                    #             #    print(AR)
                    #     #df
                        
                                #print("USED")
                                
                        #ghere
#                     except Exception as e:
#                         eros+=1
# #                         print(f"Exception type: {type(e).__name__}")
# #                         print(f"Exception message: {e}")
# #                         print("Traceback details:")
# #                         traceback.print_exc()

                    ida=indextrip[k]
                    # print("IDA "+str(ida))
                    for ia in ida:
                    #print(ia)
                    #print(df5['quer'][100])    
                        vai=  df5['quer'][ia]

                        #print("BAI-"+vai+"-")
                        #vai=fix_triple(vai2)
                        tripsin=vai.split("\t")

                        for itta in tripsin:
                            #print(itta)
                            itta=itta+" ."
                            itta=fix_triple(itta)

                            if not itta.endswith('.'):
                                itta += ' .'

                             #itta=itta+" ."

                            #print("="+itta)
                            if itta not in arra and itta not in used and itta not in tripl:

                            #noins=noins+1
                                if itta!='':
                                    #print("TRIPL")
                                    tc=itta.split()

                                    tc[0]=tc[0].replace('/n',"")
                                    tc[1]=tc[1].replace('/n',"")
                                    tc[2]=tc[2].replace('/n',"")
                                    lit=remove_non_english(tc[2])
                                    lit=tc[2]
                                    k22=tc[0]+" "+tc[1]+" "+" "+lit+" \t.";
                                    #k2 =remove_non_english(k)
                                    k22= k22.replace("^","")

                                    try:
                                        #print("SORTA "+str(sorted_trips[itta]))
                                        # print("ITA-"+itta+"-")
                                        # print("J22-"+k22+"-")
                                        #print(itta)
                                        tsf2+=sorted_trips[itta]
                                        #k22=k22.rstrip()
                                        ff.write(k22+"\n")
                                        #print("IITA 2-"+itta+"-")
                                        #or ka in sorted_trips:
                                        #print("KA -"+ka+"-")
                                        noplus5+=1
                                        tripl.add(itta)
                                        used.add(itta)
                                        ##(1)
                                    except:
                                        r1=1
                                        #print("r "+itta,end=" \n")
                                        #exit(1)
                            else:
                                poo=2
#                                 print("START")
#                                 if itta in used:
#                                     print("USED")
#                                 if itta in arra:
#                                     print("ARRRA")
#                                 if itta in tripl:
#                                     print("TTRIPLE")
#                                     for ss in tripl:
#                                         print("TALOS "+ss)
                                #print("rad")
                                aek=1
                                #print("NO TRIPLS") 
                #print("wrokte"+k2+" \n")
                colec=nodis+noexp+noplus5
                if  colec>=notr:
                    break
            else:
                bad=bad+1
#         for s in tripl:
#             print(s)
                #print("k",end="")
        print("EROS "+str(eros))  
        filea.write("\nTRIPLES IN SELECTED PORTION plus5 "+str(noplus5)+ " expanded:"+str(len(arra))+" EXPA "+str(noexp) +" DISTSEL "+str(nodis)+" athr "+str(nodis+noexp+noplus5)+" gross from from sorted and expa : "+str(tsf2)+" \n")
        print("\nTRIPLES IN SELECTED PORTION plus5 "+str(noplus5)+ " expanded:"+str(len(arra))+" EXPA "+str(noexp) +" DISTSEL "+str(nodis)+" athr "+str(nodis+noexp+noplus5)+" gross from from sorted and expa : "+str(tsf2)+" \n")
        print("bad "+str(bad)) 
        ff.close()
        
        print("Final summary nt file -"+sys.argv[3]+"- Created ...")
        print("Starting quering the nt file with the the testing queries....")
        #g = Graph()
        #g.parse(fname,format="nt")
        
        from rdflib.namespace import RDF, RDFS
        # gor = Graph()
        # with open(fname+"_or", "r") as f:
        #     for i, line in enumerate(f):
        #         line = line.strip()
        #         if not line or line.startswith("#"):
        #             continue
        #
        #         try:
        #             # Parse individual line
        #             mini_g = Graph()
        #             mini_g.parse(data=line, format="nt11")
        #             # Add triples to main graph
        #             for s, p, o in mini_g:
        #                 gor.add((s, p, o))
        #         except Exception as e:
        #             #print(f"Error on line {i+1}: {line}")
        #             #print(f"Error message: {e}")
        #             kkka=1
        #             # Continue with next line
        g = Graph()
        with open(fname, "r") as f:
            for i, line in enumerate(f):
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                
                try:
                    # Parse individual line
                    mini_g = Graph()
                    mini_g.parse(data=line, format="nt11")
                    # Add triples to main graph
                    for s, p, o in mini_g:
                        g.add((s, p, o))
                except Exception as e:
                    #print(f"Error on line {i+1}: {line}")
                    #print(f"Error message: {e}")
                    kkka=1
                    # Continue with next line
        noq=0
        ansq=0
        noall=0
        fals=0
        validlist=[]
        
        
        start = time.time()
        occu=0
        synolikos=0
        for index, row in df2.iterrows():
    
            
            if index in testlist:
                ready=0
               # print(str(noall)+" "+str(index))
                #print("LOCAL")
                synolikos+=1
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
                know = " SELECT * WHERE { "+nodi+"  } LIMIT 5"
                sta=0
                #print(know)
                try:
                    #print(" len "+str(len(know)))
                    if  len(know)<700 and ex==0 and know.count("?")<25 :
                        qres = g.query(know)
                        meg=len(qres)
                        # if meg>1:
                        #     exit(1)
                        
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
                    #qres2 = g.query(know)
                   # meg2=len(qres2)
                    # print(qres2)
                    # for row in qres2:
                    #     print(row)  # or just print(row)
                    #print("ORIGN " +str(meg)+" neon "+str(meg2))
                    #if meg==meg2:
                    if meg>=5:

                    #if meg2>=meg:
                        #print("ANSW")
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
                    
                    ae=0
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
        print(str(synolikos))
        print("\nTOTAL QUERIES "+str(synolikos)+" TOTAL VALID "+str(noall)+" total-valid "+str(synolikos-noall)+" PERCENT valid "+str(float(float(noall)/float(synolikos))))
        print("ANSWERED  "+str(ansq))
        print('Elapsed time for LFS ',str(end - start-occu))
        print("Elapsed time for ENDPOINT",str(occu))
        #print("NOALL "+str(noall)+
        filea.write("\nTOTAL QUERIES "+str(synolikos)+" TOTAL VALID "+str(noall)+" total-valid "+str(synolikos-noall)+" PERCENT valid "+str(float(float(noall)/float(synolikos))))
        filea.write("\n0NSWERED  "+str(ansq))
        filea.write('\nElapsed time for LFS '+str(end - start-occu))
        filea.write("\nElapsed time for ENDPOINT"+str(occu))
        #print("NOALL "+str(noall)+
       
              
        print("")
        filea.write("\nCOVERAGE FOR LFS "+str(float(ansq)/float(noall)))
        print("COVERAGE FOR LFS "+str(float(ansq)/float(noall)))
    
filea.close();
       
