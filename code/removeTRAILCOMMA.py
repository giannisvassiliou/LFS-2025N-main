# to erase ,\n  from query file produced by parserIT.py
import pandas as pd

file1 = open("goodqueriesYAGO1.txt", 'r',encoding="utf8")
file2 = open("goodqueriesYAGO1R.txt", 'w',encoding="utf8")

Lines = file1.readlines()

for l in Lines:
    #print(l)
    l=l.replace(',\n','\n')
    #print(l)
    file2.write(l)
file2.close()

