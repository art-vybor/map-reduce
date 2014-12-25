
#python dfs.py -rm /in_wc

time python dfs.py -put ../examples/wordcount/out /in
time python mr.py -i /in -o /out -mr ../examples/wordcount/wordcount.py
#python mr.py -i /in_1gb -o /out -mr ../wordcount.py
#python dfs.py -get /out

