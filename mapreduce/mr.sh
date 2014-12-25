#python dfs.py -rm /in
#python dfs.py -put ../examples/common_friend_1000_300.in /in
python dfs.py -rm /out
python mr.py -i /in_groupby_small -o /out -mr ../examples/groupby/groupby.py
#python mr.py -i /in_1gb -o /out -mr ../wordcount.py
#python dfs.py -get /out

