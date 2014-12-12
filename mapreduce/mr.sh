#python dfs.py -rm /in
#python dfs.py -put ../examples/common_friend_1000_300.in /in
python dfs.py -rm /out
python mr.py -i /small_in -o /out -mr ../examples/common_friends.py
python dfs.py -get /out

