map-reduce
==========

Map-reduce framework for BMSTU course project.

DFS
==========

Distributed file system is required to map-reduce framework.

On each node, you should run dfs_node.py with two arguments - port and storage_path. Like this: python dfs_node.py -p 5556 -s /home/user_name/storage
Then you should fill config.json with information about nodes.

Then you can use dfs.py. Samples of use dfs.py:
python dfs.py -ls /user/
python dfs.py -mkdir /user/user_name/user_data_folder
python dfs.py -put ./test.in /user/user_name/user_data_folder/test_file
python dfs.py -get /user/user_name/user_data_folder/test_file
python dfs.py -rm /user/user_name