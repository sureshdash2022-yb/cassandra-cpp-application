import os
import sys
import subprocess
import time

if len(sys.argv) < 4:
   print("provide the command: <python3> <script-name> <yb-admin-path> <key-space> <table-list>")
   exit(0)

yb_admin_path=sys.argv[1]
key_space = sys.argv[2]
table_list = sys.argv[3]

yb_admin_path = yb_admin_path + "/" + "yb-admin"
if False == os.path.exists(yb_admin_path):
   print("{} doesnt exist.".format(yb_admin_path))
   exit()


table_list = table_list.split(',')
idx = 0
num_snapshot= 10 
while idx < num_snapshot:
 for ech_tbl in table_list:
  cmd = yb_admin_path + " " + "create_snapshot" + " " + key_space + " " + ech_tbl
  out = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, universal_newlines=True); 
  print("snapshot: {} for table name: {}".format(out.stdout, ech_tbl))
 print("going for sleep.....")
 time.sleep(5)
 idx += 1


