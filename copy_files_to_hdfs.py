import subprocess
import glob
import os

subprocess.check_call(
    'hdfs dfs -mkdir -p /home/franciscocoelho/DE_Essentials/data/retail_db',
     shell=True)

retail_db_path = '/home/franciscocoelho/DE_Essentials/data/retail_db'

cmd_template = 'hdfs dfs -put {src_dir} /home/franciscocoelho/DE_Essentials/data/retail_db/{tgt_dir}'

for item in glob.glob(f'{retail_db_path}/*'):
    if os.path.isdir(item):
        cmd = cmd_template.format(src_dir=item, tgt_dir=os.path.split(item)[1])
        print(f'Copying the folder for {os.path.split(item)[1]}')
        subprocess.check_call(cmd, shell=True)
