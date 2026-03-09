import sys
import glob
import os
import json
import re
import pandas as pd
import multiprocessing


def get_column_names(schemas, ds_name, sorting_key='column_position'):
    column_details = schemas[ds_name]
    columns = sorted(column_details, key=lambda col: col[sorting_key])
    return [col['column_name'] for col in columns]


def read_csv(file, schemas):
    file_path_list = re.split('[/\\\]', file)
    ds_name = file_path_list[-2]
    columns = get_column_names(schemas, ds_name)
    # Chucnksize: Mitigating the issue of memory overflow by reading the csv file in chunks of 10000 rows 
    # -> app + reliable and performance oriented. 
    # Troubleshooting: If there's an exception on any of the chunks, the error message will be printed and the process will continue 
    # with the next chunk, ensuring that the entire dataset is processed without interruption. 
    df_reader = pd.read_csv(file, names=columns, chunksize=10000) 
    return df_reader


def to_sql(df, db_conn_uri, ds_name):
    df.to_sql(
        ds_name,
        db_conn_uri,
        if_exists='append',
        index=False,
        method='multi' # Ended up being worse then the default method - from 5s to 13s. 
    )


def db_loader(src_base_dir, db_conn_uri, ds_name):
    schemas = json.load(open(f'{src_base_dir}/schemas.json'))
    files = glob.glob(f'{src_base_dir}/{ds_name}/part-*')
    if len(files) == 0:
        raise NameError(f'No files found for {ds_name}')

    for file in files:
        df_reader = read_csv(file, schemas)
        for idx, df in enumerate(df_reader):
            print(f'Populating chunk {idx} of {ds_name}')
            to_sql(df, db_conn_uri, ds_name)

# def process_datasets can be commented if to run on a notebook, but it is needed to run the app in parallel.
def process_dataset(args):
    src_base_dir, db_conn_uri, ds_name = args
    db_loader(src_base_dir, db_conn_uri, ds_name)

def process_files(ds_names=None):
    src_base_dir = os.environ.get('SRC_BASE_DIR')
    db_host = os.environ.get('DB_HOST')
    db_port = os.environ.get('DB_PORT')
    db_name = os.environ.get('DB_NAME')
    db_user = os.environ.get('DB_USER')
    db_pass = os.environ.get('DB_PASSWORD')
    db_conn_uri = f'postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}'
    schemas = json.load(open(f'{src_base_dir}/schemas.json'))
    if not ds_names:
        ds_names = schemas.keys()
    pprocesses = len(ds_names) if len(ds_names) < 4 else 4
    # We don't want to create more processes than the number of datasets, and we also want to limit the number of processes to 4 to avoid overwhelming the system.
    pool = multiprocessing.Pool(pprocesses) # This is the main change needed to make the app run in parallel.
    pd_args = [(src_base_dir, db_conn_uri, ds_name) for ds_name in ds_names]
    
    # If to run on a notebook uncomment:
    # pd_args = [] 
    #for ds_name in ds_names:
    #    pd_args.append((src_base_dir, db_conn_uri, ds_name))
    pool.map(process_dataset, pd_args)
    
    # If to run on a notebook, comment:
    pool.close()
    pool.join()


if __name__ == '__main__':
    if len(sys.argv) == 2:
        ds_names = json.loads(sys.argv[1]) 
        process_files(ds_names)
    else:
        process_files()