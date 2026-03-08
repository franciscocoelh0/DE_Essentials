import glob
import os
import json
import pandas as pd

# Get the metadata (column names)
def get_column_names(schemas, ds_name, sorting_key='column_position'):
    column_details = schemas[ds_name]
    columns = sorted(column_details, key=lambda col: col[sorting_key])
    return [col['column_name'] for col in columns]

# Read the data from the source (csv) files and turn them into dfs
def read_csv(file, schemas):
    # os.path.basename gets 'part-00000'
    file_name = os.path.basename(file)
    # os.path.dirname gets the path to the folder, then basename gets 'departments'
    ds_name = os.path.basename(os.path.dirname(file))
    
    columns = get_column_names(schemas, ds_name)
    df = pd.read_csv(file, names=columns)
    return df

# Converting the dataframes into json files and writing them to the target directory
def to_json(df, tgt_base_dir, ds_name, file_name):
    json_file_path = f'{tgt_base_dir}/{ds_name}/{file_name}'
    os.makedirs(f'{tgt_base_dir}/{ds_name}', exist_ok=True)
    df.to_json(
        json_file_path,
        orient='records',
        lines=True
    )

def file_converter(src_base_dir, tgt_base_dir, ds_name):
    schemas = json.load(open(f'{src_base_dir}/schemas.json'))
    files = glob.glob(f'{src_base_dir}/{ds_name}/part-*')

    for file in files:
        df = read_csv(file, schemas)
        # Simplified file_name extraction
        file_name = os.path.basename(file)
        to_json(df, tgt_base_dir, ds_name, file_name)

def process_files(ds_names=None):
    # Update these paths if you move your project folder
    src_base_dir = '/Users/franciscocoelho/VSCodeProjects/franciscocoelh0/Random_Scripts/file-format-converter/data/retail_db'
    tgt_base_dir = '/Users/franciscocoelho/VSCodeProjects/franciscocoelh0/Random_Scripts/file-format-converter/data/retail_db_json'
    
    with open(f'{src_base_dir}/schemas.json') as f:
        schemas = json.load(f)
        
    if not ds_names:
        ds_names = schemas.keys()
        
    for ds_name in ds_names:
        print(f'Processing {ds_name}...')
        file_converter(src_base_dir, tgt_base_dir, ds_name)

if __name__ == '__main__':
    process_files()