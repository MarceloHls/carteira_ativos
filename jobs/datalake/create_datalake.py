from pathlib import Path
Path.cwd()

paths = {
    "DATALAKE_RAW": 'data/raw/',
    "DATALAKE_WORK": 'data/work/',
    "DATALAKE_TRUSTED": 'data/trusted/',
}

def create_datalake():
    local = str(Path().cwd()) + '/'
    for path in paths.keys():
        Path(local + paths[path]).mkdir(exist_ok=True)
        

