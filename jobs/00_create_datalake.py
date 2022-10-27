from pathlib import Path

paths = {
    "DATALAKE_RAW": 'data/raw/',
    "DATALAKE_WORK": 'data/work/',
    "DATALAKE_TRUSTED": 'data/trusted/'
}

for path in paths.keys():
    Path(paths[path]).mkdir(exist_ok=True)

