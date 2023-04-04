from src.gutils.environ import is_production_environ

if not is_production_environ():
    from os import path

def join(data_path, file_name):
    return f'{data_path}/{file_name}' if is_production_environ() \
        else path.join(data_path, file_name)