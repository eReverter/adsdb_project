import shutil # shutil.move() - It can both move AND rename the file

def unzip_into(zipname, zip_path=r"./", unzip_path=r"./"):
    """
    Unzips all the contents of the given file into the specified folder.

    Input:
        Zip name. It assumes the zipped file does not contain a folder structure. Additionally, containing folder and destination folder can be specified.

    Output:
        None. The file is already unzipped.
    """
    import zipfile
    from pathlib import Path

    zip_i = Path(zip_path)
    zip_f = Path(unzip_path)
    with zipfile.ZipFile(zip_i / zipname, 'r') as zip_ref:
        zip_ref.extractall(zip_f)
    return

def rename_csv(filename, newname=None, file_path=r"./persistent/", add_timestamp=False):
    """
    Self-explanatory. If a newname is not given, it renames the file with the standard form 'name_timestamp' as %Y%m%d.

    Input:
        Filename to be renamed as string. Name that wants to be give. Additionally, the path of the file can be specified. 

    Output:
        None. The file name is already changed.
    """
    from pathlib import Path
    from datetime import datetime
    import os

    if not newname:
        timestamp = datetime.today().strftime('%Y%m%d')
        newname = filename.partition('.')[0] + '_' + timestamp + '.{}'.format(filename.partition('.')[-1])
    
    if newname and add_timestamp:
        timestamp = datetime.today().strftime('%Y%m%d')
        newname = newname.partition('.')[0] + '_' + timestamp + '.{}'.format(filename.partition('.')[-1])
        print('{} moved to persistant and renamed to {} and timestamped.'.format(filename, newname))

    fpath = Path(file_path)

    if os.path.isfile(fpath / newname): 
        os.remove(fpath / newname)
    os.rename(fpath / filename, fpath / newname)
    return