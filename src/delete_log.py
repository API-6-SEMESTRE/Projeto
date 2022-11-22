import os
import shutil



def delete_log():
    path=os.getcwd()+'/logs'
    path = path.replace('\\','/')

    shutil.rmtree(path)
    os.mkdir(path)