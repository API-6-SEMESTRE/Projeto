import os
import shutil


path=os.getcwd()+'/logs'
path = path.replace('\\','/')

shutil.rmtree(path)
os.mkdir(path)